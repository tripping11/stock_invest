"""Market opportunity scanner for the whole-market framework."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import os
import sys
from pathlib import Path
from typing import Any

import akshare as ak
import yaml


SKILLS_DIR = Path(__file__).resolve().parents[3]
SHARED_DIR = SKILLS_DIR / "shared"
sys.path.insert(0, str(SHARED_DIR))

from adapters.provider_router import RADAR_ALL_STEPS, RADAR_PARTIAL_STEPS, get_all_a_share_stocks, resolve_radar_trade_date, run_named_scan_steps  # noqa: E402
from engines.report_engine import generate_market_scan_report  # noqa: E402
from engines.valuation_engine import build_three_case_valuation  # noqa: E402
from utils.framework_utils import determine_opportunity_type, normalize_text, safe_float  # noqa: E402
from utils.runtime_paths import market_scan_paths, resolve_base_dir  # noqa: E402
from validators.universal_gate import evaluate_partial_gate_dimensions, evaluate_universal_gates  # noqa: E402


BASE_DIR = resolve_base_dir()
with open(SKILLS_DIR / "market-opportunity-scanner" / "config" / "scan_defaults.yaml", "r", encoding="utf-8") as handle:
    DEFAULTS = (yaml.safe_load(handle) or {}).get("defaults", {})


def _pick_column(columns: list[str], exact: tuple[str, ...], contains: tuple[str, ...] = ()) -> str | None:
    for key in exact:
        if key in columns:
            return key
    for key in columns:
        if contains and all(token in key for token in contains):
            return key
    return None


def _fmt_price(value: Any) -> str:
    if value in (None, ""):
        return "N/A"
    return f"{float(value):.2f}"


def _sampling_market_cap(record: dict[str, Any]) -> float | None:
    return safe_float(record.get("float_market_cap")) or safe_float(record.get("market_cap"))


def _industry_key(record: dict[str, Any]) -> str:
    return normalize_text(record.get("industry")) or "unknown"


def _cap_bucket(market_cap: float | None) -> str:
    if market_cap is None:
        return "mid"
    if market_cap < 3_000_000_000:
        return "micro"
    if market_cap < 15_000_000_000:
        return "small"
    if market_cap < 50_000_000_000:
        return "mid"
    return "large"


def _prepare_liquid_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {"micro": [], "small": [], "mid": [], "large": []}
    for record in records:
        turnover = safe_float(record.get("turnover"))
        if turnover is not None and turnover < 15_000_000:
            continue
        buckets[_cap_bucket(_sampling_market_cap(record))].append(record)

    for bucket_name, bucket_records in buckets.items():
        bucket_records.sort(
            key=lambda item: (
                -(safe_float(item.get("turnover")) or 0.0),
                _sampling_market_cap(item) or (0.0 if bucket_name != "large" else float("inf")),
            )
        )
    return [item for bucket in buckets.values() for item in bucket]


def _layered_sample(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(records) <= limit:
        return records

    buckets: dict[str, list[dict[str, Any]]] = {"micro": [], "small": [], "mid": [], "large": []}
    for record in records:
        buckets[_cap_bucket(_sampling_market_cap(record))].append(record)

    selection: list[dict[str, Any]] = []
    bucket_order = ["small", "small", "mid", "micro", "small", "large"]
    while len(selection) < limit and any(buckets.values()):
        progressed = False
        for bucket_name in bucket_order:
            if not buckets[bucket_name]:
                continue
            selection.append(buckets[bucket_name].pop(0))
            progressed = True
            if len(selection) >= limit:
                break
        if not progressed:
            break

    if len(selection) < limit:
        remainder = [item for bucket in buckets.values() for item in bucket]
        selection.extend(remainder[: limit - len(selection)])
    return selection[:limit]


def _industry_stratified_sample(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(records) <= limit:
        return records

    liquid_records = _prepare_liquid_records(records)
    if not liquid_records:
        liquid_records = list(records)

    by_industry: dict[str, list[dict[str, Any]]] = {}
    for record in liquid_records:
        by_industry.setdefault(_industry_key(record), []).append(record)

    industry_order = sorted(
        by_industry,
        key=lambda industry: (
            -(safe_float(by_industry[industry][0].get("turnover")) or 0.0),
            industry,
        ),
    )
    queues = {
        industry: _layered_sample(records_for_industry, min(len(records_for_industry), limit))
        for industry, records_for_industry in by_industry.items()
    }

    selection: list[dict[str, Any]] = []
    while len(selection) < limit and any(queues.values()):
        progressed = False
        for industry in industry_order:
            if not queues[industry]:
                continue
            selection.append(queues[industry].pop(0))
            progressed = True
            if len(selection) >= limit:
                break
        if not progressed:
            break

    if len(selection) < limit:
        remainder = [item for industry in industry_order for item in queues[industry]]
        selection.extend(remainder[: limit - len(selection)])
    return selection[:limit]


def _normalize_universe_records(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    normalized = []
    for item in records:
        code = str(item.get("code", "")).split(".", 1)[0].zfill(6)
        name = normalize_text(item.get("name", code))
        if not code:
            continue
        normalized.append(
            {
                "code": code,
                "name": name or code,
                "market_cap": safe_float(item.get("market_cap")),
                "float_market_cap": safe_float(item.get("float_market_cap")),
                "turnover": safe_float(item.get("turnover")),
                "industry": normalize_text(item.get("industry")) or "unknown",
                "special_tags": list(item.get("special_tags", [])),
            }
        )
    return _industry_stratified_sample(normalized, limit)


def _load_universe(scope: str, limit: int) -> list[dict[str, Any]]:
    code_tokens = [token.strip() for token in scope.split(",") if token.strip()]
    if code_tokens and all(token.replace(".", "").isdigit() for token in code_tokens):
        return [{"code": token.split(".", 1)[0].zfill(6), "name": token.split(".", 1)[0].zfill(6)} for token in code_tokens]

    try:
        df = ak.stock_zh_a_spot_em()
        columns = [str(col) for col in df.columns]
        code_col = _pick_column(columns, ("代码", "股票代码"), contains=("代码",))
        name_col = _pick_column(columns, ("名称", "股票简称"), contains=("名称",))
        float_cap_col = _pick_column(columns, ("流通市值", "流通A股市值"), contains=("流通", "市值"))
        cap_col = _pick_column(columns, ("总市值",), contains=("总", "市值"))
        turnover_col = _pick_column(columns, ("成交额", "成交额(元)"), contains=("成交", "额"))
        industry_col = _pick_column(columns, ("行业", "所属行业", "申万行业", "申万一级行业"), contains=("行业",))
        if not code_col or not name_col:
            raise RuntimeError("unable to resolve universe columns from stock_zh_a_spot_em")

        records = []
        for _, row in df.iterrows():
            name = normalize_text(row[name_col])
            special_tags = ["special_situation"] if "ST" in name.upper() else []
            records.append(
                {
                    "code": str(row[code_col]).split(".", 1)[0].zfill(6),
                    "name": name,
                    "market_cap": safe_float(row[cap_col]) if cap_col else None,
                    "float_market_cap": safe_float(row[float_cap_col]) if float_cap_col else None,
                    "turnover": safe_float(row[turnover_col]) if turnover_col else None,
                    "industry": normalize_text(row[industry_col]) if industry_col else "unknown",
                    "special_tags": special_tags,
                }
            )
        return _normalize_universe_records(records, limit)
    except Exception:
        pass

    provider_result = get_all_a_share_stocks()
    records = []
    for row in provider_result.get("data", []) or []:
        name = normalize_text(row.get("name", ""))
        records.append(
            {
                "code": str(row.get("code", "")).split(".", 1)[0].zfill(6),
                "name": name,
                "market_cap": safe_float(row.get("market_cap")),
                "float_market_cap": safe_float(row.get("float_market_cap")),
                "turnover": safe_float(row.get("turnover")),
                "industry": normalize_text(row.get("industry")) or "unknown",
                "special_tags": ["special_situation"] if "ST" in name.upper() else [],
            }
        )
    if not records:
        raise RuntimeError("unable to load A-share universe from any provider")

    normalized = _normalize_universe_records(records, limit)
    status = normalize_text(provider_result.get("status")).lower()
    if "fallback_baostock" in status:
        return [{"code": item["code"], "name": item["name"]} for item in normalized]
    return normalized


def _coarse_filter_universe(universe: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(universe) <= limit:
        return universe

    coarse_limit = min(len(universe), max(limit * 4, limit))
    ordered = sorted(
        universe,
        key=lambda item: (
            len(item.get("special_tags", [])),
            -(safe_float(item.get("turnover")) or 0.0),
            _sampling_market_cap(item) or float("inf"),
        ),
    )
    return ordered[:coarse_limit]


def _position_state_rank(position_state: str) -> int:
    return {
        "attack": 4,
        "ready": 3,
        "cold_storage": 2,
        "harvest": 1,
        "reject": 0,
    }.get(normalize_text(position_state).lower(), 0)


def _ranked_payload_sort_key(result: dict[str, Any]) -> tuple[float, ...]:
    payload = result.get("payload", {}) or {}
    return (
        -_position_state_rank(payload.get("position_state", "reject")),
        -(safe_float(payload.get("underwrite_score")) or 0.0),
        -(safe_float(payload.get("realization_score")) or 0.0),
        -(safe_float(payload.get("floor_protection")) or -1.0),
        -(safe_float(payload.get("recognition_upside")) or -999.0),
        int(result.get("order_index", 0)),
    )


def _candidate_payload(stock_code: str, company_name: str, scan_data: dict[str, Any]) -> dict[str, Any]:
    opportunity = determine_opportunity_type(
        stock_code,
        scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=scan_data.get("revenue_breakdown", {}).get("data", []),
    )
    gate_result = evaluate_universal_gates(stock_code, scan_data, opportunity_context=opportunity)
    valuation_result = build_three_case_valuation(stock_code, scan_data, gate_result.get("driver_stack", opportunity))
    underwrite_score = safe_float(gate_result.get("underwrite_axis", {}).get("score")) or 0.0
    realization_score = safe_float(gate_result.get("realization_axis", {}).get("score")) or 0.0
    signals = gate_result.get("signals", {})
    failed_gates = [
        f"{gate_name}: {gate['reason']}"
        for gate_name, gate in (gate_result.get("gates", {}) or {}).items()
        if gate.get("status") == "fail"
    ]
    return {
        "ticker": stock_code,
        "company_name": company_name,
        "market": "A-share",
        "opportunity_type": opportunity.get("primary_label", "Unknown"),
        "primary_type": gate_result.get("driver_stack", {}).get("primary_type", opportunity.get("primary_type")),
        "sector_route": gate_result.get("driver_stack", {}).get("sector_route"),
        "score": underwrite_score,
        "underwrite_score": underwrite_score,
        "realization_score": realization_score,
        "hard_veto": bool(gate_result.get("hard_vetos")),
        "position_state": gate_result.get("position_state", "reject"),
        "prev_state": gate_result.get("prev_state", "NEW"),
        "flow_stage": gate_result.get("flow_stage", "latent"),
        "thesis": opportunity.get("sentence", "No clean thesis."),
        "mispricing": f"normalized case {_fmt_price(valuation_result.get('normalized_case', {}).get('implied_price'))} vs current {_fmt_price(valuation_result.get('current_price'))}",
        "floor_protection": valuation_result.get("summary", {}).get("floor_protection"),
        "normalized_upside": valuation_result.get("summary", {}).get("normalized_upside"),
        "recognition_upside": valuation_result.get("summary", {}).get("recognition_upside"),
        "catalysts": signals.get("catalyst", {}).get("catalysts", [])[:3],
        "risks": gate_result.get("hard_vetos", [])[:2] or failed_gates[:3],
        "why_passed": gate_result.get("gates", {}).get("business_truth", {}).get("reason", "scored best on available evidence"),
        "next_step": "deep dive now" if gate_result.get("position_state") in {"ready", "attack"} else "keep on watchlist",
        "reason": "; ".join(gate_result.get("hard_vetos", [])[:2] or failed_gates[:2]) or "insufficient edge",
    }


def _fields_to_fetch_from_partial_gate(partial_gate: dict[str, Any]) -> list[str]:
    fields = {
        field
        for dimension in (partial_gate.get("dimensions", {}) or {}).values()
        if dimension.get("confidence") != "full"
        for field in dimension.get("requires", [])
    }
    return sorted(fields)


def _should_prefilter_reject(partial_gate: dict[str, Any], secondary_cutoff: float) -> bool:
    if partial_gate.get("decidable_hard_vetos"):
        return True
    return float(partial_gate.get("score_upper_bound", 0.0)) < float(secondary_cutoff)


def _prefilter_rejected_payload(
    stock_code: str,
    company_name: str,
    partial_scan_data: dict[str, Any],
    partial_gate: dict[str, Any],
    secondary_cutoff: float,
) -> dict[str, Any]:
    opportunity = partial_gate.get("opportunity_context") or determine_opportunity_type(
        stock_code,
        partial_scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=partial_scan_data.get("revenue_breakdown", {}).get("data", []),
    )
    vetoes = partial_gate.get("decidable_hard_vetos", []) or []
    if vetoes:
        reason = f"safe_prefilter_reject: {'; '.join(vetoes[:2])}"
    else:
        reason = f"safe_prefilter_reject: upper_bound {float(partial_gate.get('score_upper_bound', 0.0)):.1f} < {secondary_cutoff:.0f}"

    return {
        "ticker": stock_code,
        "company_name": company_name,
        "market": "A-share",
        "opportunity_type": opportunity.get("primary_label", "Unknown"),
        "score": float(partial_gate.get("score_upper_bound", 0.0)),
        "hard_veto": bool(vetoes),
        "position_state": "reject",
        "prev_state": "NEW",
        "flow_stage": "latent",
        "thesis": opportunity.get("sentence", "No clean thesis."),
        "mispricing": f"safe upper bound {float(partial_gate.get('score_upper_bound', 0.0)):.1f} vs watchlist cutoff {secondary_cutoff:.0f}",
        "floor_protection": None,
        "normalized_upside": None,
        "recognition_upside": None,
        "catalysts": [],
        "risks": vetoes[:2] or partial_gate.get("blocked_hard_vetos", [])[:2],
        "why_passed": "prefilter reject",
        "next_step": "drop from radar queue",
        "reason": reason,
    }


def _init_radar_day_cache_dir(trade_date: str, enabled: bool, *, base_dir: Path) -> Path | None:
    if not enabled:
        return None

    cache_dir = market_scan_paths(base_dir)["radar_cache_root"] / trade_date
    cache_dir.mkdir(parents=True, exist_ok=True)

    meta_path = cache_dir / "_meta.json"
    if not meta_path.exists():
        meta_path.write_text(
            json.dumps(
                {
                    "trade_date": trade_date,
                    "created_at": datetime.datetime.now().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return cache_dir


def _scan_one_stock(
    item: dict[str, Any],
    *,
    secondary_cutoff: float,
    day_cache_dir: Path | None,
    retry_delays: tuple[float, ...],
) -> dict[str, Any]:
    partial_scan_data = run_named_scan_steps(
        item["code"],
        RADAR_PARTIAL_STEPS,
        day_cache_dir=day_cache_dir,
        retry_delays=retry_delays,
    )
    partial_gate = evaluate_partial_gate_dimensions(item["code"], partial_scan_data)

    if _should_prefilter_reject(partial_gate, secondary_cutoff):
        return {
            "kind": "rejected",
            "order_index": int(item.get("order_index", 0)),
            "payload": _prefilter_rejected_payload(
                item["code"],
                item["name"],
                partial_scan_data,
                partial_gate,
                secondary_cutoff,
            ),
        }

    fields_to_fetch = _fields_to_fetch_from_partial_gate(partial_gate)
    enriched_scan_data = dict(partial_scan_data)
    if fields_to_fetch:
        selected_steps = {field: RADAR_ALL_STEPS[field] for field in fields_to_fetch if field in RADAR_ALL_STEPS}
        if selected_steps:
            enriched_scan_data.update(
                run_named_scan_steps(
                    item["code"],
                    selected_steps,
                    cached_results=partial_scan_data,
                    day_cache_dir=day_cache_dir,
                    retry_delays=retry_delays,
                )
            )
    return {
        "kind": "ranked",
        "order_index": int(item.get("order_index", 0)),
        "payload": _candidate_payload(item["code"], item["name"], enriched_scan_data),
    }


def run_radar_scan(
    scope: str = "A-share",
    limit: int | None = None,
    *,
    base_dir: str | Path | None = None,
    max_workers_override: int | None = None,
) -> dict[str, Any]:
    resolved_base_dir = (
        resolve_base_dir(base_dir)
        if base_dir is not None or os.getenv("A_STOCK_BASE")
        else BASE_DIR
    )
    max_universe = int(limit or DEFAULTS.get("max_universe_size", 24))
    coarse_limit = max(max_universe, min(max_universe * 4, 400))
    raw_universe = _load_universe(scope, coarse_limit)
    coarse_universe = _coarse_filter_universe(raw_universe, max_universe)
    universe = [{**item, "order_index": index} for index, item in enumerate(coarse_universe)]

    paths = market_scan_paths(resolved_base_dir)
    report_dir = paths["report_dir"]
    processed_dir = paths["processed_dir"]
    report_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    priority_cutoff = float(DEFAULTS.get("priority_score_cutoff", 75))
    secondary_cutoff = float(DEFAULTS.get("secondary_score_cutoff", 65))
    trade_date = resolve_radar_trade_date()
    day_cache_dir = _init_radar_day_cache_dir(
        trade_date,
        enabled=bool(DEFAULTS.get("radar_day_cache_enabled", True)),
        base_dir=resolved_base_dir,
    )
    retry_delays = tuple(float(item) for item in DEFAULTS.get("radar_retry_delays", [0.5, 1.0]))
    max_workers = max(1, int(max_workers_override or DEFAULTS.get("radar_max_workers", 4)))

    ranked_with_order: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                _scan_one_stock,
                item,
                secondary_cutoff=secondary_cutoff,
                day_cache_dir=day_cache_dir,
                retry_delays=retry_delays,
            )
            for item in universe
        ]
        for future in as_completed(futures):
            ranked_with_order.append(future.result())

    ranked_with_order.sort(key=_ranked_payload_sort_key)
    ranked = [item["payload"] for item in ranked_with_order]
    priority = [
        item
        for item in ranked
        if item.get("position_state") in {"ready", "attack"}
        and not item.get("hard_veto")
    ]
    secondary = [
        item
        for item in ranked
        if item.get("position_state") in {"cold_storage", "ready", "attack"}
        and item not in priority
        and not item.get("hard_veto")
    ]
    rejected = [item for item in ranked if item not in priority and item not in secondary]

    if not priority and not secondary:
        summary = "no-action"
    elif priority and len(priority) >= 3:
        summary = "attractive opportunity set"
    elif priority:
        summary = "mixed opportunity set"
    else:
        summary = "weak opportunity set"

    report = generate_market_scan_report(
        market="A-share",
        scope_text=f"{scope} (raw={len(raw_universe)}, coarse={len(universe)}, fine={len(ranked)})",
        results_summary=summary,
        priority_shortlist=priority,
        secondary_watchlist=secondary,
        rejected=rejected,
        report_dir=str(report_dir),
    )
    payload = {
        "scope": scope,
        "universe_size": len(raw_universe),
        "coarse_candidate_count": len(universe),
        "fine_candidate_count": len(ranked),
        "summary": summary,
        "ranked": ranked,
        "priority_shortlist": priority,
        "secondary_watchlist": secondary,
        "rejected": rejected,
        "report_path": report["report_path"],
    }
    (processed_dir / "market_scan.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the market opportunity scanner.")
    parser.add_argument("scope", nargs="?", default="A-share")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--base-dir", type=Path, default=None)
    args = parser.parse_args()
    result = run_radar_scan(args.scope, args.limit, base_dir=args.base_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
