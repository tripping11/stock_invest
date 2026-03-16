"""Market opportunity scanner for the whole-market framework."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import sys
from pathlib import Path
from typing import Any

import akshare as ak
import yaml


SKILLS_DIR = Path(__file__).resolve().parents[3]
SHARED_DIR = SKILLS_DIR / "shared"
sys.path.insert(0, str(SHARED_DIR))

from adapters.akshare_adapter import RADAR_ALL_STEPS, RADAR_PARTIAL_STEPS, resolve_radar_trade_date, run_named_scan_steps  # noqa: E402
from adapters.baostock_adapter import get_all_a_share_stocks  # noqa: E402
from engines.report_engine import generate_market_scan_report  # noqa: E402
from engines.valuation_engine import build_three_case_valuation  # noqa: E402
from utils.framework_utils import determine_opportunity_type, normalize_text, safe_float  # noqa: E402
from validators.universal_gate import evaluate_partial_gate_dimensions, evaluate_universal_gates  # noqa: E402


BASE_DIR = Path(__file__).resolve().parents[5]
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


def _cap_bucket(market_cap: float | None) -> str:
    if market_cap is None:
        return "mid"
    if market_cap < 5_000_000_000:
        return "micro"
    if market_cap < 20_000_000_000:
        return "small"
    if market_cap < 50_000_000_000:
        return "mid"
    return "large"


def _layered_sample(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(records) <= limit:
        return records

    buckets: dict[str, list[dict[str, Any]]] = {"micro": [], "small": [], "mid": [], "large": []}
    for record in records:
        turnover = safe_float(record.get("turnover"))
        if turnover is not None and turnover < 20_000_000:
            continue
        buckets[_cap_bucket(safe_float(record.get("market_cap")))].append(record)

    for bucket_name, bucket_records in buckets.items():
        bucket_records.sort(
            key=lambda item: (
                -(safe_float(item.get("turnover")) or 0.0),
                safe_float(item.get("market_cap")) or (0.0 if bucket_name != "large" else float("inf")),
            )
        )

    selection: list[dict[str, Any]] = []
    bucket_order = ["micro", "small", "small", "mid", "large"]
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
                "turnover": safe_float(item.get("turnover")),
                "special_tags": list(item.get("special_tags", [])),
            }
        )
    return _layered_sample(normalized, limit)


def _load_universe(scope: str, limit: int) -> list[dict[str, Any]]:
    code_tokens = [token.strip() for token in scope.split(",") if token.strip()]
    if code_tokens and all(token.replace(".", "").isdigit() for token in code_tokens):
        return [{"code": token.split(".", 1)[0].zfill(6), "name": token.split(".", 1)[0].zfill(6)} for token in code_tokens]

    try:
        df = ak.stock_zh_a_spot_em()
        columns = [str(col) for col in df.columns]
        code_col = _pick_column(columns, ("代码", "股票代码"), contains=("代码",))
        name_col = _pick_column(columns, ("名称", "股票简称"), contains=("名称",))
        cap_col = _pick_column(columns, ("总市值",), contains=("市", "值"))
        turnover_col = _pick_column(columns, ("成交额", "成交额(元)"), contains=("成交", "额"))
        if not code_col or not name_col:
            raise RuntimeError("unable to resolve universe columns from stock_zh_a_spot_em")

        records: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            name = normalize_text(row[name_col])
            special_tags = ["special_situation"] if "ST" in name.upper() else []
            records.append(
                {
                    "code": str(row[code_col]).split(".", 1)[0].zfill(6),
                    "name": name,
                    "market_cap": safe_float(row[cap_col]) if cap_col else None,
                    "turnover": safe_float(row[turnover_col]) if turnover_col else None,
                    "special_tags": special_tags,
                }
            )
        return _normalize_universe_records(records, limit)
    except Exception:
        fallback = get_all_a_share_stocks()
        records = []
        for row in fallback.get("data", []):
            name = normalize_text(row.get("name", ""))
            records.append(
                {
                    "code": str(row.get("code", "")).split(".", 1)[0].zfill(6),
                    "name": name,
                    "market_cap": None,
                    "turnover": None,
                    "special_tags": ["special_situation"] if "ST" in name.upper() else [],
                }
            )
        if records:
            normalized = _normalize_universe_records(records, limit)
            return [{"code": item["code"], "name": item["name"]} for item in normalized]
        raise


def _coarse_filter_universe(universe: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(universe) <= limit:
        return universe

    coarse_limit = min(len(universe), max(limit * 4, limit))
    ordered = sorted(
        universe,
        key=lambda item: (
            len(item.get("special_tags", [])),
            -(safe_float(item.get("turnover")) or 0.0),
            safe_float(item.get("market_cap")) or float("inf"),
        ),
    )
    return ordered[:coarse_limit]


def _candidate_payload(stock_code: str, company_name: str, scan_data: dict[str, Any]) -> dict[str, Any]:
    opportunity = determine_opportunity_type(
        stock_code,
        scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=scan_data.get("revenue_breakdown", {}).get("data", []),
    )
    gate_result = evaluate_universal_gates(stock_code, scan_data, opportunity_context=opportunity)
    valuation_result = build_three_case_valuation(stock_code, scan_data, gate_result.get("driver_stack", opportunity))
    score = gate_result.get("scorecard", {}).get("total", 0)
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
        "score": score,
        "hard_veto": bool(gate_result.get("hard_vetos")),
        "position_state": gate_result.get("position_state", "reject"),
        "prev_state": gate_result.get("prev_state", "NEW"),
        "flow_stage": gate_result.get("flow_stage", "latent"),
        "thesis": opportunity.get("sentence", "No clean thesis."),
        "mispricing": f"base case {_fmt_price(valuation_result.get('base_case', {}).get('implied_price'))} vs current {_fmt_price(valuation_result.get('current_price'))}",
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


def _init_radar_day_cache_dir(trade_date: str, enabled: bool) -> Path | None:
    if not enabled:
        return None

    cache_dir = BASE_DIR / "data" / "processed" / "radar_cache" / trade_date
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
    max_workers_override: int | None = None,
) -> dict[str, Any]:
    max_universe = int(limit or DEFAULTS.get("max_universe_size", 24))
    coarse_limit = max(max_universe, min(max_universe * 4, 400))
    raw_universe = _load_universe(scope, coarse_limit)
    coarse_universe = _coarse_filter_universe(raw_universe, max_universe)
    universe = [{**item, "order_index": index} for index, item in enumerate(coarse_universe)]

    report_dir = BASE_DIR / "reports"
    processed_dir = BASE_DIR / "data" / "processed" / "market_scan"
    report_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    priority_cutoff = float(DEFAULTS.get("priority_score_cutoff", 75))
    secondary_cutoff = float(DEFAULTS.get("secondary_score_cutoff", 65))
    trade_date = resolve_radar_trade_date()
    day_cache_dir = _init_radar_day_cache_dir(
        trade_date,
        enabled=bool(DEFAULTS.get("radar_day_cache_enabled", True)),
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

    ranked_with_order.sort(key=lambda item: (-float(item["payload"].get("score", 0.0)), int(item.get("order_index", 0))))
    ranked = [item["payload"] for item in ranked_with_order]
    priority = [
        item
        for item in ranked
        if item.get("position_state") in {"ready", "attack"}
        and not item.get("hard_veto")
        and float(item.get("score", 0.0)) >= priority_cutoff
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
    args = parser.parse_args()
    result = run_radar_scan(args.scope, args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
