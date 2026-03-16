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


def _load_universe(scope: str, limit: int) -> list[dict[str, str]]:
    code_tokens = [token.strip() for token in scope.split(",") if token.strip()]
    if code_tokens and all(token.replace(".", "").isdigit() for token in code_tokens):
        return [{"code": token.split(".", 1)[0].zfill(6), "name": token.split(".", 1)[0].zfill(6)} for token in code_tokens]

    try:
        df = ak.stock_zh_a_spot_em()
        columns = [str(col) for col in df.columns]
        code_col = _pick_column(columns, ("代码", "股票代码"), contains=("代码",))
        name_col = _pick_column(columns, ("名称", "股票简称"), contains=("名称",))
        cap_col = _pick_column(columns, ("总市值",), contains=("市值",))
        if not code_col or not name_col:
            raise RuntimeError("unable to resolve universe columns from stock_zh_a_spot_em")
        ordered = df.copy()
        if cap_col:
            ordered[cap_col] = ordered[cap_col].map(safe_float)
            ordered = ordered.sort_values(by=cap_col, ascending=False)
        records: list[dict[str, str]] = []
        for _, row in ordered.iterrows():
            code = str(row[code_col]).split(".", 1)[0].zfill(6)
            name = normalize_text(row[name_col])
            if not code or "ST" in name.upper():
                continue
            records.append({"code": code, "name": name})
            if len(records) >= limit:
                break
        return records
    except Exception:
        fallback = get_all_a_share_stocks()
        records = []
        for row in fallback.get("data", []):
            code = str(row.get("code", "")).split(".", 1)[0].zfill(6)
            name = normalize_text(row.get("name", ""))
            if not code or "ST" in name.upper():
                continue
            records.append({"code": code, "name": name})
            if len(records) >= limit:
                break
        if records:
            return records
        raise


def _candidate_payload(stock_code: str, company_name: str, scan_data: dict[str, Any]) -> dict[str, Any]:
    opportunity = determine_opportunity_type(
        stock_code,
        scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=scan_data.get("revenue_breakdown", {}).get("data", []),
    )
    gate_result = evaluate_universal_gates(stock_code, scan_data, opportunity_context=opportunity)
    valuation_result = build_three_case_valuation(stock_code, scan_data, opportunity)
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
        "score": score,
        "hard_veto": bool(gate_result.get("hard_vetos")),
        "thesis": opportunity.get("sentence", "No clean thesis."),
        "mispricing": f"base case {_fmt_price(valuation_result.get('base_case', {}).get('implied_price'))} vs current {_fmt_price(valuation_result.get('current_price'))}",
        "catalysts": signals.get("catalyst", {}).get("catalysts", [])[:3],
        "risks": gate_result.get("hard_vetos", [])[:2] or failed_gates[:3],
        "why_passed": gate_result.get("gates", {}).get("business_truth", {}).get("reason", "scored best on available evidence"),
        "next_step": "deep dive now" if not gate_result.get("hard_vetos") and score >= DEFAULTS.get("priority_score_cutoff", 75) else "keep on watchlist",
        "reason": "; ".join(gate_result.get("hard_vetos", [])[:2] or failed_gates[:2]) or "insufficient edge",
    }


def _fmt_price(value: Any) -> str:
    if value in (None, ""):
        return "N/A"
    return f"{float(value):.2f}"


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
        "thesis": opportunity.get("sentence", "No clean thesis."),
        "mispricing": f"safe upper bound {float(partial_gate.get('score_upper_bound', 0.0)):.1f} vs watchlist cutoff {secondary_cutoff:.0f}",
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
    universe = [
        {**item, "order_index": index}
        for index, item in enumerate(_load_universe(scope, max_universe))
    ]
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
            result = future.result()
            ranked_with_order.append(result)

    ranked_with_order.sort(
        key=lambda item: (-float(item["payload"].get("score", 0.0)), int(item.get("order_index", 0)))
    )
    ranked = [item["payload"] for item in ranked_with_order]
    priority = [item for item in ranked if item["score"] >= priority_cutoff and not item["hard_veto"]]
    secondary = [item for item in ranked if secondary_cutoff <= item["score"] < priority_cutoff and not item["hard_veto"]]
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
        scope_text=f"{scope} (practical tradable sample size={len(universe)})",
        results_summary=summary,
        priority_shortlist=priority,
        secondary_watchlist=secondary,
        rejected=rejected,
        report_dir=str(report_dir),
    )
    payload = {
        "scope": scope,
        "universe_size": len(universe),
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
