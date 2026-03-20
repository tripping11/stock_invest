"""Market opportunity scanner for the whole-market framework."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
import datetime
import json
import logging
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
from utils.framework_utils import determine_opportunity_type, normalize_text, resolve_industry_group, resolve_sector_route, safe_float  # noqa: E402
from utils.runtime_paths import market_scan_paths, resolve_base_dir  # noqa: E402
from utils.source_lineage import summarize_scan_data_lineage  # noqa: E402
from validators.universal_gate import evaluate_partial_gate_dimensions, evaluate_universal_gates  # noqa: E402


LOGGER = logging.getLogger(__name__)
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
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"


def _candidate_data_lineage(scan_data: dict[str, Any]) -> dict[str, str]:
    return summarize_scan_data_lineage(scan_data)


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
    except Exception as exc:
        LOGGER.warning("stock_zh_a_spot_em failed, falling back to provider universe", exc_info=True)

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
    driver_stack = gate_result.get("driver_stack", {}) or {}
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
        "primary_type": driver_stack.get("primary_type", opportunity.get("primary_type")),
        "sector_route": driver_stack.get("sector_route"),
        "industry_group": driver_stack.get("industry_group", "unknown"),
        "sector_cycle_sensitive": bool(driver_stack.get("sector_cycle_sensitive", False)),
        "score": underwrite_score,
        "underwrite_score": underwrite_score,
        "realization_score": realization_score,
        "hard_veto": bool(gate_result.get("hard_vetos")),
        "position_state": gate_result.get("position_state", "reject"),
        "prev_state": gate_result.get("prev_state", "NEW"),
        "flow_stage": gate_result.get("flow_stage", "latent"),
        "thesis": opportunity.get("sentence", "No clean thesis."),
        "mispricing": f"normalized case {_fmt_price(valuation_result.get('normalized_case', {}).get('implied_price'))} vs current {_fmt_price(valuation_result.get('current_price'))}",
        "data_lineage": _candidate_data_lineage(scan_data),
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
    industry_group = resolve_industry_group(
        stock_code,
        partial_scan_data.get("company_profile", {}).get("data", {}),
        partial_scan_data.get("revenue_breakdown", {}).get("data", []),
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
        "industry_group": industry_group.get("industry_group", "unknown"),
        "sector_cycle_sensitive": bool(industry_group.get("cycle_sensitive", False)),
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


def _partial_underwrite_proxy(partial_gate: dict[str, Any]) -> float:
    dimensions = partial_gate.get("dimensions", {}) or {}
    total = 0.0
    max_total = 0.0
    for name in ("type_clarity", "business_quality", "survival", "management", "valuation"):
        total += float((dimensions.get(name) or {}).get("score", 0.0) or 0.0)
        max_total += float((dimensions.get(name) or {}).get("max", 0.0) or 0.0)
    if max_total <= 0:
        return 0.0
    return round(total / max_total * 100.0, 2)


def _partial_realization_proxy(partial_gate: dict[str, Any]) -> float:
    dimensions = partial_gate.get("dimensions", {}) or {}
    total = 0.0
    max_total = 0.0
    for name in ("regime_cycle", "catalyst", "market_structure"):
        total += float((dimensions.get(name) or {}).get("score", 0.0) or 0.0)
        max_total += float((dimensions.get(name) or {}).get("max", 0.0) or 0.0)
    if max_total <= 0:
        return 0.0
    return round(total / max_total * 100.0, 2)


def _build_partial_budget_context(
    stock_code: str,
    company_name: str,
    item: dict[str, Any],
    partial_scan_data: dict[str, Any],
    partial_gate: dict[str, Any],
) -> dict[str, Any]:
    profile = partial_scan_data.get("company_profile", {}).get("data", {}) or {}
    revenue_records = partial_scan_data.get("revenue_breakdown", {}).get("data", []) or []
    opportunity = partial_gate.get("opportunity_context") or determine_opportunity_type(
        stock_code,
        profile,
        revenue_records=revenue_records,
    )
    sector_route = resolve_sector_route(stock_code, profile, revenue_records).get("sector_route", "unknown")
    industry_group = resolve_industry_group(
        stock_code,
        profile,
        revenue_records,
        sector_route=sector_route,
    )
    valuation = partial_scan_data.get("valuation_history", {}).get("data", {}) or {}
    kline = partial_scan_data.get("stock_kline", {}).get("data", {}) or {}
    return {
        "ticker": stock_code,
        "company_name": company_name,
        "order_index": int(item.get("order_index", 0)),
        "primary_type": normalize_text(opportunity.get("primary_type")).lower() or "unknown",
        "opportunity_type": opportunity.get("primary_label", "Unknown"),
        "sector_route": normalize_text(sector_route).lower() or "unknown",
        "industry_group": normalize_text(industry_group.get("industry_group")).lower() or "unknown",
        "sector_cycle_sensitive": bool(industry_group.get("cycle_sensitive", False)),
        "score_upper_bound": float(partial_gate.get("score_upper_bound", 0.0) or 0.0),
        "underwrite_proxy": _partial_underwrite_proxy(partial_gate),
        "realization_proxy": _partial_realization_proxy(partial_gate),
        "current_vs_high": safe_float(kline.get("current_vs_5yr_high") or kline.get("current_vs_high")),
        "pb": safe_float(valuation.get("pb")),
        "pb_percentile": safe_float(valuation.get("pb_percentile")),
        "thesis": opportunity.get("sentence", "No clean thesis."),
    }


def _sector_budget_state(score: float, member_count: int, *, min_group_members: int) -> str:
    if member_count < min_group_members:
        return "neutral" if score >= 55.0 else "avoid"
    if score >= 72.0:
        return "favored"
    if score >= 55.0:
        return "neutral"
    return "avoid"


def _build_partial_sector_snapshot(
    partial_survivors: list[dict[str, Any]],
    *,
    priority_cutoff: float,
    secondary_cutoff: float,
    min_group_members: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for result in partial_survivors:
        context = result.get("partial_budget_context", {}) or {}
        if not context.get("sector_cycle_sensitive"):
            continue
        grouped.setdefault(str(context.get("industry_group") or "unknown"), []).append(context)

    snapshot: list[dict[str, Any]] = []
    for industry_group, rows in grouped.items():
        member_count = len(rows)
        ready_ratio = sum(1 for row in rows if float(row.get("score_upper_bound", 0.0)) >= secondary_cutoff) / member_count
        priority_ratio = sum(1 for row in rows if float(row.get("score_upper_bound", 0.0)) >= priority_cutoff) / member_count
        median_underwrite = sorted(float(row.get("underwrite_proxy", 0.0)) for row in rows)[member_count // 2]
        median_realization = sorted(float(row.get("realization_proxy", 0.0)) for row in rows)[member_count // 2]
        sector_cycle_score = (
            0.35 * ready_ratio
            + 0.25 * priority_ratio
            + 0.20 * (median_realization / 100.0)
            + 0.20 * (median_underwrite / 100.0)
        ) * 100.0
        snapshot.append(
            {
                "industry_group": industry_group,
                "sector_member_count": member_count,
                "sector_ready_ratio": round(ready_ratio, 6),
                "sector_priority_ratio": round(priority_ratio, 6),
                "sector_underwrite_proxy": round(median_underwrite, 6),
                "sector_realization_proxy": round(median_realization, 6),
                "sector_cycle_score": round(sector_cycle_score, 6),
                "sector_cycle_state": _sector_budget_state(
                    sector_cycle_score,
                    member_count,
                    min_group_members=min_group_members,
                ),
            }
        )
    return sorted(snapshot, key=lambda row: (-float(row["sector_cycle_score"]), row["industry_group"]))


def _snapshot_lookup(snapshot_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("industry_group") or "unknown"): row for row in snapshot_rows}


def _is_idiosyncratic_override(context: dict[str, Any], *, priority_cutoff: float) -> bool:
    return (
        normalize_text(context.get("primary_type")).lower() in {"turnaround", "asset_play", "special_situation"}
        and float(context.get("score_upper_bound", 0.0) or 0.0) >= max(priority_cutoff, 80.0)
    )


def _partial_budget_sort_key(result: dict[str, Any], snapshot_lookup: dict[str, dict[str, Any]]) -> tuple[float, ...]:
    context = result.get("partial_budget_context", {}) or {}
    snapshot = snapshot_lookup.get(str(context.get("industry_group") or "unknown"), {})
    return (
        -(100.0 if _is_idiosyncratic_override(context, priority_cutoff=float(DEFAULTS.get("priority_score_cutoff", 75))) else 0.0),
        -(float(snapshot.get("sector_cycle_score", 50.0)) or 50.0),
        -(float(context.get("score_upper_bound", 0.0)) or 0.0),
        -(float(context.get("realization_proxy", 0.0)) or 0.0),
        int(result.get("order_index", 0)),
    )


def _select_full_enrichment_candidates(
    partial_survivors: list[dict[str, Any]],
    *,
    max_full_enrichment: int,
    priority_cutoff: float,
    sector_budget_cfg: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if max_full_enrichment <= 0 or not partial_survivors:
        return [], []

    snapshot_rows = _build_partial_sector_snapshot(
        partial_survivors,
        priority_cutoff=priority_cutoff,
        secondary_cutoff=float(DEFAULTS.get("secondary_score_cutoff", 65)),
        min_group_members=int(sector_budget_cfg.get("min_group_members", 2) or 2),
    )
    snapshot_lookup = _snapshot_lookup(snapshot_rows)
    favored_share = float(sector_budget_cfg.get("favored_share", 0.70) or 0.70)
    neutral_share = float(sector_budget_cfg.get("neutral_share", 0.20) or 0.20)
    override_share = float(sector_budget_cfg.get("override_share", 0.10) or 0.10)

    core_pool: list[dict[str, Any]] = []
    neutral_pool: list[dict[str, Any]] = []
    override_pool: list[dict[str, Any]] = []
    avoid_pool: list[dict[str, Any]] = []

    for result in partial_survivors:
        context = result.get("partial_budget_context", {}) or {}
        state = str(snapshot_lookup.get(str(context.get("industry_group") or "unknown"), {}).get("sector_cycle_state", "neutral"))
        if _is_idiosyncratic_override(context, priority_cutoff=priority_cutoff):
            override_pool.append(result)
        elif not bool(context.get("sector_cycle_sensitive")):
            core_pool.append(result)
        elif state == "favored":
            core_pool.append(result)
        elif state == "neutral":
            neutral_pool.append(result)
        else:
            avoid_pool.append(result)

    for pool in (core_pool, neutral_pool, override_pool, avoid_pool):
        pool.sort(key=lambda item: _partial_budget_sort_key(item, snapshot_lookup))

    selected: list[dict[str, Any]] = []
    selected_tickers: set[str] = set()

    def _take_from(pool: list[dict[str, Any]], capacity: int) -> None:
        taken = 0
        for item in pool:
            ticker = str((item.get("partial_budget_context") or {}).get("ticker") or "").upper()
            if not ticker or ticker in selected_tickers:
                continue
            selected.append(item)
            selected_tickers.add(ticker)
            taken += 1
            if len(selected) >= max_full_enrichment or taken >= capacity:
                break

    override_cap = min(len(override_pool), max(1 if override_pool else 0, int(max_full_enrichment * override_share)))
    core_cap = min(len(core_pool), int(max_full_enrichment * favored_share))
    neutral_cap = min(len(neutral_pool), int(max_full_enrichment * neutral_share))

    if override_cap > 0:
        _take_from(override_pool, override_cap)
    if len(selected) < max_full_enrichment and core_cap > 0:
        _take_from(core_pool, core_cap)
    if len(selected) < max_full_enrichment and neutral_cap > 0:
        _take_from(neutral_pool, neutral_cap)

    for pool in (core_pool, neutral_pool, override_pool, avoid_pool):
        for item in pool:
            ticker = str((item.get("partial_budget_context") or {}).get("ticker") or "").upper()
            if len(selected) >= max_full_enrichment:
                break
            if not ticker or ticker in selected_tickers:
                continue
            selected.append(item)
            selected_tickers.add(ticker)
        if len(selected) >= max_full_enrichment:
            break

    return selected, snapshot_rows


def _attach_sector_overlay_to_payload(
    payload: dict[str, Any],
    *,
    partial_context: dict[str, Any] | None,
    snapshot_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    merged = dict(payload)
    context = partial_context or {}
    industry_group = normalize_text(merged.get("industry_group") or context.get("industry_group")).lower() or "unknown"
    snapshot = snapshot_lookup.get(industry_group, {})
    if context:
        merged.setdefault("primary_type", context.get("primary_type"))
        merged.setdefault("sector_route", context.get("sector_route"))
        merged.setdefault("industry_group", industry_group)
        merged.setdefault("sector_cycle_sensitive", bool(context.get("sector_cycle_sensitive", False)))
    if snapshot:
        merged["sector_member_count"] = int(snapshot.get("sector_member_count", 0))
        merged["sector_cycle_score"] = float(snapshot.get("sector_cycle_score", 50.0) or 50.0)
        merged["sector_cycle_state"] = snapshot.get("sector_cycle_state", "neutral")
    else:
        merged.setdefault("sector_member_count", 0)
        merged.setdefault("sector_cycle_score", 50.0)
        merged.setdefault("sector_cycle_state", "neutral")
    return merged


def _sector_budget_deferred_payload(
    partial_result: dict[str, Any],
    *,
    snapshot_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    context = partial_result.get("partial_budget_context", {}) or {}
    snapshot = snapshot_lookup.get(str(context.get("industry_group") or "unknown"), {})
    sector_state = snapshot.get("sector_cycle_state", "neutral")
    score_upper_bound = float(context.get("score_upper_bound", 0.0) or 0.0)
    return _attach_sector_overlay_to_payload(
        {
            "ticker": context.get("ticker"),
            "company_name": context.get("company_name"),
            "market": "A-share",
            "opportunity_type": context.get("opportunity_type", "Unknown"),
            "primary_type": context.get("primary_type", "unknown"),
            "sector_route": context.get("sector_route", "unknown"),
            "industry_group": context.get("industry_group", "unknown"),
            "sector_cycle_sensitive": bool(context.get("sector_cycle_sensitive", False)),
            "score": score_upper_bound,
            "underwrite_score": float(context.get("underwrite_proxy", 0.0) or 0.0),
            "realization_score": float(context.get("realization_proxy", 0.0) or 0.0),
            "hard_veto": False,
            "position_state": "cold_storage",
            "prev_state": "NEW",
            "flow_stage": "latent",
            "thesis": context.get("thesis", "No clean thesis."),
            "mispricing": f"partial upper bound {score_upper_bound:.1f} deferred by sector budget",
            "floor_protection": None,
            "normalized_upside": None,
            "recognition_upside": None,
            "catalysts": [],
            "risks": [f"sector budget deferred: {sector_state}"],
            "why_passed": "partial gate survivor held on watchlist until sector budget frees up",
            "next_step": "watch favored sector / override trigger",
            "reason": f"sector budget deferred: state={sector_state}, upper_bound={score_upper_bound:.1f}",
        },
        partial_context=context,
        snapshot_lookup=snapshot_lookup,
    )


def _scan_partial_stock(
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
    partial_context = _build_partial_budget_context(
        item["code"],
        item["name"],
        item,
        partial_scan_data,
        partial_gate,
    )

    if _should_prefilter_reject(partial_gate, secondary_cutoff):
        payload = _prefilter_rejected_payload(
            item["code"],
            item["name"],
            partial_scan_data,
            partial_gate,
            secondary_cutoff,
        )
        payload.update(
            {
                "primary_type": partial_context.get("primary_type"),
                "sector_route": partial_context.get("sector_route"),
                "industry_group": partial_context.get("industry_group"),
                "sector_cycle_sensitive": partial_context.get("sector_cycle_sensitive"),
            }
        )
        return {
            "kind": "rejected",
            "order_index": int(item.get("order_index", 0)),
            "payload": payload,
            "partial_budget_context": partial_context,
        }

    return {
        "kind": "survivor",
        "order_index": int(item.get("order_index", 0)),
        "item": item,
        "partial_scan_data": partial_scan_data,
        "partial_gate": partial_gate,
        "partial_budget_context": partial_context,
    }


def _enrich_partial_result(
    partial_result: dict[str, Any],
    *,
    day_cache_dir: Path | None,
    retry_delays: tuple[float, ...],
) -> dict[str, Any]:
    item = partial_result["item"]
    partial_scan_data = partial_result.get("partial_scan_data", {}) or {}
    partial_gate = partial_result.get("partial_gate", {}) or {}

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


def _init_radar_day_cache_dir(trade_date: str, enabled: bool, *, base_dir: Path) -> Path | None:
    if not enabled:
        return None

    cache_dir = market_scan_paths(base_dir)["radar_cache_root"] / trade_date
    cache_dir.mkdir(parents=True, exist_ok=True)

    meta_path = cache_dir / "_meta.json"
    if not meta_path.exists():
        meta_payload = json.dumps(
            {
                "trade_date": trade_date,
                "created_at": datetime.datetime.now().isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        )
        try:
            fd = os.open(meta_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(meta_payload)
    return cache_dir


def _collect_parallel_results(
    *,
    tasks: list[tuple[str, Any, tuple[Any, ...], dict[str, Any]]],
    max_workers: int,
    timeout_seconds: float | None,
    stage_name: str,
) -> tuple[list[Any], list[str]]:
    if not tasks:
        return [], []

    results: list[Any] = []
    errors: list[str] = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures = {
        executor.submit(func, *args, **kwargs): label
        for label, func, args, kwargs in tasks
    }
    try:
        timeout = None if timeout_seconds in (None, 0) else float(timeout_seconds)
        for future in as_completed(futures, timeout=timeout):
            label = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                message = f"{stage_name} failed for {label}: {exc}"
                errors.append(message)
                LOGGER.warning(message, exc_info=True)
    except FuturesTimeoutError:
        pending = [label for future, label in futures.items() if not future.done()]
        if pending:
            message = f"{stage_name} timed out for {', '.join(pending[:5])}"
            errors.append(message)
            LOGGER.warning(message)
    finally:
        for future in futures:
            if not future.done():
                future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
    return results, errors


def _scan_one_stock(
    item: dict[str, Any],
    *,
    secondary_cutoff: float,
    day_cache_dir: Path | None,
    retry_delays: tuple[float, ...],
) -> dict[str, Any]:
    partial_result = _scan_partial_stock(
        item,
        secondary_cutoff=secondary_cutoff,
        day_cache_dir=day_cache_dir,
        retry_delays=retry_delays,
    )
    if partial_result.get("kind") == "rejected":
        return {
            "kind": "rejected",
            "order_index": int(item.get("order_index", 0)),
            "payload": partial_result.get("payload", {}) or {},
        }
    return _enrich_partial_result(
        partial_result,
        day_cache_dir=day_cache_dir,
        retry_delays=retry_delays,
    )


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
    future_timeout_seconds = float(DEFAULTS.get("radar_future_timeout_seconds", 120.0) or 120.0)
    sector_budget_cfg = DEFAULTS.get("sector_budget", {}) or {}
    sector_budget_enabled = bool(sector_budget_cfg.get("enabled", True))
    scan_errors: list[str] = []

    partial_tasks = [
        (
            str(item.get("code") or f"partial-{index}"),
            _scan_partial_stock,
            (item,),
            {
                "secondary_cutoff": secondary_cutoff,
                "day_cache_dir": day_cache_dir,
                "retry_delays": retry_delays,
            },
        )
        for index, item in enumerate(universe)
    ]
    partial_results, partial_errors = _collect_parallel_results(
        tasks=partial_tasks,
        max_workers=max_workers,
        timeout_seconds=future_timeout_seconds,
        stage_name="partial_scan",
    )
    scan_errors.extend(partial_errors)

    partial_results.sort(key=lambda item: int(item.get("order_index", 0)))
    prefiltered_results = [item for item in partial_results if item.get("kind") == "rejected"]
    partial_survivors = [item for item in partial_results if item.get("kind") == "survivor"]

    full_enrichment_budget = min(len(partial_survivors), max_universe)
    if not sector_budget_enabled:
        selected_for_full = list(partial_survivors[:full_enrichment_budget])
        sector_snapshot_rows: list[dict[str, Any]] = []
    else:
        selected_for_full, sector_snapshot_rows = _select_full_enrichment_candidates(
            partial_survivors,
            max_full_enrichment=full_enrichment_budget,
            priority_cutoff=priority_cutoff,
            sector_budget_cfg=sector_budget_cfg,
        )
    selected_tickers = {
        str((item.get("partial_budget_context") or {}).get("ticker") or "").upper()
        for item in selected_for_full
    }
    sector_snapshot_lookup = _snapshot_lookup(sector_snapshot_rows)

    ranked_with_order: list[dict[str, Any]] = []
    for item in prefiltered_results:
        ranked_with_order.append(
            {
                "kind": "rejected",
                "order_index": int(item.get("order_index", 0)),
                "payload": _attach_sector_overlay_to_payload(
                    item.get("payload", {}) or {},
                    partial_context=item.get("partial_budget_context"),
                    snapshot_lookup=sector_snapshot_lookup,
                ),
            }
        )

    deferred_results = [
        item
        for item in partial_survivors
        if str((item.get("partial_budget_context") or {}).get("ticker") or "").upper() not in selected_tickers
    ]
    for item in deferred_results:
        ranked_with_order.append(
            {
                "kind": "deferred",
                "order_index": int(item.get("order_index", 0)),
                "payload": _sector_budget_deferred_payload(
                    item,
                    snapshot_lookup=sector_snapshot_lookup,
                ),
            }
        )

    if selected_for_full:
        enrich_tasks = [
            (
                str(((item.get("partial_budget_context") or {}).get("ticker")) or f"enrich-{index}"),
                _enrich_partial_result,
                (item,),
                {
                    "day_cache_dir": day_cache_dir,
                    "retry_delays": retry_delays,
                },
            )
            for index, item in enumerate(selected_for_full)
        ]
        enriched_results, enrich_errors = _collect_parallel_results(
            tasks=enrich_tasks,
            max_workers=max_workers,
            timeout_seconds=future_timeout_seconds,
            stage_name="full_enrichment",
        )
        scan_errors.extend(enrich_errors)
        for result in enriched_results:
            ranked_with_order.append(
                {
                    **result,
                    "payload": _attach_sector_overlay_to_payload(
                        result.get("payload", {}) or {},
                        partial_context=None,
                        snapshot_lookup=sector_snapshot_lookup,
                    ),
                }
            )

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

    scanner_diagnostics = {
        "partial_survivor_count": len(partial_survivors),
        "full_enrichment_count": len(selected_for_full),
        "deferred_watchlist_count": len(deferred_results),
    }
    report = generate_market_scan_report(
        market="A-share",
        scope_text=f"{scope} (raw={len(raw_universe)}, coarse={len(universe)}, fine={len(ranked)})",
        results_summary=summary,
        priority_shortlist=priority,
        secondary_watchlist=secondary,
        rejected=rejected,
        report_dir=str(report_dir),
        scanner_diagnostics=scanner_diagnostics,
        sector_snapshot=sector_snapshot_rows,
    )
    payload = {
        "scope": scope,
        "universe_size": len(raw_universe),
        "coarse_candidate_count": len(universe),
        "fine_candidate_count": len(ranked),
        "partial_survivor_count": len(partial_survivors),
        "full_enrichment_count": len(selected_for_full),
        "scanner_diagnostics": scanner_diagnostics,
        "summary": summary,
        "sector_snapshot": sector_snapshot_rows,
        "scan_errors": scan_errors,
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
