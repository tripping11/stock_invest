"""Route-aware Driver Stack helpers for VCRF OS 2.0."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_sector_classification
from utils.financial_snapshot import extract_latest_revenue_terms, extract_market_cap
from utils.value_utils import normalize_text, safe_float


def _combined_texts(profile: dict[str, Any], revenue_records: list[dict[str, Any]], extra_texts: list[str] | None = None) -> list[str]:
    texts = [
        normalize_text(profile.get("行业") or profile.get("所属行业") or profile.get("申万行业") or profile.get("申万一级行业")),
        normalize_text(profile.get("主营业务")),
        normalize_text(profile.get("经营范围")),
        normalize_text(profile.get("公司名称") or profile.get("股票简称") or profile.get("名称")),
    ]
    texts.extend(extract_latest_revenue_terms(revenue_records, limit=12))
    texts.extend(normalize_text(item) for item in (extra_texts or []))
    return [text for text in texts if text]


def _extract_tags(profile: dict[str, Any], extra_texts: list[str] | None = None) -> list[str]:
    combined = " ".join(
        text
        for text in [
            normalize_text(profile.get("股票简称") or profile.get("名称")),
            normalize_text(profile.get("主营业务")),
            *(normalize_text(item) for item in (extra_texts or [])),
        ]
        if text
    )
    tags: list[str] = []
    if "*ST" in combined:
        tags.append("star_st")
    elif "ST" in combined:
        tags.append("st")
    return tags


def resolve_sector_route(stock_code: str, profile: dict[str, Any], revenue_records: list[dict[str, Any]]) -> dict[str, Any]:
    mapping = load_sector_classification()
    override = (mapping.get("company_overrides", {}) or {}).get(str(stock_code), {}) or {}
    if override.get("sector_route"):
        return {
            "sector_route": normalize_text(override.get("sector_route")).lower(),
            "matched_terms": [normalize_text(override.get("name"))] if override.get("name") else [],
            "confidence": 1.0,
            "reason": "company override",
        }

    combined = " ".join(_combined_texts(profile, revenue_records))
    best_route = "unknown"
    best_hits: list[str] = []
    for route, cfg in (mapping.get("sector_routes", {}) or {}).items():
        hits = [keyword for keyword in cfg.get("keywords", []) if keyword and keyword in combined]
        if len(hits) > len(best_hits):
            best_route = route
            best_hits = hits

    confidence = 0.2
    if len(best_hits) >= 2:
        confidence = 0.95
    elif len(best_hits) == 1:
        confidence = 0.70

    return {
        "sector_route": best_route,
        "matched_terms": best_hits,
        "confidence": confidence,
        "reason": "keyword routing" if best_hits else "no route keywords matched",
    }


def infer_preliminary_cycle_state(sector_route: str, scan_data: dict[str, Any]) -> str:
    kline = scan_data.get("stock_kline", {}).get("data", {})
    current_vs_high = safe_float(kline.get("current_vs_5yr_high") or kline.get("current_vs_high"))
    if sector_route in {"core_resource", "rigid_shovel"}:
        if current_vs_high is None:
            return "repair"
        if current_vs_high <= 50:
            return "trough"
        if current_vs_high <= 75:
            return "repair"
        if current_vs_high >= 90:
            return "peak"
        return "expansion"
    if current_vs_high is not None and current_vs_high <= 60:
        return "repair"
    if current_vs_high is not None and current_vs_high >= 90:
        return "peak"
    return "expansion"


def determine_primary_type(
    sector_route: str,
    preliminary_cycle_state: str,
    financials_3y: dict[str, Any],
    tags: list[str],
    events: dict[str, Any],
    big_bath_result: dict[str, Any],
) -> tuple[str, float]:
    normalized_tags = {normalize_text(tag).lower() for tag in tags}
    if "st" in normalized_tags or "star_st" in normalized_tags:
        return "special_situation", 0.90

    losses_2y = bool(financials_3y.get("losses_2y"))
    repair_evidence = bool(financials_3y.get("repair_evidence")) or bool(events.get("repair_evidence"))
    if losses_2y and (normalize_text(big_bath_result.get("verdict")).lower() == "big_bath" or repair_evidence):
        return "turnaround", 0.80

    if sector_route in {"core_resource", "rigid_shovel"} and preliminary_cycle_state in {"trough", "repair"}:
        return "cyclical", 0.75

    deep_discount_to_nav = bool(financials_3y.get("deep_discount_to_nav"))
    asset_unlock_path = bool(events.get("asset_unlock_path"))
    if deep_discount_to_nav and asset_unlock_path:
        return "asset_play", 0.75

    return "compounder", 0.60


def _infer_losses_and_repair(scan_data: dict[str, Any]) -> dict[str, Any]:
    income_records = scan_data.get("income_statement", {}).get("data", []) or []
    profits: list[float] = []
    for row in income_records[:3]:
        for key in ("归属于母公司所有者的净利润", "归属于母公司股东的净利润", "净利润"):
            value = safe_float(row.get(key))
            if value is not None:
                profits.append(value)
                break
    losses_2y = sum(1 for value in profits[:2] if value < 0) >= 2
    repair_evidence = len(profits) >= 2 and profits[0] < profits[-1]
    return {
        "losses_2y": losses_2y,
        "repair_evidence": repair_evidence,
        "deep_discount_to_nav": False,
    }


def _infer_repair_state(financials_3y: dict[str, Any], primary_type: str) -> str:
    if primary_type == "turnaround" and financials_3y.get("repair_evidence"):
        return "repairing"
    if primary_type == "cyclical" and financials_3y.get("repair_evidence"):
        return "stabilizing"
    return "none"


def _infer_realization_path(texts: list[str]) -> str:
    mapping = load_sector_classification().get("realization_path_keywords", {}) or {}
    combined = " ".join(texts)
    for path, keywords in mapping.items():
        if any(keyword and keyword in combined for keyword in keywords or []):
            return path
    return "repricing"


def _elasticity_bucket(scan_data: dict[str, Any]) -> str:
    market_cap = extract_market_cap(scan_data.get("realtime_quote", {}).get("data", {}))
    if market_cap is None:
        return "mid"
    if market_cap < 5_000_000_000:
        return "micro"
    if market_cap < 20_000_000_000:
        return "small"
    if market_cap < 50_000_000_000:
        return "mid"
    if market_cap < 200_000_000_000:
        return "large"
    return "mega"


def build_driver_stack(stock_code: str, scan_data: dict[str, Any], *, extra_texts: list[str] | None = None) -> dict[str, Any]:
    profile = scan_data.get("company_profile", {}).get("data", {}) or {}
    revenue_records = scan_data.get("revenue_breakdown", {}).get("data", []) or []
    route_result = resolve_sector_route(stock_code, profile, revenue_records)
    cycle_state = infer_preliminary_cycle_state(route_result["sector_route"], scan_data)
    financials_3y = _infer_losses_and_repair(scan_data)
    tags = _extract_tags(profile, extra_texts=extra_texts)
    primary_type, confidence = determine_primary_type(
        route_result["sector_route"],
        cycle_state,
        financials_3y,
        tags,
        events={},
        big_bath_result={"verdict": "inconclusive"},
    )
    texts = _combined_texts(profile, revenue_records, extra_texts=extra_texts)
    return {
        "market": "A-share",
        "sector_route": route_result["sector_route"],
        "primary_type": primary_type,
        "primary_type_confidence": confidence,
        "modifiers": {
            "cycle_state": cycle_state,
            "repair_state": _infer_repair_state(financials_3y, primary_type),
            "distress_source": "cyclical" if primary_type == "cyclical" else "operational" if financials_3y.get("losses_2y") else "one_off",
            "realization_path": _infer_realization_path(texts),
            "flow_stage": "latent",
            "elasticity_bucket": _elasticity_bucket(scan_data),
        },
        "special_tags": tags,
        "routing_reason": route_result["reason"],
        "routing_terms": route_result["matched_terms"],
    }
