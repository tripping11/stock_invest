"""Route-aware Driver Stack helpers for VCRF OS 2.0."""
from __future__ import annotations

import os
from typing import Any

from utils.config_loader import load_sector_classification
from utils.financial_snapshot import (
    extract_latest_revenue_terms,
    extract_float_market_cap,
    extract_market_cap,
    get_latest_cashflow_snapshot,
    get_latest_income_snapshot,
)
from utils.vcrf_probes import detect_big_bath
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


def _normalize_margin(value: float | None) -> float | None:
    if value is None:
        return None
    if abs(value) > 1.0:
        return value / 100.0
    return value


def _record_date_key(row: dict[str, Any]) -> str:
    for key in ("报告日期", "报告期", "日期", "报告日"):
        value = normalize_text(row.get(key))
        if value:
            return value
    return ""


def _extract_recent_gross_margins(revenue_records: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    sorted_records = sorted(revenue_records or [], key=_record_date_key, reverse=True)
    latest = previous = None
    seen_dates: set[str] = set()
    for row in sorted_records:
        report_date = _record_date_key(row) or str(len(seen_dates))
        if report_date in seen_dates:
            continue
        seen_dates.add(report_date)
        for key in ("毛利率", "主营毛利率", "gross_margin"):
            margin = _normalize_margin(safe_float(row.get(key)))
            if margin is not None:
                if latest is None:
                    latest = margin
                else:
                    previous = margin
                break
        if latest is not None and previous is not None:
            break
    return latest, previous


def _infer_gross_margin_trend(revenue_records: list[dict[str, Any]]) -> tuple[str | None, float | None]:
    latest_margin, previous_margin = _extract_recent_gross_margins(revenue_records)
    if latest_margin is None or previous_margin is None:
        return None, None
    delta = latest_margin - previous_margin
    if delta > 0:
        return "recovering", delta
    if delta >= -0.02:
        return "stable", delta
    return "declining", delta


def _infer_market(stock_code: str) -> str:
    return "A-share" if str(stock_code).isdigit() and len(str(stock_code)) == 6 else "US-share"


def _derive_big_bath_features(scan_data: dict[str, Any]) -> dict[str, Any]:
    income_records = scan_data.get("income_statement", {}).get("data", []) or []
    cashflow_records = scan_data.get("cashflow_statement", {}).get("data", []) or []
    revenue_records = scan_data.get("revenue_breakdown", {}).get("data", []) or []

    latest_income = get_latest_income_snapshot(income_records)
    latest_profit = safe_float(latest_income.get("net_profit"))
    latest_income_row = latest_income.get("raw", {}) or {}
    impairment_total = 0.0
    impairment_present = False
    for key in ("资产减值损失", "信用减值损失", "资产减值准备", "商誉减值损失", "商誉减值准备"):
        value = safe_float(latest_income_row.get(key))
        if value is None:
            continue
        impairment_present = True
        impairment_total += value

    one_off_impairment_ratio = None
    if impairment_present and latest_profit not in (None, 0):
        one_off_impairment_ratio = abs(impairment_total) / max(abs(latest_profit), 1.0)

    latest_cashflow = get_latest_cashflow_snapshot(cashflow_records)
    latest_ocf = safe_float(latest_cashflow.get("operating_cashflow"))
    ocf_vs_net_income_divergence = None
    if latest_ocf is not None and latest_profit not in (None, 0):
        ocf_vs_net_income_divergence = (latest_ocf - latest_profit) / max(abs(latest_profit), 1.0)

    gross_margin_trend, gross_margin_delta = _infer_gross_margin_trend(revenue_records)
    availability = {
        "one_off_impairment_ratio": one_off_impairment_ratio is not None,
        "ocf_vs_net_income_divergence": ocf_vs_net_income_divergence is not None,
        "gross_margin_delta": gross_margin_delta is not None,
    }

    return {
        "one_off_impairment_ratio": one_off_impairment_ratio,
        "ocf_vs_net_income_divergence": ocf_vs_net_income_divergence,
        "gross_margin_delta": gross_margin_delta,
        "gross_margin_trend": gross_margin_trend,
        "feature_availability": availability,
    }


def _evaluate_big_bath(scan_data: dict[str, Any]) -> dict[str, Any]:
    features = _derive_big_bath_features(scan_data)
    if not all(features["feature_availability"].values()):
        return {
            "verdict": "inconclusive",
            "confidence": "low",
            "feature_availability": features["feature_availability"],
            "gross_margin_trend": features["gross_margin_trend"],
            "gross_margin_delta": features["gross_margin_delta"],
            "one_off_impairment_ratio": features["one_off_impairment_ratio"],
            "ocf_vs_net_income_divergence": features["ocf_vs_net_income_divergence"],
        }

    result = detect_big_bath(features)
    result["feature_availability"] = features["feature_availability"]
    result["gross_margin_delta"] = features["gross_margin_delta"]
    result["gross_margin_trend"] = features["gross_margin_trend"]
    return result


def infer_preliminary_cycle_state(
    sector_route: str,
    scan_data: dict[str, Any],
    *,
    big_bath_result: dict[str, Any] | None = None,
    gross_margin_trend: str | None = None,
) -> tuple[str, str]:
    kline = scan_data.get("stock_kline", {}).get("data", {})
    current_vs_high = safe_float(kline.get("current_vs_5yr_high") or kline.get("current_vs_high"))
    big_bath_verdict = normalize_text((big_bath_result or {}).get("verdict")).lower()
    confidence = "high"

    if sector_route in {"core_resource", "rigid_shovel"}:
        if big_bath_verdict == "genuine_collapse":
            return "peak", confidence
        if current_vs_high is None:
            return "repair", confidence
        if current_vs_high <= 50:
            if gross_margin_trend == "declining":
                confidence = "low"
            return "trough", confidence
        if current_vs_high <= 75:
            return "repair", confidence
        if current_vs_high >= 90:
            return "peak", confidence
        return "expansion", confidence
    if big_bath_verdict == "genuine_collapse":
        return "peak", confidence
    if current_vs_high is not None and current_vs_high <= 60:
        return "repair", confidence
    if current_vs_high is not None and current_vs_high >= 90:
        return "peak", confidence
    return "expansion", confidence


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

    deep_discount_to_nav = bool(financials_3y.get("deep_discount_to_nav"))
    asset_unlock_path = bool(events.get("asset_unlock_path"))
    if deep_discount_to_nav and asset_unlock_path:
        return "asset_play", 0.75

    if sector_route in {"core_resource", "rigid_shovel"} and preliminary_cycle_state in {"trough", "repair"}:
        return "cyclical", 0.75

    return "compounder", 0.60


def _has_asset_play_hint(profile: dict[str, Any], revenue_records: list[dict[str, Any]], extra_texts: list[str] | None = None) -> bool:
    asset_cfg = (load_sector_classification().get("opportunity_types", {}) or {}).get("asset_play", {}) or {}
    combined = " ".join(_combined_texts(profile, revenue_records, extra_texts=extra_texts))
    keywords = [normalize_text(item) for item in (asset_cfg.get("keywords", []) or []) + (asset_cfg.get("signals", []) or [])]
    return any(keyword and keyword in combined for keyword in keywords)


def _infer_deep_discount_to_nav(
    scan_data: dict[str, Any],
    sector_route: str,
    *,
    asset_play_hint: bool = False,
) -> bool:
    if sector_route != "financial_asset" and not asset_play_hint:
        return False
    valuation = scan_data.get("valuation_history", {}).get("data", {}) or {}
    kline = scan_data.get("stock_kline", {}).get("data", {}) or {}
    pb = safe_float(valuation.get("pb"))
    pb_percentile = safe_float(valuation.get("pb_percentile"))
    current_vs_high = safe_float(kline.get("current_vs_5yr_high") or kline.get("current_vs_high"))

    if pb is not None and pb <= 0.85:
        return True
    if pb_percentile is not None and pb_percentile <= 20 and (pb is None or pb <= 1.0):
        return True
    if pb is not None and pb <= 1.0 and current_vs_high is not None and current_vs_high <= 65:
        return True
    return False


def _infer_losses_and_repair(
    scan_data: dict[str, Any],
    sector_route: str,
    *,
    asset_play_hint: bool = False,
) -> dict[str, Any]:
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
        "deep_discount_to_nav": _infer_deep_discount_to_nav(scan_data, sector_route, asset_play_hint=asset_play_hint),
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
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    market_cap = extract_float_market_cap(quote) or extract_market_cap(quote)
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


def _validate_modifiers(modifiers: dict[str, Any]) -> None:
    allowed_values = load_sector_classification().get("modifier_enums", {}) or {}
    for key, allowed in allowed_values.items():
        value = normalize_text(modifiers.get(key))
        if not value:
            continue
        normalized_allowed = {normalize_text(item) for item in allowed or []}
        if value not in normalized_allowed:
            raise ValueError(f"invalid modifier {key}={value}")


def build_driver_stack(stock_code: str, scan_data: dict[str, Any], *, extra_texts: list[str] | None = None) -> dict[str, Any]:
    profile = scan_data.get("company_profile", {}).get("data", {}) or {}
    revenue_records = scan_data.get("revenue_breakdown", {}).get("data", []) or []
    route_result = resolve_sector_route(stock_code, profile, revenue_records)
    asset_play_hint = _has_asset_play_hint(profile, revenue_records, extra_texts=extra_texts)
    financials_3y = _infer_losses_and_repair(
        scan_data,
        route_result["sector_route"],
        asset_play_hint=asset_play_hint,
    )
    big_bath_result = _evaluate_big_bath(scan_data)
    cycle_state, cycle_state_confidence = infer_preliminary_cycle_state(
        route_result["sector_route"],
        scan_data,
        big_bath_result=big_bath_result,
        gross_margin_trend=big_bath_result.get("gross_margin_trend"),
    )
    tags = _extract_tags(profile, extra_texts=extra_texts)
    texts = _combined_texts(profile, revenue_records, extra_texts=extra_texts)
    realization_path = _infer_realization_path(texts)
    events = {
        "asset_unlock_path": realization_path == "asset_unlock",
        "repair_evidence": financials_3y.get("repair_evidence", False),
    }
    primary_type, confidence = determine_primary_type(
        route_result["sector_route"],
        cycle_state,
        financials_3y,
        tags,
        events=events,
        big_bath_result=big_bath_result,
    )
    modifiers = {
        "cycle_state": cycle_state,
        "cycle_state_confidence": cycle_state_confidence,
        "repair_state": _infer_repair_state(financials_3y, primary_type),
        "distress_source": "cyclical" if primary_type == "cyclical" else "operational" if financials_3y.get("losses_2y") else "one_off",
        "realization_path": realization_path,
        "flow_stage": "latent",
        "elasticity_bucket": _elasticity_bucket(scan_data),
    }
    if os.environ.get("A_STOCK_VALIDATE_MODIFIERS") == "1":
        _validate_modifiers(modifiers)
    return {
        "market": _infer_market(stock_code),
        "sector_route": route_result["sector_route"],
        "primary_type": primary_type,
        "primary_type_confidence": confidence,
        "modifiers": modifiers,
        "big_bath_result": big_bath_result,
        "repair_evidence": financials_3y.get("repair_evidence", False),
        "special_tags": tags,
        "routing_reason": route_result["reason"],
        "routing_terms": route_result["matched_terms"],
    }
