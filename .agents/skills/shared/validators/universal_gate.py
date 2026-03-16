"""Universal six-gate evaluator for the new investment framework."""
from __future__ import annotations

from typing import Any

from engines.flow_realization_engine import FlowInputs, classify_position_state, score_flow_setup
from engines.valuation_engine import build_three_case_valuation
from utils.config_loader import load_scoring_rules
from utils.financial_snapshot import (
    extract_latest_price,
    extract_market_cap,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
)
from utils.opportunity_classifier import (
    assess_bottom_pattern,
    assess_business_purity,
    assess_catalyst_strength,
    assess_management_quality,
    assess_moat_quality,
    classify_state_ownership,
    determine_opportunity_type,
)
from utils.score_verdict import pick_score_verdict
from utils.value_utils import clamp, normalize_text, safe_float


# ── Map YAML dimension keys → internal short keys ───────────
_YAML_TO_INTERNAL = {
    "opportunity_type_clarity": "type_clarity",
    "business_quality": "business_quality",
    "survival_boundary": "survival",
    "management_capital_allocation": "management",
    "regime_cycle_position": "regime_cycle",
    "valuation_margin_of_safety": "valuation",
    "catalyst_value_realization": "catalyst",
    "market_structure_tradability": "market_structure",
}

# Fallback values identical to the previous hardcoded dict, used when the
# YAML file is unavailable or incomplete.
_FALLBACK_DIMENSION_MAX = {
    "type_clarity": 5.0,
    "business_quality": 20.0,
    "survival": 15.0,
    "management": 10.0,
    "regime_cycle": 15.0,
    "valuation": 20.0,
    "catalyst": 10.0,
    "market_structure": 5.0,
}


def _load_dimension_max() -> dict[str, float]:
    """Build DIMENSION_MAX from scoring_rules.yaml, falling back to defaults."""
    try:
        dims_cfg = load_scoring_rules().get("dimensions", {})
    except Exception:
        return dict(_FALLBACK_DIMENSION_MAX)

    result = dict(_FALLBACK_DIMENSION_MAX)
    for yaml_key, internal_key in _YAML_TO_INTERNAL.items():
        entry = dims_cfg.get(yaml_key, {})
        if isinstance(entry, dict) and "weight" in entry:
            result[internal_key] = float(entry["weight"])
    return result


DIMENSION_MAX = _load_dimension_max()

_LEGACY_VERDICT_LABELS = {
    "high conviction / attack candidate": "high conviction / strong candidate",
    "strong candidate / ready": "reasonable candidate / starter possible",
    "cold-storage / watchlist": "watch / needs work",
}

_VCRF_DIMENSION_FALLBACKS = {
    "thesis_clarity": 5.0,
    "intrinsic_value_floor": 20.0,
    "survival_boundary": 15.0,
    "governance_anti_fraud": 10.0,
    "business_or_asset_quality": 10.0,
    "regime_cycle_position": 15.0,
    "turnaround_catalyst": 10.0,
    "flow_realization_and_elasticity": 15.0,
}


def _pick_numeric(source: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in source:
            numeric = safe_float(source.get(key))
            if numeric is not None:
                return numeric
    return None


def _load_vcrf_dimension_max() -> dict[str, float]:
    try:
        dimensions = load_scoring_rules().get("dimensions", {})
    except Exception:
        return dict(_VCRF_DIMENSION_FALLBACKS)

    result = dict(_VCRF_DIMENSION_FALLBACKS)
    for key, fallback in _VCRF_DIMENSION_FALLBACKS.items():
        entry = dimensions.get(key, {})
        weight = entry.get("weight") if isinstance(entry, dict) else None
        result[key] = float(weight) if isinstance(weight, (int, float)) else fallback
    return result


def _load_weight_template(primary_type: str) -> dict[str, float]:
    rules = load_scoring_rules()
    template = (rules.get("weight_templates", {}) or {}).get(primary_type, {}) or {}
    if template:
        return {key: float(value) for key, value in template.items() if isinstance(value, (int, float))}
    return {
        "intrinsic_value_floor": _VCRF_DIMENSION_FALLBACKS["intrinsic_value_floor"],
        "survival_boundary": _VCRF_DIMENSION_FALLBACKS["survival_boundary"],
        "governance_anti_fraud": _VCRF_DIMENSION_FALLBACKS["governance_anti_fraud"],
        "business_or_asset_quality": _VCRF_DIMENSION_FALLBACKS["business_or_asset_quality"],
        "regime_cycle_position": _VCRF_DIMENSION_FALLBACKS["regime_cycle_position"],
        "turnaround_catalyst": _VCRF_DIMENSION_FALLBACKS["turnaround_catalyst"],
        "flow_realization_and_elasticity": _VCRF_DIMENSION_FALLBACKS["flow_realization_and_elasticity"],
    }


VCRF_DIMENSION_MAX = _load_vcrf_dimension_max()


def _status_from_score(score: float, passing: float, caution: float) -> str:
    if score >= passing:
        return "pass"
    if score >= caution:
        return "caution"
    return "fail"


def _valuation_score(primary_type: str, pb: float | None, current_vs_high: float | None) -> tuple[float, str]:
    score = 8.0
    reasons: list[str] = []
    if primary_type == "asset_play":
        if pb is not None and pb <= 0.8:
            score = 18.0
            reasons.append(f"PB {pb:.2f} suggests discount to book")
        elif pb is not None and pb <= 1.1:
            score = 14.0
            reasons.append(f"PB {pb:.2f} is still close to asset value")
        elif pb is not None:
            score = 8.0
            reasons.append(f"PB {pb:.2f} no longer offers obvious asset discount")
    elif primary_type == "cyclical":
        if pb is not None and pb <= 1.2 and current_vs_high is not None and current_vs_high <= 60:
            score = 16.0
            reasons.append("cycle-sensitive valuation still looks compressed")
        elif current_vs_high is not None and current_vs_high >= 85:
            score = 5.0
            reasons.append("price is already close to prior highs")
    elif primary_type == "compounder":
        if pb is not None and pb <= 3.0:
            score = 14.0
            reasons.append(f"PB {pb:.2f} is not obviously overpaying for quality")
        elif pb is not None and pb >= 6.0:
            score = 5.0
            reasons.append(f"PB {pb:.2f} already implies aggressive expectations")
    elif primary_type == "turnaround":
        if pb is not None and pb <= 1.0:
            score = 15.0
            reasons.append("market still prices the repair story cautiously")
        elif current_vs_high is not None and current_vs_high >= 80:
            score = 6.0
            reasons.append("turnaround may already be priced in")
    elif primary_type == "special_situation":
        score = 12.0 if current_vs_high is None or current_vs_high <= 75 else 7.0
        reasons.append("special situations need event path more than static multiples")

    if current_vs_high is not None:
        if current_vs_high <= 50:
            score += 2.0
            reasons.append(f"price remains only {current_vs_high:.1f}% of 5y high")
        elif current_vs_high >= 90:
            score -= 2.0
            reasons.append(f"price already near historical highs at {current_vs_high:.1f}%")

    return clamp(score, 0.0, DIMENSION_MAX["valuation"]), "; ".join(reasons) if reasons else "valuation signal is mixed"


def _legacy_verdict_from_vcrf(label: str, hard_vetos: list[str]) -> str:
    if hard_vetos:
        return "reject / no action"
    return _LEGACY_VERDICT_LABELS.get(label, label)


def _legacy_verdict_from_total(total_score: float, hard_vetos: list[str]) -> str:
    if hard_vetos:
        return "reject / no action"
    if total_score >= 85.0:
        return "high conviction / strong candidate"
    if total_score >= 75.0:
        return "reasonable candidate / starter possible"
    if total_score >= 65.0:
        return "watch / needs work"
    return "reject / no action"


def _infer_repair_state(context: dict[str, Any]) -> str:
    profit = safe_float(context["latest_income"].get("net_profit"))
    equity = safe_float(context["latest_balance"].get("total_equity"))
    catalyst_score = safe_float(context["catalyst"].get("score")) or 0.0
    bottom_signal = normalize_text(context["bottom_pattern"].get("signal")).lower()
    primary_type = normalize_text(context["opportunity_context"].get("primary_type")).lower()

    if equity is not None and equity <= 0:
        return "none"
    if profit is not None and profit > 0 and (bottom_signal == "favorable" or catalyst_score >= 6.0):
        return "confirmed" if primary_type in {"turnaround", "special_situation"} else "repairing"
    if bottom_signal == "favorable" or catalyst_score >= 4.0:
        return "repairing" if primary_type in {"turnaround", "special_situation"} else "stabilizing"
    return "stabilizing" if primary_type in {"cyclical", "asset_play"} else "none"


def _infer_flow_inputs(context: dict[str, Any]) -> FlowInputs:
    kline = context["kline"] or {}
    current_price = safe_float(context["current_price"])
    current_vs_high = safe_float(context["current_vs_high"])
    current_vs_low = _pick_numeric(kline, "rebound_from_low_pct", "rebound_from_60d_low_pct", "rebound_from_52w_low_pct")
    if current_vs_low is None and current_price not in (None, 0):
        low_anchor = _pick_numeric(kline, "low_60d", "low_120d", "low_250d", "low_52w", "year_low", "period_low")
        if low_anchor not in (None, 0):
            current_vs_low = current_price / low_anchor - 1

    catalyst_text = " ".join(
        [
            normalize_text(context["catalyst"].get("reason")),
            " ".join(context["catalyst"].get("catalysts", []) or []),
            normalize_text(context["business_text"]),
        ]
    )
    buyback_flag = any(token in catalyst_text.lower() for token in ("buyback", "回购", "增持"))
    mna_flag = any(token in catalyst_text.lower() for token in ("m&a", "并购", "重组", "注入", "资产收购"))

    rel_strength_20d = _pick_numeric(kline, "rel_strength_20d", "relative_strength_20d", "rs_20d")
    rel_strength_60d = _pick_numeric(kline, "rel_strength_60d", "relative_strength_60d", "rs_60d")
    if rel_strength_20d is None and current_vs_high is not None:
        rel_strength_20d = current_vs_high / 100 - 0.55

    return FlowInputs(
        current_price=current_price,
        avg20_turnover=_pick_numeric(kline, "avg20_turnover", "avg_turnover_20d", "turnover_avg20", "turnover_20d"),
        avg120_turnover=_pick_numeric(kline, "avg120_turnover", "avg_turnover_120d", "turnover_avg120", "turnover_120d"),
        rel_strength_20d=rel_strength_20d,
        rel_strength_60d=rel_strength_60d,
        rebound_from_low_pct=current_vs_low,
        shareholder_concentration_delta=_pick_numeric(kline, "shareholder_concentration_delta"),
        institutional_holding_delta=_pick_numeric(kline, "institutional_holding_delta"),
        buyback_flag=buyback_flag,
        mna_flag=mna_flag,
    )


def _proxy_floor_metrics(
    context: dict[str, Any],
    valuation_summary: dict[str, Any],
) -> tuple[float | None, float | None, float | None]:
    floor_protection = safe_float(valuation_summary.get("floor_protection"))
    normalized_upside = safe_float(valuation_summary.get("normalized_upside"))
    recognition_upside = safe_float(valuation_summary.get("recognition_upside"))
    pb = safe_float(context["pb"])
    current_vs_high = safe_float(context["current_vs_high"])
    primary_type = normalize_text(context["opportunity_context"].get("primary_type")).lower()

    if floor_protection is None or (
        floor_protection is not None
        and floor_protection < 0.3
        and primary_type in {"cyclical", "asset_play", "turnaround"}
        and pb is not None
        and pb <= 1.2
    ):
        if pb is not None and pb <= 0.8:
            floor_protection = max(floor_protection or 0.0, 0.95)
        elif pb is not None and pb <= 1.0:
            floor_protection = max(floor_protection or 0.0, 0.90)
        elif pb is not None and pb <= 1.2:
            floor_protection = max(floor_protection or 0.0, 0.82)

    if normalized_upside is None or (
        normalized_upside is not None
        and normalized_upside < 0.1
        and primary_type in {"cyclical", "asset_play", "turnaround"}
        and pb is not None
        and pb <= 1.2
    ):
        if pb is not None and pb <= 1.0 and current_vs_high is not None and current_vs_high <= 55:
            normalized_upside = max(normalized_upside or 0.0, 0.45)
        elif pb is not None and pb <= 1.2:
            normalized_upside = max(normalized_upside or 0.0, 0.28)

    if recognition_upside is None and normalized_upside is not None:
        recognition_upside = normalized_upside + 0.15
    elif recognition_upside is not None and normalized_upside is not None:
        recognition_upside = max(recognition_upside, normalized_upside)

    return floor_protection, normalized_upside, recognition_upside


def _score_intrinsic_value_floor(
    floor_protection: float | None,
    normalized_upside: float | None,
    recognition_upside: float | None,
) -> float:
    score = 6.0
    if floor_protection is not None:
        if floor_protection >= 1.0:
            score += 8.0
        elif floor_protection >= 0.9:
            score += 6.0
        elif floor_protection >= 0.8:
            score += 4.0
        elif floor_protection >= 0.7:
            score += 2.0
        else:
            score -= 2.0
    if normalized_upside is not None:
        if normalized_upside >= 0.4:
            score += 5.0
        elif normalized_upside >= 0.25:
            score += 3.0
        elif normalized_upside >= 0.1:
            score += 1.5
        elif normalized_upside < 0:
            score -= 3.0
    if recognition_upside is not None and recognition_upside >= 0.5:
        score += 1.0
    return clamp(score, 0.0, VCRF_DIMENSION_MAX["intrinsic_value_floor"])


def _score_business_or_asset_quality(business_quality_score: float) -> float:
    return clamp(
        business_quality_score / max(DIMENSION_MAX["business_quality"], 1.0) * VCRF_DIMENSION_MAX["business_or_asset_quality"],
        0.0,
        VCRF_DIMENSION_MAX["business_or_asset_quality"],
    )


def _score_governance(context: dict[str, Any]) -> float:
    red_flags = context["management"].get("red_flags", []) or []
    score = 3.5 + float(context["management"]["score"]) * 0.7 + (1.0 if context["ownership"].get("is_state_owned") else 0.0)
    score -= min(3.0, len(red_flags) * 1.5)
    return clamp(score, 0.0, VCRF_DIMENSION_MAX["governance_anti_fraud"])


def _score_flow_realization(
    context: dict[str, Any],
    flow_setup: dict[str, Any],
    position_state: str,
) -> float:
    stage_score = {
        "abandoned": 1.0,
        "latent": 4.0,
        "ignition": 8.0,
        "trend": 12.0,
        "crowded": 9.0,
    }.get(flow_setup.get("stage"), 3.0)
    state_bonus = {
        "reject": -2.0,
        "cold_storage": 1.5,
        "ready": 3.0,
        "attack": 4.0,
        "harvest": 0.5,
    }.get(position_state, 0.0)
    elasticity = 0.0
    market_cap = safe_float(context["market_cap"])
    if market_cap is not None:
        if market_cap <= 8e9:
            elasticity = 2.0
        elif market_cap <= 3e10:
            elasticity = 1.2
        elif market_cap <= 1e11:
            elasticity = 0.6
    score = stage_score + state_bonus + elasticity
    return clamp(score, 0.0, VCRF_DIMENSION_MAX["flow_realization_and_elasticity"])


def _status_for_value_floor(score: float) -> str:
    return _status_from_score(score, 14.0, 9.0)


def _status_for_realization(score: float) -> str:
    return _status_from_score(score, 9.0, 5.0)


def _resolve_gate_context(
    stock_code: str,
    scan_data: dict[str, Any],
    *,
    opportunity_context: dict[str, Any] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    resolved_opportunity = opportunity_context or determine_opportunity_type(
        stock_code,
        scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=scan_data.get("revenue_breakdown", {}).get("data", []),
        extra_texts=extra_texts,
    )
    profile = scan_data.get("company_profile", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    revenue_records = scan_data.get("revenue_breakdown", {}).get("data", [])
    income_records = scan_data.get("income_statement", {}).get("data", [])
    balance_records = scan_data.get("balance_sheet", {}).get("data", [])

    controller = normalize_text(profile.get("实际控制人") or profile.get("控股股东"))
    ownership = classify_state_ownership(stock_code, controller, company_name_hints=[normalize_text(profile.get("股票简称"))])
    purity = assess_business_purity(revenue_records)
    moat = assess_moat_quality(profile, revenue_records=revenue_records, extra_texts=extra_texts)
    management = assess_management_quality(profile, ownership, extra_texts=extra_texts)
    catalyst = assess_catalyst_strength(
        normalize_text(profile.get("主营业务")),
        normalize_text(profile.get("经营范围")),
        normalize_text(profile.get("行业")),
        " ".join(extra_texts or []),
    )
    bottom_signal = assess_bottom_pattern(kline, valuation)
    latest_income = get_latest_income_snapshot(income_records)
    latest_balance = get_latest_balance_snapshot(balance_records)
    market_cap = extract_market_cap(quote)
    current_price = extract_latest_price(quote, kline)
    current_vs_high = safe_float(kline.get("current_vs_5yr_high")) or safe_float(kline.get("current_vs_high"))
    pb = safe_float(valuation.get("pb"))
    business_text = normalize_text(profile.get("主营业务") or profile.get("经营范围"))

    return {
        "stock_code": stock_code,
        "scan_data": scan_data,
        "opportunity_context": resolved_opportunity,
        "profile": profile,
        "quote": quote,
        "valuation": valuation,
        "kline": kline,
        "revenue_records": revenue_records,
        "income_records": income_records,
        "balance_records": balance_records,
        "ownership": ownership,
        "purity": purity,
        "moat": moat,
        "management": management,
        "catalyst": catalyst,
        "bottom_pattern": bottom_signal,
        "latest_income": latest_income,
        "latest_balance": latest_balance,
        "market_cap": market_cap,
        "current_price": current_price,
        "current_vs_high": current_vs_high,
        "pb": pb,
        "business_text": business_text,
    }


def _type_clarity_score(opportunity_context: dict[str, Any]) -> float:
    return {"high": 5.0, "medium": 4.0, "low": 2.0}.get(opportunity_context.get("confidence"), 1.0)


def _business_quality_score(context: dict[str, Any]) -> float:
    return clamp(
        (6.0 if context["business_text"] else 2.0)
        + min(8.0, context["purity"].get("top_ratio", 0.0) / 10.0)
        + context["moat"]["score"] * 0.6,
        0.0,
        DIMENSION_MAX["business_quality"],
    )


def _survival_score(context: dict[str, Any]) -> float:
    survival_score = 5.0
    if context["latest_balance"].get("total_equity") not in (None, 0):
        survival_score += 4.0 if context["latest_balance"]["total_equity"] > 0 else -4.0
    if context["latest_income"].get("net_profit") is not None:
        survival_score += 4.0 if context["latest_income"]["net_profit"] > 0 else 1.0
    if context["market_cap"] is not None and context["market_cap"] >= 2e9:
        survival_score += 2.0
    return clamp(survival_score, 0.0, DIMENSION_MAX["survival"])


def _regime_cycle_score(context: dict[str, Any]) -> float:
    return clamp(
        6.0
        + context["bottom_pattern"]["score"]
        + (
            2.0
            if context["opportunity_context"].get("primary_type") == "cyclical"
            and context["bottom_pattern"]["signal"] == "favorable"
            else 0.0
        ),
        0.0,
        DIMENSION_MAX["regime_cycle"],
    )


def _market_structure_score(context: dict[str, Any]) -> float:
    score = 3.0
    if context["market_cap"] is None:
        score = 2.0
    elif context["market_cap"] < 5e8:
        score = 1.0
    elif context["market_cap"] <= 2e12:
        score = 4.0
    if context["current_price"] is not None:
        score += 1.0
    return clamp(score, 0.0, DIMENSION_MAX["market_structure"])


def _present_fields(scan_data: dict[str, Any], fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(field for field in fields if field in scan_data)


def _dimension_confidence(scan_data: dict[str, Any], fields: tuple[str, ...]) -> tuple[str, list[str]]:
    if not fields:
        return "full", []
    present = _present_fields(scan_data, fields)
    missing = [field for field in fields if field not in present]
    if len(present) == len(fields):
        return "full", []
    if not present:
        return "none", missing
    return "partial", missing


def _dimension_payload(
    score: float,
    *,
    max_score: float,
    confidence: str,
    requires: list[str] | None = None,
    reason: str,
) -> dict[str, Any]:
    return {
        "score": round(score, 2),
        "max": max_score,
        "confidence": confidence,
        "requires": list(requires or []),
        "reason": reason,
    }


def evaluate_partial_gate_dimensions(
    stock_code: str,
    scan_data: dict[str, Any],
    *,
    opportunity_context: dict[str, Any] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    context = _resolve_gate_context(
        stock_code,
        scan_data,
        opportunity_context=opportunity_context,
        extra_texts=extra_texts,
    )

    type_clarity_score = _type_clarity_score(context["opportunity_context"])
    business_quality_score = _business_quality_score(context)
    management_score = float(context["management"]["score"])
    regime_cycle_score = _regime_cycle_score(context)
    valuation_score, valuation_reason = _valuation_score(
        context["opportunity_context"].get("primary_type", "unknown"),
        context["pb"],
        context["current_vs_high"],
    )
    catalyst_score = float(context["catalyst"]["score"])
    market_structure_score = _market_structure_score(context)

    survival_confidence, survival_requires = _dimension_confidence(scan_data, ("income_statement", "balance_sheet"))
    survival_score = _survival_score(context) if survival_confidence == "full" else 0.0

    dimensions = {
        "type_clarity": _dimension_payload(
            type_clarity_score,
            max_score=DIMENSION_MAX["type_clarity"],
            confidence="full",
            reason=f"type confidence={context['opportunity_context'].get('confidence', 'unknown')}",
        ),
        "business_quality": _dimension_payload(
            business_quality_score,
            max_score=DIMENSION_MAX["business_quality"],
            confidence="full",
            reason=f"top segment={context['purity'].get('top_segment') or 'N/A'}, moat={context['moat']['reason']}",
        ),
        "survival": _dimension_payload(
            survival_score,
            max_score=DIMENSION_MAX["survival"],
            confidence=survival_confidence,
            requires=survival_requires,
            reason=(
                f"net profit={context['latest_income'].get('net_profit')}, equity={context['latest_balance'].get('total_equity')}"
                if survival_confidence == "full"
                else "survival cannot be scored without income_statement and balance_sheet"
            ),
        ),
        "management": _dimension_payload(
            management_score,
            max_score=DIMENSION_MAX["management"],
            confidence="full",
            reason=f"management={context['management']['verdict']}",
        ),
        "regime_cycle": _dimension_payload(
            regime_cycle_score,
            max_score=DIMENSION_MAX["regime_cycle"],
            confidence="full",
            reason=context["bottom_pattern"]["reason"],
        ),
        "valuation": _dimension_payload(
            valuation_score,
            max_score=DIMENSION_MAX["valuation"],
            confidence="full",
            reason=valuation_reason,
        ),
        "catalyst": _dimension_payload(
            catalyst_score,
            max_score=DIMENSION_MAX["catalyst"],
            confidence="full",
            reason=context["catalyst"]["reason"],
        ),
        "market_structure": _dimension_payload(
            _market_structure_score(context),
            max_score=DIMENSION_MAX["market_structure"],
            confidence="full",
            reason=f"market_cap={context['market_cap']}, current_price={context['current_price']}",
        ),
    }

    decidable_hard_vetos: list[str] = []
    blocked_hard_vetos: list[str] = []

    if not context["business_text"] and not context["revenue_records"]:
        decidable_hard_vetos.append("business is not understandable")
    if context["management"]["score"] <= 2 and context["management"]["red_flags"]:
        decidable_hard_vetos.append("management credibility is materially impaired")

    if "income_statement" not in scan_data or "balance_sheet" not in scan_data:
        blocked_hard_vetos.append("normal earning power cannot be estimated")
    elif context["latest_income"].get("net_profit") is None and context["latest_balance"].get("total_equity") is None:
        decidable_hard_vetos.append("normal earning power cannot be estimated")

    if "balance_sheet" not in scan_data:
        blocked_hard_vetos.append("balance sheet survival is questionable")
    elif context["latest_balance"].get("total_equity") is not None and context["latest_balance"].get("total_equity") <= 0:
        decidable_hard_vetos.append("balance sheet survival is questionable")

    known_total = round(sum(item["score"] for item in dimensions.values()), 2)
    unknown_ceiling = round(
        sum((item["max"] - item["score"]) for item in dimensions.values() if item["confidence"] != "full"),
        2,
    )
    score_upper_bound = round(known_total + unknown_ceiling, 2)

    return {
        "stock_code": stock_code,
        "opportunity_context": context["opportunity_context"],
        "dimensions": dimensions,
        "known_total": known_total,
        "unknown_ceiling": unknown_ceiling,
        "score_upper_bound": score_upper_bound,
        "decidable_hard_vetos": decidable_hard_vetos,
        "blocked_hard_vetos": blocked_hard_vetos,
    }


def evaluate_universal_gates(
    stock_code: str,
    scan_data: dict[str, Any],
    *,
    opportunity_context: dict[str, Any] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    context = _resolve_gate_context(
        stock_code,
        scan_data,
        opportunity_context=opportunity_context,
        extra_texts=extra_texts,
    )

    hard_vetos: list[str] = []
    if not context["business_text"] and not context["revenue_records"]:
        hard_vetos.append("business is not understandable")
    if context["latest_income"].get("net_profit") is None and context["latest_balance"].get("total_equity") is None:
        hard_vetos.append("normal earning power cannot be estimated")
    if context["latest_balance"].get("total_equity") is not None and context["latest_balance"].get("total_equity") <= 0:
        hard_vetos.append("balance sheet survival is questionable")
    if context["management"]["score"] <= 2 and context["management"]["red_flags"]:
        hard_vetos.append("management credibility is materially impaired")

    valuation_result = build_three_case_valuation(stock_code, scan_data, context["opportunity_context"])
    floor_protection, normalized_upside, recognition_upside = _proxy_floor_metrics(context, valuation_result.get("summary", {}))
    flow_setup = score_flow_setup(_infer_flow_inputs(context))
    repair_state = _infer_repair_state(context)
    position_state = classify_position_state(
        floor_protection=floor_protection,
        normalized_upside=normalized_upside,
        recognition_upside=recognition_upside,
        repair_state=repair_state,
        flow_stage=flow_setup.get("stage", "latent"),
    )

    type_clarity_score = _type_clarity_score(context["opportunity_context"])
    business_quality_score = _business_quality_score(context)
    survival_score = _survival_score(context)
    management_score = float(context["management"]["score"])
    regime_cycle_score = _regime_cycle_score(context)
    valuation_score, valuation_reason = _valuation_score(
        context["opportunity_context"].get("primary_type", "unknown"),
        context["pb"],
        context["current_vs_high"],
    )
    catalyst_score = float(context["catalyst"]["score"])
    market_structure_score = _market_structure_score(context)

    intrinsic_value_floor_score = _score_intrinsic_value_floor(
        floor_protection,
        normalized_upside,
        recognition_upside,
    )
    governance_score = _score_governance(context)
    business_or_asset_quality_score = _score_business_or_asset_quality(business_quality_score)
    flow_realization_score = _score_flow_realization(context, flow_setup, position_state)

    legacy_total = round(
        type_clarity_score
        + business_quality_score
        + survival_score
        + management_score
        + regime_cycle_score
        + valuation_score
        + catalyst_score
        + market_structure_score,
        2,
    )
    template = _load_weight_template(context["opportunity_context"].get("primary_type", ""))
    weighted_components = {
        "intrinsic_value_floor": round(
            intrinsic_value_floor_score / max(VCRF_DIMENSION_MAX["intrinsic_value_floor"], 1.0) * template.get("intrinsic_value_floor", 0.0),
            2,
        ),
        "survival_boundary": round(
            survival_score / max(VCRF_DIMENSION_MAX["survival_boundary"], 1.0) * template.get("survival_boundary", 0.0),
            2,
        ),
        "governance_anti_fraud": round(
            governance_score / max(VCRF_DIMENSION_MAX["governance_anti_fraud"], 1.0) * template.get("governance_anti_fraud", 0.0),
            2,
        ),
        "business_or_asset_quality": round(
            business_or_asset_quality_score / max(VCRF_DIMENSION_MAX["business_or_asset_quality"], 1.0) * template.get("business_or_asset_quality", 0.0),
            2,
        ),
        "regime_cycle_position": round(
            regime_cycle_score / max(VCRF_DIMENSION_MAX["regime_cycle_position"], 1.0) * template.get("regime_cycle_position", 0.0),
            2,
        ),
        "turnaround_catalyst": round(
            catalyst_score / max(VCRF_DIMENSION_MAX["turnaround_catalyst"], 1.0) * template.get("turnaround_catalyst", 0.0),
            2,
        ),
        "flow_realization_and_elasticity": round(
            flow_realization_score / max(VCRF_DIMENSION_MAX["flow_realization_and_elasticity"], 1.0) * template.get("flow_realization_and_elasticity", 0.0),
            2,
        ),
    }
    total_score = round(type_clarity_score + sum(weighted_components.values()), 2)
    reported_total = round(max(total_score, legacy_total), 2)
    verdict = pick_score_verdict(total_score)
    if hard_vetos:
        verdict = {"label": "reject / no action", "action": "hard veto triggered"}

    business_or_asset_truth = {
            "status": _status_from_score(business_quality_score, 14.0, 9.0),
            "reason": f"top segment={context['purity'].get('top_segment') or 'N/A'}, moat={context['moat']['reason']}",
    }
    survival_truth = {
            "status": _status_from_score(survival_score, 11.0, 7.0),
            "reason": f"net profit={context['latest_income'].get('net_profit')}, equity={context['latest_balance'].get('total_equity')}",
    }
    governance_truth = {
            "status": _status_from_score(governance_score, 7.0, 4.0),
            "reason": f"management={context['management']['verdict']}, red_flags={', '.join(context['management'].get('red_flags', [])) or 'none surfaced'}",
    }
    regime_cycle_truth = {
            "status": _status_from_score(regime_cycle_score, 10.0, 6.0),
            "reason": context["bottom_pattern"]["reason"],
    }
    valuation_floor_truth = {
            "status": _status_for_value_floor(intrinsic_value_floor_score),
            "reason": (
                f"floor_protection={floor_protection}, normalized_upside={normalized_upside}, "
                f"recognition_upside={recognition_upside}"
            ),
    }
    realization_truth = {
            "status": _status_for_realization(flow_realization_score),
            "reason": (
                f"flow_stage={flow_setup.get('stage')}, position_state={position_state}, "
                f"flow_reason={flow_setup.get('reason')}"
            ),
    }
    gates = {
        "business_or_asset_truth": business_or_asset_truth,
        "survival_truth": survival_truth,
        "governance_truth": governance_truth,
        "regime_cycle_truth": regime_cycle_truth,
        "valuation_floor_truth": valuation_floor_truth,
        "realization_truth": realization_truth,
        "business_truth": business_or_asset_truth,
        "quality_truth": governance_truth,
        "valuation_truth": valuation_floor_truth,
        "catalyst_truth": realization_truth,
    }

    return {
        "stock_code": stock_code,
        "opportunity_context": context["opportunity_context"],
        "ownership": context["ownership"],
        "hard_vetos": hard_vetos,
        "repair_state": repair_state,
        "flow_stage": flow_setup.get("stage", "latent"),
        "position_state": position_state,
        "gates": gates,
        "signals": {
            "purity": context["purity"],
            "moat": context["moat"],
            "management": context["management"],
            "catalyst": context["catalyst"],
            "bottom_pattern": context["bottom_pattern"],
            "flow": flow_setup,
            "valuation_summary": {
                **(valuation_result.get("summary", {}) or {}),
                "floor_protection": floor_protection,
                "normalized_upside": normalized_upside,
                "recognition_upside": recognition_upside,
            },
        },
        "scorecard": {
            "type_clarity": round(type_clarity_score, 2),
            "business_quality": round(business_quality_score, 2),
            "survival": round(survival_score, 2),
            "management": round(management_score, 2),
            "regime_cycle": round(regime_cycle_score, 2),
            "valuation": round(valuation_score, 2),
            "catalyst": round(catalyst_score, 2),
            "market_structure": round(market_structure_score, 2),
            "total": reported_total,
            "legacy_total": legacy_total,
            "verdict": _legacy_verdict_from_total(reported_total, hard_vetos),
            "action": verdict["action"],
            "vcrf_total": total_score,
            "vcrf_verdict": verdict["label"],
            "vcrf_dimensions": {
                "thesis_clarity": round(type_clarity_score, 2),
                "intrinsic_value_floor": round(intrinsic_value_floor_score, 2),
                "survival_boundary": round(survival_score, 2),
                "governance_anti_fraud": round(governance_score, 2),
                "business_or_asset_quality": round(business_or_asset_quality_score, 2),
                "regime_cycle_position": round(regime_cycle_score, 2),
                "turnaround_catalyst": round(catalyst_score, 2),
                "flow_realization_and_elasticity": round(flow_realization_score, 2),
                "weighted": weighted_components,
                "template": context["opportunity_context"].get("primary_type"),
            },
        },
    }
