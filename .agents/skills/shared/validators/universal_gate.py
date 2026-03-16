"""Universal six-gate evaluator for the new investment framework."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_scoring_rules, load_vcrf_degradation
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
_STATE_ORDER = {
    "REJECT": 0,
    "COLD_STORAGE": 1,
    "READY": 2,
    "ATTACK": 3,
    "HARVEST": 4,
}


def _status_from_score(score: float, passing: float, caution: float) -> str:
    if score >= passing:
        return "pass"
    if score >= caution:
        return "caution"
    return "fail"


def _apply_degradation_caps(proposed_state: str, component_availability: dict[str, str]) -> str:
    degradation = load_vcrf_degradation().get("degradation_rules", {})
    current_state = str(proposed_state or "REJECT").upper()
    current_rank = _STATE_ORDER.get(current_state, 0)
    for component, availability in component_availability.items():
        if str(availability).lower() != "missing":
            continue
        rule = (degradation.get("underwrite", {}) or {}).get(component, {})
        cap_state = str(rule.get("cap_state", "")).upper()
        if cap_state and _STATE_ORDER.get(cap_state, current_rank) < current_rank:
            current_state = cap_state
            current_rank = _STATE_ORDER[cap_state]
    return current_state


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

    total_score = round(
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
    verdict = pick_score_verdict(total_score)
    if hard_vetos:
        verdict = {"label": "reject / no action", "action": "hard veto triggered"}

    gates = {
        "business_truth": {
            "status": _status_from_score(business_quality_score, 14.0, 9.0),
            "reason": f"top segment={context['purity'].get('top_segment') or 'N/A'}, moat={context['moat']['reason']}",
        },
        "survival_truth": {
            "status": _status_from_score(survival_score, 11.0, 7.0),
            "reason": f"net profit={context['latest_income'].get('net_profit')}, equity={context['latest_balance'].get('total_equity')}",
        },
        "quality_truth": {
            "status": _status_from_score(management_score + context['moat']['score'], 14.0, 9.0),
            "reason": f"management={context['management']['verdict']}, moat={context['moat']['verdict']}",
        },
        "regime_cycle_truth": {
            "status": _status_from_score(regime_cycle_score, 10.0, 6.0),
            "reason": context["bottom_pattern"]["reason"],
        },
        "valuation_truth": {
            "status": _status_from_score(valuation_score, 14.0, 9.0),
            "reason": valuation_reason,
        },
        "catalyst_truth": {
            "status": _status_from_score(catalyst_score, 6.0, 3.0),
            "reason": context["catalyst"]["reason"],
        },
    }

    return {
        "stock_code": stock_code,
        "opportunity_context": context["opportunity_context"],
        "ownership": context["ownership"],
        "hard_vetos": hard_vetos,
        "gates": gates,
        "signals": {
            "purity": context["purity"],
            "moat": context["moat"],
            "management": context["management"],
            "catalyst": context["catalyst"],
            "bottom_pattern": context["bottom_pattern"],
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
            "total": total_score,
            "verdict": verdict["label"],
            "action": verdict["action"],
        },
    }
