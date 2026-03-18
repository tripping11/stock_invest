"""Universal six-gate evaluator for the new investment framework."""
from __future__ import annotations

import logging
from typing import Any

from engines.flow_realization_engine import score_realization_axis
from engines.state_transition_tracker import enforce_transition
from engines.valuation_engine import build_three_case_valuation
from utils.config_loader import load_scoring_rules, load_vcrf_degradation, load_vcrf_state_machine
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
from utils.primary_type_router import build_driver_stack
from utils.score_verdict import pick_score_verdict
from utils.vcrf_probes import assess_survival_boundary, score_underwrite_axis
from utils.vcrf_state_utils import classify_vcrf_position_state
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

logger = logging.getLogger(__name__)


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


def _valuation_score(context: dict[str, Any]) -> tuple[float, str]:
    driver_stack = context.get("driver_stack")
    if not driver_stack:
        logger.warning("valuation bridge fallback for %s: missing driver_stack", context.get("stock_code"))
        return 8.0, "valuation bridge unavailable: missing driver_stack"

    valuation_result = context.get("_valuation_result")
    if valuation_result is None:
        valuation_result = build_three_case_valuation(context["stock_code"], context["scan_data"], driver_stack)
        context["_valuation_result"] = valuation_result
    valuation_summary = (valuation_result or {}).get("summary", {}) or {}
    floor_protection = safe_float(valuation_summary.get("floor_protection"))
    if floor_protection is None:
        logger.warning("valuation bridge fallback for %s: missing floor_protection", context.get("stock_code"))
        return 8.0, "valuation bridge unavailable: missing floor_protection"
    if floor_protection >= 1.00:
        score = 20.0
    elif floor_protection >= 0.90:
        score = 16.0
    elif floor_protection >= 0.80:
        score = 12.0
    elif floor_protection >= 0.70:
        score = 8.0
    else:
        score = 4.0
    return clamp(score, 0.0, DIMENSION_MAX["valuation"]), f"floor_protection={floor_protection:.2f}"


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
    driver_stack = build_driver_stack(stock_code, scan_data, extra_texts=extra_texts)
    merged_opportunity = _merge_opportunity_context(resolved_opportunity, driver_stack)
    survival_probe = assess_survival_boundary(scan_data, driver_stack)

    return {
        "stock_code": stock_code,
        "scan_data": scan_data,
        "opportunity_context": merged_opportunity,
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
        "driver_stack": driver_stack,
        "_survival_probe": survival_probe,
        "_valuation_result": None,
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
    driver_stack = context.get("driver_stack")
    if not driver_stack:
        logger.warning("survival bridge fallback for %s: missing driver_stack", context.get("stock_code"))
        return 5.0
    survival_probe = context.get("_survival_probe") or assess_survival_boundary(context["scan_data"], driver_stack)
    raw_score = safe_float((survival_probe or {}).get("score"))
    if raw_score is None:
        logger.warning("survival bridge fallback for %s: missing raw survival score", context.get("stock_code"))
        return 5.0
    return clamp(raw_score / 100.0 * DIMENSION_MAX["survival"], 0.0, DIMENSION_MAX["survival"])


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


def _component_gate(component: dict[str, Any], *, passing: float, caution: float) -> dict[str, Any]:
    score = safe_float(component.get("score")) or 0.0
    return {
        "status": _status_from_score(score, passing, caution),
        "reason": component.get("reason", ""),
        "score": round(score, 2),
        "availability": component.get("availability", "unknown"),
        "confidence": component.get("confidence", "unknown"),
    }


def _merge_opportunity_context(legacy_context: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    merged = dict(legacy_context or {})
    primary_type = normalize_text(driver_stack.get("primary_type")).lower()
    merged["primary_type"] = primary_type or merged.get("primary_type", "unknown")
    merged["sector_route"] = normalize_text(driver_stack.get("sector_route")).lower()
    merged["primary_type_confidence"] = driver_stack.get("primary_type_confidence")
    if not merged.get("primary_label"):
        merged["primary_label"] = (primary_type or "unknown").replace("_", " ").title()
    return merged


def _classify_vcrf_state(
    underwrite_score: float,
    realization_score: float,
    *,
    flow_stage: str,
    valuation_summary: dict[str, Any],
) -> str:
    return classify_vcrf_position_state(
        underwrite_score,
        realization_score,
        flow_stage=flow_stage,
        valuation_summary=valuation_summary,
    )


def _legacy_scorecard_alias(
    *,
    context: dict[str, Any],
    hard_vetos: list[str],
) -> dict[str, Any]:
    type_clarity_score = _type_clarity_score(context["opportunity_context"])
    business_quality_score = _business_quality_score(context)
    survival_score = _survival_score(context)
    management_score = float(context["management"]["score"])
    regime_cycle_score = _regime_cycle_score(context)
    valuation_score, _ = _valuation_score(context)
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
    legacy_label = {
        "high conviction / attack candidate": "high conviction / strong candidate",
        "strong candidate / ready": "reasonable candidate / starter possible",
        "cold-storage / watchlist": "watch / needs work",
    }.get(verdict["label"], verdict["label"])
    return {
        "type_clarity": round(type_clarity_score, 2),
        "business_quality": round(business_quality_score, 2),
        "survival": round(survival_score, 2),
        "management": round(management_score, 2),
        "regime_cycle": round(regime_cycle_score, 2),
        "valuation": round(valuation_score, 2),
        "catalyst": round(catalyst_score, 2),
        "market_structure": round(market_structure_score, 2),
        "total": total_score,
        "verdict": legacy_label,
        "action": verdict["action"],
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
    valuation_score, valuation_reason = _valuation_score(context)
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
    elif bool((context.get("_survival_probe") or {}).get("tripwire_reject")):
        decidable_hard_vetos.append("debt wall coverage fails survival tripwire")

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
    prior_state: str | None = None,
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
    if bool((context.get("_survival_probe") or {}).get("tripwire_reject")):
        hard_vetos.append("debt wall coverage fails survival tripwire")
    if context["management"]["score"] <= 2 and context["management"]["red_flags"]:
        hard_vetos.append("management credibility is materially impaired")

    driver_stack = context["driver_stack"]
    merged_opportunity = context["opportunity_context"]

    underwrite_axis = score_underwrite_axis(scan_data, driver_stack)
    initial_realization_axis = score_realization_axis(scan_data, driver_stack)
    driver_stack.setdefault("modifiers", {})["flow_stage"] = initial_realization_axis.get("flow_stage", "latent")
    realization_axis = score_realization_axis(scan_data, driver_stack)
    flow_stage = normalize_text(realization_axis.get("flow_stage") or driver_stack.get("modifiers", {}).get("flow_stage") or "latent").lower()
    driver_stack["modifiers"]["flow_stage"] = flow_stage

    valuation_result = context.get("_valuation_result")
    if valuation_result is None:
        valuation_result = build_three_case_valuation(stock_code, scan_data, driver_stack)
        context["_valuation_result"] = valuation_result
    valuation_summary = valuation_result.get("summary", {}) or {}
    proposed_state = _classify_vcrf_state(
        underwrite_axis.get("score", 0.0),
        realization_axis.get("score", 0.0),
        flow_stage=flow_stage,
        valuation_summary=valuation_summary,
    )
    component_availability = {
        name: component.get("availability", "missing")
        for name, component in (underwrite_axis.get("components", {}) or {}).items()
    }
    capped_state = _apply_degradation_caps(proposed_state, component_availability)
    prev_state = str(prior_state or "NEW").upper()
    enforced_state, transition_allowed, transition_reason = enforce_transition(
        prev_state,
        capped_state,
        cfg=load_vcrf_state_machine(),
    )
    position_state = normalize_text(enforced_state).lower()

    legacy_scorecard = _legacy_scorecard_alias(context=context, hard_vetos=hard_vetos)
    gates = {
        "business_or_asset_truth": _component_gate(
            underwrite_axis["components"]["business_or_asset_quality"],
            passing=70.0,
            caution=50.0,
        ),
        "governance_truth": _component_gate(
            underwrite_axis["components"]["governance_anti_fraud"],
            passing=75.0,
            caution=50.0,
        ),
        "valuation_floor_truth": _component_gate(
            underwrite_axis["components"]["intrinsic_value_floor"],
            passing=65.0,
            caution=45.0,
        ),
        "survival_truth": _component_gate(
            underwrite_axis["components"]["survival_boundary"],
            passing=60.0,
            caution=40.0,
        ),
        "realization_truth": {
            "status": _status_from_score(realization_axis.get("score", 0.0), 70.0, 40.0),
            "reason": f"flow_stage={flow_stage}; {realization_axis['components']['flow_confirmation'].get('reason', '')}",
            "score": round(realization_axis.get("score", 0.0), 2),
            "availability": realization_axis.get("confidence", "unknown"),
            "confidence": realization_axis.get("confidence", "unknown"),
        },
        "regime_cycle_truth": _component_gate(
            realization_axis["components"]["regime_cycle_position"],
            passing=70.0,
            caution=45.0,
        ),
        "catalyst_truth": _component_gate(
            realization_axis["components"]["catalyst_quality"],
            passing=65.0,
            caution=45.0,
        ),
    }
    gates["business_truth"] = dict(gates["business_or_asset_truth"])
    gates["quality_truth"] = dict(gates["governance_truth"])
    gates["valuation_truth"] = dict(gates["valuation_floor_truth"])

    return {
        "stock_code": stock_code,
        "opportunity_context": merged_opportunity,
        "driver_stack": driver_stack,
        "underwrite_axis": underwrite_axis,
        "realization_axis": realization_axis,
        "ownership": context["ownership"],
        "hard_vetos": hard_vetos,
        "position_state": position_state,
        "prev_state": prev_state,
        "transition_allowed": transition_allowed,
        "transition_reason": transition_reason,
        "flow_stage": flow_stage,
        "floor_protection": valuation_summary.get("floor_protection"),
        "normalized_upside": valuation_summary.get("normalized_upside"),
        "recognition_upside": valuation_summary.get("recognition_upside"),
        "wind_dependency": valuation_summary.get("wind_dependency"),
        "gates": gates,
        "signals": {
            "purity": context["purity"],
            "moat": context["moat"],
            "management": context["management"],
            "catalyst": context["catalyst"],
            "bottom_pattern": context["bottom_pattern"],
            "flow_confirmation": realization_axis["components"]["flow_confirmation"],
        },
        "scorecard": legacy_scorecard,
    }
