"""Realization-axis scoring and flow-stage helpers for VCRF."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from utils.config_loader import load_scoring_rules, load_vcrf_state_machine, resolve_vcrf_weight_template
from utils.financial_snapshot import extract_float_market_cap, extract_latest_price, extract_market_cap
from utils.vcrf_state_utils import classify_vcrf_position_state
from utils.value_utils import clamp, normalize_text, safe_float


FLOW_STAGE_ORDER = {
    str(name): int(rank)
    for name, rank in (load_vcrf_state_machine().get("flow_stage_order", {}) or {}).items()
}
ELASTICITY_MODEL = (load_scoring_rules().get("elasticity_model", {}) or {})

REPAIR_STATE_SCORE = {
    "none": 20,
    "stabilizing": 45,
    "repairing": 70,
    "confirmed": 90,
}

_CYCLE_BASE_SCORE = {
    "trough": 78,
    "repair": 65,
    "expansion": 52,
    "peak": 22,
}

_ELASTICITY_BUCKET_SCORE = {
    "micro": 95,
    "small": 80,
    "mid": 60,
    "large": 35,
    "mega": 20,
}

_CATALYST_PATH_SCORE = {
    "asset_unlock": 85,
    "mna": 80,
    "buyback": 70,
    "capital_return": 65,
    "policy": 60,
    "institutional_entry": 55,
    "repricing": 50,
}

_REALIZATION_REDISTRIBUTION_TARGETS = (
    "regime_cycle_position",
    "flow_confirmation",
)
_REALIZATION_NEUTRAL_DROP_COMPONENTS = (
    "marginal_buyer_probability",
    "catalyst_quality",
)


@dataclass(slots=True)
class FlowInputs:
    current_price: float | None
    avg20_turnover: float | None = None
    avg120_turnover: float | None = None
    rel_strength_20d: float | None = None
    rel_strength_60d: float | None = None
    rebound_from_low_pct: float | None = None
    shareholder_concentration_delta: float | None = None
    institutional_holding_delta: float | None = None
    buyback_flag: bool = False
    mna_flag: bool = False


def _component_payload(
    score: float,
    *,
    availability: str,
    confidence: str,
    reason: str,
    inputs_used: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "score": round(clamp(score, 0.0, 100.0), 2),
        "availability": availability,
        "confidence": confidence,
        "reason": reason,
        "inputs_used": inputs_used,
    }
    if extra:
        payload.update(extra)
    return payload


def _linear_interpolate(value: float, *, left: float, right: float, left_score: float, right_score: float) -> float:
    if right <= left:
        return left_score
    ratio = (value - left) / (right - left)
    return left_score + ratio * (right_score - left_score)


def _continuous_float_cap_score(float_market_cap: float | None, fallback_bucket: str) -> float:
    if float_market_cap is None:
        return float(_ELASTICITY_BUCKET_SCORE.get(fallback_bucket, 60))

    curve = ELASTICITY_MODEL.get("free_float_cap_curve", {}) or {}
    low_cutoff = float(curve.get("low_cutoff", 5_000_000_000))
    mid_cutoff = float(curve.get("mid_cutoff", 10_000_000_000))
    upper_mid_cutoff = float(curve.get("upper_mid_cutoff", 20_000_000_000))
    high_cutoff = float(curve.get("high_cutoff", 30_000_000_000))
    low_score = float(curve.get("low_score", 100))
    mid_score = float(curve.get("mid_score", 80))
    upper_mid_score = float(curve.get("upper_mid_score", 50))
    high_score = float(curve.get("high_score", 10))

    if float_market_cap <= low_cutoff:
        return low_score
    if float_market_cap <= mid_cutoff:
        return _linear_interpolate(
            float_market_cap,
            left=low_cutoff,
            right=mid_cutoff,
            left_score=low_score,
            right_score=mid_score,
        )
    if float_market_cap <= upper_mid_cutoff:
        return _linear_interpolate(
            float_market_cap,
            left=mid_cutoff,
            right=upper_mid_cutoff,
            left_score=mid_score,
            right_score=upper_mid_score,
        )
    if float_market_cap <= high_cutoff:
        return _linear_interpolate(
            float_market_cap,
            left=upper_mid_cutoff,
            right=high_cutoff,
            left_score=upper_mid_score,
            right_score=high_score,
        )
    return high_score


def score_flow_setup(inputs: FlowInputs) -> dict[str, Any]:
    volume_ratio = None
    if inputs.avg20_turnover not in (None, 0) and inputs.avg120_turnover not in (None, 0):
        volume_ratio = inputs.avg20_turnover / inputs.avg120_turnover

    positive_signals = 0
    if volume_ratio is not None and volume_ratio >= 1.1:
        positive_signals += 1
    if (inputs.rel_strength_20d or 0.0) >= 0.05 or (inputs.rel_strength_60d or 0.0) >= 0.08:
        positive_signals += 1
    if (inputs.rebound_from_low_pct or 0.0) >= 0.10:
        positive_signals += 1
    if (inputs.shareholder_concentration_delta or 0.0) > 0:
        positive_signals += 1
    if (inputs.institutional_holding_delta or 0.0) > 0:
        positive_signals += 1
    if inputs.buyback_flag or inputs.mna_flag:
        positive_signals += 1

    if positive_signals == 0 and volume_ratio is None:
        stage = "latent"
    elif positive_signals <= 1:
        stage = "latent"
    elif positive_signals <= 3:
        stage = "ignition"
    elif positive_signals <= 5:
        stage = "trend"
    else:
        stage = "crowded"

    return {
        "stage": stage,
        "score": FLOW_STAGE_ORDER.get(stage, 0) * 20,
        "volume_ratio": volume_ratio,
        "positive_signals": positive_signals,
    }


def classify_position_state(
    *,
    floor_protection: float | None,
    normalized_upside: float | None,
    recognition_upside: float | None,
    repair_state: str,
    flow_stage: str,
) -> str:
    flow_rank = FLOW_STAGE_ORDER.get(flow_stage, 0)
    repair_rank = {"none": 0, "stabilizing": 1, "repairing": 2, "confirmed": 3}.get(repair_state, 0)
    if floor_protection is None or floor_protection < 0.75:
        underwrite_score = 0.0
    else:
        underwrite_score = 80.0

    realization_score = 20.0
    if flow_rank >= FLOW_STAGE_ORDER.get("trend", 3) and repair_rank >= 2 and (recognition_upside or 0.0) >= 0.30:
        realization_score = 75.0
    elif flow_rank >= FLOW_STAGE_ORDER.get("ignition", 2) and repair_rank >= 1 and (normalized_upside or 0.0) >= 0.25:
        realization_score = 50.0

    state = classify_vcrf_position_state(
        underwrite_score,
        realization_score,
        flow_stage=flow_stage,
        valuation_summary={
            "floor_protection": floor_protection,
            "normalized_upside": normalized_upside,
            "recognition_upside": recognition_upside,
        },
    )
    return normalize_text(state).lower()


def score_repair_state(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    repair_state = normalize_text(driver_stack.get("modifiers", {}).get("repair_state") or "none").lower() or "none"
    score = REPAIR_STATE_SCORE.get(repair_state, 20)
    big_bath_verdict = normalize_text(driver_stack.get("big_bath_result", {}).get("verdict")).lower()
    if big_bath_verdict == "big_bath":
        score += 5
    if driver_stack.get("repair_evidence"):
        score += 5
    return _component_payload(
        score,
        availability="full",
        confidence="partial",
        reason=f"repair_state={repair_state}, big_bath={big_bath_verdict or 'n/a'}",
        inputs_used={"repair_state": repair_state, "big_bath_verdict": big_bath_verdict},
        extra={"repair_state": repair_state},
    )


def score_regime_cycle_position(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    cycle_state = normalize_text(driver_stack.get("modifiers", {}).get("cycle_state") or "repair").lower() or "repair"
    kline = scan_data.get("stock_kline", {}).get("data", {})
    current_vs_high = safe_float(kline.get("current_vs_5yr_high") or kline.get("current_vs_high"))
    drawdown_score = 20.0
    if current_vs_high is not None:
        if current_vs_high <= 50:
            drawdown_score = 85.0
        elif current_vs_high <= 75:
            drawdown_score = 65.0
        elif current_vs_high >= 90:
            drawdown_score = 20.0
        else:
            drawdown_score = 45.0
    route_cycle_score = float(_CYCLE_BASE_SCORE.get(cycle_state, 55))
    score = clamp(route_cycle_score * 0.6 + drawdown_score * 0.4, 0, 100)
    return _component_payload(
        score,
        availability="partial" if current_vs_high is None else "full",
        confidence="partial",
        reason=f"cycle_state={cycle_state}, current_vs_high={current_vs_high}",
        inputs_used={"cycle_state": cycle_state, "current_vs_high": current_vs_high},
    )


def score_marginal_buyer_probability(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    shareholder_records = scan_data.get("shareholder_count", {}).get("data", []) or []
    trend_score = 50.0
    neutral_default = True
    if len(shareholder_records) >= 2:
        latest = shareholder_records[-1]
        previous = shareholder_records[-2]
        latest_count = safe_float(latest.get("股东户数") or latest.get("股东人数") or latest.get("户数"))
        previous_count = safe_float(previous.get("股东户数") or previous.get("股东人数") or previous.get("户数"))
        if latest_count is not None and previous_count not in (None, 0):
            delta = (latest_count - previous_count) / previous_count
            neutral_default = False
            if delta <= -0.10:
                trend_score = 80.0
            elif delta < 0:
                trend_score = 65.0
            elif delta >= 0.10:
                trend_score = 30.0
    return _component_payload(
        trend_score,
        availability="partial" if shareholder_records else "missing",
        confidence="partial",
        reason="shareholder-count proxy",
        inputs_used={"shareholder_points": len(shareholder_records)},
        extra={"neutral_default": neutral_default},
    )


def _flow_level1_score(kline: dict[str, Any]) -> tuple[float, float, float, float]:
    volume_ratio = safe_float(kline.get("volume_ratio_20_vs_120"))
    if volume_ratio is None:
        volume_ratio_score = 25.0
    elif volume_ratio < 0.8:
        volume_ratio_score = 15.0
    elif volume_ratio <= 1.1:
        volume_ratio_score = 35.0
    elif volume_ratio <= 1.5:
        volume_ratio_score = 60.0
    elif volume_ratio <= 2.0:
        volume_ratio_score = 80.0
    else:
        volume_ratio_score = 90.0

    drawdown = safe_float(kline.get("drawdown_from_5yr_high_pct"))
    rebound = None
    latest_close = safe_float(kline.get("latest_close"))
    low_5y = safe_float(kline.get("low_5y"))
    if latest_close not in (None, 0) and low_5y is not None:
        rebound = latest_close / low_5y - 1
    if drawdown is None and rebound is None:
        drawdown_rebound_score = 30.0
    elif (drawdown or 0.0) >= 40 and (rebound or 0.0) >= 0.10:
        drawdown_rebound_score = 80.0
    elif (drawdown or 0.0) >= 25:
        drawdown_rebound_score = 55.0
    else:
        drawdown_rebound_score = 30.0

    avg_turnover = safe_float(kline.get("avg_turnover_1y"))
    if avg_turnover is None:
        turnover_expansion_score = 30.0
    elif avg_turnover >= 1_000_000_000:
        turnover_expansion_score = 80.0
    elif avg_turnover >= 300_000_000:
        turnover_expansion_score = 60.0
    else:
        turnover_expansion_score = 35.0

    level1_score = volume_ratio_score * 0.40 + drawdown_rebound_score * 0.35 + turnover_expansion_score * 0.25
    return level1_score, volume_ratio_score, drawdown_rebound_score, turnover_expansion_score


def score_flow_confirmation(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    market = normalize_text(driver_stack.get("market") or "A-share")
    if market != "A-share":
        return _component_payload(
            0.0,
            availability="missing",
            confidence="degraded",
            reason="non_a_share_flow_confirmation_not_implemented",
            inputs_used={"market": market},
            extra={"flow_stage": "latent"},
        )

    kline = scan_data.get("stock_kline", {}).get("data", {})
    level1_score, volume_ratio_score, drawdown_rebound_score, turnover_expansion_score = _flow_level1_score(kline)
    event_signals = scan_data.get("event_signals", {}) or {}
    pulse_events_30d = int(safe_float(kline.get("pulse_volume_events_30d")) or 0)
    drawdown = safe_float(kline.get("drawdown_from_5yr_high_pct")) or 0.0
    left_side_absorption = pulse_events_30d >= 2 and drawdown >= 50.0
    level2_bonus = 0.0
    if event_signals.get("buyback"):
        level2_bonus += 7.0
    if event_signals.get("shareholder_support"):
        level2_bonus += 6.0
    if event_signals.get("asset_unlock") or event_signals.get("restructuring"):
        level2_bonus += 7.0
    score = clamp(level1_score + level2_bonus, 0, 100)
    if left_side_absorption:
        score = max(score, 92.0)
    flow_stage = "latent"
    if left_side_absorption:
        flow_stage = "trend"
    elif score >= 85:
        flow_stage = "crowded"
    elif score >= 70:
        flow_stage = "trend"
    elif score >= 50:
        flow_stage = "ignition"
    elif score < 25:
        flow_stage = "abandoned"
    return _component_payload(
        score,
        availability="full",
        confidence="partial" if not event_signals else "full",
        reason=f"l1_raw={level1_score:.1f}, l2_raw={level2_bonus:.1f}, clamped={score:.1f}",
        inputs_used={
            "volume_ratio_score": volume_ratio_score,
            "drawdown_rebound_score": drawdown_rebound_score,
            "turnover_expansion_score": turnover_expansion_score,
            "pulse_volume_events_30d": pulse_events_30d,
            "left_side_absorption": left_side_absorption,
            "event_signals": sorted(event_signals.keys()),
        },
        extra={"flow_stage": flow_stage},
    )


def score_elasticity(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    bucket = normalize_text(driver_stack.get("modifiers", {}).get("elasticity_bucket") or "mid").lower() or "mid"
    primary_type = normalize_text(driver_stack.get("primary_type") or "compounder").lower() or "compounder"
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    float_market_cap = extract_float_market_cap(quote) or extract_market_cap(quote)
    size_score = _continuous_float_cap_score(float_market_cap, bucket)
    raw_size_score = size_score
    kline = scan_data.get("stock_kline", {}).get("data", {})
    turnover_20d = safe_float(kline.get("avg_turnover_20d"))
    turnover = turnover_20d if turnover_20d is not None else safe_float(kline.get("avg_turnover_1y"))
    liquidity_cfg = ELASTICITY_MODEL.get("liquidity", {}) or {}
    hard_floor = float(liquidity_cfg.get("hard_floor_20d", 15_000_000))
    soft_floor = float(liquidity_cfg.get("soft_floor_20d", 50_000_000))
    flow_stage = normalize_text(driver_stack.get("modifiers", {}).get("flow_stage") or "latent").lower() or "latent"
    crowding_penalty_cfg = ELASTICITY_MODEL.get("crowding_penalty", {}) or {}
    crowding_penalty = float(crowding_penalty_cfg.get(flow_stage, 0.0))
    if turnover is None:
        liquidity_score = 55.0
    elif turnover < hard_floor:
        liquidity_score = 5.0
    elif turnover < soft_floor:
        liquidity_score = 55.0
    elif turnover >= 300_000_000:
        liquidity_score = 90.0
    else:
        liquidity_score = 75.0
    size_relief_applied = False
    if turnover is not None and turnover >= 300_000_000:
        if primary_type == "cyclical" and float_market_cap not in (None, 0) and float_market_cap >= 30_000_000_000:
            size_score = max(size_score, 45.0)
            size_relief_applied = size_score != raw_size_score
        elif primary_type == "compounder" and float_market_cap not in (None, 0) and float_market_cap >= 50_000_000_000:
            size_score = max(size_score, 40.0)
            size_relief_applied = size_score != raw_size_score
    if turnover is not None and turnover < hard_floor:
        score = 15.0
    else:
        score = clamp(0.85 * size_score + 0.15 * liquidity_score - crowding_penalty, 0, 100)
    return _component_payload(
        score,
        availability="full",
        confidence="partial",
        reason=(
            f"primary_type={primary_type}, float_market_cap={float_market_cap}, bucket={bucket}, "
            f"avg_turnover_20d={turnover_20d}, crowding_penalty={crowding_penalty}, "
            f"size_relief_applied={size_relief_applied}"
        ),
        inputs_used={
            "bucket": bucket,
            "primary_type": primary_type,
            "float_market_cap": float_market_cap,
            "avg_turnover_20d": turnover_20d,
            "flow_stage": flow_stage,
        },
        extra={"size_score_raw": raw_size_score, "size_score_used": size_score, "size_relief_applied": size_relief_applied},
    )


def score_catalyst_quality(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    path = normalize_text(driver_stack.get("modifiers", {}).get("realization_path") or "repricing").lower() or "repricing"
    event_signals = scan_data.get("event_signals", {}) or {}
    score = float(_CATALYST_PATH_SCORE.get(path, 50))
    if event_signals.get("approved"):
        score += 10
    elif path != "repricing":
        score -= 10
    neutral_default = path == "repricing" and not event_signals
    return _component_payload(
        score,
        availability="partial" if neutral_default else "full",
        confidence="partial",
        reason=f"realization_path={path}",
        inputs_used={"realization_path": path},
        extra={"neutral_default": neutral_default},
    )


def _normalize_axis_weights(weights: dict[str, Any]) -> dict[str, float]:
    cleaned = {name: max(float(value), 0.0) for name, value in (weights or {}).items()}
    total = sum(cleaned.values())
    if total <= 0:
        return {name: 0.0 for name in cleaned}
    return {name: value / total for name, value in cleaned.items()}


def _resolve_realization_weights(
    *,
    base_weights: dict[str, Any],
    components: dict[str, dict[str, Any]],
) -> tuple[dict[str, float], list[str]]:
    weights = _normalize_axis_weights(base_weights)
    released_weight = 0.0
    neutral_dropped: list[str] = []
    for name in _REALIZATION_NEUTRAL_DROP_COMPONENTS:
        if bool((components.get(name) or {}).get("neutral_default")) and weights.get(name, 0.0) > 0:
            released_weight += weights[name]
            weights[name] = 0.0
            neutral_dropped.append(name)
    if released_weight > 0:
        target_total = sum(weights.get(name, 0.0) for name in _REALIZATION_REDISTRIBUTION_TARGETS)
        if target_total > 0:
            for name in _REALIZATION_REDISTRIBUTION_TARGETS:
                current_weight = weights.get(name, 0.0)
                if current_weight <= 0:
                    continue
                weights[name] = current_weight + released_weight * current_weight / target_total
    return _normalize_axis_weights(weights), neutral_dropped


def score_realization_axis(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    primary_type = normalize_text(driver_stack.get("primary_type") or "compounder").lower() or "compounder"
    sector_route = normalize_text(driver_stack.get("sector_route") or "unknown").lower() or "unknown"
    components = {
        "repair_state": score_repair_state(scan_data, driver_stack),
        "regime_cycle_position": score_regime_cycle_position(scan_data, driver_stack),
        "marginal_buyer_probability": score_marginal_buyer_probability(scan_data, driver_stack),
        "flow_confirmation": score_flow_confirmation(scan_data, driver_stack),
        "elasticity": score_elasticity(scan_data, driver_stack),
        "catalyst_quality": score_catalyst_quality(scan_data, driver_stack),
    }
    base_weights = resolve_vcrf_weight_template(primary_type, sector_route)["realization"]
    weights, neutral_dropped = _resolve_realization_weights(base_weights=base_weights, components=components)
    score = 0.0
    for name, component in components.items():
        score += component["score"] * float(weights.get(name, 0.0))
    confidence = "full"
    if any(component["availability"] == "missing" for component in components.values()):
        confidence = "degraded"
    elif any(component["availability"] == "partial" for component in components.values()):
        confidence = "partial"
    return {
        "score": round(score, 2),
        "confidence": confidence,
        "components": components,
        "flow_stage": components["flow_confirmation"].get("flow_stage", "latent"),
        "weights_used": {name: round(float(value), 6) for name, value in weights.items()},
        "neutral_dropped_components": neutral_dropped,
    }
