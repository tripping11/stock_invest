"""Prototype flow/realization engine for VCRF-style stock ranking.

This file is a design skeleton, not a drop-in production module.
It shows how to turn "等风来" into a systematic state machine.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


@dataclass
class FlowInputs:
    current_price: float | None
    avg20_turnover: float | None
    avg120_turnover: float | None
    rel_strength_20d: float | None
    rel_strength_60d: float | None
    rebound_from_low_pct: float | None
    shareholder_concentration_delta: float | None
    institutional_holding_delta: float | None
    buyback_flag: bool = False
    insider_buy_flag: bool = False
    activist_flag: bool = False
    mna_flag: bool = False


FLOW_STAGE_ORDER = {
    "abandoned": 0,
    "latent": 1,
    "ignition": 2,
    "trend": 3,
    "crowded": 4,
}


def score_flow_setup(inputs: FlowInputs, *, market: str = "A-share") -> dict[str, Any]:
    score = 0.0
    reasons: list[str] = []

    turnover_ratio = None
    if inputs.avg20_turnover not in (None, 0) and inputs.avg120_turnover not in (None, 0):
        turnover_ratio = inputs.avg20_turnover / inputs.avg120_turnover
        if turnover_ratio >= 1.8:
            score += 3.0
            reasons.append(f"turnover expansion {turnover_ratio:.2f}x")
        elif turnover_ratio >= 1.2:
            score += 1.5
            reasons.append(f"moderate turnover expansion {turnover_ratio:.2f}x")

    if inputs.rel_strength_20d is not None:
        if inputs.rel_strength_20d >= 0.08:
            score += 2.0
            reasons.append(f"20d relative strength {inputs.rel_strength_20d:.1%}")
        elif inputs.rel_strength_20d <= -0.05:
            score -= 1.5
            reasons.append(f"20d relative weakness {inputs.rel_strength_20d:.1%}")

    if inputs.rel_strength_60d is not None:
        if inputs.rel_strength_60d >= 0.15:
            score += 1.5
            reasons.append(f"60d relative strength {inputs.rel_strength_60d:.1%}")
        elif inputs.rel_strength_60d <= -0.08:
            score -= 1.0
            reasons.append(f"60d relative weakness {inputs.rel_strength_60d:.1%}")

    if inputs.rebound_from_low_pct is not None:
        if 0.08 <= inputs.rebound_from_low_pct <= 0.35:
            score += 1.5
            reasons.append(f"healthy rebound from low {inputs.rebound_from_low_pct:.1%}")
        elif inputs.rebound_from_low_pct > 0.60:
            score -= 1.5
            reasons.append(f"rebound may already be crowded {inputs.rebound_from_low_pct:.1%}")

    if inputs.shareholder_concentration_delta is not None and inputs.shareholder_concentration_delta > 0:
        score += 1.0
        reasons.append("shareholder base is concentrating")

    if inputs.institutional_holding_delta is not None and inputs.institutional_holding_delta > 0:
        score += 1.0
        reasons.append("institutional ownership is rising")

    event_flags = [
        (inputs.buyback_flag, "buyback / capital return"),
        (inputs.mna_flag, "M&A / asset unlock"),
    ]
    if market == "US":
        event_flags.extend(
            [
                (inputs.insider_buy_flag, "insider buying"),
                (inputs.activist_flag, "activist / beneficial owner signal"),
            ]
        )

    for enabled, label in event_flags:
        if enabled:
            score += 1.0
            reasons.append(label)

    if score <= 0:
        stage = "abandoned"
    elif score <= 2.5:
        stage = "latent"
    elif score <= 5.5:
        stage = "ignition"
    elif score <= 8.0:
        stage = "trend"
    else:
        stage = "crowded"

    return {
        "score": round(max(0.0, min(score, 10.0)), 2),
        "stage": stage,
        "stage_order": FLOW_STAGE_ORDER[stage],
        "reason": "; ".join(reasons) if reasons else "no strong flow evidence yet",
        "raw": {
            "turnover_ratio": turnover_ratio,
            "current_price": inputs.current_price,
        },
    }


def classify_position_state(
    *,
    floor_protection: float | None,
    normalized_upside: float | None,
    recognition_upside: float | None,
    repair_state: str,
    flow_stage: str,
) -> str:
    """Classify names into portfolio states.

    floor_protection: floor_value / current_price
    normalized_upside: normalized_value / current_price - 1
    recognition_upside: recognition_value / current_price - 1
    """
    if floor_protection is None or normalized_upside is None:
        return "reject"

    if floor_protection < 0.75:
        return "reject"

    if normalized_upside < 0.20:
        return "harvest" if flow_stage in {"trend", "crowded"} else "reject"

    if flow_stage in {"abandoned", "latent"}:
        if floor_protection >= 0.90 and normalized_upside >= 0.40:
            return "cold_storage"
        return "secondary_watch"

    if flow_stage == "ignition":
        if repair_state in {"repairing", "confirmed"} and normalized_upside >= 0.25:
            return "ready"
        return "secondary_watch"

    if flow_stage == "trend":
        if recognition_upside is None or recognition_upside >= 0.20:
            return "attack"
        return "harvest"

    return "harvest"
