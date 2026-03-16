"""Flow and realization helpers for the VCRF trading state machine."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from utils.value_utils import safe_float


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
    avg20 = safe_float(inputs.avg20_turnover)
    avg120 = safe_float(inputs.avg120_turnover)
    if avg20 not in (None, 0) and avg120 not in (None, 0):
        turnover_ratio = avg20 / avg120
        if turnover_ratio >= 1.8:
            score += 3.0
            reasons.append(f"turnover expansion {turnover_ratio:.2f}x")
        elif turnover_ratio >= 1.2:
            score += 1.5
            reasons.append(f"moderate turnover expansion {turnover_ratio:.2f}x")

    rel_strength_20d = safe_float(inputs.rel_strength_20d)
    if rel_strength_20d is not None:
        if rel_strength_20d >= 0.08:
            score += 2.0
            reasons.append(f"20d relative strength {rel_strength_20d:.1%}")
        elif rel_strength_20d <= -0.05:
            score -= 1.5
            reasons.append(f"20d relative weakness {rel_strength_20d:.1%}")

    rel_strength_60d = safe_float(inputs.rel_strength_60d)
    if rel_strength_60d is not None:
        if rel_strength_60d >= 0.15:
            score += 1.5
            reasons.append(f"60d relative strength {rel_strength_60d:.1%}")
        elif rel_strength_60d <= -0.08:
            score -= 1.0
            reasons.append(f"60d relative weakness {rel_strength_60d:.1%}")

    rebound_from_low_pct = safe_float(inputs.rebound_from_low_pct)
    if rebound_from_low_pct is not None:
        if 0.08 <= rebound_from_low_pct <= 0.35:
            score += 1.5
            reasons.append(f"healthy rebound from low {rebound_from_low_pct:.1%}")
        elif rebound_from_low_pct > 0.60:
            score -= 1.5
            reasons.append(f"rebound may already be crowded {rebound_from_low_pct:.1%}")

    if safe_float(inputs.shareholder_concentration_delta) not in (None, 0):
        if float(inputs.shareholder_concentration_delta) > 0:
            score += 1.0
            reasons.append("shareholder base is concentrating")

    if safe_float(inputs.institutional_holding_delta) not in (None, 0):
        if float(inputs.institutional_holding_delta) > 0:
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

    clamped = round(max(0.0, min(score, 10.0)), 2)
    if clamped <= 0:
        stage = "abandoned"
    elif clamped <= 2.5:
        stage = "latent"
    elif clamped <= 6.5:
        stage = "ignition"
    elif clamped <= 8.5:
        stage = "trend"
    else:
        stage = "crowded"

    return {
        "score": clamped,
        "stage": stage,
        "stage_order": FLOW_STAGE_ORDER[stage],
        "reason": "; ".join(reasons) if reasons else "no strong flow evidence yet",
        "raw": {
            "turnover_ratio": round(turnover_ratio, 4) if turnover_ratio is not None else None,
            "current_price": safe_float(inputs.current_price),
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
    """Collapse VCRF setup into the five legal portfolio states.

    Phase 1 intentionally keeps only five user-facing states and folds the
    prototype's `secondary_watch` back into the cold-storage/reject boundary.
    """
    floor_protection = safe_float(floor_protection)
    normalized_upside = safe_float(normalized_upside)
    recognition_upside = safe_float(recognition_upside)

    if floor_protection is None or normalized_upside is None:
        return "reject"

    if floor_protection < 0.75:
        return "reject"

    if normalized_upside < 0.20:
        return "harvest" if flow_stage in {"trend", "crowded"} else "reject"

    if flow_stage in {"abandoned", "latent"}:
        if floor_protection >= 0.85 and normalized_upside >= 0.40:
            return "cold_storage"
        return "reject"

    if flow_stage == "ignition":
        if repair_state in {"repairing", "confirmed"} and normalized_upside >= 0.25:
            return "ready"
        if floor_protection >= 0.85 and normalized_upside >= 0.40:
            return "cold_storage"
        return "reject"

    if flow_stage == "trend":
        if recognition_upside is None or recognition_upside >= 0.20:
            return "attack"
        return "harvest"

    return "harvest"
