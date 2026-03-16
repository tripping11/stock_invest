"""Calibration helpers for VCRF score distributions."""
from __future__ import annotations

from typing import Any


def summarize_axis_distribution(scores: list[float]) -> dict[str, Any]:
    cleaned = sorted(float(score) for score in scores if score is not None)
    if not cleaned:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "histogram": {},
        }

    def _quantile(q: float) -> float:
        index = (len(cleaned) - 1) * q
        lower = int(index)
        upper = min(lower + 1, len(cleaned) - 1)
        weight = index - lower
        return cleaned[lower] * (1 - weight) + cleaned[upper] * weight

    histogram: dict[str, int] = {"0-20": 0, "20-40": 0, "40-60": 0, "60-80": 0, "80-100": 0}
    for score in cleaned:
        if score < 20:
            histogram["0-20"] += 1
        elif score < 40:
            histogram["20-40"] += 1
        elif score < 60:
            histogram["40-60"] += 1
        elif score < 80:
            histogram["60-80"] += 1
        else:
            histogram["80-100"] += 1

    return {
        "count": len(cleaned),
        "min": cleaned[0],
        "max": cleaned[-1],
        "p25": round(_quantile(0.25), 2),
        "p50": round(_quantile(0.50), 2),
        "p75": round(_quantile(0.75), 2),
        "histogram": histogram,
    }


def build_calibration_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    underwrite_scores = [record.get("underwrite_axis", {}).get("score") for record in records]
    realization_scores = [record.get("realization_axis", {}).get("score") for record in records]
    states: dict[str, int] = {}
    for record in records:
        state = str(record.get("position_state", "unknown"))
        states[state] = states.get(state, 0) + 1
    return {
        "record_count": len(records),
        "underwrite": summarize_axis_distribution(underwrite_scores),
        "realization": summarize_axis_distribution(realization_scores),
        "state_counts": states,
    }
