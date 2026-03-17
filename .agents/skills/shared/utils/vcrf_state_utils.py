"""Pure VCRF position-state classification helpers."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_vcrf_state_machine
from utils.value_utils import normalize_text, safe_float


def classify_vcrf_position_state(
    underwrite_score: float,
    realization_score: float,
    *,
    flow_stage: str,
    valuation_summary: dict[str, Any],
    cfg: dict[str, Any] | None = None,
) -> str:
    """Classify runtime state from numeric inputs only."""
    thresholds = (cfg or load_vcrf_state_machine()).get("state_thresholds", {}) or {}
    reject_floor = float(thresholds.get("reject_underwrite_below", 60))
    cold_cfg = thresholds.get("cold_storage", {}) or {}
    ready_cfg = thresholds.get("ready", {}) or {}
    attack_cfg = thresholds.get("attack", {}) or {}
    harvest_cfg = thresholds.get("harvest", {}) or {}

    recognition_upside = safe_float((valuation_summary or {}).get("recognition_upside"))
    crowded_flow_stage = normalize_text(harvest_cfg.get("crowded_flow_stage")).lower()
    flow_stage_normalized = normalize_text(flow_stage).lower()

    if underwrite_score < reject_floor:
        return "REJECT"
    if recognition_upside is not None and recognition_upside <= float(harvest_cfg.get("recognition_upside_max", 0.0)):
        return "HARVEST"
    if crowded_flow_stage and flow_stage_normalized == crowded_flow_stage:
        return "HARVEST"
    if underwrite_score >= float(attack_cfg.get("min_underwrite", 75)) and realization_score >= float(attack_cfg.get("min_realization", 70)):
        return "ATTACK"
    if (
        underwrite_score >= float(ready_cfg.get("min_underwrite", 70))
        and realization_score >= float(ready_cfg.get("min_realization", 40))
        and realization_score <= float(ready_cfg.get("max_realization", 70))
    ):
        return "READY"
    if underwrite_score >= float(cold_cfg.get("min_underwrite", 75)) and realization_score <= float(cold_cfg.get("max_realization", 39.999)):
        return "COLD_STORAGE"
    return "REJECT"
