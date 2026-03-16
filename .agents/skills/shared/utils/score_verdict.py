"""Score verdict lookup from scoring_rules.yaml."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_scoring_rules
from utils.value_utils import normalize_text


def pick_score_verdict(total_score: float) -> dict[str, Any]:
    for item in load_scoring_rules().get("verdict", []):
        start_text, end_text = normalize_text(item.get("range")).split("-", 1)
        start = float(start_text)
        end = float(end_text)
        if start <= total_score <= end:
            return item
    return {"label": "reject / no action", "action": "do not allocate capital"}
