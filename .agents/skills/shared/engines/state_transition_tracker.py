"""Append-only VCRF state history helpers."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_latest_state(stock_code: str, *, history_path: str | Path) -> str:
    path = Path(history_path)
    if not path.exists():
        return "NEW"

    latest_state = "NEW"
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(record.get("code", "")) != str(stock_code):
                continue
            latest_state = str(record.get("next_state") or record.get("state") or latest_state)
    return latest_state or "NEW"


def enforce_transition(prev_state: str, proposed_state: str, *, cfg: dict[str, Any]) -> tuple[str, bool, str]:
    previous = prev_state or "NEW"
    allowed_map = cfg.get("allowed_transitions", {})
    allowed_targets = list(allowed_map.get(previous, []))
    if proposed_state in allowed_targets:
        return proposed_state, True, f"{previous} -> {proposed_state} allowed"
    if allowed_targets:
        fallback = allowed_targets[0]
        return fallback, False, f"{previous} -> {proposed_state} forbidden; downgraded to {fallback}"
    return proposed_state, False, f"{previous} has no configured transitions; kept {proposed_state}"


def append_state_record(record: dict[str, Any], *, history_path: str | Path) -> None:
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
