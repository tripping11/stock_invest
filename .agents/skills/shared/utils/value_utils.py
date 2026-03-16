"""Low-level value normalisation and selection helpers."""
from __future__ import annotations

import math
from typing import Any


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        result = float(str(value).replace(",", ""))
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def extract_first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _sortable_date(value: Any) -> str:
    text = normalize_text(value).replace("-", "").replace("/", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits if len(digits) >= 6 else ""


def select_latest_record(records: list[dict[str, Any]], date_keys: tuple[str, ...] = ("报告日期", "报告期", "日期", "报告日")) -> dict[str, Any]:
    valid = [row for row in records if isinstance(row, dict)]
    if not valid:
        return {}

    def key_func(row: dict[str, Any]) -> str:
        for key in date_keys:
            sortable = _sortable_date(row.get(key))
            if sortable:
                return sortable
        return ""

    return max(valid, key=key_func)


def _pick_revenue_col(records: list[dict[str, Any]], exact_keys: tuple[str, ...], contains: tuple[str, ...] = ()) -> str | None:
    if not records:
        return None
    sample = records[0]
    for key in exact_keys:
        if key in sample:
            return key
    for key in sample:
        text = normalize_text(key)
        if contains and all(token in text for token in contains):
            return key
    return None
