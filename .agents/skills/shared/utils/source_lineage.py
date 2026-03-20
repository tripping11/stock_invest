"""Shared data-lineage helpers for scanner and deep-dive outputs."""
from __future__ import annotations

from typing import Any

from utils.value_utils import normalize_text


def source_label(step_result: dict[str, Any] | None) -> str:
    result = step_result or {}
    meta = result.get("_source_meta", {}) if isinstance(result, dict) else {}
    evidence = result.get("evidence", {}) if isinstance(result, dict) else {}
    source_type = normalize_text(meta.get("source_type") or evidence.get("source_type") or "unknown").lower() or "unknown"
    status = normalize_text(meta.get("status") or result.get("status") or "unknown").lower() or "unknown"
    cache_kind = normalize_text(meta.get("cache_kind"))
    if cache_kind:
        return f"{source_type} ({status}, {cache_kind})"
    return f"{source_type} ({status})"


def merge_source_labels(*labels: str) -> str:
    unique: list[str] = []
    for label in labels:
        normalized = normalize_text(label)
        if not normalized or normalized in unique:
            continue
        unique.append(normalized)
    if not unique:
        return "unknown"
    if len(unique) == 1:
        return unique[0]
    return " / ".join(unique)


def summarize_scan_data_lineage(scan_data: dict[str, Any]) -> dict[str, str]:
    return {
        "quote": merge_source_labels(
            source_label(scan_data.get("realtime_quote")),
            source_label(scan_data.get("stock_kline")),
        ),
        "valuation": source_label(scan_data.get("valuation_history")),
        "fundamentals": merge_source_labels(
            source_label(scan_data.get("income_statement")),
            source_label(scan_data.get("balance_sheet")),
        ),
    }


def format_data_lineage(data_lineage: dict[str, Any] | None, *, separator: str = " | ") -> str:
    lineage = data_lineage or {}
    return separator.join(
        [
            f"quote={lineage.get('quote', 'unknown')}",
            f"valuation={lineage.get('valuation', 'unknown')}",
            f"fundamentals={lineage.get('fundamentals', 'unknown')}",
        ]
    )
