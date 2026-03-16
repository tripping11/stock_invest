"""Shared helpers for periodic-report metadata and report scorecards."""
from __future__ import annotations


def report_kind(title: str) -> str:
    if "第三季度报告" in title:
        return "三季报"
    if "半年度报告" in title:
        return "半年报"
    if "年度报告" in title:
        return "年报"
    if "第一季度报告" in title:
        return "一季报"
    return "其他"
