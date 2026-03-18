"""Cross-market ticker helpers shared by adapters and routing logic."""
from __future__ import annotations

from utils.value_utils import normalize_text


def infer_market_from_stock_code(stock_code: str) -> str:
    text = normalize_text(stock_code).upper()
    if "." in text:
        base, suffix = text.split(".", 1)
        if suffix in {"SH", "SZ", "BJ"}:
            return "A-share"
        if suffix == "HK":
            return "HK-share"
        return "US-share"
    if text.isdigit():
        if len(text) == 6:
            return "A-share"
        if len(text) == 5:
            return "HK-share"
    return "US-share"


def normalize_display_code(stock_code: str) -> str:
    text = normalize_text(stock_code).upper()
    if "." in text:
        base, suffix = text.split(".", 1)
        if suffix == "HK":
            return base.zfill(5)
        return base
    if text.isdigit():
        if len(text) == 5:
            return text.zfill(5)
        if len(text) == 6:
            return text.zfill(6)
    return text


def to_tushare_code(stock_code: str) -> str:
    text = normalize_text(stock_code).upper()
    if "." in text:
        base, suffix = text.split(".", 1)
        if suffix == "HK":
            return f"{base.zfill(5)}.HK"
        return f"{base}.{suffix}"
    if text.isdigit():
        if len(text) == 5:
            return f"{text.zfill(5)}.HK"
        if text.startswith("6"):
            return f"{text}.SH"
        if text.startswith(("8", "4", "9")):
            return f"{text}.BJ"
        return f"{text}.SZ"
    return text
