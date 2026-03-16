"""Financial statement snapshot extraction helpers."""
from __future__ import annotations

from typing import Any

from utils.value_utils import (
    _pick_revenue_col,
    _sortable_date,
    extract_first_value,
    normalize_text,
    safe_float,
    select_latest_record,
)


def extract_latest_revenue_snapshot(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_row = select_latest_record(records)
    if not latest_row:
        return []
    latest_date = ""
    for key in ("报告日期", "报告期", "日期", "报告日"):
        latest_date = _sortable_date(latest_row.get(key))
        if latest_date:
            break
    if not latest_date:
        return records
    snapshot: list[dict[str, Any]] = []
    for row in records:
        row_date = ""
        for key in ("报告日期", "报告期", "日期", "报告日"):
            row_date = _sortable_date(row.get(key))
            if row_date:
                break
        if row_date == latest_date:
            snapshot.append(row)
    return snapshot or records


def extract_latest_revenue_terms(records: list[dict[str, Any]], limit: int = 10) -> list[str]:
    snapshot = extract_latest_revenue_snapshot(records)
    if not snapshot:
        return []
    name_col = _pick_revenue_col(snapshot, ("主营构成", "产品名称", "分类名称", "名称"), contains=("名称",))
    revenue_col = _pick_revenue_col(snapshot, ("主营收入", "营业收入"), contains=("收入",))
    ratio_col = _pick_revenue_col(snapshot, ("收入比例", "营业收入占比", "占比"), contains=("占比",))
    ranked: list[tuple[float, str]] = []
    for row in snapshot:
        name = normalize_text(row.get(name_col or ""))
        if not name or any(token in name for token in ("其他", "合计", "国内", "国外")):
            continue
        ratio = safe_float(row.get(ratio_col or ""))
        revenue = safe_float(row.get(revenue_col or ""))
        score = ratio if ratio is not None else (revenue or 0.0)
        ranked.append((score, name))
    ranked.sort(reverse=True)
    return [name for _, name in ranked[:limit]]


def extract_market_cap(quote: dict[str, Any]) -> float | None:
    for key, value in quote.items():
        key_text = normalize_text(key)
        if "总市值" in key_text or key_text.lower() == "market_cap":
            return safe_float(value)
    return None


def extract_latest_price(quote: dict[str, Any], kline: dict[str, Any] | None = None) -> float | None:
    for key, value in quote.items():
        key_text = normalize_text(key)
        if "最新价" in key_text or key_text.lower() in {"latest_price", "price"}:
            price = safe_float(value)
            if price is not None:
                return price
    if kline:
        return safe_float(kline.get("latest_close"))
    return None


def get_latest_income_snapshot(records: list[dict[str, Any]]) -> dict[str, Any]:
    row = select_latest_record(records)
    if not row:
        return {"report_date": "", "net_profit": None, "raw": {}}
    return {
        "report_date": normalize_text(extract_first_value(row, ("报告日期", "报告期", "报告日"))),
        "net_profit": safe_float(extract_first_value(row, ("归属于母公司所有者的净利润", "归属于母公司股东的净利润", "净利润"))),
        "raw": row,
    }


def get_latest_balance_snapshot(records: list[dict[str, Any]]) -> dict[str, Any]:
    row = select_latest_record(records)
    if not row:
        return {"report_date": "", "total_equity": None, "raw": {}}
    return {
        "report_date": normalize_text(extract_first_value(row, ("报告日期", "报告期", "报告日"))),
        "total_equity": safe_float(
            extract_first_value(
                row,
                ("归属于母公司股东权益合计", "归属于母公司所有者权益合计", "所有者权益合计", "股东权益合计"),
            )
        ),
        "raw": row,
    }
