"""BaoStock fallback helpers for A-share market data."""
from __future__ import annotations

import datetime
import threading
from contextlib import contextmanager
from typing import Any

from utils.evidence_helpers import now_iso as _now_iso
from utils.vendor_support import ensure_vendor_path

_BAOSTOCK_LOCK = threading.Lock()


def _load_baostock():
    ensure_vendor_path("baostock")
    try:
        import baostock as bs  # type: ignore

        return bs
    except Exception:
        return None


@contextmanager
def _session():
    bs = _load_baostock()
    if bs is None:
        raise RuntimeError("baostock is not installed")
    with _BAOSTOCK_LOCK:
        login_result = bs.login()
        if getattr(login_result, "error_code", "") != "0":
            raise RuntimeError(getattr(login_result, "error_msg", "baostock login failed"))

        try:
            yield bs
        finally:
            try:
                bs.logout()
            except Exception:
                pass


def _to_bs_code(stock_code: str) -> str:
    code = str(stock_code).strip().lower()
    if "." in code:
        return code
    return f"sh.{code}" if code.startswith("6") else f"sz.{code}"


def _from_bs_code(code: str) -> str:
    return str(code).split(".", 1)[-1].zfill(6)


def _resultset_to_records(resultset: Any) -> list[dict[str, Any]]:
    if str(getattr(resultset, "error_code", "")) != "0":
        raise RuntimeError(getattr(resultset, "error_msg", "unknown baostock error"))

    records: list[dict[str, Any]] = []
    fields = list(getattr(resultset, "fields", []) or [])
    while resultset.next():
        records.append(dict(zip(fields, resultset.get_row_data())))
    return records


def _is_a_share_equity(code: str) -> bool:
    return code.startswith(
        (
            "sh.600",
            "sh.601",
            "sh.603",
            "sh.605",
            "sh.688",
            "sh.689",
            "sz.000",
            "sz.001",
            "sz.002",
            "sz.003",
            "sz.300",
            "sz.301",
        )
    )


def _latest_trade_day(bs: Any) -> str:
    trading_days = _recent_trade_days(bs)
    if trading_days:
        return trading_days[-1]
    return datetime.date.today().isoformat()


def _recent_trade_days(bs: Any) -> list[str]:
    today = datetime.date.today()
    start_day = (today - datetime.timedelta(days=14)).isoformat()
    end_day = today.isoformat()
    resultset = bs.query_trade_dates(start_date=start_day, end_date=end_day)
    records = _resultset_to_records(resultset)
    return [row["calendar_date"] for row in records if str(row.get("is_trading_day")) == "1"]


def _normalize_all_stock_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "code": _from_bs_code(row.get("code", "")),
            "name": str(row.get("code_name", "")).strip(),
            "trade_status": str(row.get("tradeStatus", "")).strip(),
            "bs_code": str(row.get("code", "")).strip(),
        }
        for row in records
        if _is_a_share_equity(str(row.get("code", "")).strip().lower())
    ]


def _preferred_universe_codes(bs: Any) -> list[str]:
    preferred_codes: list[str] = []
    seen: set[str] = set()
    for query_name in ("query_sz50_stocks", "query_hs300_stocks", "query_zz500_stocks"):
        query_fn = getattr(bs, query_name, None)
        if query_fn is None:
            continue
        try:
            for row in _resultset_to_records(query_fn()):
                code = str(row.get("code", "")).strip()
                if not _is_a_share_equity(code.lower()) or code in seen:
                    continue
                preferred_codes.append(code)
                seen.add(code)
        except Exception:
            continue
    return preferred_codes


def _prioritize_stock_rows(rows: list[dict[str, Any]], preferred_codes: list[str]) -> list[dict[str, Any]]:
    if not preferred_codes:
        return rows
    preferred_set = set(preferred_codes)
    code_to_rank = {code: idx for idx, code in enumerate(preferred_codes)}
    prioritized = [row for row in rows if row.get("bs_code") in preferred_set]
    remaining = [row for row in rows if row.get("bs_code") not in preferred_set]
    prioritized.sort(key=lambda row: code_to_rank.get(str(row.get("bs_code", "")), len(preferred_codes)))
    return prioritized + remaining


def get_all_a_share_stocks(day: str | None = None) -> dict[str, Any]:
    try:
        with _session() as bs:
            trade_days = _recent_trade_days(bs)
            preferred_codes = _preferred_universe_codes(bs)
            query_day = day or (trade_days[-1] if trade_days else datetime.date.today().isoformat())
            records = _resultset_to_records(bs.query_all_stock(day=query_day))
            normalized = _prioritize_stock_rows(_normalize_all_stock_rows(records), preferred_codes)

            if not normalized and day is None:
                for candidate_day in reversed(trade_days[:-1]):
                    records = _resultset_to_records(bs.query_all_stock(day=candidate_day))
                    normalized = _prioritize_stock_rows(_normalize_all_stock_rows(records), preferred_codes)
                    if normalized:
                        query_day = candidate_day
                        break
        return {"data": normalized, "status": "ok", "day": query_day, "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": [], "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


def get_stock_basic(stock_code: str) -> dict[str, Any]:
    try:
        with _session() as bs:
            records = _resultset_to_records(bs.query_stock_basic(code=_to_bs_code(stock_code)))
        if not records:
            return {"data": {}, "status": "error: empty baostock stock basic result", "fetch_timestamp": _now_iso()}
        latest = records[0]
        return {
            "data": {
                "code": latest.get("code", ""),
                "code_name": latest.get("code_name", ""),
                "ipoDate": latest.get("ipoDate", ""),
                "outDate": latest.get("outDate", ""),
                "type": latest.get("type", ""),
                "status": latest.get("status", ""),
            },
            "status": "ok",
            "fetch_timestamp": _now_iso(),
        }
    except Exception as exc:
        return {"data": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


def get_daily_history(stock_code: str, start_date: str, end_date: str, fields: str) -> dict[str, Any]:
    try:
        with _session() as bs:
            records = _resultset_to_records(
                bs.query_history_k_data_plus(
                    _to_bs_code(stock_code),
                    fields,
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="3",
                )
            )
        return {"data": records, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": [], "status": f"error: {exc}", "fetch_timestamp": _now_iso()}
