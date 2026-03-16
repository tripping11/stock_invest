"""
akshare_adapter.py

Tier 1 aggregation layer for A-share scans.
This module intentionally stays on the "convenient fetch" layer:
important conclusions still need Tier 0 verification.

Improvements:
- Cache timestamp & freshness detection (stale > 24h)
- Column name probing with explicit error on mismatch
- fetch_timestamp on every result
"""

from __future__ import annotations

import datetime
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd

from adapters import baostock_adapter
from adapters.baostock_adapter import get_daily_history, get_stock_basic
from utils.evidence_helpers import make_evidence as _shared_make_evidence, now_iso, now_ts
from utils.research_utils import CACHE_STALE_HOURS
from utils.value_utils import safe_float
from utils.vendor_support import ensure_vendor_path

# ── 缓存新鲜度阈值（小时）──────────────────────────────


def _make_evidence(field: str, value: Any, source_desc: str, url: str = "", confidence: str = "medium") -> dict[str, Any]:
    return _shared_make_evidence(field, value, source_desc, source_type="akshare", tier=1, url=url, confidence=confidence)


def _stock_symbol(stock_code: str) -> str:
    return f"sh{stock_code}" if stock_code.startswith("6") else f"sz{stock_code}"


def _market_prefix(stock_code: str) -> str:
    return "SH" if stock_code.startswith("6") else "SZ"


def _load_efinance():
    if not ensure_vendor_path("efinance"):
        return None
    try:
        import efinance as ef  # type: ignore

        return ef
    except Exception:
        return None


def _pick_first_column(df: pd.DataFrame, candidates: tuple[str, ...], contains: tuple[str, ...] = ()) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    for column in df.columns:
        if any(token in str(column) for token in contains):
            return str(column)
    return None


def _probe_columns(df: pd.DataFrame, expected: tuple[str, ...], context: str, contains: tuple[str, ...] = ()) -> str | None:
    """探测列名是否存在，找不到时打印警告并返回 None"""
    col = _pick_first_column(df, expected, contains)
    if col is None:
        actual_cols = list(df.columns[:20])
        print(f"  [WARNING] 列名不匹配 ({context}): 期望 {expected}, 实际列 {actual_cols}")
    return col


def _sort_df_by_date(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    date_column = _pick_first_column(
        df,
        (
            "日期",
            "报告日",
            "报告日期",
            "报表日期",
            "截止日期",
            "公告日期",
            "报告期",
            "财报日期",
        ),
        contains=("日期", "报告", "截止"),
    )
    if not date_column:
        return df

    parsed = pd.to_datetime(df[date_column], errors="coerce")
    if parsed.notna().any():
        return (
            df.assign(__sort_key=parsed)
            .sort_values(by="__sort_key", kind="stable")
            .drop(columns=["__sort_key"])
        )

    normalized = (
        df[date_column]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .replace("", pd.NA)
    )
    if normalized.notna().any():
        return (
            df.assign(__sort_key=normalized)
            .sort_values(by="__sort_key", kind="stable")
            .drop(columns=["__sort_key"])
        )
    return df


def _recent_records(df: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    ordered = _sort_df_by_date(df)
    return ordered.tail(limit).to_dict("records")


def _latest_non_null(series: pd.Series) -> float | None:
    values = series.dropna()
    if values.empty:
        return None
    try:
        return float(values.iloc[-1])
    except (TypeError, ValueError):
        return None


def _estimate_consolidation_months(monthly_closes: pd.Series) -> int:
    """Estimate how long the stock has stayed in a broad sideways band near the latest price."""
    if monthly_closes is None or monthly_closes.empty:
        return 0

    latest_close = safe_float(monthly_closes.iloc[-1])
    if latest_close in (None, 0):
        return 0

    lower_bound = latest_close * 0.75
    upper_bound = latest_close * 1.35
    months = 0
    for value in reversed(monthly_closes.tolist()):
        close = safe_float(value)
        if close is None or close < lower_bound or close > upper_bound:
            break
        months += 1
    return months


def _is_ok_status(status: Any) -> bool:
    return str(status or "").lower().startswith("ok")


def _extract_latest_share_capital(stock_code: str) -> float | None:
    balance_result = get_balance_sheet(stock_code)
    if not _is_ok_status(balance_result.get("status")):
        return None
    records = balance_result.get("data", [])
    if not records:
        return None
    latest = records[-1]
    for key in ("实收资本(或股本)", "股本", "总股本"):
        value = safe_float(latest.get(key))
        if value is not None and value > 0:
            return value
    return None


def _derive_quote_snapshot(stock_code: str) -> dict[str, Any]:
    kline_result = get_stock_kline(stock_code, period="daily", years=1)
    latest_close = safe_float(kline_result.get("data", {}).get("latest_close"))
    share_capital = _extract_latest_share_capital(stock_code)
    market_cap = latest_close * share_capital if latest_close is not None and share_capital is not None else None
    return {
        "代码": stock_code,
        "最新价": latest_close,
        "总股本": share_capital,
        "流通股": None,
        "总市值": market_cap,
        "流通市值": None,
    }


def get_company_profile(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_individual_info_em(symbol=stock_code)
        info: dict[str, Any] = {}
        for _, row in df.iterrows():
            info[row["item"]] = row["value"]
        evidence = _make_evidence(
            "company_profile",
            info,
            f"akshare stock_individual_info_em ({stock_code})",
        )
        return {"data": info, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        ef = _load_efinance()
        if ef is not None:
            try:
                quote_df = ef.stock.get_latest_quote(stock_code)
                if quote_df is not None and len(quote_df) > 0:
                    row = quote_df.iloc[0].to_dict()
                    info = {
                        "股票简称": row.get("名称", ""),
                        "最新价": row.get("最新价"),
                        "总市值": row.get("总市值"),
                        "流通市值": row.get("流通市值"),
                        "市场类型": row.get("市场类型", ""),
                    }
                    evidence = _make_evidence(
                        "company_profile",
                        info,
                        f"efinance stock.get_latest_quote ({stock_code})",
                        confidence="medium",
                    )
                    return {"data": info, "evidence": evidence, "status": "ok_fallback_efinance", "fetch_timestamp": now_iso()}
            except Exception:
                pass
        derived = _derive_quote_snapshot(stock_code)
        if derived.get("最新价") is not None:
            evidence = _make_evidence(
                "company_profile",
                derived,
                f"derived from stock_zh_a_hist + balance_sheet ({stock_code})",
                confidence="medium",
            )
            return {"data": derived, "evidence": evidence, "status": "ok_fallback_derived", "fetch_timestamp": now_iso()}
        return {"data": {}, "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def get_financial_summary(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2018")
        records = _recent_records(df, 10)
        evidence = _make_evidence(
            "financial_summary",
            f"{len(records)} periods",
            f"akshare stock_financial_analysis_indicator ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def get_revenue_breakdown(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_zygc_em(symbol=f"{_market_prefix(stock_code)}{stock_code}")
        records = _recent_records(df, 50)
        evidence = _make_evidence(
            "revenue_breakdown",
            f"{len(records)} segments",
            f"akshare stock_zygc_em ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def get_valuation_history(stock_code: str) -> dict[str, Any]:
    try:
        start = (datetime.datetime.now() - datetime.timedelta(days=5 * 365)).strftime("%Y%m%d")
        end = datetime.datetime.now().strftime("%Y%m%d")
        df_kline = ak.stock_zh_a_hist(symbol=stock_code, period="weekly", start_date=start, end_date=end, adjust="")
        df_fin = ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2020")

        latest: dict[str, Any] = {}
        df_fin = _sort_df_by_date(df_fin)

        # 列名探测（替代静默返回空）
        bvps_col = _probe_columns(
            df_fin,
            (
                "每股净资产_调整前(元)",
                "每股净资产_调整后(元)",
                "调整后的每股净资产(元)",
                "每股净资产(元)",
            ),
            "BVPS",
            contains=("每股净资产",),
        )
        date_col = _probe_columns(df_fin, ("日期", "报告日", "报告日期"), "fin_date", contains=("日期", "报告"))

        if bvps_col is None:
            return {
                "data": {},
                "evidence": {},
                "status": "error: column_mismatch_bvps",
                "fetch_timestamp": now_iso(),
                "column_mismatch": {"expected": "每股净资产相关列", "actual_columns": list(df_fin.columns[:15])},
            }

        if bvps_col:
            bvps = _latest_non_null(df_fin[bvps_col])
            if bvps is not None:
                latest["bvps"] = round(bvps, 4)
                if date_col:
                    valid_df = df_fin[df_fin[bvps_col].notna()]
                    if not valid_df.empty:
                        latest["bvps_report_date"] = str(valid_df.iloc[-1][date_col])

        close_col = _probe_columns(df_kline, ("收盘",), "kline_close", contains=("收盘",))
        if not df_kline.empty and close_col and "bvps" in latest:
            close_series = df_kline[close_col].astype(float)
            latest_close = float(close_series.iloc[-1])
            bvps = float(latest["bvps"])
            if bvps > 0:
                pb_series = close_series / bvps
                current_pb = float(pb_series.iloc[-1])
                percentile = float(round((pb_series < current_pb).sum() / len(pb_series) * 100, 2))
                latest.update(
                    {
                        "pb": round(current_pb, 4),
                        "pb_percentile": percentile,
                        "pb_min": round(float(pb_series.min()), 4),
                        "pb_max": round(float(pb_series.max()), 4),
                        "pb_median": round(float(pb_series.median()), 4),
                        "latest_close": round(latest_close, 4),
                    }
                )

        evidence = _make_evidence(
            "valuation_history",
            f"pb={latest.get('pb', 'N/A')}, percentile={latest.get('pb_percentile', 'N/A')}%",
            "Derived from akshare stock_zh_a_hist and stock_financial_analysis_indicator",
        )
        return {"data": latest, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        try:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=5 * 365)).strftime("%Y-%m-%d")
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
            history = get_daily_history(stock_code, start_date, end_date, "date,code,close,pbMRQ,peTTM")
            rows = history.get("data", [])
            if rows:
                history_df = pd.DataFrame(rows)
                history_df["pbMRQ"] = pd.to_numeric(history_df["pbMRQ"], errors="coerce")
                history_df["close"] = pd.to_numeric(history_df["close"], errors="coerce")
                history_df = history_df.dropna(subset=["pbMRQ", "close"])
                if not history_df.empty:
                    pb_series = history_df["pbMRQ"]
                    current_pb = float(pb_series.iloc[-1])
                    latest = {
                        "pb": round(current_pb, 4),
                        "pb_percentile": round(float((pb_series < current_pb).sum() / len(pb_series) * 100), 2),
                        "pb_min": round(float(pb_series.min()), 4),
                        "pb_max": round(float(pb_series.max()), 4),
                        "pb_median": round(float(pb_series.median()), 4),
                        "latest_close": round(float(history_df["close"].iloc[-1]), 4),
                    }
                    evidence = _make_evidence(
                        "valuation_history",
                        f"pb={latest.get('pb', 'N/A')}, percentile={latest.get('pb_percentile', 'N/A')}%",
                        f"baostock query_history_k_data_plus ({stock_code})",
                    )
                    evidence["source_type"] = "baostock"
                    return {
                        "data": latest,
                        "evidence": evidence,
                        "status": "ok_fallback_baostock_history",
                        "fetch_timestamp": now_iso(),
                    }
        except Exception:
            pass
        return {"data": {}, "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def get_stock_kline(stock_code: str, period: str = "daily", years: int = 5) -> dict[str, Any]:
    try:
        start = (datetime.datetime.now() - datetime.timedelta(days=years * 365)).strftime("%Y%m%d")
        end = datetime.datetime.now().strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(symbol=stock_code, period=period, start_date=start, end_date=end, adjust="qfq")

        summary: dict[str, Any] = {}
        if not df.empty:
            date_col = _probe_columns(df, ("日期",), "kline_date", contains=("日期",))
            close_col = _probe_columns(df, ("收盘",), "kline_close", contains=("收盘",))
            volume_col = _pick_first_column(df, ("成交量",), contains=("成交量",))
            amount_col = _pick_first_column(df, ("成交额",), contains=("成交额",))

            if close_col and date_col:
                ordered = df.copy()
                ordered["__date__"] = pd.to_datetime(ordered[date_col], errors="coerce")
                ordered["__close__"] = pd.to_numeric(ordered[close_col], errors="coerce")
                ordered = ordered.dropna(subset=["__date__", "__close__"]).sort_values("__date__")
                if ordered.empty:
                    return {
                        "data": {},
                        "evidence": {},
                        "status": "error: empty_kline_after_cleanup",
                        "fetch_timestamp": now_iso(),
                    }

                latest_close = float(ordered["__close__"].iloc[-1])
                high_5y = float(ordered["__close__"].max())
                low_5y = float(ordered["__close__"].min())
                summary["latest_close"] = latest_close
                summary["latest_date"] = ordered["__date__"].iloc[-1].strftime("%Y-%m-%d")
                summary["high_5y"] = high_5y
                summary["low_5y"] = low_5y
                summary["current_vs_high"] = round(latest_close / high_5y * 100, 1) if high_5y else None
                summary["current_vs_5yr_high"] = summary["current_vs_high"]
                summary["drawdown_from_5yr_high_pct"] = round(100 - summary["current_vs_high"], 1) if summary["current_vs_high"] is not None else None
                summary["total_bars"] = len(ordered)

                monthly_closes = ordered.set_index("__date__")["__close__"].resample("ME").last().dropna()
                summary["consolidation_months"] = _estimate_consolidation_months(monthly_closes)
            elif close_col is None:
                return {
                    "data": {},
                    "evidence": {},
                    "status": "error: column_mismatch_close",
                    "fetch_timestamp": now_iso(),
                    "column_mismatch": {"expected": "收盘", "actual_columns": list(df.columns[:15])},
                }

            numeric_df = df.copy()
            if volume_col:
                numeric_df["__volume__"] = pd.to_numeric(numeric_df[volume_col], errors="coerce")
            if amount_col:
                numeric_df["__amount__"] = pd.to_numeric(numeric_df[amount_col], errors="coerce")
            recent_252 = numeric_df.tail(252)
            if volume_col and not recent_252.empty:
                recent_vol = recent_252["__volume__"].dropna()
                if not recent_vol.empty:
                    summary["avg_vol_1y"] = float(recent_vol.mean())
                    summary["avg_vol_20d"] = float(recent_vol.tail(20).mean()) if len(recent_vol) >= 20 else None
                    summary["avg_vol_120d"] = float(recent_vol.tail(120).mean()) if len(recent_vol) >= 120 else None
                    if summary.get("avg_vol_20d") not in (None, 0) and summary.get("avg_vol_120d") not in (None, 0):
                        summary["volume_ratio_20_vs_120"] = round(summary["avg_vol_20d"] / summary["avg_vol_120d"], 2)
            if amount_col and not recent_252.empty:
                recent_amount = recent_252["__amount__"].dropna()
                if not recent_amount.empty:
                    summary["avg_turnover_1y"] = float(recent_amount.mean())

        evidence = _make_evidence(
            "stock_kline",
            f"close={summary.get('latest_close', 'N/A')}",
            f"akshare stock_zh_a_hist ({stock_code})",
        )
        return {"data": summary, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        try:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=years * 365)).strftime("%Y-%m-%d")
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
            history = get_daily_history(stock_code, start_date, end_date, "date,open,high,low,close,volume,amount,turn,pctChg")
            rows = history.get("data", [])
            if rows:
                df = pd.DataFrame(rows)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                if "volume" in df.columns:
                    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
                if "amount" in df.columns:
                    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
                df = df.dropna(subset=["date", "close"]).sort_values("date")
                if not df.empty:
                    latest_close = float(df["close"].iloc[-1])
                    high_5y = float(df["close"].max())
                    low_5y = float(df["close"].min())
                    summary = {
                        "latest_close": latest_close,
                        "latest_date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
                        "high_5y": high_5y,
                        "low_5y": low_5y,
                        "current_vs_high": round(latest_close / high_5y * 100, 1) if high_5y else None,
                        "current_vs_5yr_high": round(latest_close / high_5y * 100, 1) if high_5y else None,
                        "drawdown_from_5yr_high_pct": round(100 - (latest_close / high_5y * 100), 1) if high_5y else None,
                        "total_bars": len(df),
                    }
                    monthly_closes = df.set_index("date")["close"].resample("ME").last().dropna()
                    summary["consolidation_months"] = _estimate_consolidation_months(monthly_closes)
                    if "volume" in df.columns:
                        recent_252 = df.tail(252)
                        recent_vol = recent_252["volume"].dropna()
                        if not recent_vol.empty:
                            summary["avg_vol_1y"] = float(recent_vol.mean())
                            summary["avg_vol_20d"] = float(recent_vol.tail(20).mean()) if len(recent_vol) >= 20 else None
                            summary["avg_vol_120d"] = float(recent_vol.tail(120).mean()) if len(recent_vol) >= 120 else None
                            if summary.get("avg_vol_20d") not in (None, 0) and summary.get("avg_vol_120d") not in (None, 0):
                                summary["volume_ratio_20_vs_120"] = round(summary["avg_vol_20d"] / summary["avg_vol_120d"], 2)
                    if "amount" in df.columns:
                        recent_amount = df.tail(252)["amount"].dropna()
                        if not recent_amount.empty:
                            summary["avg_turnover_1y"] = float(recent_amount.mean())
                    evidence = _make_evidence(
                        "stock_kline",
                        f"close={summary.get('latest_close', 'N/A')}",
                        f"baostock query_history_k_data_plus ({stock_code})",
                    )
                    evidence["source_type"] = "baostock"
                    return {
                        "data": summary,
                        "evidence": evidence,
                        "status": "ok_fallback_baostock_history",
                        "fetch_timestamp": now_iso(),
                    }
        except Exception:
            pass
        return {"data": {}, "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def get_realtime_quote(stock_code: str) -> dict[str, Any]:
    errors: list[str] = []

    try:
        df = ak.stock_zh_a_spot_em()
        code_col = _pick_first_column(df, ("代码", "股票代码"), contains=("代码",))
        if code_col:
            row = df[df[code_col].astype(str) == stock_code]
            if not row.empty:
                record = row.iloc[0].to_dict()
                evidence = _make_evidence(
                    "realtime_quote",
                    f"price={record.get('最新价', 'N/A')}, mktcap={record.get('总市值', 'N/A')}",
                    f"akshare stock_zh_a_spot_em ({stock_code})",
                )
                return {"data": record, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
            errors.append("stock_zh_a_spot_em returned no matching row")
        else:
            errors.append("stock_zh_a_spot_em missing code column")
    except Exception as exc:
        errors.append(f"stock_zh_a_spot_em failed: {exc}")

    ef = _load_efinance()
    if ef is not None:
        try:
            quote_df = ef.stock.get_latest_quote(stock_code)
            if quote_df is not None and len(quote_df) > 0:
                record = quote_df.iloc[0].to_dict()
                evidence = _make_evidence(
                    "realtime_quote",
                    f"price={record.get('最新价', 'N/A')}, mktcap={record.get('总市值', 'N/A')}",
                    f"efinance stock.get_latest_quote ({stock_code})",
                    confidence="medium",
                )
                return {"data": record, "evidence": evidence, "status": "ok_fallback_efinance", "fetch_timestamp": now_iso()}
            errors.append("efinance get_latest_quote returned empty result")
        except Exception as exc:
            errors.append(f"efinance get_latest_quote failed: {exc}")

    try:
        profile = get_company_profile(stock_code)
        info = profile.get("data", {})
        fallback_record = {
            "代码": stock_code,
            "股票简称": info.get("股票简称", ""),
                        "最新价": safe_float(info.get("最新")),
                        "总市值": safe_float(info.get("总市值")),
                        "流通市值": safe_float(info.get("流通市值")),
                        "总股本": safe_float(info.get("总股本")),
                        "流通股": safe_float(info.get("流通股")),
            "行业": info.get("行业", ""),
        }
        if fallback_record["最新价"] is not None or fallback_record["总市值"] is not None:
            evidence = _make_evidence(
                "realtime_quote",
                f"price={fallback_record.get('最新价', 'N/A')}, mktcap={fallback_record.get('总市值', 'N/A')}",
                f"akshare stock_individual_info_em fallback ({stock_code})",
                confidence="medium",
            )
            return {"data": fallback_record, "evidence": evidence, "status": "ok_fallback_profile", "fetch_timestamp": now_iso()}
        errors.append("stock_individual_info_em fallback missing latest price and market cap")
    except Exception as exc:
        errors.append(f"stock_individual_info_em fallback failed: {exc}")

    derived_record = _derive_quote_snapshot(stock_code)
    if derived_record.get("最新价") is not None or derived_record.get("总市值") is not None:
        evidence = _make_evidence(
            "realtime_quote",
            f"price={derived_record.get('最新价', 'N/A')}, mktcap={derived_record.get('总市值', 'N/A')}",
            f"derived from stock_zh_a_hist + balance_sheet ({stock_code})",
            confidence="medium",
        )
        return {"data": derived_record, "evidence": evidence, "status": "ok_fallback_derived", "fetch_timestamp": now_iso()}

    try:
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
        history = get_daily_history(stock_code, start_date, end_date, "date,code,close,pbMRQ,peTTM")
        rows = history.get("data", [])
        if rows:
            latest = rows[-1]
            basic_info = get_stock_basic(stock_code).get("data", {})
            snapshot_record = {
                "代码": stock_code,
                "名称": basic_info.get("code_name", stock_code),
                    "最新价": safe_float(latest.get("close")),
                "最新交易日": str(latest.get("date", "")),
                    "市净率MRQ": safe_float(latest.get("pbMRQ")),
                    "滚动市盈率TTM": safe_float(latest.get("peTTM")),
                "总市值": None,
                "流通市值": None,
            }
            evidence = _make_evidence(
                "realtime_quote",
                f"price={snapshot_record.get('最新价', 'N/A')}, mktcap={snapshot_record.get('总市值', 'N/A')}",
                f"baostock daily snapshot ({stock_code}) [latest trading day snapshot, not realtime]",
                confidence="medium",
            )
            evidence["source_type"] = "baostock"
            return {
                "data": snapshot_record,
                "evidence": evidence,
                "status": "ok_fallback_baostock_daily_snapshot",
                "fetch_timestamp": now_iso(),
            }
        errors.append("baostock daily snapshot returned empty result")
    except Exception as exc:
        errors.append(f"baostock daily snapshot failed: {exc}")

    return {"data": {}, "evidence": {}, "status": f"error: {' | '.join(errors)}", "fetch_timestamp": now_iso()}


def get_income_statement(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_financial_report_sina(stock=_stock_symbol(stock_code), symbol="利润表")
        records = _recent_records(df, 8)
        evidence = _make_evidence(
            "income_statement",
            f"{len(records)} periods",
            f"akshare stock_financial_report_sina income ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def get_balance_sheet(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_financial_report_sina(stock=_stock_symbol(stock_code), symbol="资产负债表")
        records = _recent_records(df, 8)
        evidence = _make_evidence(
            "balance_sheet",
            f"{len(records)} periods",
            f"akshare stock_financial_report_sina balance ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": now_iso()}


RADAR_PARTIAL_STEPS = {
    "company_profile": get_company_profile,
    "revenue_breakdown": get_revenue_breakdown,
    "valuation_history": get_valuation_history,
    "stock_kline": get_stock_kline,
    "realtime_quote": get_realtime_quote,
}

RADAR_EXPENSIVE_STEPS = {
    "income_statement": get_income_statement,
    "balance_sheet": get_balance_sheet,
}

RADAR_ALL_STEPS = {**RADAR_PARTIAL_STEPS, **RADAR_EXPENSIVE_STEPS}

FULL_SCAN_STEPS = [
    ("company_profile", get_company_profile),
    ("financial_summary", get_financial_summary),
    ("revenue_breakdown", get_revenue_breakdown),
    ("valuation_history", get_valuation_history),
    ("stock_kline", get_stock_kline),
    ("realtime_quote", get_realtime_quote),
    ("income_statement", get_income_statement),
    ("balance_sheet", get_balance_sheet),
]


def _load_trade_days_from_baostock(
    reference_date: datetime.date | None = None,
    *,
    lookback_days: int = 14,
) -> list[str]:
    current = reference_date or datetime.date.today()
    start_day = (current - datetime.timedelta(days=lookback_days)).isoformat()
    end_day = current.isoformat()
    with baostock_adapter._session() as bs:
        records = baostock_adapter._resultset_to_records(bs.query_trade_dates(start_date=start_day, end_date=end_day))
    return [str(row.get("calendar_date", "")) for row in records if str(row.get("is_trading_day")) == "1"]


def resolve_radar_trade_date(reference_date: datetime.date | None = None) -> str:
    current = reference_date or datetime.date.today()
    try:
        trade_days = _load_trade_days_from_baostock(current)
        if trade_days:
            return trade_days[-1]
    except Exception:
        pass

    while current.weekday() >= 5:
        current -= datetime.timedelta(days=1)
    return current.isoformat()


def _radar_day_cache_file(day_cache_dir: Path, stock_code: str) -> Path:
    return day_cache_dir / f"{stock_code}.json"


def _load_radar_day_cache_fields(day_cache_dir: Path, stock_code: str) -> dict[str, Any]:
    cache_file = _radar_day_cache_file(day_cache_dir, stock_code)
    if not cache_file.exists():
        return {}
    try:
        return json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _day_cache_scan_result(cached: dict[str, Any]) -> dict[str, Any]:
    result = dict(cached)
    original_status = str(cached.get("status", ""))
    result["status"] = "ok_day_cache"
    result["original_status"] = original_status
    result["original_fetch_time"] = cached.get("fetch_timestamp", "unknown")

    evidence = dict(result.get("evidence") or {})
    description = str(evidence.get("description", "")).strip()
    suffix = f"[day_cache: original_status={original_status or 'unknown'}, original_fetch={result['original_fetch_time']}]"
    evidence["description"] = f"{description} {suffix}".strip()
    result["evidence"] = evidence
    return result


def _write_radar_day_cache_fields(day_cache_dir: Path, stock_code: str, updates: dict[str, Any]) -> None:
    day_cache_dir.mkdir(parents=True, exist_ok=True)
    target_path = _radar_day_cache_file(day_cache_dir, stock_code)
    payload = _load_radar_day_cache_fields(day_cache_dir, stock_code)
    payload.update(updates)

    temp_path = target_path.with_name(f"{target_path.name}.tmp-{os.getpid()}-{time.time_ns()}")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    try:
        for attempt in range(3):
            try:
                os.replace(temp_path, target_path)
                return
            except PermissionError:
                if attempt == 2:
                    raise
                time.sleep(0.05 * (2**attempt))
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def _check_cache_freshness(cached_results: dict[str, Any]) -> tuple[bool, float]:
    """检查缓存是否新鲜。返回 (is_fresh, age_hours)"""
    cache_ts = cached_results.get("_cache_timestamp", "")
    if not cache_ts:
        return False, float("inf")
    try:
        cache_time = datetime.datetime.fromisoformat(cache_ts)
        age = datetime.datetime.now() - cache_time
        age_hours = age.total_seconds() / 3600
        return age_hours <= CACHE_STALE_HOURS, age_hours
    except (ValueError, TypeError):
        return False, float("inf")


def _cached_scan_result(
    cached_results: dict[str, Any],
    step_name: str,
    *,
    cache_is_fresh: bool,
    cache_age_hours: float,
) -> dict[str, Any] | None:
    cached = cached_results.get(step_name, {})
    if not _is_ok_status(cached.get("status")):
        return None

    result = dict(cached)
    if cache_is_fresh:
        result["status"] = "ok_cached_fallback"
    else:
        result["status"] = "stale_cached_fallback"
        result["freshness_warning"] = f"缓存已过期 {cache_age_hours:.1f} 小时（阈值 {CACHE_STALE_HOURS}h），数据可能不准确"

    evidence = dict(result.get("evidence") or {})
    original_fetch = cached.get("fetch_timestamp", "unknown")
    evidence["description"] = f"{evidence.get('description', '')} [cache_fallback: age={cache_age_hours:.1f}h, original_fetch={original_fetch}]".strip()
    if not cache_is_fresh:
        evidence["confidence"] = "low"
        evidence["freshness_warning"] = result.get("freshness_warning", "")
    result["evidence"] = evidence
    result["original_fetch_time"] = original_fetch
    result["cache_age_hours"] = round(cache_age_hours, 1)
    return result


def _resolve_scan_step(
    stock_code: str,
    step_name: str,
    fetcher,
    *,
    cached_results: dict[str, Any] | None = None,
    retry_delays: tuple[float, ...] = (1.0, 2.0),
) -> dict[str, Any]:
    cached_results = cached_results or {}
    cache_is_fresh, cache_age_hours = _check_cache_freshness(cached_results)

    result = fetcher(stock_code)
    if _is_ok_status(result.get("status")):
        return result

    for delay in retry_delays:
        time.sleep(delay)
        retry_result = fetcher(stock_code)
        if _is_ok_status(retry_result.get("status")):
            return retry_result

    cached = _cached_scan_result(
        cached_results,
        step_name,
        cache_is_fresh=cache_is_fresh,
        cache_age_hours=cache_age_hours,
    )
    return cached or result


def run_named_scan_steps(
    stock_code: str,
    step_map: dict[str, Any],
    *,
    cached_results: dict[str, Any] | None = None,
    day_cache_dir: Path | None = None,
    retry_delays: tuple[float, ...] | None = None,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    day_cache_fields = _load_radar_day_cache_fields(day_cache_dir, stock_code) if day_cache_dir else {}
    for step_name, fetcher in step_map.items():
        cached_step = (cached_results or {}).get(step_name, {})
        if _is_ok_status(cached_step.get("status")):
            results[step_name] = cached_step
            continue

        day_cached = day_cache_fields.get(step_name, {})
        if _is_ok_status(day_cached.get("status")):
            results[step_name] = _day_cache_scan_result(day_cached)
            continue

        results[step_name] = _resolve_scan_step(
            stock_code,
            step_name,
            fetcher,
            cached_results=cached_results,
            retry_delays=retry_delays or (1.0, 2.0),
        )
        if day_cache_dir and _is_ok_status(results[step_name].get("status")):
            _write_radar_day_cache_fields(day_cache_dir, stock_code, {step_name: results[step_name]})
    return results


def run_full_scan(stock_code: str, output_dir: str | None = None) -> dict[str, Any]:
    print(f"[akshare_adapter] start scan: {stock_code}")

    results: dict[str, Any] = {}
    evidence_list: list[dict[str, Any]] = []
    cached_results: dict[str, Any] = {}
    cache_file = os.path.join(output_dir, "akshare_scan.json") if output_dir else ""
    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached_results = json.load(f)
        except Exception:
            cached_results = {}

    for name, func in FULL_SCAN_STEPS:
        print(f"  [{name}] ...", end=" ")
        result = _resolve_scan_step(
            stock_code,
            name,
            func,
            cached_results=cached_results,
        )
        results[name] = result
        if result.get("evidence"):
            evidence_list.append(result["evidence"])
        print(result["status"])

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        # 写入缓存时间戳
        results["_cache_timestamp"] = now_iso()
        with open(os.path.join(output_dir, "akshare_scan.json"), "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        with open(os.path.join(output_dir, "akshare_evidence.json"), "w", encoding="utf-8") as f:
            json.dump(evidence_list, f, ensure_ascii=False, indent=2, default=str)
        print(f"[akshare_adapter] saved to {output_dir}")

        # 打印陈旧数据警告
        stale_fields = [name for name, result in results.items() if not name.startswith("_") and "stale" in str(result.get("status", "")).lower()]
        if stale_fields:
            print(f"[akshare_adapter] [WARNING] {len(stale_fields)} 个字段使用了过期缓存: {stale_fields}")

    return results


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    out = sys.argv[2] if len(sys.argv) > 2 else str(Path(__file__).resolve().parents[5] / "data" / "raw" / code)
    run_full_scan(code, out)
