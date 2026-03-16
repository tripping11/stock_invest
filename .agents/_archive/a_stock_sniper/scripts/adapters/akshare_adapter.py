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
from typing import Any

import akshare as ak
import pandas as pd

from utils.research_utils import CACHE_STALE_HOURS
from utils.vendor_support import ensure_vendor_path


# ── 缓存新鲜度阈值（小时）──────────────────────────────
def _now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_iso() -> str:
    return datetime.datetime.now().isoformat()


def _make_evidence(field: str, value: Any, source_desc: str, url: str = "", confidence: str = "medium") -> dict[str, Any]:
    return {
        "field_name": field,
        "value": value,
        "source_tier": 1,
        "source_type": "akshare",
        "source_url": url,
        "description": source_desc,
        "fetch_time": _now(),
        "confidence": confidence,
    }


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


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _estimate_consolidation_months(monthly_closes: pd.Series) -> int:
    """Estimate how long the stock has stayed in a broad sideways band near the latest price."""
    if monthly_closes is None or monthly_closes.empty:
        return 0

    latest_close = _safe_float(monthly_closes.iloc[-1])
    if latest_close in (None, 0):
        return 0

    lower_bound = latest_close * 0.75
    upper_bound = latest_close * 1.35
    months = 0
    for value in reversed(monthly_closes.tolist()):
        close = _safe_float(value)
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
        value = _safe_float(latest.get(key))
        if value is not None and value > 0:
            return value
    return None


def _derive_quote_snapshot(stock_code: str) -> dict[str, Any]:
    kline_result = get_stock_kline(stock_code, period="daily", years=1)
    latest_close = _safe_float(kline_result.get("data", {}).get("latest_close"))
    share_capital = _extract_latest_share_capital(stock_code)
    market_cap = latest_close * share_capital if latest_close is not None and share_capital is not None else None
    return {
        "代码": stock_code,
        "最新价": latest_close,
        "总股本": share_capital,
        "流通股": share_capital,
        "总市值": market_cap,
        "流通市值": market_cap,
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
        return {"data": info, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
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
                    return {"data": info, "evidence": evidence, "status": "ok_fallback_efinance", "fetch_timestamp": _now_iso()}
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
            return {"data": derived, "evidence": evidence, "status": "ok_fallback_derived", "fetch_timestamp": _now_iso()}
        return {"data": {}, "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


def get_financial_summary(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2018")
        records = _recent_records(df, 10)
        evidence = _make_evidence(
            "financial_summary",
            f"{len(records)} periods",
            f"akshare stock_financial_analysis_indicator ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


def get_revenue_breakdown(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_zygc_em(symbol=f"{_market_prefix(stock_code)}{stock_code}")
        records = _recent_records(df, 50)
        evidence = _make_evidence(
            "revenue_breakdown",
            f"{len(records)} segments",
            f"akshare stock_zygc_em ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


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
                "fetch_timestamp": _now_iso(),
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
        return {"data": latest, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": {}, "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


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
                        "fetch_timestamp": _now_iso(),
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
                    "fetch_timestamp": _now_iso(),
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
        return {"data": summary, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": {}, "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


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
                return {"data": record, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
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
                return {"data": record, "evidence": evidence, "status": "ok_fallback_efinance", "fetch_timestamp": _now_iso()}
            errors.append("efinance get_latest_quote returned empty result")
        except Exception as exc:
            errors.append(f"efinance get_latest_quote failed: {exc}")

    try:
        profile = get_company_profile(stock_code)
        info = profile.get("data", {})
        fallback_record = {
            "代码": stock_code,
            "股票简称": info.get("股票简称", ""),
            "最新价": _safe_float(info.get("最新")),
            "总市值": _safe_float(info.get("总市值")),
            "流通市值": _safe_float(info.get("流通市值")),
            "总股本": _safe_float(info.get("总股本")),
            "流通股": _safe_float(info.get("流通股")),
            "行业": info.get("行业", ""),
        }
        if fallback_record["最新价"] is not None or fallback_record["总市值"] is not None:
            evidence = _make_evidence(
                "realtime_quote",
                f"price={fallback_record.get('最新价', 'N/A')}, mktcap={fallback_record.get('总市值', 'N/A')}",
                f"akshare stock_individual_info_em fallback ({stock_code})",
                confidence="medium",
            )
            return {"data": fallback_record, "evidence": evidence, "status": "ok_fallback_profile", "fetch_timestamp": _now_iso()}
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
        return {"data": derived_record, "evidence": evidence, "status": "ok_fallback_derived", "fetch_timestamp": _now_iso()}

    return {"data": {}, "evidence": {}, "status": f"error: {' | '.join(errors)}", "fetch_timestamp": _now_iso()}


def get_income_statement(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_financial_report_sina(stock=_stock_symbol(stock_code), symbol="利润表")
        records = _recent_records(df, 8)
        evidence = _make_evidence(
            "income_statement",
            f"{len(records)} periods",
            f"akshare stock_financial_report_sina income ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


def get_balance_sheet(stock_code: str) -> dict[str, Any]:
    try:
        df = ak.stock_financial_report_sina(stock=_stock_symbol(stock_code), symbol="资产负债表")
        records = _recent_records(df, 8)
        evidence = _make_evidence(
            "balance_sheet",
            f"{len(records)} periods",
            f"akshare stock_financial_report_sina balance ({stock_code})",
        )
        return {"data": records, "evidence": evidence, "status": "ok", "fetch_timestamp": _now_iso()}
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}", "fetch_timestamp": _now_iso()}


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

    cache_is_fresh, cache_age_hours = _check_cache_freshness(cached_results)

    steps = [
        ("company_profile", get_company_profile),
        ("financial_summary", get_financial_summary),
        ("revenue_breakdown", get_revenue_breakdown),
        ("valuation_history", get_valuation_history),
        ("stock_kline", get_stock_kline),
        ("realtime_quote", get_realtime_quote),
        ("income_statement", get_income_statement),
        ("balance_sheet", get_balance_sheet),
    ]

    for name, func in steps:
        print(f"  [{name}] ...", end=" ")
        result = func(stock_code)
        if not _is_ok_status(result.get("status")):
            for retry_idx in range(2):
                time.sleep(1.0 + retry_idx)
                retry_result = func(stock_code)
                if _is_ok_status(retry_result.get("status")):
                    result = retry_result
                    break
            if not _is_ok_status(result.get("status")):
                cached = cached_results.get(name, {})
                if _is_ok_status(cached.get("status")):
                    result = dict(cached)
                    # 区分新鲜缓存和陈旧缓存
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
        results[name] = result
        if result.get("evidence"):
            evidence_list.append(result["evidence"])
        print(result["status"])

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        # 写入缓存时间戳
        results["_cache_timestamp"] = _now_iso()
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
