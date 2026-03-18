"""Tushare-backed backtest input builder for the deterministic VCRF engine."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from adapters.tushare_adapter import (
    discover_tushare_universe_tickers,
    query_adj_factor,
    query_balancesheet,
    query_cashflow,
    query_daily,
    query_daily_basic,
    query_fina_indicator,
    query_fina_mainbz,
    query_income,
    query_stock_basic,
    query_stock_company,
)
from engines.public_backtest_dataset_engine import (
    DEFAULT_MIN_TURNOVER,
    OUTPUT_SIGNAL_COLUMNS,
    _build_kline_snapshot,
    _build_valuation_snapshot,
    _composite_total_score,
    _next_trading_day,
    _state_label,
    _tradable_flag,
    filter_records_as_of,
    month_end_trade_dates,
)
from engines.valuation_engine import build_three_case_valuation
from utils.value_utils import normalize_text, safe_float
from validators.universal_gate import evaluate_universal_gates


def _to_ts_date(value: str) -> str:
    return str(value).replace("-", "").strip()


def _to_ts_code(ticker: str) -> str:
    text = normalize_text(ticker).upper()
    if "." in text:
        return text
    if text.startswith("6"):
        return f"{text}.SH"
    if text.startswith(("8", "4")):
        return f"{text}.BJ"
    return f"{text}.SZ"


def _normalize_tushare_daily_bars(
    daily_records: list[dict[str, Any]],
    daily_basic_records: list[dict[str, Any]],
    adj_factor_records: list[dict[str, Any]],
    ticker: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    bars = pd.DataFrame(daily_records or [])
    if bars.empty:
        empty_bars = pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume", "amount"])
        empty_basic = pd.DataFrame(columns=["date", "ticker", "total_mv", "circ_mv", "pb", "turnover_rate"])
        return empty_bars, empty_basic

    bars = bars.rename(
        columns={
            "trade_date": "date",
            "vol": "volume",
        }
    )
    bars["date"] = pd.to_datetime(bars["date"], format="%Y%m%d", errors="coerce").dt.normalize()
    bars["ticker"] = str(ticker).upper()
    for column in ("open", "high", "low", "close", "volume", "amount"):
        if column not in bars.columns:
            bars[column] = pd.NA
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    adj_factor = pd.DataFrame(adj_factor_records or [])
    if not adj_factor.empty:
        adj_factor = adj_factor.rename(columns={"trade_date": "date"})
        adj_factor["date"] = pd.to_datetime(adj_factor["date"], format="%Y%m%d", errors="coerce").dt.normalize()
        adj_factor["adj_factor"] = pd.to_numeric(adj_factor.get("adj_factor"), errors="coerce")
        adj_factor = adj_factor.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        if not adj_factor.empty:
            latest_adj_factor = pd.to_numeric(adj_factor["adj_factor"], errors="coerce").dropna()
            latest_adj_factor = float(latest_adj_factor.iloc[-1]) if not latest_adj_factor.empty else None
            bars = bars.merge(adj_factor[["date", "adj_factor"]], on="date", how="left")
            if latest_adj_factor not in (None, 0):
                price_scale = bars["adj_factor"].fillna(latest_adj_factor) / latest_adj_factor
                for column in ("open", "high", "low", "close"):
                    bars[column] = bars[column] * price_scale
    if "volume" in bars.columns:
        bars["volume"] = bars["volume"] * 100.0
    if "amount" in bars.columns:
        bars["amount"] = bars["amount"] * 1000.0
    bars = bars.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    bars_out = bars[["date", "ticker", "open", "high", "low", "close", "volume", "amount"]]

    basic = pd.DataFrame(daily_basic_records or [])
    if basic.empty:
        empty_basic = pd.DataFrame(columns=["date", "ticker", "total_mv", "circ_mv", "pb", "turnover_rate"])
        return bars_out, empty_basic
    basic = basic.rename(columns={"trade_date": "date", "turnover_rate": "turnover_rate"})
    basic["date"] = pd.to_datetime(basic["date"], format="%Y%m%d", errors="coerce").dt.normalize()
    basic["ticker"] = str(ticker).upper()
    for column in ("total_mv", "circ_mv", "pb", "turnover_rate"):
        if column not in basic.columns:
            basic[column] = pd.NA
        basic[column] = pd.to_numeric(basic[column], errors="coerce")
    basic["total_mv"] = basic["total_mv"] * 10_000.0
    basic["circ_mv"] = basic["circ_mv"] * 10_000.0
    basic_out = basic[["date", "ticker", "total_mv", "circ_mv", "pb", "turnover_rate"]].sort_values("date").reset_index(drop=True)
    return bars_out, basic_out


def _normalize_tushare_income_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "报告日": row.get("end_date"),
                "公告日期": row.get("f_ann_date") or row.get("ann_date"),
                "营业总收入": safe_float(row.get("total_revenue") or row.get("revenue")),
                "营业收入": safe_float(row.get("revenue")),
                "归属于母公司所有者的净利润": safe_float(row.get("n_income_attr_p") or row.get("n_income")),
                "资产减值损失": safe_float(row.get("assets_impair_loss")),
                "信用减值损失": safe_float(row.get("credit_impa_loss")),
                "商誉减值": safe_float(row.get("goodwill")),
            }
        )
    return normalized


def _normalize_tushare_balance_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        total_share = safe_float(row.get("total_share"))
        normalized.append(
            {
                **row,
                "报告日": row.get("end_date"),
                "公告日期": row.get("f_ann_date") or row.get("ann_date"),
                "资产总计": safe_float(row.get("total_assets")),
                "归属于母公司所有者权益合计": safe_float(row.get("total_hldr_eqy_exc_min_int") or row.get("total_hldr_eqy_inc_min_int")),
                "货币资金": safe_float(row.get("money_cap")),
                "交易性金融资产": safe_float(row.get("trad_asset")),
                "短期借款": safe_float(row.get("st_borr")),
                "一年内到期的非流动负债": safe_float(row.get("non_cur_liab_due_1y")),
                "实收资本(或股本)": total_share,
            }
        )
    return normalized


def _normalize_tushare_cashflow_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "报告日": row.get("end_date"),
                "公告日期": row.get("f_ann_date") or row.get("ann_date"),
                "经营活动产生的现金流量净额": safe_float(row.get("n_cashflow_act")),
            }
        )
    return normalized


def _normalize_tushare_revenue_breakdown(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        sales = safe_float(row.get("bz_sales"))
        cost = safe_float(row.get("bz_cost"))
        profit = safe_float(row.get("bz_profit"))
        gross_margin = None
        if sales not in (None, 0) and profit is not None:
            gross_margin = profit / sales
        elif sales not in (None, 0) and cost is not None:
            gross_margin = (sales - cost) / sales
        normalized.append(
            {
                **row,
                "报告期": row.get("end_date"),
                "主营构成": row.get("bz_item"),
                "主营收入": sales,
                "主营成本": cost,
                "主营利润": profit,
                "毛利率": gross_margin,
            }
        )
    return normalized


def _normalize_tushare_profile(
    ticker: str,
    basic_rows: list[dict[str, Any]],
    company_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    basic = (basic_rows or [{}])[0]
    company = (company_rows or [{}])[0]
    return {
        **basic,
        **company,
        "股票代码": ticker,
        "股票简称": basic.get("name") or company.get("fullname") or ticker,
        "行业": basic.get("industry"),
        "上市时间": basic.get("list_date"),
        "退市日期": basic.get("delist_date"),
        "主营业务": company.get("main_business"),
        "经营范围": company.get("business_scope"),
        "公司介绍": company.get("introduction"),
        "地域": basic.get("area") or company.get("province"),
    }


def _build_tushare_quote_snapshot(
    profile: dict[str, Any],
    latest_close: float,
    latest_basic: dict[str, Any],
) -> dict[str, Any]:
    total_mv = safe_float(latest_basic.get("total_mv"))
    circ_mv = safe_float(latest_basic.get("circ_mv"))
    total_shares = total_mv / latest_close if total_mv not in (None, 0) and latest_close not in (None, 0) else None
    float_shares = circ_mv / latest_close if circ_mv not in (None, 0) and latest_close not in (None, 0) else None
    return {
        "股票代码": profile.get("股票代码"),
        "股票简称": profile.get("股票简称"),
        "行业": profile.get("行业"),
        "最新价": latest_close,
        "总股本": total_shares,
        "流通股": float_shares,
        "总市值": total_mv,
        "流通市值": circ_mv,
    }


def fetch_tushare_history_bundle(ticker: str, start_date: str, end_date: str) -> dict[str, Any]:
    ts_code = _to_ts_code(ticker)
    query_start = _to_ts_date(start_date)
    query_end = _to_ts_date(end_date)

    stock_basic = query_stock_basic(
        ts_code=ts_code,
        fields="ts_code,symbol,name,area,industry,market,list_status,list_date,delist_date",
    )
    stock_company = query_stock_company(
        ts_code=ts_code,
        fields="ts_code,chairman,manager,secretary,province,city,introduction,website,email,office,business_scope,main_business",
    )
    revenue_breakdown = query_fina_mainbz(ts_code=ts_code, start_date=query_start, end_date=query_end, type="P")
    income_statement = query_income(ts_code=ts_code, start_date=query_start, end_date=query_end)
    balance_sheet = query_balancesheet(ts_code=ts_code, start_date=query_start, end_date=query_end)
    cashflow_statement = query_cashflow(ts_code=ts_code, start_date=query_start, end_date=query_end)
    fina_indicator = query_fina_indicator(ts_code=ts_code, start_date=query_start, end_date=query_end)
    daily = query_daily(ts_code=ts_code, start_date=query_start, end_date=query_end)
    adj_factor = query_adj_factor(ts_code=ts_code, start_date=query_start, end_date=query_end)
    daily_basic = query_daily_basic(
        ts_code=ts_code,
        start_date=query_start,
        end_date=query_end,
        fields="ts_code,trade_date,total_mv,circ_mv,pb,turnover_rate",
    )
    bars_df, basic_df = _normalize_tushare_daily_bars(
        daily.get("data", []) or [],
        daily_basic.get("data", []) or [],
        adj_factor.get("data", []) or [],
        ticker,
    )
    profile = _normalize_tushare_profile(str(ticker).upper(), stock_basic.get("data", []) or [], stock_company.get("data", []) or [])

    return {
        "ticker": str(ticker).upper(),
        "company_profile": {"data": profile},
        "revenue_breakdown": {"data": _normalize_tushare_revenue_breakdown(revenue_breakdown.get("data", []) or [])},
        "income_statement": {"data": _normalize_tushare_income_records(income_statement.get("data", []) or [])},
        "balance_sheet": {"data": _normalize_tushare_balance_records(balance_sheet.get("data", []) or [])},
        "cashflow_statement": {"data": _normalize_tushare_cashflow_records(cashflow_statement.get("data", []) or [])},
        "fina_indicator": {"data": fina_indicator.get("data", []) or []},
        "daily_bars": bars_df,
        "daily_basic": basic_df,
    }


def _build_scan_data_as_of(bundle: dict[str, Any], signal_date: pd.Timestamp) -> dict[str, Any] | None:
    daily_bars = bundle.get("daily_bars")
    if daily_bars is None or daily_bars.empty:
        return None
    bars_until_date = daily_bars[daily_bars["date"] <= signal_date].copy()
    if bars_until_date.empty:
        return None

    profile = (bundle.get("company_profile") or {}).get("data", {}) or {}
    revenue_breakdown = (bundle.get("revenue_breakdown") or {}).get("data", []) or []
    income_records = filter_records_as_of((bundle.get("income_statement") or {}).get("data", []) or [], signal_date)
    balance_records = filter_records_as_of((bundle.get("balance_sheet") or {}).get("data", []) or [], signal_date)
    cashflow_records = filter_records_as_of((bundle.get("cashflow_statement") or {}).get("data", []) or [], signal_date)
    if not income_records or not balance_records:
        return None

    kline_snapshot = _build_kline_snapshot(bars_until_date)
    latest_close = safe_float(kline_snapshot.get("latest_close"))
    if latest_close in (None, 0):
        return None

    daily_basic = bundle.get("daily_basic")
    daily_basic_until_date = daily_basic[daily_basic["date"] <= signal_date].copy() if isinstance(daily_basic, pd.DataFrame) else pd.DataFrame()
    latest_basic = daily_basic_until_date.sort_values("date").iloc[-1].to_dict() if not daily_basic_until_date.empty else {}
    quote_snapshot = _build_tushare_quote_snapshot(profile, latest_close, latest_basic)
    valuation_snapshot = _build_valuation_snapshot(bars_until_date, balance_records, safe_float(quote_snapshot.get("总股本")))
    pb = safe_float(latest_basic.get("pb"))
    if pb is not None:
        valuation_snapshot["pb"] = pb

    return {
        "company_profile": {"data": profile},
        "revenue_breakdown": {"data": revenue_breakdown},
        "income_statement": {"data": income_records},
        "balance_sheet": {"data": balance_records},
        "cashflow_statement": {"data": cashflow_records},
        "realtime_quote": {"data": quote_snapshot},
        "stock_kline": {"data": kline_snapshot},
        "valuation_history": {"data": valuation_snapshot},
        "event_signals": {},
    }


def build_tushare_backtest_inputs(
    *,
    tickers: list[str],
    start_date: str,
    end_date: str,
    bundle_provider: Callable[[str, str, str], dict[str, Any]] = fetch_tushare_history_bundle,
    min_turnover: float = DEFAULT_MIN_TURNOVER,
) -> dict[str, Any]:
    normalized_tickers = [normalize_text(item).upper() for item in tickers if normalize_text(item)]
    all_bars: list[pd.DataFrame] = []
    signal_rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    bundles: dict[str, dict[str, Any]] = {}

    for ticker in normalized_tickers:
        bundle = bundle_provider(ticker, start_date, end_date)
        daily_bars = bundle.get("daily_bars", pd.DataFrame())
        if daily_bars is None or daily_bars.empty:
            skipped.append({"ticker": ticker, "reason": "missing_daily_bars"})
            continue
        bundle["daily_bars"] = daily_bars
        bundles[ticker] = bundle
        all_bars.append(daily_bars)

    if not all_bars:
        return {
            "signals_month_end": pd.DataFrame(columns=OUTPUT_SIGNAL_COLUMNS),
            "daily_bars": pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume", "amount"]),
            "manifest": {"tickers": normalized_tickers, "skipped": skipped, "rows": {"signals_month_end": 0, "daily_bars": 0}},
        }

    merged_bars = pd.concat(all_bars, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
    trading_days = pd.DatetimeIndex(sorted(merged_bars["date"].unique()))

    for ticker, bundle in bundles.items():
        daily_bars = bundle["daily_bars"]
        signal_dates = month_end_trade_dates(daily_bars)
        profile = (bundle.get("company_profile") or {}).get("data", {}) or {}
        for signal_date in signal_dates:
            scan_data = _build_scan_data_as_of(bundle, signal_date)
            if scan_data is None:
                continue
            gate_result = evaluate_universal_gates(ticker, scan_data)
            valuation_result = build_three_case_valuation(ticker, scan_data, gate_result["driver_stack"])
            signal_rows.append(
                {
                    "signal_date": signal_date.normalize(),
                    "effective_date": _next_trading_day(signal_date, trading_days),
                    "ticker": ticker,
                    "vcrf_state": _state_label(gate_result.get("position_state")),
                    "floor_price": (valuation_result.get("floor_case") or {}).get("implied_price"),
                    "recognition_price": (valuation_result.get("recognition_case") or {}).get("implied_price"),
                    "total_score": _composite_total_score(gate_result),
                    "tradable_flag": _tradable_flag(scan_data, profile, min_turnover=min_turnover),
                    "signal_version": "tushare_pit_v1",
                    "underwrite_score": safe_float((gate_result.get("underwrite_axis") or {}).get("score")),
                    "realization_score": safe_float((gate_result.get("realization_axis") or {}).get("score")),
                    "position_state": normalize_text(gate_result.get("position_state")).lower(),
                    "primary_type": normalize_text((gate_result.get("driver_stack") or {}).get("primary_type")).lower(),
                    "sector_route": normalize_text((gate_result.get("driver_stack") or {}).get("sector_route")).lower(),
                    "announcement_date": signal_date.normalize(),
                    "reject_reason": "; ".join(gate_result.get("hard_vetos", []) or []),
                }
            )

    signals_month_end = pd.DataFrame(signal_rows)
    if signals_month_end.empty:
        signals_month_end = pd.DataFrame(columns=OUTPUT_SIGNAL_COLUMNS)
    else:
        signals_month_end = signals_month_end.sort_values(["effective_date", "total_score", "ticker"], ascending=[True, False, True]).reset_index(drop=True)

    return {
        "signals_month_end": signals_month_end,
        "daily_bars": merged_bars,
        "manifest": {
            "tickers": sorted(bundles.keys()),
            "skipped": skipped,
            "rows": {
                "signals_month_end": int(len(signals_month_end)),
                "daily_bars": int(len(merged_bars)),
            },
            "limitations": [
                "tushare_pit_v1",
                "requires TUSHARE_TOKEN and a reachable Tushare Pro account",
                "revenue breakdown depends on fina_mainbz availability per ticker",
                "order execution still follows the deterministic local backtest protocol",
            ],
        },
    }
