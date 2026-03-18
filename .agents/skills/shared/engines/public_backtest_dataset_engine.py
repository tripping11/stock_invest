"""Approximate public-source backtest input builder for watchlist-scale VCRF runs."""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from adapters.akshare_adapter import (
    get_balance_sheet,
    get_cashflow_statement,
    get_company_profile,
    get_income_statement,
    get_revenue_breakdown,
)
from adapters.baostock_adapter import get_all_a_share_stocks, get_daily_history
from adapters.provider_router import load_scan_cache
from engines.valuation_engine import build_three_case_valuation
from utils.financial_snapshot import extract_market_cap
from utils.value_utils import normalize_text, safe_float
from validators.universal_gate import evaluate_universal_gates


DEFAULT_MIN_TURNOVER = 15_000_000.0
OUTPUT_SIGNAL_COLUMNS = [
    "signal_date",
    "effective_date",
    "ticker",
    "vcrf_state",
    "floor_price",
    "recognition_price",
    "total_score",
    "tradable_flag",
    "signal_version",
    "underwrite_score",
    "realization_score",
    "position_state",
    "primary_type",
    "sector_route",
    "announcement_date",
    "reject_reason",
]
REPO_ROOT = Path(__file__).resolve().parents[4]


def discover_local_watchlist_tickers(repo_root: Path) -> list[str]:
    tickers: set[str] = set()
    for relative in ("evidence", "data/raw", "data/processed"):
        base = repo_root / relative
        if not base.exists():
            continue
        for child in base.iterdir():
            name = child.name.strip()
            if child.is_dir() and len(name) == 6 and name.isdigit():
                tickers.add(name)
    return sorted(tickers)


def discover_baostock_universe_tickers(limit: int | None = None) -> list[str]:
    result = get_all_a_share_stocks()
    rows = result.get("data", []) or []
    tickers = [normalize_text(row.get("code")) for row in rows if normalize_text(row.get("trade_status")) == "1"]
    if limit is not None and limit > 0:
        return tickers[:limit]
    return tickers


def discover_public_financial_usable_tickers(candidate_tickers: list[str], target_count: int | None = None) -> list[str]:
    usable: list[str] = []
    for ticker in candidate_tickers:
        income = get_income_statement(ticker)
        balance = get_balance_sheet(ticker)
        cashflow = get_cashflow_statement(ticker)
        if income.get("data") and balance.get("data"):
            usable.append(normalize_text(ticker).upper())
        if target_count is not None and target_count > 0 and len(usable) >= target_count:
            break
    return usable


def month_end_trade_dates(daily_bars: pd.DataFrame) -> pd.DatetimeIndex:
    if daily_bars is None or daily_bars.empty:
        return pd.DatetimeIndex([])
    dates = pd.to_datetime(daily_bars["date"]).dt.normalize()
    return pd.DatetimeIndex(pd.Series(dates).groupby(dates.dt.to_period("M")).max().tolist())


def _parse_date(value: Any) -> pd.Timestamp | None:
    text = normalize_text(value)
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 8:
        return pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(text, errors="coerce")


def _statutory_available_date(report_date: pd.Timestamp) -> pd.Timestamp:
    month_day = (report_date.month, report_date.day)
    if month_day == (3, 31):
        return report_date + pd.Timedelta(days=30)
    if month_day == (6, 30):
        return report_date + pd.Timedelta(days=60)
    if month_day == (9, 30):
        return report_date + pd.Timedelta(days=30)
    if month_day == (12, 31):
        return report_date + pd.Timedelta(days=120)
    return report_date + pd.Timedelta(days=45)


def _record_available_date(row: dict[str, Any]) -> pd.Timestamp | None:
    report_date = None
    for key in ("报告日", "报告期", "截止日期", "报告日期", "日期"):
        report_date = _parse_date(row.get(key))
        if report_date is not None:
            break
    if report_date is None or pd.isna(report_date):
        return None

    announcement_date = _parse_date(row.get("公告日期"))
    if announcement_date is not None and not pd.isna(announcement_date):
        if report_date <= announcement_date <= report_date + pd.Timedelta(days=365):
            return announcement_date.normalize()
    return _statutory_available_date(report_date.normalize())


def filter_records_as_of(records: list[dict[str, Any]], as_of_date: Any) -> list[dict[str, Any]]:
    cutoff = pd.Timestamp(as_of_date).normalize()
    filtered: list[dict[str, Any]] = []
    for row in records or []:
        available_date = _record_available_date(row)
        if available_date is not None and available_date <= cutoff:
            filtered.append(dict(row))
    return filtered


def _rolling_mean(series: pd.Series, window: int) -> float | None:
    if series.empty:
        return None
    tail = series.tail(window).dropna()
    if tail.empty:
        return None
    return float(tail.mean())


def _estimate_consolidation_months(monthly_closes: pd.Series) -> int:
    if monthly_closes.empty:
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


def _count_pulse_volume_events(close_series: pd.Series, volume_series: pd.Series) -> int:
    if len(close_series) < 121 or len(volume_series) < 121:
        return 0
    rolling_volume = volume_series.rolling(120).mean().shift(1)
    daily_returns = close_series.pct_change()
    pulse_mask = (volume_series > rolling_volume * 2.5) & daily_returns.between(-0.02, 0.04, inclusive="both")
    return int(pulse_mask.tail(30).fillna(False).sum())


def _latest_share_count(balance_records: list[dict[str, Any]], profile: dict[str, Any]) -> float | None:
    for row in reversed(balance_records or []):
        for key in ("实收资本(或股本)", "股本", "总股本"):
            value = safe_float(row.get(key))
            if value not in (None, 0):
                return value
    for key in ("总股本", "股本"):
        value = safe_float(profile.get(key))
        if value not in (None, 0):
            return value
    return None


def _float_share_count(profile: dict[str, Any]) -> float | None:
    for key in ("流通股", "流通股本"):
        value = safe_float(profile.get(key))
        if value not in (None, 0):
            return value
    return None


def _build_valuation_snapshot(
    bars_until_date: pd.DataFrame,
    balance_records: list[dict[str, Any]],
    share_count: float | None,
) -> dict[str, Any]:
    if bars_until_date.empty or share_count in (None, 0):
        return {}
    latest_close = safe_float(bars_until_date["close"].iloc[-1])
    if latest_close in (None, 0):
        return {}
    latest_balance = balance_records[-1] if balance_records else {}
    equity = None
    for key in ("归属于母公司股东权益合计", "归属于母公司所有者权益合计", "所有者权益(或股东权益)合计"):
        equity = safe_float(latest_balance.get(key))
        if equity is not None:
            break
    if equity in (None, 0):
        return {}
    bvps = equity / share_count
    if bvps in (None, 0):
        return {}
    pb_series = bars_until_date["close"] / bvps
    current_pb = float(pb_series.iloc[-1])
    return {
        "pb": round(current_pb, 4),
        "pb_percentile": round(float((pb_series < current_pb).sum() / len(pb_series) * 100), 2),
        "pb_min": round(float(pb_series.min()), 4),
        "pb_max": round(float(pb_series.max()), 4),
        "pb_median": round(float(pb_series.median()), 4),
        "latest_close": round(float(latest_close), 4),
    }


def _build_kline_snapshot(bars_until_date: pd.DataFrame) -> dict[str, Any]:
    ordered = bars_until_date.sort_values("date").reset_index(drop=True)
    latest_close = float(ordered["close"].iloc[-1])
    latest_date = pd.Timestamp(ordered["date"].iloc[-1]).normalize()
    rolling_window = ordered[ordered["date"] >= latest_date - pd.DateOffset(years=5)].copy()
    if rolling_window.empty:
        rolling_window = ordered
    high_5y = float(rolling_window["close"].max())
    low_5y = float(rolling_window["close"].min())
    summary = {
        "latest_close": latest_close,
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "high_5y": high_5y,
        "low_5y": low_5y,
        "current_vs_high": round(latest_close / high_5y * 100, 1) if high_5y else None,
        "current_vs_5yr_high": round(latest_close / high_5y * 100, 1) if high_5y else None,
        "drawdown_from_5yr_high_pct": round(100 - latest_close / high_5y * 100, 1) if high_5y else None,
        "total_bars": len(ordered),
    }
    monthly_closes = ordered.set_index("date")["close"].resample("ME").last().dropna()
    summary["consolidation_months"] = _estimate_consolidation_months(monthly_closes)
    summary["avg_vol_1y"] = _rolling_mean(ordered["volume"], 252)
    summary["avg_vol_20d"] = _rolling_mean(ordered["volume"], 20)
    summary["avg_vol_120d"] = _rolling_mean(ordered["volume"], 120)
    if summary["avg_vol_20d"] not in (None, 0) and summary["avg_vol_120d"] not in (None, 0):
        summary["volume_ratio_20_vs_120"] = round(summary["avg_vol_20d"] / summary["avg_vol_120d"], 2)
    summary["avg_turnover_1y"] = _rolling_mean(ordered["amount"], 252)
    summary["avg_turnover_20d"] = _rolling_mean(ordered["amount"], 20)
    summary["avg_turnover_120d"] = _rolling_mean(ordered["amount"], 120)
    summary["pulse_volume_events_30d"] = _count_pulse_volume_events(ordered["close"], ordered["volume"])
    return summary


def _build_quote_snapshot(
    profile: dict[str, Any],
    latest_close: float,
    share_count: float | None,
    float_share_count: float | None,
) -> dict[str, Any]:
    total_market_cap = latest_close * share_count if share_count not in (None, 0) else None
    float_market_cap = latest_close * float_share_count if float_share_count not in (None, 0) else None
    return {
        "代码": normalize_text(profile.get("代码")),
        "股票简称": normalize_text(profile.get("股票简称") or profile.get("名称")),
        "行业": normalize_text(profile.get("行业") or profile.get("所属行业")),
        "最新价": latest_close,
        "总股本": share_count,
        "流通股": float_share_count,
        "总市值": total_market_cap,
        "流通市值": float_market_cap,
    }


def _next_trading_day(signal_date: pd.Timestamp, trading_days: pd.DatetimeIndex) -> pd.Timestamp:
    index = trading_days.searchsorted(signal_date.normalize(), side="right")
    if index >= len(trading_days):
        return signal_date.normalize()
    return pd.Timestamp(trading_days[index]).normalize()


def _state_label(position_state: Any) -> str:
    mapping = {
        "reject": "REJECT",
        "cold_storage": "COLD_STORAGE",
        "cold": "COLD_STORAGE",
        "ready": "READY",
        "attack": "ATTACK",
        "harvest": "HARVEST",
    }
    return mapping.get(normalize_text(position_state).lower(), "REJECT")


def _composite_total_score(gate_result: dict[str, Any]) -> float:
    underwrite = safe_float((gate_result.get("underwrite_axis") or {}).get("score")) or 0.0
    realization = safe_float((gate_result.get("realization_axis") or {}).get("score")) or 0.0
    return round(underwrite * 0.6 + realization * 0.4, 2)


def _tradable_flag(scan_data: dict[str, Any], profile: dict[str, Any], *, min_turnover: float) -> int:
    kline = (scan_data.get("stock_kline") or {}).get("data", {}) or {}
    avg_turnover_20d = safe_float(kline.get("avg_turnover_20d")) or 0.0
    combined_name = " ".join(
        text
        for text in (
            normalize_text(profile.get("股票简称")),
            normalize_text(profile.get("名称")),
        )
        if text
    ).upper()
    if "ST" in combined_name:
        return 0
    return 1 if avg_turnover_20d >= min_turnover else 0


def _normalize_daily_bars(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume", "amount"])
    bars = df.copy()
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()
    bars["ticker"] = str(ticker).upper()
    for column in ("open", "high", "low", "close", "volume", "amount"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = bars.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    if "volume" not in bars.columns:
        bars["volume"] = pd.NA
    if "amount" not in bars.columns:
        bars["amount"] = pd.NA
    return bars[["date", "ticker", "open", "high", "low", "close", "volume", "amount"]]


def _load_local_cached_scan(ticker: str, repo_root: Path | None = None) -> dict[str, Any]:
    base = (repo_root or REPO_ROOT) / "data" / "raw" / str(ticker).upper()
    return load_scan_cache(base)


def _prefer_live_or_cache(live_result: dict[str, Any], cached_scan: dict[str, Any], key: str) -> dict[str, Any]:
    live_data = (live_result or {}).get("data")
    if isinstance(live_data, list) and live_data:
        return live_result
    if isinstance(live_data, dict) and live_data:
        return live_result
    cached_result = cached_scan.get(key, {}) if isinstance(cached_scan, dict) else {}
    cached_data = cached_result.get("data")
    if isinstance(cached_data, list) and cached_data:
        return cached_result
    if isinstance(cached_data, dict) and cached_data:
        return cached_result
    return live_result


def fetch_public_history_bundle(ticker: str, start_date: str, end_date: str, *, prefer_local_cache: bool = False) -> dict[str, Any]:
    history = get_daily_history(ticker, start_date, end_date, "date,open,high,low,close,volume,amount")
    daily_bars = _normalize_daily_bars(pd.DataFrame(history.get("data", []) or []), ticker)
    cached_scan = _load_local_cached_scan(ticker)
    if prefer_local_cache:
        company_profile = _prefer_live_or_cache({}, cached_scan, "company_profile")
        revenue_breakdown = _prefer_live_or_cache({}, cached_scan, "revenue_breakdown")
        income_statement = _prefer_live_or_cache({}, cached_scan, "income_statement")
        balance_sheet = _prefer_live_or_cache({}, cached_scan, "balance_sheet")
        cashflow_statement = _prefer_live_or_cache({}, cached_scan, "cashflow_statement")
    else:
        company_profile = _prefer_live_or_cache(get_company_profile(ticker), cached_scan, "company_profile")
        revenue_breakdown = _prefer_live_or_cache(get_revenue_breakdown(ticker), cached_scan, "revenue_breakdown")
        income_statement = _prefer_live_or_cache(get_income_statement(ticker), cached_scan, "income_statement")
        balance_sheet = _prefer_live_or_cache(get_balance_sheet(ticker), cached_scan, "balance_sheet")
        cashflow_statement = _prefer_live_or_cache(get_cashflow_statement(ticker), cached_scan, "cashflow_statement")
    return {
        "ticker": str(ticker).upper(),
        "company_profile": company_profile,
        "revenue_breakdown": revenue_breakdown,
        "income_statement": income_statement,
        "balance_sheet": balance_sheet,
        "cashflow_statement": cashflow_statement,
        "daily_bars": daily_bars,
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

    share_count = _latest_share_count(balance_records, profile)
    float_shares = _float_share_count(profile)
    kline_snapshot = _build_kline_snapshot(bars_until_date)
    latest_close = safe_float(kline_snapshot.get("latest_close"))
    if latest_close in (None, 0):
        return None
    quote_snapshot = _build_quote_snapshot(profile, latest_close, share_count, float_shares)
    valuation_snapshot = _build_valuation_snapshot(bars_until_date, balance_records, share_count)

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


def build_public_backtest_inputs(
    *,
    tickers: list[str],
    start_date: str,
    end_date: str,
    bundle_provider: Callable[[str, str, str], dict[str, Any]] = fetch_public_history_bundle,
    min_turnover: float = DEFAULT_MIN_TURNOVER,
) -> dict[str, Any]:
    normalized_tickers = [normalize_text(item).upper() for item in tickers if normalize_text(item)]
    all_bars: list[pd.DataFrame] = []
    signal_rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    bundles: dict[str, dict[str, Any]] = {}

    for ticker in normalized_tickers:
        bundle = bundle_provider(ticker, start_date, end_date)
        daily_bars = _normalize_daily_bars(bundle.get("daily_bars", pd.DataFrame()), ticker)
        if daily_bars.empty:
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
                    "signal_version": "public_source_watchlist_v1",
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
                "public_source_watchlist_v1",
                "statement availability uses announcement date when plausible, otherwise statutory lag fallback",
                "sector/profile text uses latest available public profile rather than strict historical snapshots",
                "float market cap may be missing and then falls back to total market cap",
            ],
        },
    }
