"""Research signal normalization and daily expansion helpers."""
from __future__ import annotations

from typing import Any

import pandas as pd


_STATE_MAP = {
    0: "REJECT",
    1: "COLD_STORAGE",
    2: "READY",
    3: "ATTACK",
    4: "HARVEST",
    "0": "REJECT",
    "1": "COLD_STORAGE",
    "2": "READY",
    "3": "ATTACK",
    "4": "HARVEST",
    "REJECT": "REJECT",
    "COLD": "COLD_STORAGE",
    "COLD_STORAGE": "COLD_STORAGE",
    "READY": "READY",
    "ATTACK": "ATTACK",
    "HARVEST": "HARVEST",
}


def _canonical_state(value: Any) -> str:
    if pd.isna(value):
        return "REJECT"
    text = str(value).strip().upper()
    return _STATE_MAP.get(value, _STATE_MAP.get(text, text))


def resolve_effective_date(raw_date: Any, trading_days: pd.DatetimeIndex) -> pd.Timestamp:
    if raw_date is None or (isinstance(raw_date, float) and pd.isna(raw_date)):
        raise ValueError("raw_date is required to resolve effective_date")
    if trading_days.empty:
        raise ValueError("trading_days must not be empty")
    target = pd.Timestamp(raw_date).normalize()
    index = trading_days.searchsorted(target, side="left")
    if index >= len(trading_days):
        raise ValueError(f"no trading day on or after {target.date()}")
    return pd.Timestamp(trading_days[index]).normalize()


def normalize_signal_month_end(signals: pd.DataFrame, trading_days: pd.DatetimeIndex) -> pd.DataFrame:
    if signals is None or signals.empty:
        return pd.DataFrame(
            columns=[
                "signal_date",
                "effective_date",
                "ticker",
                "vcrf_state",
                "floor_price",
                "recognition_price",
                "total_score",
                "tradable_flag",
                "signal_version",
            ]
        )

    df = signals.copy()
    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.normalize()
    if "effective_date" in df.columns and df["effective_date"].notna().any():
        df["effective_date"] = pd.to_datetime(df["effective_date"]).dt.normalize()
    else:
        base_dates = df["announcement_date"] if "announcement_date" in df.columns else df["signal_date"]
        df["effective_date"] = [resolve_effective_date(value, trading_days) for value in base_dates]
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["vcrf_state"] = df["vcrf_state"].map(_canonical_state)
    for column, default in {
        "floor_price": None,
        "recognition_price": None,
        "total_score": 0.0,
        "tradable_flag": 1,
        "signal_version": "vcrf_os_signal_v1",
        "reject_reason": "",
        "v_score": None,
        "c_score": None,
        "r_score": None,
        "f_score": None,
    }.items():
        if column not in df.columns:
            df[column] = default
    return df.sort_values(["ticker", "effective_date", "signal_date", "total_score"], ascending=[True, True, True, False]).reset_index(drop=True)


def expand_signal_daily(month_end_signals: pd.DataFrame, daily_bars: pd.DataFrame) -> pd.DataFrame:
    if daily_bars is None or daily_bars.empty:
        return pd.DataFrame()
    bars = daily_bars.copy()
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()
    bars["ticker"] = bars["ticker"].astype(str).str.strip().str.upper()
    bars = bars.sort_values(["ticker", "date"]).reset_index(drop=True)
    if month_end_signals is None or month_end_signals.empty:
        return bars

    signals = month_end_signals.copy()
    signals["effective_date"] = pd.to_datetime(signals["effective_date"]).dt.normalize()
    signals["ticker"] = signals["ticker"].astype(str).str.strip().str.upper()
    signals = signals.sort_values(["ticker", "effective_date", "signal_date"]).reset_index(drop=True)

    expanded: list[pd.DataFrame] = []
    signal_columns = [column for column in signals.columns if column not in {"ticker"}]
    for ticker, bars_ticker in bars.groupby("ticker", sort=False):
        ticker_signals = signals[signals["ticker"] == ticker]
        if ticker_signals.empty:
            expanded.append(bars_ticker.copy())
            continue
        merged = pd.merge_asof(
            bars_ticker.sort_values("date"),
            ticker_signals.sort_values("effective_date"),
            left_on="date",
            right_on="effective_date",
            direction="backward",
        )
        merged["ticker"] = ticker
        expanded.append(merged)
    result = pd.concat(expanded, ignore_index=True, sort=False)
    ordered_columns = list(bars.columns) + [column for column in signal_columns if column not in bars.columns]
    return result.loc[:, [column for column in ordered_columns if column in result.columns]].sort_values(["ticker", "date"]).reset_index(drop=True)
