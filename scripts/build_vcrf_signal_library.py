#!/usr/bin/env python3
"""Normalize month-end research signals and expand them to daily execution signals."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = REPO_ROOT / ".agents" / "skills" / "shared"
sys.path.insert(0, str(SHARED_DIR))

from engines.signal_library_engine import expand_signal_daily, normalize_signal_month_end  # noqa: E402


def _read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _write_frame(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        try:
            df.to_parquet(path, index=False)
            return path
        except Exception:
            fallback = path.with_suffix(".csv")
            df.to_csv(fallback, index=False)
            return fallback
    df.to_csv(path, index=False)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build VCRF month-end and daily signal libraries.")
    parser.add_argument("--signals-month-end", required=True, help="CSV/Parquet with research-layer month-end signals.")
    parser.add_argument("--daily-bars", required=True, help="CSV/Parquet with daily OHLCV bars.")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "reports" / "backtests" / "signal_library"), help="Output directory.")
    args = parser.parse_args()

    month_end_raw = _read_frame(Path(args.signals_month_end))
    daily_bars = _read_frame(Path(args.daily_bars))
    trading_days = pd.DatetimeIndex(sorted(pd.to_datetime(daily_bars["date"]).dt.normalize().unique()))
    month_end = normalize_signal_month_end(month_end_raw, trading_days)
    signal_daily = expand_signal_daily(month_end, daily_bars)

    out_dir = Path(args.out_dir)
    month_end_path = _write_frame(month_end, out_dir / "signal_month_end.parquet")
    daily_path = _write_frame(signal_daily, out_dir / "signal_daily.parquet")

    print(f"signal_month_end -> {month_end_path}")
    print(f"signal_daily -> {daily_path}")
    print(f"month_end_rows={len(month_end)}, daily_rows={len(signal_daily)}")


if __name__ == "__main__":
    main()
