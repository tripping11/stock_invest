#!/usr/bin/env python3
"""Build Tushare-backed backtest inputs for the deterministic VCRF engine."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = REPO_ROOT / ".agents" / "skills" / "shared"
sys.path.insert(0, str(SHARED_DIR))

from adapters.tushare_adapter import discover_tushare_universe_tickers, resolve_tushare_tokens  # noqa: E402
from engines.tushare_backtest_dataset_engine import build_tushare_backtest_inputs  # noqa: E402


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


def _load_tickers(args: argparse.Namespace) -> list[str]:
    if args.tickers:
        return [item.strip().upper() for item in args.tickers.split(",") if item.strip()]
    if args.ticker_file:
        path = Path(args.ticker_file)
        return [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    list_statuses = tuple(item.strip().upper() for item in args.list_statuses.split(",") if item.strip())
    limit = args.limit if args.limit and args.limit > 0 else None
    return discover_tushare_universe_tickers(list_statuses=list_statuses, limit=limit)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Tushare-backed VCRF backtest inputs.")
    parser.add_argument("--tickers", help="Comma-separated tickers, e.g. 600328,600348.")
    parser.add_argument("--ticker-file", help="Optional file with one ticker per line.")
    parser.add_argument("--list-statuses", default="L,D,P", help="Universe discovery statuses when --tickers is omitted.")
    parser.add_argument("--limit", type=int, default=0, help="Universe discovery cap when using stock_basic discovery.")
    parser.add_argument("--start-date", required=True, help="History start date, e.g. 2020-01-01.")
    parser.add_argument("--end-date", required=True, help="History end date, e.g. 2025-12-31.")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "reports" / "backtests" / "tushare_inputs"), help="Output directory.")
    parser.add_argument("--min-turnover", type=float, default=15_000_000.0, help="20-day average turnover floor for tradable_flag.")
    args = parser.parse_args()

    tokens = resolve_tushare_tokens(REPO_ROOT)
    if not tokens:
        raise SystemExit("TUSHARE_TOKEN or TUSHARE_TOKENS is not configured in environment or repo .env")

    tickers = _load_tickers(args)
    if not tickers:
        raise SystemExit("no tickers resolved from tushare universe discovery")

    result = build_tushare_backtest_inputs(
        tickers=tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        min_turnover=args.min_turnover,
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    signals_path = _write_frame(result["signals_month_end"], out_dir / "signals_month_end.parquet")
    bars_path = _write_frame(result["daily_bars"], out_dir / "daily_bars.parquet")
    manifest = {
        **(result.get("manifest") or {}),
        "signals_month_end_path": str(signals_path),
        "daily_bars_path": str(bars_path),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
