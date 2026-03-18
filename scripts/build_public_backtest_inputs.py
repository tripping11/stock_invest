#!/usr/bin/env python3
"""Build approximate public-source backtest inputs for a watchlist-scale VCRF run."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = REPO_ROOT / ".agents" / "skills" / "shared"
sys.path.insert(0, str(SHARED_DIR))

from engines.public_backtest_dataset_engine import (  # noqa: E402
    build_public_backtest_inputs,
    discover_baostock_universe_tickers,
    discover_local_watchlist_tickers,
    discover_public_financial_usable_tickers,
    fetch_public_history_bundle,
)


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
    tickers: list[str]
    if args.tickers:
        tickers = [item.strip().upper() for item in args.tickers.split(",") if item.strip()]
    elif args.ticker_file:
        path = Path(args.ticker_file)
        rows = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        tickers = rows
    elif args.universe == "baostock-preferred":
        discovery_limit = args.discovery_limit if args.discovery_limit and args.discovery_limit > 0 else args.limit
        tickers = discover_baostock_universe_tickers(limit=discovery_limit)
    else:
        tickers = discover_local_watchlist_tickers(REPO_ROOT)

    if args.require_financials:
        target_count = args.limit if args.limit and args.limit > 0 else None
        tickers = discover_public_financial_usable_tickers(tickers, target_count=target_count)
    elif args.limit and args.limit > 0:
        tickers = tickers[: args.limit]
    return tickers


def main() -> None:
    parser = argparse.ArgumentParser(description="Build public-source watchlist backtest inputs.")
    parser.add_argument("--universe", choices=("local-watchlist", "baostock-preferred"), default="local-watchlist", help="Ticker source when --tickers/--ticker-file are omitted.")
    parser.add_argument("--tickers", help="Comma-separated tickers. Defaults to local repo coverage universe.")
    parser.add_argument("--ticker-file", help="Optional file with one ticker per line.")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on ticker count after universe resolution.")
    parser.add_argument("--discovery-limit", type=int, default=0, help="When using baostock-preferred, probe at most this many raw candidates before later filtering.")
    parser.add_argument("--require-financials", action="store_true", help="Keep only tickers with non-empty income and balance statements in current public-source adapters.")
    parser.add_argument("--prefer-local-cache", action="store_true", help="Prefer local Tier1 scan cache under data/raw/<ticker>/ for profile and statements when available.")
    parser.add_argument("--start-date", required=True, help="History start date, e.g. 2020-01-01.")
    parser.add_argument("--end-date", required=True, help="History end date, e.g. 2025-12-31.")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "reports" / "backtests" / "public_source_inputs"), help="Output directory.")
    parser.add_argument("--min-turnover", type=float, default=15_000_000.0, help="20-day average turnover floor for tradable_flag.")
    args = parser.parse_args()

    tickers = _load_tickers(args)
    if not tickers:
        raise SystemExit("no tickers resolved; pass --tickers or --ticker-file, or keep local evidence/data universe populated")

    bundle_provider = (
        (lambda ticker, start_date, end_date: fetch_public_history_bundle(ticker, start_date, end_date, prefer_local_cache=True))
        if args.prefer_local_cache
        else fetch_public_history_bundle
    )

    result = build_public_backtest_inputs(
        tickers=tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        min_turnover=args.min_turnover,
        bundle_provider=bundle_provider,
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
