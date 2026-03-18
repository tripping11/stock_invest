#!/usr/bin/env python3
"""Run the deterministic VCRF round backtest."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = REPO_ROOT / ".agents" / "skills" / "shared"
sys.path.insert(0, str(SHARED_DIR))

from engines.backtest_engine import run_vcrf_backtest  # noqa: E402


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


def _round_report_markdown(round_result: dict) -> str:
    summary = round_result["summary"]
    lines = [
        f"# Round {summary['round_id']:02d} Report",
        "",
        f"- Tickers: {', '.join(summary['tickers'])}",
        f"- Round Final Value: {summary['round_final_value']:.2f}",
        f"- Avg Stock CAGR: {summary['avg_stock_cagr']:.4f}" if summary["avg_stock_cagr"] is not None else "- Avg Stock CAGR: N/A",
        f"- Portfolio CAGR: {summary['portfolio_cagr']:.4f}" if summary["portfolio_cagr"] is not None else "- Portfolio CAGR: N/A",
        f"- Max Drawdown: {summary['max_drawdown']:.4f}",
        f"- Win Rate: {summary['win_rate']:.4f}" if summary["win_rate"] is not None else "- Win Rate: N/A",
        f"- Used Cash Ratio: {summary['used_cash_ratio']:.4f}",
        "",
        "## Exit Reasons",
    ]
    trades = round_result["trades"]
    if trades.empty:
        lines.append("- No completed trades")
    else:
        for reason, count in trades["exit_reason"].value_counts().items():
            lines.append(f"- {reason}: {count}")
    anomalies = summary.get("anomalies") or []
    lines.extend(["", "## Anomalies"])
    if anomalies:
        lines.extend(f"- {item}" for item in anomalies)
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the VCRF round backtest from research-layer signals and daily bars.")
    parser.add_argument("--signals-month-end", required=True, help="CSV/Parquet with research-layer month-end signals.")
    parser.add_argument("--daily-bars", required=True, help="CSV/Parquet with daily OHLCV bars.")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "reports" / "backtests" / "latest"), help="Output directory.")
    args = parser.parse_args()

    month_end = _read_frame(Path(args.signals_month_end))
    daily_bars = _read_frame(Path(args.daily_bars))
    result = run_vcrf_backtest(month_end, daily_bars)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    selected_path = _write_frame(result["selected_candidates"], out_dir / "selected_candidates.csv")
    summary_path = _write_frame(result["summary"], out_dir / "round_summary.csv")

    for round_result in result["rounds"]:
        round_id = int(round_result["round_id"])
        _write_frame(round_result["trades"], out_dir / f"round_{round_id:02d}_trades.csv")
        _write_frame(round_result["equity"], out_dir / f"round_{round_id:02d}_equity.csv")
        (out_dir / f"round_{round_id:02d}_report.md").write_text(_round_report_markdown(round_result), encoding="utf-8")

    summary = {
        "selected_candidates": str(selected_path),
        "round_summary": str(summary_path),
        "round_count": len(result["rounds"]),
    }
    (out_dir / "backtest_manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
