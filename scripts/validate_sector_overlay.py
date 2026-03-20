#!/usr/bin/env python3
"""Compare sector-overlay selection variants on exported VCRF signals."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = REPO_ROOT / ".agents" / "skills" / "shared"
sys.path.insert(0, str(SHARED_DIR))

from engines.backtest_engine import run_vcrf_backtest  # noqa: E402
from utils.config_loader import load_backtest_protocol  # noqa: E402


def _read_frame(value: pd.DataFrame | str | Path) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    path = Path(value)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, dtype={"ticker": str})


def _json_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _write_frame(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = df.copy()
    for column in frame.columns:
        frame[column] = frame[column].map(_json_cell)
    frame.to_csv(path, index=False)
    return path


def _safe_mean(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.mean())


def _safe_median(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.median())


def _variant_protocols() -> dict[str, dict[str, Any]]:
    base_protocol = load_backtest_protocol()
    base_overlay = (base_protocol.get("sector_overlay", {}) or {}).copy()
    sector_score_weight = float(base_overlay.get("sector_score_weight", 0.20) or 0.20)
    diversified_group_limit = int(base_overlay.get("max_positions_per_industry_group", 1) or 1)
    diversified_route_limit = int(base_overlay.get("max_positions_per_sector_route", 2) or 2)

    def _overlay(**updates: Any) -> dict[str, Any]:
        return {**base_overlay, **updates}

    return {
        "baseline": {
            "sector_overlay": _overlay(
                enabled=False,
                sector_score_weight=0.0,
                max_positions_per_industry_group=0,
                max_positions_per_sector_route=0,
            )
        },
        "overlay_rank": {
            "sector_overlay": _overlay(
                enabled=False,
                sector_score_weight=sector_score_weight,
                max_positions_per_industry_group=0,
                max_positions_per_sector_route=0,
            )
        },
        "overlay_gate": {
            "sector_overlay": _overlay(
                enabled=True,
                sector_score_weight=sector_score_weight,
                max_positions_per_industry_group=0,
                max_positions_per_sector_route=0,
            )
        },
        "overlay_gate_diversified": {
            "sector_overlay": _overlay(
                enabled=True,
                sector_score_weight=sector_score_weight,
                max_positions_per_industry_group=diversified_group_limit,
                max_positions_per_sector_route=diversified_route_limit,
            )
        },
    }


def _aggregate_variant_result(variant: str, result: dict[str, Any]) -> dict[str, Any]:
    summary = result.get("summary", pd.DataFrame())
    if not isinstance(summary, pd.DataFrame):
        summary = pd.DataFrame(summary)
    selected = result.get("selected_candidates", pd.DataFrame())
    if not isinstance(selected, pd.DataFrame):
        selected = pd.DataFrame(selected)

    return {
        "variant": variant,
        "round_count": int(len(result.get("rounds", []) or [])),
        "selected_candidate_count": int(len(selected)),
        "selected_ticker_count": int(selected["ticker"].astype(str).nunique()) if "ticker" in selected.columns and not selected.empty else 0,
        "avg_portfolio_cagr": _safe_mean(summary, "portfolio_cagr"),
        "median_portfolio_cagr": _safe_median(summary, "portfolio_cagr"),
        "avg_target_hit_rate": _safe_mean(summary, "target_hit_rate"),
        "avg_max_loss_stop_rate": _safe_mean(summary, "max_loss_stop_rate"),
        "avg_state_reject_rate": _safe_mean(summary, "state_reject_rate"),
        "avg_peak_gross_exposure_ratio": _safe_mean(summary, "peak_gross_exposure_ratio"),
    }


def run_overlay_validation(
    *,
    month_end_signals: pd.DataFrame | str | Path,
    daily_bars: pd.DataFrame | str | Path,
    out_dir: str | Path,
) -> dict[str, str]:
    month_end = _read_frame(month_end_signals)
    bars = _read_frame(daily_bars)
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    variant_summary_rows: list[dict[str, Any]] = []
    round_summary_frames: list[pd.DataFrame] = []
    selected_frames: list[pd.DataFrame] = []

    for variant, protocol in _variant_protocols().items():
        result = run_vcrf_backtest(month_end, bars, protocol=protocol)
        variant_summary_rows.append(_aggregate_variant_result(variant, result))

        summary = result.get("summary", pd.DataFrame())
        if isinstance(summary, pd.DataFrame) and not summary.empty:
            round_summary_frames.append(summary.assign(variant=variant))

        selected = result.get("selected_candidates", pd.DataFrame())
        if isinstance(selected, pd.DataFrame) and not selected.empty:
            selected_frames.append(selected.assign(variant=variant))

    variant_summary = pd.DataFrame(variant_summary_rows).sort_values("variant").reset_index(drop=True)
    round_summary = pd.concat(round_summary_frames, ignore_index=True) if round_summary_frames else pd.DataFrame(columns=["variant"])
    selected_candidates = pd.concat(selected_frames, ignore_index=True) if selected_frames else pd.DataFrame(columns=["variant"])

    variant_summary_path = _write_frame(variant_summary, output_dir / "variant_summary.csv")
    round_summary_path = _write_frame(round_summary, output_dir / "variant_round_summary.csv")
    selected_candidates_path = _write_frame(selected_candidates, output_dir / "variant_selected_candidates.csv")

    manifest = {
        "variant_summary": str(variant_summary_path),
        "variant_round_summary": str(round_summary_path),
        "variant_selected_candidates": str(selected_candidates_path),
    }
    (output_dir / "overlay_validation_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate sector overlay variants with the existing VCRF backtest engine.")
    parser.add_argument("--signals-month-end", required=True, help="CSV/Parquet with month-end signals.")
    parser.add_argument("--daily-bars", required=True, help="CSV/Parquet with daily OHLCV bars.")
    parser.add_argument(
        "--out-dir",
        default=str(REPO_ROOT / "reports" / "backtests" / "sector_overlay_validation"),
        help="Output directory for variant comparison tables.",
    )
    args = parser.parse_args()

    manifest = run_overlay_validation(
        month_end_signals=args.signals_month_end,
        daily_bars=args.daily_bars,
        out_dir=args.out_dir,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
