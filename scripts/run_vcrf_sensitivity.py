#!/usr/bin/env python3
"""Offline VCRF sensitivity / ablation runner from exported month-end signals."""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Any

import pandas as pd


REALIZATION_COMPONENTS = (
    "repair_state",
    "regime_cycle_position",
    "marginal_buyer_probability",
    "flow_confirmation",
    "elasticity",
    "catalyst_quality",
)
NEUTRAL_REALIZATION_COMPONENTS = (
    "marginal_buyer_probability",
    "catalyst_quality",
)
REDISTRIBUTION_TARGETS = (
    "regime_cycle_position",
    "flow_confirmation",
)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _banded_score(value: float | None, bands: list[tuple[float, float]]) -> float:
    if value is None:
        return 0.0
    score = 0.0
    for threshold, threshold_score in bands:
        if value >= threshold:
            score = threshold_score
    return score


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {name: max(float(value), 0.0) for name, value in weights.items()}
    total = sum(cleaned.values())
    if total <= 0:
        return {name: 0.0 for name in cleaned}
    return {name: value / total for name, value in cleaned.items()}


def _exported_weight_logic(row: pd.Series) -> dict[str, float]:
    weights: dict[str, float] = {}
    for name in REALIZATION_COMPONENTS:
        weights[name] = _safe_float(row.get(f"realization_weight_{name}")) or 0.0
    return _normalize_weights(weights)


def _equal_weight_logic(_row: pd.Series) -> dict[str, float]:
    weight = 1.0 / len(REALIZATION_COMPONENTS)
    return {name: weight for name in REALIZATION_COMPONENTS}


def _drop_50s_redistribute_logic(row: pd.Series) -> dict[str, float]:
    weights = _equal_weight_logic(row)
    released_weight = 0.0
    for name in NEUTRAL_REALIZATION_COMPONENTS:
        if _safe_bool(row.get(f"realization_{name}_neutral_default")):
            released_weight += weights[name]
            weights[name] = 0.0
    if released_weight > 0:
        target_total = sum(weights[name] for name in REDISTRIBUTION_TARGETS)
        if target_total > 0:
            for name in REDISTRIBUTION_TARGETS:
                weights[name] += released_weight * weights[name] / target_total
    return _normalize_weights(weights)


def _dynamic_by_sleeve_logic(row: pd.Series) -> dict[str, float]:
    exported = _exported_weight_logic(row)
    if any(weight > 0 for weight in exported.values()):
        return exported
    return _drop_50s_redistribute_logic(row)


def _realization_score_for_logic(row: pd.Series, weight_logic: str) -> float:
    if weight_logic == "static_equal":
        weights = _equal_weight_logic(row)
    elif weight_logic == "drop_50s_redistribute":
        weights = _drop_50s_redistribute_logic(row)
    elif weight_logic == "dynamic_by_sleeve":
        weights = _dynamic_by_sleeve_logic(row)
    else:
        raise ValueError(f"unsupported weight_logic={weight_logic}")

    score = 0.0
    for name in REALIZATION_COMPONENTS:
        score += (_safe_float(row.get(f"realization_{name}_score")) or 0.0) * weights[name]
    return round(score, 4)


def _survival_tripwire_threshold(row: pd.Series, debt_tripwire: float) -> float:
    primary_type = str(row.get("primary_type") or "").strip().lower()
    state_owned_support = _safe_bool(row.get("underwrite_survival_boundary_state_owned_support"))
    if state_owned_support or primary_type == "cyclical":
        return min(float(debt_tripwire), 0.30)
    return float(debt_tripwire)


def _recomputed_survival_component(row: pd.Series, debt_tripwire: float) -> tuple[float, float, bool]:
    cash_coverage = _safe_float(row.get("underwrite_survival_boundary_cash_coverage"))
    cfo_support = _safe_float(row.get("underwrite_survival_boundary_cfo_support"))
    net_cash_ratio = _safe_float(row.get("underwrite_survival_boundary_net_cash_ratio"))
    z_score = _safe_float(row.get("underwrite_survival_boundary_z_score"))
    equity_positive = _safe_bool(row.get("underwrite_survival_boundary_equity_positive"))
    interest_coverage = _safe_float(row.get("underwrite_survival_boundary_interest_coverage"))
    tripwire_threshold = _survival_tripwire_threshold(row, debt_tripwire)
    interest_bypass = interest_coverage is not None and interest_coverage >= 1.50
    tripwire_reject = bool(
        cash_coverage is not None
        and cash_coverage < tripwire_threshold
        and not interest_bypass
    )

    score = (
        _banded_score(cash_coverage, [(0.40, 6), (0.80, 15), (1.00, 25), (1.50, 35)])
        + _banded_score(cfo_support, [(0.00, 5), (0.20, 10), (0.50, 15), (1.00, 20)])
        + _banded_score(net_cash_ratio, [(-0.10, 4), (0.00, 12), (0.10, 20)])
        + _banded_score(z_score, [(1.10, 8), (1.80, 15), (3.00, 20)])
        + (5.0 if equity_positive else 0.0)
    )
    if tripwire_reject:
        score = min(score, 20.0)
    return round(score, 4), tripwire_threshold, tripwire_reject


def _recomputed_underwrite_score(row: pd.Series, debt_tripwire: float) -> tuple[float, float, bool]:
    component_scores = {
        "intrinsic_value_floor": _safe_float(row.get("underwrite_intrinsic_value_floor_score")) or 0.0,
        "survival_boundary": _safe_float(row.get("underwrite_survival_boundary_score")) or 0.0,
        "governance_anti_fraud": _safe_float(row.get("underwrite_governance_anti_fraud_score")) or 0.0,
        "business_or_asset_quality": _safe_float(row.get("underwrite_business_or_asset_quality_score")) or 0.0,
        "normalized_earnings_power": _safe_float(row.get("underwrite_normalized_earnings_power_score")) or 0.0,
    }
    component_scores["survival_boundary"], tripwire_threshold, tripwire_reject = _recomputed_survival_component(row, debt_tripwire)
    weights = _normalize_weights(
        {
            "intrinsic_value_floor": _safe_float(row.get("underwrite_weight_intrinsic_value_floor")) or 0.0,
            "survival_boundary": _safe_float(row.get("underwrite_weight_survival_boundary")) or 0.0,
            "governance_anti_fraud": _safe_float(row.get("underwrite_weight_governance_anti_fraud")) or 0.0,
            "business_or_asset_quality": _safe_float(row.get("underwrite_weight_business_or_asset_quality")) or 0.0,
            "normalized_earnings_power": _safe_float(row.get("underwrite_weight_normalized_earnings_power")) or 0.0,
        }
    )
    score = sum(component_scores[name] * weights.get(name, 0.0) for name in component_scores)
    return round(score, 4), tripwire_threshold, tripwire_reject


def run_sensitivity_ablation(
    *,
    csv_path: Path,
    out_path: Path,
    debt_tripwires: list[float],
    weight_logics: list[str],
    min_realizations: list[float],
) -> pd.DataFrame:
    df_raw = pd.read_csv(csv_path, dtype={"ticker": str})
    results: list[dict[str, Any]] = []

    keys = ("debt_tripwire", "weight_logic", "min_realization")
    for bundle in itertools.product(debt_tripwires, weight_logics, min_realizations):
        params = dict(zip(keys, bundle))
        underwrite_scores: list[float] = []
        tripwire_thresholds: list[float] = []
        tripwire_rejects: list[bool] = []
        realization_scores: list[float] = []

        for _, row in df_raw.iterrows():
            underwrite_score, tripwire_threshold, tripwire_reject = _recomputed_underwrite_score(row, float(params["debt_tripwire"]))
            realization_score = _realization_score_for_logic(row, str(params["weight_logic"]))
            underwrite_scores.append(underwrite_score)
            tripwire_thresholds.append(tripwire_threshold)
            tripwire_rejects.append(tripwire_reject)
            realization_scores.append(realization_score)

        df = df_raw.copy()
        df["sim_underwrite_score"] = underwrite_scores
        df["sim_tripwire_threshold"] = tripwire_thresholds
        df["sim_tripwire_reject"] = tripwire_rejects
        df["sim_realization_score"] = realization_scores

        attack_mask = (
            (df["sim_underwrite_score"] >= 75.0)
            & (df["sim_realization_score"] >= float(params["min_realization"]))
            & (~df["sim_tripwire_reject"])
        )
        tradable_attack_mask = attack_mask & (pd.to_numeric(df.get("tradable_flag"), errors="coerce").fillna(0) >= 1)

        results.append(
            {
                "Debt_Tripwire": float(params["debt_tripwire"]),
                "Weight_Logic": str(params["weight_logic"]),
                "Min_Realization": float(params["min_realization"]),
                "Debt_Kills": int(df["sim_tripwire_reject"].sum()),
                "Avg_Tripwire_Threshold": round(float(pd.Series(tripwire_thresholds).mean()), 4),
                "Avg_Underwrite": round(float(pd.Series(underwrite_scores).mean()), 4),
                "Avg_Realization": round(float(pd.Series(realization_scores).mean()), 4),
                "ATTACK_Signals": int(attack_mask.sum()),
                "Tradable_ATTACK_Signals": int(tradable_attack_mask.sum()),
                "Unique_ATTACK_Tickers": int(df.loc[attack_mask, "ticker"].astype(str).nunique()),
            }
        )

    result_df = pd.DataFrame(results).sort_values(
        ["Tradable_ATTACK_Signals", "ATTACK_Signals", "Avg_Realization"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(out_path, index=False)
    return result_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline VCRF sensitivity / ablation runner.")
    parser.add_argument(
        "--signals-month-end",
        default="reports/backtests/curated24_tushare_inputs_v3/signals_month_end.csv",
        help="CSV exported by build_tushare_backtest_inputs.py",
    )
    parser.add_argument(
        "--out-path",
        default="reports/backtests/sensitivity_matrix.csv",
        help="Where to write the sensitivity matrix CSV",
    )
    parser.add_argument("--debt-tripwires", default="0.8,0.5,0.3", help="Comma-separated tripwire grid")
    parser.add_argument(
        "--weight-logics",
        default="static_equal,drop_50s_redistribute,dynamic_by_sleeve",
        help="Comma-separated weight logic grid",
    )
    parser.add_argument("--min-realizations", default="60,65,70", help="Comma-separated min_realization grid")
    args = parser.parse_args()

    result_df = run_sensitivity_ablation(
        csv_path=Path(args.signals_month_end),
        out_path=Path(args.out_path),
        debt_tripwires=[float(item.strip()) for item in args.debt_tripwires.split(",") if item.strip()],
        weight_logics=[item.strip() for item in args.weight_logics.split(",") if item.strip()],
        min_realizations=[float(item.strip()) for item in args.min_realizations.split(",") if item.strip()],
    )
    print(json.dumps(result_df.head(15).to_dict(orient="records"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
