"""Industry-group overlay helpers for sector-aware selection."""
from __future__ import annotations

from typing import Any

import pandas as pd


def _normalize_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _bool_series(frame: pd.DataFrame, column: str, *, default: bool = False) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="bool")
    raw = frame[column]
    mapped = raw.map(lambda value: str(value).strip().lower() in {"1", "true", "yes", "y"} if pd.notna(value) else default)
    return mapped.astype(bool)


def _fallback_group_from_route(route: str) -> str:
    normalized = _normalize_label(route)
    if normalized:
        return normalized
    return "unknown"


def _resolve_industry_group_series(frame: pd.DataFrame) -> pd.Series:
    if "industry_group" in frame.columns:
        series = frame["industry_group"].fillna("").astype(str).str.strip().str.lower()
    else:
        route = frame.get("sector_route", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
        series = route.map(_fallback_group_from_route)
    series = series.replace("", "unknown")
    return series


def _resolve_cycle_sensitive_series(frame: pd.DataFrame) -> pd.Series:
    if "sector_cycle_sensitive" in frame.columns:
        return _bool_series(frame, "sector_cycle_sensitive")

    primary_type = frame.get("primary_type", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    sector_route = frame.get("sector_route", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    return primary_type.eq("cyclical") | sector_route.isin({"core_resource", "rigid_shovel"})


def classify_sector_cycle(score: float, breadth: float, member_count: int) -> str:
    if member_count <= 0:
        return "neutral"
    if member_count < 2:
        if score >= 78.0 and breadth >= 0.9:
            return "favored"
        return "neutral"
    if score >= 72.0 and breadth >= 0.45:
        return "favored"
    if score < 55.0 or breadth < 0.20:
        return "avoid"
    return "neutral"


def build_sector_snapshot(month_end_signals: pd.DataFrame) -> pd.DataFrame:
    if month_end_signals is None or month_end_signals.empty:
        return pd.DataFrame(
            columns=[
                "signal_date",
                "industry_group",
                "sector_member_count",
                "sector_cycle_sensitive_ratio",
                "sector_attack_ready_ratio",
                "sector_trend_ratio",
                "sector_median_underwrite",
                "sector_median_realization",
                "sector_median_upside",
                "sector_cycle_score",
                "sector_cycle_state",
            ]
        )

    frame = month_end_signals.copy()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"]).dt.normalize()
    frame["industry_group"] = _resolve_industry_group_series(frame)
    frame["sector_cycle_sensitive"] = _resolve_cycle_sensitive_series(frame)
    frame["vcrf_state_normalized"] = frame.get("vcrf_state", pd.Series("REJECT", index=frame.index)).fillna("REJECT").astype(str).str.upper()
    frame["flow_stage_normalized"] = frame.get("flow_stage", pd.Series("", index=frame.index)).fillna("").astype(str).str.lower()
    frame["recognition_upside_signal"] = _numeric_series(frame, "recognition_upside_signal").combine_first(_numeric_series(frame, "recognition_upside")).fillna(0.0)
    frame["underwrite_score_numeric"] = _numeric_series(frame, "underwrite_score").fillna(0.0)
    frame["realization_score_numeric"] = _numeric_series(frame, "realization_score").fillna(0.0)

    grouped_rows: list[dict[str, Any]] = []
    for (signal_date, industry_group), group in frame.groupby(["signal_date", "industry_group"], dropna=False, sort=False):
        member_count = int(len(group))
        attack_ready_ratio = float(group["vcrf_state_normalized"].isin({"READY", "ATTACK", "HARVEST"}).mean()) if member_count else 0.0
        trend_ratio = float(group["flow_stage_normalized"].isin({"ignition", "trend", "crowded"}).mean()) if member_count else 0.0
        cycle_sensitive_ratio = float(group["sector_cycle_sensitive"].mean()) if member_count else 0.0
        median_realization = float(group["realization_score_numeric"].median()) if member_count else 0.0
        median_underwrite = float(group["underwrite_score_numeric"].median()) if member_count else 0.0
        median_upside = float(group["recognition_upside_signal"].median()) if member_count else 0.0
        sector_cycle_score = (
            0.30 * attack_ready_ratio
            + 0.25 * trend_ratio
            + 0.20 * (median_realization / 100.0)
            + 0.15 * (median_underwrite / 100.0)
            + 0.10 * max(min(median_upside, 1.0), -1.0)
        ) * 100.0
        grouped_rows.append(
            {
                "signal_date": pd.Timestamp(signal_date).normalize(),
                "industry_group": industry_group,
                "sector_member_count": member_count,
                "sector_cycle_sensitive_ratio": round(cycle_sensitive_ratio, 6),
                "sector_attack_ready_ratio": round(attack_ready_ratio, 6),
                "sector_trend_ratio": round(trend_ratio, 6),
                "sector_median_underwrite": round(median_underwrite, 6),
                "sector_median_realization": round(median_realization, 6),
                "sector_median_upside": round(median_upside, 6),
                "sector_cycle_score": round(sector_cycle_score, 6),
                "sector_cycle_state": classify_sector_cycle(sector_cycle_score, attack_ready_ratio, member_count),
            }
        )

    return pd.DataFrame(grouped_rows).sort_values(["signal_date", "sector_cycle_score", "industry_group"], ascending=[True, False, True]).reset_index(drop=True)


def merge_sector_overlay(candidates: pd.DataFrame, sector_snapshot: pd.DataFrame) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return candidates.copy()

    merged = candidates.copy()
    merged["signal_date"] = pd.to_datetime(merged["signal_date"]).dt.normalize()
    merged["industry_group"] = _resolve_industry_group_series(merged)
    merged["sector_cycle_sensitive"] = _resolve_cycle_sensitive_series(merged)

    if sector_snapshot is None or sector_snapshot.empty:
        merged["sector_member_count"] = 0
        merged["sector_cycle_score"] = 50.0
        merged["sector_cycle_state"] = "neutral"
        return merged

    snapshot = sector_snapshot.copy()
    snapshot["signal_date"] = pd.to_datetime(snapshot["signal_date"]).dt.normalize()
    merged = merged.merge(snapshot, on=["signal_date", "industry_group"], how="left")
    merged["sector_member_count"] = pd.to_numeric(
        merged.get("sector_member_count", pd.Series(index=merged.index, dtype="float64")),
        errors="coerce",
    ).fillna(0).astype(int)
    merged["sector_cycle_score"] = pd.to_numeric(
        merged.get("sector_cycle_score", pd.Series(index=merged.index, dtype="float64")),
        errors="coerce",
    ).fillna(50.0)
    merged["sector_cycle_state"] = merged.get(
        "sector_cycle_state",
        pd.Series("neutral", index=merged.index, dtype="object"),
    ).fillna("neutral").astype(str).str.lower()
    return merged
