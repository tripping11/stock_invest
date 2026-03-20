"""Deterministic event-driven backtest engine for VCRF signals."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from engines.sector_cycle_engine import build_sector_snapshot, merge_sector_overlay
from engines.signal_library_engine import expand_signal_daily, normalize_signal_month_end
from utils.config_loader import load_backtest_protocol


@dataclass(slots=True)
class Position:
    ticker: str
    shares: int
    entry_date: pd.Timestamp
    entry_price: float
    floor_price: float | None
    recognition_price: float | None
    cost_basis_cash: float
    primary_type: str = "unknown"
    sector_route: str = "unknown"
    industry_group: str = "unknown"
    sector_cycle_sensitive: bool = False
    sleeve: str = "unslotted"
    slot_in_round: int = 0
    bars_held: int = 0
    last_known_price: float = 0.0
    max_high_price: float = 0.0
    min_low_price: float = 0.0


def _merge_protocol(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    protocol = load_backtest_protocol()
    if not overrides:
        return protocol
    merged = dict(protocol)
    for key, value in overrides.items():
        if key == "costs":
            merged["costs"] = {**(protocol.get("costs", {}) or {}), **(value or {})}
        else:
            merged[key] = value
    return merged


def _normalize_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _state_rank(value: Any) -> int:
    normalized = str(value or "REJECT").upper()
    return {"REJECT": 0, "COLD_STORAGE": 1, "READY": 2, "ATTACK": 3, "HARVEST": 4}.get(normalized, 0)


def _lot_round_shares(target_value: float, price: float, lot_size: int) -> int:
    if price <= 0 or lot_size <= 0:
        return 0
    raw_shares = int(target_value / price)
    return (raw_shares // lot_size) * lot_size


def _stamp_duty_bps(trade_date: pd.Timestamp, cost_cfg: dict[str, Any]) -> float:
    for schedule in cost_cfg.get("stamp_duty", []) or []:
        start = pd.Timestamp(schedule.get("start")).normalize()
        end = pd.Timestamp(schedule.get("end")).normalize()
        if start <= trade_date.normalize() <= end:
            return float(schedule.get("sell_bps", 0.0))
    return 0.0


def _fill_price(side: str, raw_price: float, cost_cfg: dict[str, Any]) -> float:
    bps = float(cost_cfg.get("slippage_bps_buy" if side == "buy" else "slippage_bps_sell", 0.0))
    multiplier = 1 + bps / 10_000 if side == "buy" else 1 - bps / 10_000
    return raw_price * multiplier


def _trade_cost(side: str, trade_date: pd.Timestamp, price: float, shares: int, cost_cfg: dict[str, Any]) -> float:
    notional = price * shares
    commission = max(
        notional * float(cost_cfg.get("broker_commission_bps", 0.0)) / 10_000,
        float(cost_cfg.get("broker_min_commission", 0.0)),
    )
    transfer_fee = notional * float(cost_cfg.get("transfer_fee_bps", 0.0)) / 10_000
    stamp_duty = 0.0
    if side == "sell":
        stamp_duty = notional * _stamp_duty_bps(trade_date, cost_cfg) / 10_000
    return commission + transfer_fee + stamp_duty


def _resolve_sleeve(primary_type: Any) -> str:
    normalized = _normalize_label(primary_type)
    if normalized == "compounder":
        return "quality"
    if normalized == "cyclical":
        return "cycle"
    if normalized in {"turnaround", "asset_play", "special_situation"}:
        return "value_repair"
    return "unslotted"


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(0.0, index=series.index, dtype="float64")
    std = float(numeric.std(ddof=0))
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index, dtype="float64")
    mean = float(numeric.mean())
    return ((numeric - mean) / std).fillna(0.0)


def _resolve_current_price(frame: pd.DataFrame) -> pd.Series:
    current_price = _numeric_series(frame, "current_price")
    if current_price.notna().any():
        return current_price

    floor_price = _numeric_series(frame, "floor_price")
    floor_protection = _numeric_series(frame, "underwrite_intrinsic_value_floor_floor_protection")
    from_floor = floor_price / floor_protection

    normalized_price = _numeric_series(frame, "underwrite_normalized_earnings_power_implied_price")
    value_ratio = _numeric_series(frame, "underwrite_normalized_earnings_power_value_ratio")
    from_normalized = normalized_price / value_ratio

    return current_price.combine_first(from_floor.replace([pd.NA, pd.NaT], pd.NA)).combine_first(from_normalized)


def _resolve_recognition_upside(frame: pd.DataFrame, current_price: pd.Series) -> pd.Series:
    direct = _numeric_series(frame, "recognition_upside")
    if direct.notna().any():
        return direct
    direct = _numeric_series(frame, "realization_recognition_upside")
    if direct.notna().any():
        return direct
    recognition_price = _numeric_series(frame, "recognition_price")
    derived = recognition_price / current_price - 1
    return direct.combine_first(derived)


def _resolve_fundamental_momentum(frame: pd.DataFrame) -> pd.Series:
    for column in ("fundamental_momentum_score", "realization_flow_confirmation_fundamental_momentum_score"):
        direct = _numeric_series(frame, column)
        if direct.notna().any():
            return direct.fillna(0.0)

    repair = _numeric_series(frame, "realization_repair_state_score").fillna(0.0)
    normalized = _numeric_series(frame, "underwrite_normalized_earnings_power_score").fillna(0.0)
    regime = _numeric_series(frame, "realization_regime_cycle_position_score").fillna(0.0)
    return (repair * 0.35 + normalized * 0.40 + regime * 0.25).fillna(0.0)


def _resolve_price_strength(frame: pd.DataFrame) -> pd.Series:
    direct = _numeric_series(frame, "price_strength_12m")
    if direct.notna().any():
        return direct.fillna(0.0)

    current_vs_high = _numeric_series(frame, "current_vs_5yr_high")
    if current_vs_high.notna().any():
        return (current_vs_high / 100.0).fillna(0.0)

    flow_stage = frame.get("flow_stage", pd.Series("", index=frame.index)).astype(str).str.lower()
    proxy = flow_stage.map(
        {
            "abandoned": 0.10,
            "latent": 0.20,
            "ignition": 0.40,
            "trend": 0.70,
            "crowded": 0.95,
        }
    )
    return pd.to_numeric(proxy, errors="coerce").fillna(0.0)


def _resolve_philosophy_fit(
    frame: pd.DataFrame,
    recognition_upside: pd.Series,
    fundamental_momentum: pd.Series,
    price_strength: pd.Series,
) -> pd.Series:
    direct = _numeric_series(frame, "philosophy_fit_score")
    if direct.notna().any():
        return direct.fillna(0.0)

    floor_protection = _numeric_series(frame, "underwrite_intrinsic_value_floor_floor_protection")
    reject_reason = frame.get("reject_reason", pd.Series("", index=frame.index)).fillna("").astype(str)

    score = pd.Series(65.0, index=frame.index, dtype="float64")
    score = score + (floor_protection >= 0.90).astype(float) * 10.0
    score = score + (recognition_upside >= 0.25).astype(float) * 10.0
    score = score + (fundamental_momentum >= 60.0).astype(float) * 10.0
    score = score - (price_strength >= 0.85).astype(float) * 10.0
    score = score - reject_reason.ne("").astype(float) * 25.0
    return score.clip(lower=0.0, upper=100.0)


def _annotate_selection_metrics(month_end_signals: pd.DataFrame) -> pd.DataFrame:
    if month_end_signals is None or month_end_signals.empty:
        return month_end_signals.copy()

    annotated = month_end_signals.copy()
    annotated["sleeve"] = annotated.get("primary_type", pd.Series("", index=annotated.index)).map(_resolve_sleeve)
    current_price = _resolve_current_price(annotated)
    recognition_upside = _resolve_recognition_upside(annotated, current_price).fillna(0.0)
    fundamental_momentum = _resolve_fundamental_momentum(annotated).fillna(0.0)
    price_strength = _resolve_price_strength(annotated).fillna(0.0)
    philosophy_fit = _resolve_philosophy_fit(annotated, recognition_upside, fundamental_momentum, price_strength).fillna(0.0)
    flow_confirmation = _numeric_series(annotated, "realization_flow_confirmation_score").combine_first(_numeric_series(annotated, "realization_score")).fillna(0.0)
    total_score = _numeric_series(annotated, "total_score").fillna(0.0)
    underwrite_score = _numeric_series(annotated, "underwrite_score").fillna(0.0)

    expectation_error = _zscore(recognition_upside) + _zscore(fundamental_momentum) - _zscore(price_strength)
    candidate_rank = (
        0.30 * _zscore(total_score)
        + 0.25 * expectation_error
        + 0.20 * _zscore(underwrite_score)
        + 0.15 * _zscore(flow_confirmation)
        + 0.10 * _zscore(philosophy_fit)
    )

    annotated["current_price"] = current_price
    annotated["recognition_upside_signal"] = recognition_upside
    annotated["fundamental_momentum_score"] = fundamental_momentum
    annotated["price_strength_12m"] = price_strength
    annotated["philosophy_fit_score"] = philosophy_fit
    annotated["expectation_error_score"] = expectation_error.round(6)
    annotated["candidate_rank"] = candidate_rank.round(6)
    return annotated


def _apply_sector_overlay(month_end_signals: pd.DataFrame, protocol: dict[str, Any]) -> pd.DataFrame:
    if month_end_signals is None or month_end_signals.empty:
        return month_end_signals.copy()

    overlay_cfg = protocol.get("sector_overlay", {}) or {}
    allow_idiosyncratic_override = bool(overlay_cfg.get("allow_idiosyncratic_override", True))
    sector_score_weight = float(overlay_cfg.get("sector_score_weight", 0.0) or 0.0)

    sector_snapshot = build_sector_snapshot(month_end_signals)
    annotated = merge_sector_overlay(month_end_signals, sector_snapshot)
    annotated["idiosyncratic_override"] = (
        annotated.get("primary_type", pd.Series("", index=annotated.index)).fillna("").astype(str).str.lower().isin({"turnaround", "asset_play", "special_situation"})
        & (_numeric_series(annotated, "underwrite_score").fillna(0.0) >= 80.0)
        & (_resolve_recognition_upside(annotated, _resolve_current_price(annotated)).fillna(0.0) >= 0.35)
    )
    if not allow_idiosyncratic_override:
        annotated["idiosyncratic_override"] = False

    sector_component = _zscore(pd.to_numeric(annotated.get("sector_cycle_score"), errors="coerce").fillna(50.0))
    annotated["selection_rank"] = (annotated["candidate_rank"] + sector_score_weight * sector_component).round(6)
    return annotated


def select_round_candidates(month_end_signals: pd.DataFrame, protocol: dict[str, Any] | None = None) -> pd.DataFrame:
    cfg = _merge_protocol(protocol)
    derived_columns = [
        "vcrf_state_rank",
        "sleeve",
        "candidate_rank",
        "expectation_error_score",
        "industry_group",
        "sector_cycle_sensitive",
        "sector_member_count",
        "sector_cycle_score",
        "sector_cycle_state",
        "idiosyncratic_override",
        "selection_rank",
        "round_id",
        "slot_in_round",
    ]
    base_columns = list(month_end_signals.columns) + derived_columns if month_end_signals is not None else derived_columns
    if month_end_signals is None or month_end_signals.empty:
        return pd.DataFrame(columns=base_columns)

    overlay_cfg = cfg.get("sector_overlay", {}) or {}
    overlay_enabled = bool(overlay_cfg.get("enabled", False))
    min_sector_members = int(overlay_cfg.get("min_sector_members_for_gate", 3) or 0)
    candidates = _apply_sector_overlay(_annotate_selection_metrics(month_end_signals), cfg)
    candidates["vcrf_state_rank"] = candidates["vcrf_state"].map(_state_rank)

    if overlay_enabled and bool(overlay_cfg.get("favored_only_for_cycle_sensitive", True)):
        cycle_sensitive = candidates.get("sector_cycle_sensitive", pd.Series(False, index=candidates.index)).fillna(False).astype(bool)
        enough_members = pd.to_numeric(candidates.get("sector_member_count"), errors="coerce").fillna(0).astype(int) >= min_sector_members
        favored = candidates.get("sector_cycle_state", pd.Series("neutral", index=candidates.index)).fillna("neutral").astype(str).str.lower().eq("favored")
        overlay_pass = (~cycle_sensitive) | (~enough_members) | favored | candidates["idiosyncratic_override"].astype(bool)
    else:
        overlay_pass = pd.Series(True, index=candidates.index, dtype="bool")

    candidates = candidates[
        (candidates["vcrf_state_rank"] == _state_rank("ATTACK"))
        & (candidates["tradable_flag"].fillna(1).astype(int) == 1)
        & overlay_pass
    ].copy()
    if candidates.empty:
        return pd.DataFrame(columns=base_columns)

    sort_columns = [column for column in ("selection_rank", "candidate_rank", "total_score", "underwrite_score", "ticker") if column in candidates.columns]
    ascending = [False if column != "ticker" else True for column in sort_columns]
    candidates = candidates.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)

    selected_rows: list[dict[str, Any]] = []
    used_tickers: set[str] = set()
    round_size = int(cfg.get("round_size", 3))
    total_rounds = int(cfg.get("total_rounds", 10))
    exclude_reuse = bool(cfg.get("exclude_used_tickers_across_rounds", True))
    max_positions_per_industry_group = int(overlay_cfg.get("max_positions_per_industry_group", 0) or 0)
    max_positions_per_sector_route = int(overlay_cfg.get("max_positions_per_sector_route", 0) or 0)

    for round_id in range(1, total_rounds + 1):
        pool = candidates[~candidates["ticker"].astype(str).str.upper().isin(used_tickers)].copy()
        if pool.empty:
            break

        round_selected: list[dict[str, Any]] = []
        round_tickers: set[str] = set()
        used_sleeves: set[str] = set()
        used_routes: set[str] = set()
        industry_counts: dict[str, int] = {}
        route_counts: dict[str, int] = {}

        def _try_fill(*, enforce_unique_sleeve: bool, enforce_unique_route: bool) -> None:
            nonlocal round_selected
            if len(round_selected) >= round_size:
                return
            for _, row in pool.iterrows():
                ticker = str(row["ticker"]).upper()
                sleeve = _normalize_label(row.get("sleeve")) or "unslotted"
                route = _normalize_label(row.get("sector_route")) or "unknown"
                industry_group = _normalize_label(row.get("industry_group")) or "unknown"
                if ticker in round_tickers:
                    continue
                if enforce_unique_sleeve and sleeve in used_sleeves:
                    continue
                if enforce_unique_route and route not in {"", "unknown"} and route in used_routes:
                    continue
                if max_positions_per_industry_group > 0 and industry_group not in {"", "unknown"} and industry_counts.get(industry_group, 0) >= max_positions_per_industry_group:
                    continue
                if max_positions_per_sector_route > 0 and route not in {"", "unknown"} and route_counts.get(route, 0) >= max_positions_per_sector_route:
                    continue
                round_selected.append(row.to_dict())
                round_tickers.add(ticker)
                used_sleeves.add(sleeve)
                if industry_group not in {"", "unknown"}:
                    industry_counts[industry_group] = industry_counts.get(industry_group, 0) + 1
                if route not in {"", "unknown"}:
                    used_routes.add(route)
                    route_counts[route] = route_counts.get(route, 0) + 1
                if len(round_selected) >= round_size:
                    break

        _try_fill(enforce_unique_sleeve=True, enforce_unique_route=True)
        _try_fill(enforce_unique_sleeve=False, enforce_unique_route=True)
        _try_fill(enforce_unique_sleeve=False, enforce_unique_route=False)

        if not round_selected:
            break

        for slot_in_round, row in enumerate(round_selected, start=1):
            selected_rows.append({**row, "round_id": round_id, "slot_in_round": slot_in_round})
            if exclude_reuse:
                used_tickers.add(str(row["ticker"]).upper())

    if not selected_rows:
        return pd.DataFrame(columns=base_columns)
    return pd.DataFrame(selected_rows)


def _annualized_return(gross_return: float, hold_days: int) -> float | None:
    if hold_days <= 0:
        return None
    years = hold_days / 365.0
    if years <= 0 or gross_return <= -1.0:
        return None
    return (1 + gross_return) ** (1 / years) - 1


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    return float(drawdown.min())


def _mfe_pct(position: Position) -> float | None:
    if position.entry_price <= 0 or position.max_high_price in (None, 0):
        return None
    return position.max_high_price / position.entry_price - 1


def _mae_pct(position: Position) -> float | None:
    if position.entry_price <= 0 or position.min_low_price in (None, 0):
        return None
    return position.min_low_price / position.entry_price - 1


def _close_position(
    *,
    position: Position,
    trade_date: pd.Timestamp,
    raw_exit_price: float,
    exit_reason: str,
    cash: float,
    cost_cfg: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    exit_price = _fill_price("sell", raw_exit_price, cost_cfg)
    costs = _trade_cost("sell", trade_date, exit_price, position.shares, cost_cfg)
    proceeds = exit_price * position.shares - costs
    new_cash = cash + proceeds
    gross_return = exit_price / position.entry_price - 1 if position.entry_price else 0.0
    hold_days = max((trade_date - position.entry_date).days, 0) + 1
    return new_cash, {
        "ticker": position.ticker,
        "entry_date": position.entry_date,
        "exit_date": trade_date,
        "entry_price": round(position.entry_price, 4),
        "exit_price": round(exit_price, 4),
        "shares": position.shares,
        "primary_type": position.primary_type,
        "sector_route": position.sector_route,
        "industry_group": position.industry_group,
        "sleeve": position.sleeve,
        "slot_in_round": position.slot_in_round,
        "hold_bars": position.bars_held + 1,
        "hold_days": hold_days,
        "days_to_target": hold_days if exit_reason == "target_hit" else None,
        "exit_reason": exit_reason,
        "gross_return": round(gross_return, 6),
        "net_pnl": round(proceeds - position.cost_basis_cash, 2),
        "annualized_return": _annualized_return(gross_return, hold_days),
        "mfe_pct": round(_mfe_pct(position), 6) if _mfe_pct(position) is not None else None,
        "mae_pct": round(_mae_pct(position), 6) if _mae_pct(position) is not None else None,
    }


def _max_loss_pct_for_type(primary_type: str, protocol: dict[str, Any]) -> float | None:
    overrides = protocol.get("max_loss_pct_by_type", {}) or {}
    normalized = _normalize_label(primary_type)
    if normalized in overrides:
        value = overrides.get(normalized)
        if value in (None, "", False):
            return None
        return float(value)
    fallback = protocol.get("max_loss_pct")
    if fallback in (None, "", False):
        return None
    return float(fallback)


def _refresh_position_from_signal(position: Position, signal_row: dict[str, Any], protocol: dict[str, Any]) -> None:
    min_state = str(protocol.get("dynamic_target_refresh_min_state", "READY")).upper()
    if _state_rank(signal_row.get("vcrf_state")) < _state_rank(min_state):
        return
    floor_price = signal_row.get("floor_price")
    recognition_price = signal_row.get("recognition_price")
    if pd.notna(floor_price):
        position.floor_price = float(floor_price)
    if pd.notna(recognition_price):
        position.recognition_price = float(recognition_price)
    primary_type = _normalize_label(signal_row.get("primary_type"))
    sector_route = _normalize_label(signal_row.get("sector_route"))
    industry_group = _normalize_label(signal_row.get("industry_group"))
    if primary_type:
        position.primary_type = primary_type
        position.sleeve = _resolve_sleeve(primary_type)
    if sector_route:
        position.sector_route = sector_route
    if industry_group:
        position.industry_group = industry_group
    if "sector_cycle_sensitive" in signal_row:
        position.sector_cycle_sensitive = _as_bool(signal_row.get("sector_cycle_sensitive"))


def _open_position(
    *,
    entry: dict[str, Any],
    trade_date: pd.Timestamp,
    bar: pd.Series,
    cash: float,
    target_value: float,
    lot_size: int,
    cost_cfg: dict[str, Any],
    slot_in_round: int,
) -> tuple[float, Position | None, str | None]:
    entry_price = _fill_price("buy", float(bar["open"]), cost_cfg)
    shares = _lot_round_shares(target_value, entry_price, lot_size)
    if shares <= 0:
        return cash, None, "target value too small for one lot"
    costs = _trade_cost("buy", trade_date, entry_price, shares, cost_cfg)
    total_cost = entry_price * shares + costs
    if total_cost > cash:
        return cash, None, "insufficient cash"

    position = Position(
        ticker=str(entry["ticker"]).upper(),
        shares=shares,
        entry_date=trade_date,
        entry_price=entry_price,
        floor_price=float(entry["floor_price"]) if pd.notna(entry.get("floor_price")) else None,
        recognition_price=float(entry["recognition_price"]) if pd.notna(entry.get("recognition_price")) else None,
        cost_basis_cash=total_cost,
        primary_type=_normalize_label(entry.get("primary_type")) or "unknown",
        sector_route=_normalize_label(entry.get("sector_route")) or "unknown",
        industry_group=_normalize_label(entry.get("industry_group")) or "unknown",
        sector_cycle_sensitive=_as_bool(entry.get("sector_cycle_sensitive")),
        sleeve=_normalize_label(entry.get("sleeve")) or _resolve_sleeve(entry.get("primary_type")),
        slot_in_round=slot_in_round,
        last_known_price=entry_price,
        max_high_price=float(bar["high"]),
        min_low_price=float(bar["low"]),
    )
    return cash - total_cost, position, None


def _median_or_none(series: pd.Series) -> float | None:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    return float(cleaned.median())


def _expectancy(trades: pd.DataFrame) -> float | None:
    if trades.empty:
        return None
    gross = pd.to_numeric(trades["gross_return"], errors="coerce").dropna()
    if gross.empty:
        return None
    wins = gross[gross > 0]
    losses = gross[gross <= 0]
    win_rate = len(wins) / len(gross)
    avg_win = float(wins.mean()) if not wins.empty else 0.0
    avg_loss = float(losses.mean()) if not losses.empty else 0.0
    return win_rate * avg_win + (1 - win_rate) * avg_loss


def _mfe_mae_ratio(trades: pd.DataFrame) -> float | None:
    if trades.empty or "mfe_pct" not in trades.columns or "mae_pct" not in trades.columns:
        return None
    mfe = pd.to_numeric(trades["mfe_pct"], errors="coerce")
    mae = pd.to_numeric(trades["mae_pct"], errors="coerce")
    ratio = mfe / mae.abs()
    ratio = ratio[(mae < 0) & ratio.notna()]
    if ratio.empty:
        return None
    return float(ratio.median())


def _run_one_round(
    round_id: int,
    candidates: pd.DataFrame,
    signal_daily: pd.DataFrame,
    daily_bars: pd.DataFrame,
    protocol: dict[str, Any],
) -> dict[str, Any]:
    initial_cash = float(protocol.get("initial_cash", 1_000_000))
    round_size = int(protocol.get("round_size", 3))
    lot_size = int(protocol.get("lot_size", 100))
    max_holding_bars = int(protocol.get("max_holding_bars", 504))
    same_bar_conflict = str(protocol.get("same_bar_conflict", "stop_first")).lower()
    cost_cfg = protocol.get("costs", {}) or {}
    allow_refill = bool(protocol.get("allow_refill_from_waitlist", False))
    allow_reentry = bool(protocol.get("allow_reentry_within_round", False))
    overlay_cfg = protocol.get("sector_overlay", {}) or {}
    max_positions_per_industry_group = int(overlay_cfg.get("max_positions_per_industry_group", 0) or 0)

    slot_sleeves = set(candidates.get("sleeve", pd.Series(dtype="object")).fillna("").astype(str).str.lower())
    relevant_signals = signal_daily[signal_daily.get("sleeve", pd.Series("", index=signal_daily.index)).fillna("").astype(str).str.lower().isin(slot_sleeves)].copy()
    relevant_signals = pd.concat([relevant_signals, signal_daily[signal_daily["ticker"].isin(candidates["ticker"])]], ignore_index=True).drop_duplicates(subset=["ticker", "date", "effective_date"], keep="last")
    relevant_tickers = relevant_signals["ticker"].astype(str).str.upper().unique().tolist()

    bars = daily_bars.copy()
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()
    bars["ticker"] = bars["ticker"].astype(str).str.upper()
    if relevant_tickers:
        bars = bars[bars["ticker"].isin(relevant_tickers)]
    bars = bars.sort_values(["date", "ticker"]).reset_index(drop=True)

    daily_signals = relevant_signals.copy()
    daily_signals["date"] = pd.to_datetime(daily_signals["date"]).dt.normalize()
    daily_signals["ticker"] = daily_signals["ticker"].astype(str).str.upper()
    rank_column = "selection_rank" if "selection_rank" in daily_signals.columns else "candidate_rank"
    daily_signals = daily_signals.sort_values(["date", rank_column, "ticker"], ascending=[True, False, True]).reset_index(drop=True)

    cash = initial_cash
    target_value = initial_cash / max(len(candidates), 1)
    positions: dict[str, Position] = {}
    used_tickers: set[str] = set()
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    anomalies: list[str] = []
    peak_gross_exposure = 0.0

    slot_specs = {
        int(row["slot_in_round"]): {
            "sleeve": _normalize_label(row.get("sleeve")) or _resolve_sleeve(row.get("primary_type")),
        }
        for _, row in candidates.iterrows()
    }
    vacant_slots: set[int] = set(slot_specs)

    entries_by_date: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    for _, row in candidates.iterrows():
        entries_by_date.setdefault(pd.Timestamp(row["effective_date"]).normalize(), []).append(row.to_dict())

    for trade_date, bars_today in bars.groupby("date", sort=True):
        bars_map = {row["ticker"]: row for _, row in bars_today.iterrows()}
        signals_today_df = daily_signals[daily_signals["date"] == trade_date].copy()
        signals_map = {row["ticker"]: row for _, row in signals_today_df.iterrows()}

        for ticker, position in list(positions.items()):
            bar = bars_map.get(ticker)
            if bar is None:
                continue
            position.max_high_price = max(position.max_high_price, float(bar["high"]))
            position.min_low_price = min(position.min_low_price, float(bar["low"]))

            signal_row = signals_map.get(ticker, {})
            _refresh_position_from_signal(position, signal_row, protocol)
            if _state_rank(signal_row.get("vcrf_state")) == _state_rank("REJECT"):
                cash, trade = _close_position(
                    position=position,
                    trade_date=trade_date,
                    raw_exit_price=float(bar["open"]),
                    exit_reason="state_reject",
                    cash=cash,
                    cost_cfg=cost_cfg,
                )
                trade_rows.append(trade)
                vacant_slots.add(position.slot_in_round)
                del positions[ticker]
                continue
            if position.bars_held >= max_holding_bars:
                cash, trade = _close_position(
                    position=position,
                    trade_date=trade_date,
                    raw_exit_price=float(bar["open"]),
                    exit_reason="time_exit",
                    cash=cash,
                    cost_cfg=cost_cfg,
                )
                trade_rows.append(trade)
                vacant_slots.add(position.slot_in_round)
                del positions[ticker]

        for entry in entries_by_date.get(trade_date, []):
            ticker = str(entry["ticker"]).upper()
            slot_in_round = int(entry.get("slot_in_round") or 0)
            if ticker in positions or slot_in_round not in vacant_slots:
                continue
            if not allow_reentry and ticker in used_tickers:
                continue
            bar = bars_map.get(ticker)
            signal_row = signals_map.get(ticker, {})
            if bar is None or _state_rank(signal_row.get("vcrf_state")) != _state_rank("ATTACK"):
                continue
            if int(signal_row.get("tradable_flag", entry.get("tradable_flag", 1)) or 0) != 1:
                anomalies.append(f"{trade_date.date()} {ticker} not tradable at entry")
                continue
            cash, position, error = _open_position(
                entry=entry,
                trade_date=trade_date,
                bar=bar,
                cash=cash,
                target_value=target_value,
                lot_size=lot_size,
                cost_cfg=cost_cfg,
                slot_in_round=slot_in_round,
            )
            if position is None:
                anomalies.append(f"{trade_date.date()} {ticker} {error}")
                continue
            positions[ticker] = position
            vacant_slots.discard(slot_in_round)
            if not allow_reentry:
                used_tickers.add(ticker)

        if allow_refill and vacant_slots:
            refill_sort_columns = [rank_column]
            refill_sort_ascending = [False]
            if rank_column != "candidate_rank":
                refill_sort_columns.append("candidate_rank")
                refill_sort_ascending.append(False)
            refill_sort_columns.extend(["total_score", "ticker"])
            refill_sort_ascending.extend([False, True])
            refill_pool = signals_today_df[
                pd.to_datetime(signals_today_df["effective_date"]).dt.normalize() == trade_date
            ].sort_values(refill_sort_columns, ascending=refill_sort_ascending)
            for slot_in_round in sorted(vacant_slots):
                slot_sleeve = slot_specs.get(slot_in_round, {}).get("sleeve", "")
                active_routes = {_normalize_label(pos.sector_route) for pos in positions.values() if _normalize_label(pos.sector_route) not in {"", "unknown"}}
                active_groups: dict[str, int] = {}
                for pos in positions.values():
                    group = _normalize_label(pos.industry_group)
                    if group in {"", "unknown"}:
                        continue
                    active_groups[group] = active_groups.get(group, 0) + 1
                eligible = refill_pool[
                    (refill_pool["sleeve"].fillna("").astype(str).str.lower() == slot_sleeve)
                    & (refill_pool["vcrf_state"].map(_state_rank) == _state_rank("ATTACK"))
                    & (refill_pool["tradable_flag"].fillna(1).astype(int) == 1)
                    & (~refill_pool["ticker"].astype(str).str.upper().isin(positions.keys()))
                    & (~refill_pool["ticker"].astype(str).str.upper().isin(used_tickers))
                ]
                if eligible.empty:
                    continue

                eligible_routes = eligible.get("sector_route", pd.Series("", index=eligible.index)).fillna("").astype(str).str.lower()
                eligible_groups = eligible.get("industry_group", pd.Series("", index=eligible.index)).fillna("").astype(str).str.lower()
                if max_positions_per_industry_group > 0:
                    group_mask = ~eligible_groups.isin([group for group, count in active_groups.items() if count >= max_positions_per_industry_group])
                    eligible = eligible[group_mask]
                    eligible_routes = eligible.get("sector_route", pd.Series("", index=eligible.index)).fillna("").astype(str).str.lower()
                if eligible.empty:
                    continue
                route_first = eligible[~eligible_routes.isin(active_routes)]
                chosen = route_first.iloc[0].to_dict() if not route_first.empty else eligible.iloc[0].to_dict()
                ticker = str(chosen["ticker"]).upper()
                bar = bars_map.get(ticker)
                if bar is None:
                    continue
                cash, position, error = _open_position(
                    entry=chosen,
                    trade_date=trade_date,
                    bar=bar,
                    cash=cash,
                    target_value=target_value,
                    lot_size=lot_size,
                    cost_cfg=cost_cfg,
                    slot_in_round=slot_in_round,
                )
                if position is None:
                    anomalies.append(f"{trade_date.date()} {ticker} {error}")
                    continue
                positions[ticker] = position
                vacant_slots.discard(slot_in_round)
                if not allow_reentry:
                    used_tickers.add(ticker)
                refill_pool = refill_pool[refill_pool["ticker"].astype(str).str.upper() != ticker]

        for ticker, position in list(positions.items()):
            bar = bars_map.get(ticker)
            if bar is None:
                continue
            floor_price = position.floor_price
            recognition_price = position.recognition_price
            max_loss_pct = _max_loss_pct_for_type(position.primary_type, protocol)
            hit_floor = floor_price is not None and floor_price < position.entry_price and float(bar["low"]) <= floor_price
            max_loss_price = position.entry_price * (1 - max_loss_pct) if max_loss_pct not in (None, 0) else None
            hit_max_loss = max_loss_price is not None and max_loss_price < position.entry_price and float(bar["low"]) <= max_loss_price
            hit_target = recognition_price is not None and float(bar["high"]) >= recognition_price

            stop_candidates: list[tuple[str, float]] = []
            if hit_floor and floor_price is not None:
                stop_candidates.append(("floor_stop", float(floor_price)))
            if hit_max_loss and max_loss_price is not None:
                stop_candidates.append(("max_loss_stop", float(max_loss_price)))

            if stop_candidates:
                stop_reason, stop_price = max(stop_candidates, key=lambda item: item[1])
            else:
                stop_reason, stop_price = None, None

            if stop_price is not None and hit_target:
                raw_exit_price = stop_price if same_bar_conflict == "stop_first" else recognition_price
                exit_reason = stop_reason if same_bar_conflict == "stop_first" else "target_hit"
            elif stop_price is not None:
                raw_exit_price = stop_price
                exit_reason = str(stop_reason)
            elif hit_target:
                raw_exit_price = recognition_price
                exit_reason = "target_hit"
            else:
                continue

            cash, trade = _close_position(
                position=position,
                trade_date=trade_date,
                raw_exit_price=float(raw_exit_price),
                exit_reason=exit_reason,
                cash=cash,
                cost_cfg=cost_cfg,
            )
            trade_rows.append(trade)
            vacant_slots.add(position.slot_in_round)
            del positions[ticker]

        deployed_value = 0.0
        gross_exposure = 0.0
        for ticker, position in positions.items():
            bar = bars_map.get(ticker)
            gross_exposure += position.entry_price * position.shares
            if bar is not None:
                close_price = float(bar["close"])
                deployed_value += close_price * position.shares
                position.last_known_price = close_price
                position.bars_held += 1
            else:
                deployed_value += position.last_known_price * position.shares
        peak_gross_exposure = max(peak_gross_exposure, gross_exposure)
        equity_rows.append({"date": trade_date, "equity": round(cash + deployed_value, 2)})

    if positions and not bars.empty:
        last_date = pd.Timestamp(bars["date"].max()).normalize()
        last_bars = bars[bars["date"] == last_date].set_index("ticker")
        for ticker, position in list(positions.items()):
            if ticker in last_bars.index:
                exit_price = float(last_bars.loc[ticker, "close"])
                exit_reason = "end_of_data"
            else:
                exit_price = position.last_known_price
                exit_reason = "end_of_data_suspended"
            cash, trade = _close_position(
                position=position,
                trade_date=last_date,
                raw_exit_price=exit_price,
                exit_reason=exit_reason,
                cash=cash,
                cost_cfg=cost_cfg,
            )
            trade_rows.append(trade)
            del positions[ticker]

    trades = pd.DataFrame(trade_rows).sort_values(["entry_date", "ticker"]).reset_index(drop=True) if trade_rows else pd.DataFrame()
    equity = pd.DataFrame(equity_rows).sort_values("date").reset_index(drop=True) if equity_rows else pd.DataFrame(columns=["date", "equity"])
    portfolio_cagr = None
    if not equity.empty and len(equity) >= 2:
        days = max((equity["date"].iloc[-1] - equity["date"].iloc[0]).days, 1)
        portfolio_cagr = (equity["equity"].iloc[-1] / initial_cash) ** (365.0 / days) - 1
    avg_stock_cagr = None
    if not trades.empty and trades["annualized_return"].notna().any():
        avg_stock_cagr = float(trades["annualized_return"].dropna().mean())
    win_rate = None
    if not trades.empty:
        win_rate = float((trades["net_pnl"] > 0).mean())

    candidate_diagnostics = candidates[
        [
            column
            for column in (
                "ticker",
                "underwrite_score",
                "realization_score",
                "candidate_rank",
                "selection_rank",
                "expectation_error_score",
                "sleeve",
                "industry_group",
                "sector_cycle_score",
                "sector_cycle_state",
                "idiosyncratic_override",
                "floor_price",
                "recognition_price",
                "vcrf_state",
                "position_state",
                "primary_type",
                "flow_stage",
            )
            if column in candidates.columns
        ]
    ].to_dict(orient="records")

    summary = {
        "round_id": round_id,
        "tickers": candidates["ticker"].tolist(),
        "round_final_value": round(float(equity["equity"].iloc[-1]) if not equity.empty else cash, 2),
        "portfolio_cagr": portfolio_cagr,
        "avg_stock_cagr": avg_stock_cagr,
        "median_trade_irr": _median_or_none(trades["annualized_return"]) if not trades.empty and "annualized_return" in trades.columns else None,
        "expectancy": _expectancy(trades),
        "mfe_mae_ratio": _mfe_mae_ratio(trades),
        "median_days_to_target": _median_or_none(trades.loc[trades["exit_reason"] == "target_hit", "days_to_target"]) if not trades.empty else None,
        "max_drawdown": _max_drawdown(equity["equity"]) if not equity.empty else 0.0,
        "win_rate": win_rate,
        "used_cash_ratio": peak_gross_exposure / initial_cash if initial_cash else 0.0,
        "peak_gross_exposure_ratio": peak_gross_exposure / initial_cash if initial_cash else 0.0,
        "target_hit_rate": float((trades["exit_reason"] == "target_hit").mean()) if not trades.empty else None,
        "floor_stop_rate": float((trades["exit_reason"] == "floor_stop").mean()) if not trades.empty else None,
        "max_loss_stop_rate": float((trades["exit_reason"] == "max_loss_stop").mean()) if not trades.empty else None,
        "state_reject_rate": float((trades["exit_reason"] == "state_reject").mean()) if not trades.empty else None,
        "exit_reason_counts": trades["exit_reason"].value_counts().to_dict() if not trades.empty else {},
        "candidate_diagnostics": candidate_diagnostics,
        "anomalies": anomalies,
    }
    return {"round_id": round_id, "summary": summary, "trades": trades, "equity": equity}


def run_vcrf_backtest(
    month_end_signals: pd.DataFrame,
    daily_bars: pd.DataFrame,
    *,
    protocol: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = _merge_protocol(protocol)
    bars = daily_bars.copy()
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()
    bars["ticker"] = bars["ticker"].astype(str).str.upper()
    bars = bars.sort_values(["ticker", "date"]).reset_index(drop=True)
    trading_days = pd.DatetimeIndex(sorted(bars["date"].unique()))
    normalized_month_end = normalize_signal_month_end(month_end_signals, trading_days)
    annotated_month_end = _annotate_selection_metrics(normalized_month_end)
    overlay_month_end = _apply_sector_overlay(annotated_month_end, cfg)
    signal_daily = expand_signal_daily(overlay_month_end, bars)
    selected = select_round_candidates(overlay_month_end, cfg)
    if selected.empty or "round_id" not in selected.columns:
        return {
            "protocol": cfg,
            "selected_candidates": selected,
            "signal_month_end": overlay_month_end,
            "signal_daily": signal_daily,
            "rounds": [],
            "summary": pd.DataFrame(),
        }

    rounds: list[dict[str, Any]] = []
    for round_id, candidates in selected.groupby("round_id", sort=True):
        rounds.append(_run_one_round(int(round_id), candidates, signal_daily, bars, cfg))

    summary = pd.DataFrame([round_result["summary"] for round_result in rounds]) if rounds else pd.DataFrame()
    return {
        "protocol": cfg,
        "selected_candidates": selected,
        "signal_month_end": overlay_month_end,
        "signal_daily": signal_daily,
        "rounds": rounds,
        "summary": summary,
    }
