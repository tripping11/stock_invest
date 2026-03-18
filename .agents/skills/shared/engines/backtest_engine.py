"""Deterministic event-driven backtest engine for VCRF signals."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

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
    bars_held: int = 0
    last_known_price: float = 0.0


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


def select_round_candidates(month_end_signals: pd.DataFrame, protocol: dict[str, Any] | None = None) -> pd.DataFrame:
    cfg = _merge_protocol(protocol)
    base_columns = list(month_end_signals.columns) + ["vcrf_state_rank", "round_id", "slot_in_round"] if month_end_signals is not None else ["vcrf_state_rank", "round_id", "slot_in_round"]
    if month_end_signals is None or month_end_signals.empty:
        return pd.DataFrame(columns=base_columns)

    candidates = month_end_signals.copy()
    candidates["vcrf_state_rank"] = candidates["vcrf_state"].map(_state_rank)
    candidates = candidates[
        (candidates["vcrf_state_rank"] == _state_rank("ATTACK"))
        & (candidates["tradable_flag"].fillna(1).astype(int) == 1)
    ].sort_values(["effective_date", "total_score", "ticker"], ascending=[True, False, True])

    selected_rows: list[dict[str, Any]] = []
    used_tickers: set[str] = set()
    round_size = int(cfg.get("round_size", 3))
    total_rounds = int(cfg.get("total_rounds", 10))
    exclude_reuse = bool(cfg.get("exclude_used_tickers_across_rounds", True))

    for _, row in candidates.iterrows():
        ticker = str(row["ticker"])
        if exclude_reuse and ticker in used_tickers:
            continue
        round_id = len(selected_rows) // round_size + 1
        if round_id > total_rounds:
            break
        slot_in_round = len(selected_rows) % round_size + 1
        selected_rows.append({**row.to_dict(), "round_id": round_id, "slot_in_round": slot_in_round})
        if exclude_reuse:
            used_tickers.add(ticker)

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
        "hold_bars": position.bars_held + 1,
        "hold_days": hold_days,
        "exit_reason": exit_reason,
        "gross_return": round(gross_return, 6),
        "net_pnl": round(proceeds - position.cost_basis_cash, 2),
        "annualized_return": _annualized_return(gross_return, hold_days),
    }


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

    bars = daily_bars.copy()
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()
    bars["ticker"] = bars["ticker"].astype(str).str.upper()
    bars = bars.sort_values(["date", "ticker"]).reset_index(drop=True)

    daily_signals = signal_daily.copy()
    daily_signals["date"] = pd.to_datetime(daily_signals["date"]).dt.normalize()
    daily_signals["ticker"] = daily_signals["ticker"].astype(str).str.upper()
    daily_signals = daily_signals.sort_values(["date", "ticker"]).reset_index(drop=True)

    cash = initial_cash
    target_value = initial_cash / max(round_size, 1)
    positions: dict[str, Position] = {}
    used_tickers: set[str] = set()
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    anomalies: list[str] = []
    peak_deployed = 0.0

    entries_by_date: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    for _, row in candidates.iterrows():
        entries_by_date.setdefault(pd.Timestamp(row["effective_date"]).normalize(), []).append(row.to_dict())

    for trade_date, bars_today in bars.groupby("date", sort=True):
        bars_map = {row["ticker"]: row for _, row in bars_today.iterrows()}
        signals_today_df = daily_signals[daily_signals["date"] == trade_date]
        signals_map = {row["ticker"]: row for _, row in signals_today_df.iterrows()}

        for ticker, position in list(positions.items()):
            bar = bars_map.get(ticker)
            if bar is None:
                continue
            signal_row = signals_map.get(ticker, {})
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
                del positions[ticker]

        for entry in entries_by_date.get(trade_date, []):
            ticker = str(entry["ticker"]).upper()
            if ticker in positions or ticker in used_tickers:
                continue
            bar = bars_map.get(ticker)
            signal_row = signals_map.get(ticker, {})
            if bar is None or _state_rank(signal_row.get("vcrf_state")) != _state_rank("ATTACK"):
                continue
            if int(signal_row.get("tradable_flag", entry.get("tradable_flag", 1)) or 0) != 1:
                anomalies.append(f"{trade_date.date()} {ticker} not tradable at entry")
                continue
            entry_price = _fill_price("buy", float(bar["open"]), cost_cfg)
            shares = _lot_round_shares(target_value, entry_price, lot_size)
            if shares <= 0:
                anomalies.append(f"{trade_date.date()} {ticker} target value too small for one lot")
                continue
            costs = _trade_cost("buy", trade_date, entry_price, shares, cost_cfg)
            total_cost = entry_price * shares + costs
            if total_cost > cash:
                continue
            cash -= total_cost
            positions[ticker] = Position(
                ticker=ticker,
                shares=shares,
                entry_date=trade_date,
                entry_price=entry_price,
                floor_price=float(entry["floor_price"]) if pd.notna(entry.get("floor_price")) else None,
                recognition_price=float(entry["recognition_price"]) if pd.notna(entry.get("recognition_price")) else None,
                cost_basis_cash=total_cost,
                last_known_price=entry_price,
            )
            used_tickers.add(ticker)

        for ticker, position in list(positions.items()):
            bar = bars_map.get(ticker)
            if bar is None:
                continue
            floor_price = position.floor_price
            recognition_price = position.recognition_price
            hit_floor = floor_price is not None and float(bar["low"]) <= floor_price
            hit_target = recognition_price is not None and float(bar["high"]) >= recognition_price
            if hit_floor and hit_target:
                raw_exit_price = floor_price if same_bar_conflict == "stop_first" else recognition_price
                exit_reason = "floor_stop" if same_bar_conflict == "stop_first" else "target_hit"
            elif hit_floor:
                raw_exit_price = floor_price
                exit_reason = "floor_stop"
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
            del positions[ticker]

        deployed_value = 0.0
        for ticker, position in positions.items():
            bar = bars_map.get(ticker)
            if bar is not None:
                close_price = float(bar["close"])
                deployed_value += close_price * position.shares
                position.last_known_price = close_price
                position.bars_held += 1
            else:
                # Ticker suspended today — carry last known value and do NOT increment bars_held.
                deployed_value += position.last_known_price * position.shares
        peak_deployed = max(peak_deployed, deployed_value)
        equity_rows.append({"date": trade_date, "equity": round(cash + deployed_value, 2)})

    if positions:
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
    summary = {
        "round_id": round_id,
        "tickers": candidates["ticker"].tolist(),
        "round_final_value": round(float(equity["equity"].iloc[-1]) if not equity.empty else cash, 2),
        "avg_stock_cagr": avg_stock_cagr,
        "portfolio_cagr": portfolio_cagr,
        "max_drawdown": _max_drawdown(equity["equity"]) if not equity.empty else 0.0,
        "win_rate": win_rate,
        "used_cash_ratio": peak_deployed / initial_cash if initial_cash else 0.0,
        "target_hit_rate": float((trades["exit_reason"] == "target_hit").mean()) if not trades.empty else None,
        "floor_stop_rate": float((trades["exit_reason"] == "floor_stop").mean()) if not trades.empty else None,
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
    signal_daily = expand_signal_daily(normalized_month_end, bars)
    selected = select_round_candidates(normalized_month_end, cfg)
    if selected.empty or "round_id" not in selected.columns:
        return {
            "protocol": cfg,
            "selected_candidates": selected,
            "signal_month_end": normalized_month_end,
            "signal_daily": signal_daily,
            "rounds": [],
            "summary": pd.DataFrame(),
        }

    rounds: list[dict[str, Any]] = []
    for round_id, candidates in selected.groupby("round_id", sort=True):
        tickers = candidates["ticker"].astype(str).str.upper().tolist()
        round_bars = bars[bars["ticker"].isin(tickers)].copy()
        round_signals = signal_daily[signal_daily["ticker"].isin(tickers)].copy()
        rounds.append(_run_one_round(int(round_id), candidates, round_signals, round_bars, cfg))

    summary = pd.DataFrame([round_result["summary"] for round_result in rounds]) if rounds else pd.DataFrame()
    return {
        "protocol": cfg,
        "selected_candidates": selected,
        "signal_month_end": normalized_month_end,
        "signal_daily": signal_daily,
        "rounds": rounds,
        "summary": summary,
    }
