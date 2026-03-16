"""Low-frequency harvest candidate monitor for ATTACK positions."""
from __future__ import annotations

from typing import Any


def evaluate_harvest_candidate(
    *,
    closes: list[float],
    recognition_price: float,
    daily_returns: list[float],
    flow_stage: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    consecutive = int(cfg.get("consecutive_closes_above_recognition", 3))
    breakout_day_return = float(cfg.get("breakout_day_return_pct", 0.10))
    breakout_ratio = float(cfg.get("breakout_close_to_recognition_ratio", 0.95))
    required_flow_stage = str(cfg.get("require_flow_stage_deterioration_to", "crowded"))

    consecutive_ok = len(closes) >= consecutive and all(close > recognition_price for close in closes[-consecutive:])
    breakout_ok = bool(daily_returns) and bool(closes) and daily_returns[-1] > breakout_day_return and closes[-1] > recognition_price * breakout_ratio
    flow_ok = str(flow_stage) == required_flow_stage

    harvest_candidate = (consecutive_ok or breakout_ok) and flow_ok
    return {
        "harvest_candidate": harvest_candidate,
        "consecutive_ok": consecutive_ok,
        "breakout_ok": breakout_ok,
        "flow_ok": flow_ok,
        "reason": f"consecutive_ok={consecutive_ok}, breakout_ok={breakout_ok}, flow_ok={flow_ok}",
    }
