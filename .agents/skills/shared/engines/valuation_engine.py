"""Scenario valuation helpers for the whole-market framework."""
from __future__ import annotations

from typing import Any

from utils.framework_utils import (
    extract_latest_price,
    extract_market_cap,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
    load_valuation_discipline,
    normalize_text,
    safe_float,
)


def _implied_price(equity_value: float | None, share_count: float | None) -> float | None:
    if equity_value in (None, 0) or share_count in (None, 0):
        return None
    return equity_value / share_count


def _case_payload(method: str, assumptions: list[str], equity_value: float | None, share_count: float | None) -> dict[str, Any]:
    return {
        "valuation_method": method,
        "assumptions": assumptions,
        "implied_equity_value": round(equity_value, 2) if equity_value is not None else None,
        "implied_price": round(_implied_price(equity_value, share_count), 2) if _implied_price(equity_value, share_count) is not None else None,
    }


def _scaled_value(anchor: float | None, multiple: Any) -> float | None:
    numeric_multiple = safe_float(multiple)
    if anchor is None or numeric_multiple is None:
        return None
    return anchor * numeric_multiple


def _product(*values: Any) -> float | None:
    result = 1.0
    for value in values:
        numeric = safe_float(value)
        if numeric is None:
            return None
        result *= numeric
    return result


def _sum_known(*values: float | None) -> float | None:
    known_values = [value for value in values if value is not None]
    if not known_values:
        return None
    return float(sum(known_values))


def build_three_case_valuation(
    stock_code: str,
    scan_data: dict[str, Any],
    opportunity_context: dict[str, Any],
) -> dict[str, Any]:
    discipline = load_valuation_discipline()
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    primary_type = normalize_text(opportunity_context.get("primary_type")).lower()
    type_cfg = (discipline.get("opportunity_types", {}) or {}).get(primary_type, {}) or {}

    market_cap = extract_market_cap(quote)
    current_price = extract_latest_price(quote, kline)
    share_count = market_cap / current_price if market_cap not in (None, 0) and current_price not in (None, 0) else None
    latest_income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    latest_balance = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
    profit = latest_income.get("net_profit")
    equity = latest_balance.get("total_equity")

    if primary_type == "compounder":
        multiples = type_cfg.get("multiples", {}) or {}
        bear_profit = profit * 0.9 if profit is not None else None
        base_profit = profit * 1.05 if profit is not None else None
        bull_profit = profit * 1.20 if profit is not None else None
        bear = _case_payload("owner_earnings_pe", ["flat earnings", "no rerating"], _product(bear_profit, multiples.get("bear")), share_count)
        base = _case_payload("owner_earnings_pe", ["stable quality", "mid-single-digit growth"], _product(base_profit, multiples.get("base")), share_count)
        bull = _case_payload("owner_earnings_pe", ["durable pricing power", "premium multiple"], _product(bull_profit, multiples.get("bull")), share_count)
    elif primary_type == "cyclical":
        multiples = type_cfg.get("multiples", {}) or {}
        haircuts = type_cfg.get("normalized_haircuts", {}) or {}
        normalized_profit = profit if profit is not None else (equity * 0.06 if equity is not None else None)
        bear_value = _product(normalized_profit, haircuts.get("bear"), multiples.get("bear"))
        base_value = _product(normalized_profit, haircuts.get("base"), multiples.get("base"))
        bull_value = _product(normalized_profit, haircuts.get("bull"), multiples.get("bull"))
        bear = _case_payload("normalized_earnings", ["trough conditions persist", "mid-cycle multiple at low end"], bear_value, share_count)
        base = _case_payload("normalized_earnings", ["earnings normalize", "mid-cycle valuation"], base_value, share_count)
        bull = _case_payload("normalized_earnings", ["pricing and utilization recover strongly"], bull_value, share_count)
    elif primary_type == "turnaround":
        haircuts = type_cfg.get("balance_sheet_haircuts", {}) or {}
        multiples = type_cfg.get("recovery_multiples", {}) or {}
        bear_value = _scaled_value(equity, haircuts.get("bear"))
        base_value = _sum_known(_scaled_value(equity, haircuts.get("base")), _scaled_value(max(profit or 0, 0), multiples.get("base")))
        bull_value = _sum_known(_scaled_value(equity, haircuts.get("bull")), _scaled_value(max(profit or 0, 0), multiples.get("bull")))
        bear = _case_payload("survival_value", ["repair stalls", "value anchored by surviving assets"], bear_value, share_count)
        base = _case_payload("recovery_value", ["operations stabilize", "partial rerating"], base_value if base_value else None, share_count)
        bull = _case_payload("normalized_value", ["full repair path works", "market prices normalized earnings"], bull_value if bull_value else None, share_count)
    elif primary_type == "asset_play":
        book_multiples = type_cfg.get("book_multiples", {}) or {}
        bear = _case_payload("book_value", ["discount persists"], _scaled_value(equity, book_multiples.get("bear")), share_count)
        base = _case_payload("book_value", ["assets are recognized closer to book"], _scaled_value(equity, book_multiples.get("base")), share_count)
        bull = _case_payload("book_value", ["asset unlock closes the discount"], _scaled_value(equity, book_multiples.get("bull")), share_count)
    else:
        outcome_multiples = type_cfg.get("outcome_multiples", {}) or {}
        anchor = market_cap or (equity if equity is not None else None)
        bear = _case_payload("scenario_weighted_outcome", ["event path slips"], _scaled_value(anchor, outcome_multiples.get("bear")), share_count)
        base = _case_payload("scenario_weighted_outcome", ["base event probability"], _scaled_value(anchor, outcome_multiples.get("base")), share_count)
        bull = _case_payload("scenario_weighted_outcome", ["event lands cleanly"], _scaled_value(anchor, outcome_multiples.get("bull")), share_count)

    current_vs_base = None
    if current_price not in (None, 0) and base.get("implied_price") not in (None, 0):
        current_vs_base = current_price / base["implied_price"]

    return {
        "stock_code": stock_code,
        "primary_type": primary_type,
        "current_price": current_price,
        "market_cap": market_cap,
        "share_count": share_count,
        "bear_case": bear,
        "base_case": base,
        "bull_case": bull,
        "summary": {
            "margin_of_safety": None if current_vs_base is None else round(1 - current_vs_base, 4),
            "priced_in": "optimistic" if current_vs_base is not None and current_vs_base > 1.0 else "conservative_to_fair",
        },
    }
