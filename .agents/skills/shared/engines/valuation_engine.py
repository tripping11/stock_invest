"""Scenario valuation helpers for the whole-market framework."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_valuation_discipline
from utils.financial_snapshot import (
    extract_latest_price,
    extract_market_cap,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
)
from utils.value_utils import normalize_text, safe_float


def _implied_price(equity_value: float | None, share_count: float | None) -> float | None:
    if equity_value in (None, 0) or share_count in (None, 0):
        return None
    return equity_value / share_count


def _case_payload(method: str, assumptions: list[str], equity_value: float | None, share_count: float | None) -> dict[str, Any]:
    implied_price = _implied_price(equity_value, share_count)
    return {
        "valuation_method": method,
        "assumptions": assumptions,
        "implied_equity_value": round(equity_value, 2) if equity_value is not None else None,
        "implied_price": round(implied_price, 2) if implied_price is not None else None,
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


def _historical_profit_anchor(records: list[dict[str, Any]]) -> float | None:
    profits: list[float] = []
    for row in records:
        if not isinstance(row, dict):
            continue
        for key in (
            "褰掑睘浜庢瘝鍏徃鎵€鏈夎€呯殑鍑€鍒╂鼎",
            "褰掑睘浜庢瘝鍏徃鑲′笢鐨勫噣鍒╂鼎",
            "net_profit",
            "profit",
        ):
            numeric = safe_float(row.get(key))
            if numeric is not None:
                profits.append(numeric)
                break
    if not profits:
        return None
    return profits[-1]


def _resolve_case_multiple(type_cfg: dict[str, Any], case_key: str, legacy_key: str | None = None) -> float | None:
    case_multiples = type_cfg.get("case_multiples", {}) or {}
    multiples = type_cfg.get("multiples", {}) or {}
    if case_key in case_multiples:
        return safe_float(case_multiples.get(case_key))
    if case_key in multiples:
        return safe_float(multiples.get(case_key))
    if legacy_key:
        return safe_float(multiples.get(legacy_key))
    return None


def _resolve_book_multiple(type_cfg: dict[str, Any], case_key: str, legacy_key: str | None = None) -> float | None:
    for bucket_name in ("book_multiples", "balance_sheet_haircuts", "recovery_multiples", "outcome_multiples"):
        bucket = type_cfg.get(bucket_name, {}) or {}
        if case_key in bucket:
            return safe_float(bucket.get(case_key))
        if legacy_key and legacy_key in bucket:
            return safe_float(bucket.get(legacy_key))
    return None


def _case_price_ratio(case_payload: dict[str, Any], current_price: float | None) -> float | None:
    implied_price = safe_float(case_payload.get("implied_price"))
    if implied_price in (None, 0) or current_price in (None, 0):
        return None
    return implied_price / current_price


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
    income_records = scan_data.get("income_statement", {}).get("data", [])
    latest_income = get_latest_income_snapshot(income_records)
    latest_balance = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
    profit = latest_income.get("net_profit")
    equity = latest_balance.get("total_equity")
    normalized_profit = _historical_profit_anchor(income_records) or profit

    if primary_type == "compounder":
        floor_value = _product(normalized_profit, _resolve_case_multiple(type_cfg, "floor", "bear"))
        normalized_value = _product(normalized_profit, _resolve_case_multiple(type_cfg, "normalized", "base"))
        recognition_value = _product(normalized_profit, _resolve_case_multiple(type_cfg, "recognition", "bull"))
        floor = _case_payload("owner_earnings_no_growth", ["no-growth owner earnings"], floor_value, share_count)
        normalized = _case_payload("owner_earnings_fair_multiple", ["quality is maintained"], normalized_value, share_count)
        recognition = _case_payload("premium_quality_multiple", ["market pays for durable quality"], recognition_value, share_count)
    elif primary_type == "cyclical":
        floor_value = _scaled_value(equity, _resolve_case_multiple(type_cfg, "floor"))
        normalized_value = _product(normalized_profit, _resolve_case_multiple(type_cfg, "normalized", "base"))
        recognition_value = _product(normalized_profit, _resolve_case_multiple(type_cfg, "recognition", "bull"))
        floor = _case_payload("tangible_book_or_replacement_cost", ["asset floor / replacement-cost style anchor"], floor_value, share_count)
        normalized = _case_payload("mid_cycle_earnings", ["mid-cycle earnings power"], normalized_value, share_count)
        recognition = _case_payload("rerated_mid_cycle_earnings", ["market recognizes normalized earnings"], recognition_value, share_count)
    elif primary_type == "turnaround":
        floor_value = _scaled_value(equity, _resolve_book_multiple(type_cfg, "floor", "bear"))
        normalized_value = _sum_known(
            _scaled_value(equity, _resolve_book_multiple(type_cfg, "base", "base")),
            _scaled_value(max(profit or 0, 0), _resolve_book_multiple(type_cfg, "normalized", "base")),
        )
        recognition_value = _sum_known(
            _scaled_value(equity, _resolve_book_multiple(type_cfg, "bull", "bull")),
            _scaled_value(max(profit or 0, 0), _resolve_book_multiple(type_cfg, "recognition", "bull")),
        )
        floor = _case_payload("survival_value", ["survival floor if repair stalls"], floor_value, share_count)
        normalized = _case_payload("repaired_earnings", ["repair path partially works"], normalized_value, share_count)
        recognition = _case_payload("post_repair_rerating", ["market rerates after repair"], recognition_value, share_count)
    elif primary_type == "asset_play":
        floor_value = _scaled_value(equity, _resolve_book_multiple(type_cfg, "floor", "bear"))
        normalized_value = _scaled_value(equity, _resolve_book_multiple(type_cfg, "normalized", "base"))
        recognition_value = _scaled_value(equity, _resolve_book_multiple(type_cfg, "recognition", "bull"))
        floor = _case_payload("stressed_nav", ["discount persists"], floor_value, share_count)
        normalized = _case_payload("book_or_nav", ["assets are valued closer to book"], normalized_value, share_count)
        recognition = _case_payload("discount_close", ["discount closes as realization path lands"], recognition_value, share_count)
    else:
        anchor = market_cap or equity
        floor_value = _scaled_value(anchor, _resolve_book_multiple(type_cfg, "downside", "bear"))
        normalized_value = _scaled_value(anchor, _resolve_book_multiple(type_cfg, "base", "base"))
        recognition_value = _scaled_value(anchor, _resolve_book_multiple(type_cfg, "upside", "bull"))
        floor = _case_payload("downside_case_weighted", ["downside scenario weighting"], floor_value, share_count)
        normalized = _case_payload("base_case_weighted", ["base scenario weighting"], normalized_value, share_count)
        recognition = _case_payload("upside_case_weighted", ["upside scenario weighting"], recognition_value, share_count)

    floor_protection = _case_price_ratio(floor, current_price)
    normalized_ratio = _case_price_ratio(normalized, current_price)
    recognition_ratio = _case_price_ratio(recognition, current_price)
    normalized_equity_value = safe_float(normalized.get("implied_equity_value"))
    recognition_equity_value = safe_float(recognition.get("implied_equity_value"))
    wind_dependency = None
    if normalized_equity_value not in (None, 0) and recognition_equity_value is not None:
        wind_dependency = round(recognition_equity_value / normalized_equity_value - 1, 4)

    current_vs_normalized = None
    if current_price not in (None, 0) and normalized.get("implied_price") not in (None, 0):
        current_vs_normalized = current_price / normalized["implied_price"]

    summary = {
        "floor_protection": None if floor_protection is None else round(floor_protection, 4),
        "normalized_upside": None if normalized_ratio is None else round(normalized_ratio - 1, 4),
        "recognition_upside": None if recognition_ratio is None else round(recognition_ratio - 1, 4),
        "wind_dependency": wind_dependency,
        "margin_of_safety": None if current_vs_normalized is None else round(1 - current_vs_normalized, 4),
        "priced_in": "optimistic" if current_vs_normalized is not None and current_vs_normalized > 1.0 else "conservative_to_fair",
    }

    return {
        "stock_code": stock_code,
        "primary_type": primary_type,
        "current_price": current_price,
        "market_cap": market_cap,
        "share_count": share_count,
        "floor_case": floor,
        "normalized_case": normalized,
        "recognition_case": recognition,
        "bear_case": floor,
        "base_case": normalized,
        "bull_case": recognition,
        "summary": summary,
    }
