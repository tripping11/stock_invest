"""Route-aware three-case valuation helpers for VCRF."""
from __future__ import annotations

from statistics import median
from typing import Any

from utils.framework_utils import (
    extract_first_value,
    extract_latest_price,
    extract_market_cap,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
    load_valuation_discipline,
    normalize_text,
    safe_float,
    select_latest_record,
)
from utils.valuation_case_config import resolve_case_equity_value, resolve_route_case_overrides

_EPSILON = 1e-9


def _implied_price(equity_value: float | None, share_count: float | None) -> float | None:
    if equity_value in (None, 0) or share_count in (None, 0):
        return None
    return equity_value / share_count


def _case_payload(method: str, assumptions: list[str], equity_value: float | None, share_count: float | None) -> dict[str, Any]:
    price = _implied_price(equity_value, share_count)
    return {
        "valuation_method": method,
        "assumptions": assumptions,
        "implied_equity_value": round(equity_value, 2) if equity_value is not None else None,
        "implied_price": round(price, 2) if price is not None else None,
    }


def _empty_case(method: str, assumption: str) -> dict[str, Any]:
    return _case_payload(method, [assumption], None, None)


def _empty_summary() -> dict[str, Any]:
    return {
        "floor_protection": None,
        "normalized_upside": None,
        "recognition_upside": None,
        "wind_dependency": None,
        "priced_state": "unknown",
        "priced_in": "unknown",
    }


def _valuation_payload(
    *,
    stock_code: str,
    primary_type: str,
    sector_route: str,
    route_anchor: str,
    current_price: float | None,
    market_cap: float | None,
    share_count: float | None,
    floor_case: dict[str, Any],
    normalized_case: dict[str, Any],
    recognition_case: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "stock_code": stock_code,
        "primary_type": primary_type,
        "sector_route": sector_route or "unknown",
        "route_anchor": route_anchor,
        "current_price": current_price,
        "market_cap": market_cap,
        "share_count": share_count,
        "floor_case": floor_case,
        "normalized_case": normalized_case,
        "recognition_case": recognition_case,
        "summary": summary,
        "bear_case": floor_case,
        "base_case": normalized_case,
        "bull_case": recognition_case,
    }


def _unavailable_valuation(
    *,
    stock_code: str,
    primary_type: str,
    sector_route: str,
    route_anchor: str,
    current_price: float | None,
    market_cap: float | None,
    share_count: float | None,
    assumption: str,
) -> dict[str, Any]:
    floor_case = _empty_case("unavailable", assumption)
    normalized_case = _empty_case("unavailable", assumption)
    recognition_case = _empty_case("unavailable", assumption)
    return _valuation_payload(
        stock_code=stock_code,
        primary_type=primary_type,
        sector_route=sector_route,
        route_anchor=route_anchor,
        current_price=current_price,
        market_cap=market_cap,
        share_count=share_count,
        floor_case=floor_case,
        normalized_case=normalized_case,
        recognition_case=recognition_case,
        summary=_empty_summary(),
    )


def _resolve_route_anchor(discipline: dict[str, Any], sector_route: str, primary_type: str) -> tuple[str, str]:
    route_methods = discipline.get("route_methods", {}) or {}
    route_key = sector_route or "unknown"
    route_cfg = route_methods.get(route_key, {}) or {}
    route_anchor = normalize_text(route_cfg.get("normalized_anchor")) or f"{primary_type or 'unknown'}_fallback"
    return route_key, route_anchor


def _share_count(market_cap: float | None, current_price: float | None) -> float | None:
    if market_cap in (None, 0) or current_price in (None, 0):
        return None
    return market_cap / current_price


def _report_date_key(row: dict[str, Any]) -> str:
    return normalize_text(
        extract_first_value(row, ("报告日期", "报告期", "报告日", "日期", "财报日期"))
    ).replace("-", "").replace("/", "")


def _extract_revenue(row: dict[str, Any]) -> float | None:
    return safe_float(
        extract_first_value(
            row,
            (
                "营业总收入",
                "营业总收入(元)",
                "营业收入",
                "营业收入(元)",
                "主营业务收入",
            ),
        )
    )


def _trimmed_normalized_profit(income_records: list[dict[str, Any]]) -> tuple[float | None, str | None, float | None]:
    if not income_records:
        return None, None, None
    sorted_rows = sorted(income_records, key=_report_date_key, reverse=True)
    annual_rows = [row for row in sorted_rows if _report_date_key(row).endswith("1231")]
    rows = annual_rows[:10] if len(annual_rows) >= 3 else sorted_rows[:10]
    margins: list[float] = []
    latest_revenue = None
    for index, row in enumerate(rows):
        revenue = _extract_revenue(row)
        profit = safe_float(extract_first_value(row, ("归属于母公司所有者的净利润", "归属于母公司股东的净利润", "净利润")))
        if index == 0:
            latest_revenue = revenue
        if revenue in (None, 0) or profit is None:
            continue
        margins.append(profit / revenue)
    if latest_revenue in (None, 0) or len(margins) < 3:
        return None, None, latest_revenue
    trimmed = sorted(margins)
    if len(trimmed) >= 5:
        trimmed = trimmed[1:-1]
    normalized_margin = float(median(trimmed))
    return normalized_margin * latest_revenue, "trimmed_margin_x_latest_revenue", normalized_margin


def _build_floor_case(
    sector_route: str,
    discipline: dict[str, Any],
    equity: float | None,
    profit: float | None,
    share_count: float | None,
) -> dict[str, Any]:
    case_cfg = resolve_route_case_overrides(discipline, sector_route).get("floor", {})
    equity_value = resolve_case_equity_value(case_cfg, equity=equity, profit=profit)
    valuation_method = normalize_text(case_cfg.get("valuation_method")) or "conservative_book"
    assumption = normalize_text(case_cfg.get("assumption")) or "book-value fallback"
    return _case_payload(valuation_method, [assumption], equity_value, share_count)


def _build_normalized_case(
    sector_route: str,
    route_anchor: str,
    primary_type: str,
    discipline: dict[str, Any],
    equity: float | None,
    profit: float | None,
    normalized_profit: float | None,
    share_count: float | None,
) -> dict[str, Any]:
    earnings_anchor = normalized_profit if normalized_profit is not None else profit
    case_cfg = resolve_route_case_overrides(discipline, sector_route).get("normalized", {})
    equity_value = resolve_case_equity_value(case_cfg, equity=equity, profit=earnings_anchor)
    if primary_type == "asset_play":
        equity_value = equity if equity is not None else equity_value
        assumption = "assets recognized near book"
    else:
        assumption = normalize_text(case_cfg.get("assumption")) or "conservative normalized earnings"
    return _case_payload(route_anchor, [assumption], equity_value, share_count)


def _build_recognition_case(
    sector_route: str,
    discipline: dict[str, Any],
    normalized_equity_value: float | None,
    normalized_profit: float | None,
    profit: float | None,
    share_count: float | None,
) -> dict[str, Any]:
    earnings_anchor = normalized_profit if normalized_profit is not None else profit
    case_cfg = resolve_route_case_overrides(discipline, sector_route).get("recognition", {})
    equity_value = resolve_case_equity_value(case_cfg, profit=earnings_anchor, normalized=normalized_equity_value)
    valuation_method = normalize_text(case_cfg.get("valuation_method")) or "recognition_multiple"
    assumption = normalize_text(case_cfg.get("assumption")) or "sentiment-driven upside"
    return _case_payload(valuation_method, [assumption], equity_value, share_count)


def _upside(implied_price: float | None, current_price: float | None) -> float | None:
    if implied_price in (None, 0) or current_price in (None, 0):
        return None
    return implied_price / current_price - 1


def build_three_case_valuation(
    stock_code: str,
    scan_data: dict[str, Any],
    opportunity_context: dict[str, Any],
) -> dict[str, Any]:
    discipline = load_valuation_discipline()
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    primary_type = normalize_text(opportunity_context.get("primary_type")).lower()
    sector_route = normalize_text(opportunity_context.get("sector_route")).lower()
    market_cap = extract_market_cap(quote)
    current_price = extract_latest_price(quote, kline)
    share_count = _share_count(market_cap, current_price)
    latest_income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    latest_balance = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
    normalized_profit, normalized_profit_source, normalized_margin = _trimmed_normalized_profit(
        scan_data.get("income_statement", {}).get("data", [])
    )
    profit = latest_income.get("net_profit")
    equity = latest_balance.get("total_equity")
    route_methods = discipline.get("route_methods", {}) or {}

    if primary_type == "unknown":
        return _unavailable_valuation(
            stock_code=stock_code,
            primary_type=primary_type,
            sector_route=sector_route,
            route_anchor="unavailable",
            current_price=current_price,
            market_cap=market_cap,
            share_count=share_count,
            assumption="unknown opportunity type",
        )

    if primary_type == "compounder" and sector_route in ("", "unknown") and profit is None:
        return _unavailable_valuation(
            stock_code=stock_code,
            primary_type=primary_type,
            sector_route=sector_route,
            route_anchor="compounder_missing_profit",
            current_price=current_price,
            market_cap=market_cap,
            share_count=share_count,
            assumption="missing normalized earnings anchor",
        )

    if not route_methods and sector_route in ("", "unknown") and primary_type == "cyclical":
        return _unavailable_valuation(
            stock_code=stock_code,
            primary_type=primary_type,
            sector_route=sector_route,
            route_anchor="missing_route_methods",
            current_price=current_price,
            market_cap=market_cap,
            share_count=share_count,
            assumption="missing valuation discipline route methods",
        )

    resolved_route, route_anchor = _resolve_route_anchor(discipline, sector_route, primary_type)
    floor_case = _build_floor_case(resolved_route, discipline, equity, profit, share_count)
    normalized_case = _build_normalized_case(
        resolved_route,
        route_anchor,
        primary_type,
        discipline,
        equity,
        profit,
        normalized_profit,
        share_count,
    )
    recognition_case = _build_recognition_case(
        resolved_route,
        discipline,
        normalized_case.get("implied_equity_value"),
        normalized_profit,
        profit,
        share_count,
    )

    floor_protection = None
    if floor_case.get("implied_price") not in (None, 0) and current_price not in (None, 0):
        floor_protection = floor_case["implied_price"] / current_price
    normalized_upside = _upside(normalized_case.get("implied_price"), current_price)
    recognition_upside = _upside(recognition_case.get("implied_price"), current_price)
    wind_dependency = None
    if recognition_case.get("implied_price") not in (None, 0) and normalized_case.get("implied_price") is not None and current_price not in (None, 0):
        denominator = recognition_case["implied_price"] - current_price
        if denominator is not None and abs(float(denominator)) > _EPSILON:
            wind_dependency = (recognition_case["implied_price"] - normalized_case["implied_price"]) / denominator

    return _valuation_payload(
        stock_code=stock_code,
        primary_type=primary_type,
        sector_route=resolved_route,
        route_anchor=route_anchor,
        current_price=current_price,
        market_cap=market_cap,
        share_count=share_count,
        floor_case=floor_case,
        normalized_case=normalized_case,
        recognition_case=recognition_case,
        summary={
            "floor_protection": round(floor_protection, 4) if floor_protection is not None else None,
            "normalized_upside": round(normalized_upside, 4) if normalized_upside is not None else None,
            "recognition_upside": round(recognition_upside, 4) if recognition_upside is not None else None,
            "wind_dependency": round(wind_dependency, 4) if wind_dependency is not None else None,
            "normalized_profit_source": normalized_profit_source,
            "normalized_margin": round(normalized_margin, 4) if normalized_margin is not None else None,
            "priced_state": "optimistic" if normalized_upside is not None and normalized_upside < 0 else "conservative_to_fair",
            "priced_in": "optimistic" if normalized_upside is not None and normalized_upside < 0 else "conservative_to_fair",
        },
    )
