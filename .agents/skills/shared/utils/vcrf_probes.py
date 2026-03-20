"""Conservative VCRF underwrite probes."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_valuation_discipline, resolve_vcrf_weight_template
from utils.financial_snapshot import (
    extract_cash_and_equivalents,
    extract_trading_financial_assets,
    extract_latest_price,
    extract_market_cap,
    extract_short_term_debt_wall,
    get_latest_balance_snapshot,
    get_latest_cashflow_snapshot,
    get_latest_income_snapshot,
)
from utils.opportunity_classifier import assess_business_purity, assess_moat_quality
from utils.valuation_case_config import (
    DEFAULT_FLOOR_PROTECTION_SCORE_BANDS,
    DEFAULT_NORMALIZED_VALUE_RATIO_BANDS,
    resolve_case_equity_value,
    resolve_route_case_overrides,
    resolve_score_bands,
)
from utils.value_utils import clamp, normalize_text, safe_float


def _share_count(scan_data: dict[str, Any]) -> float | None:
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    market_cap = extract_market_cap(quote)
    current_price = extract_latest_price(quote, kline)
    if market_cap in (None, 0) or current_price in (None, 0):
        return None
    return market_cap / current_price


def _banded_score(value: float | None, bands: list[tuple[float, float]]) -> float:
    if value is None:
        return 0.0
    score = 0.0
    for threshold, threshold_score in bands:
        if value >= threshold:
            score = threshold_score
    return score


def _component_payload(
    score: float,
    *,
    availability: str,
    confidence: str,
    reason: str,
    inputs_used: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "score": round(clamp(score, 0.0, 100.0), 2),
        "availability": availability,
        "confidence": confidence,
        "reason": reason,
        "inputs_used": inputs_used,
    }
    if extra:
        payload.update(extra)
    return payload


def assess_intrinsic_value_floor(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    discipline = load_valuation_discipline()
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    current_price = extract_latest_price(quote, kline)
    equity = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", [])).get("total_equity")
    profit = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", [])).get("net_profit")
    share_count = _share_count(scan_data)
    route = normalize_text(driver_stack.get("sector_route")).lower()

    floor_price: float | None = None
    anchor = "unknown"
    if share_count not in (None, 0):
        case_cfg = resolve_route_case_overrides(discipline, route).get("floor", {})
        anchor = normalize_text(case_cfg.get("valuation_method")) or "unknown"
        floor_equity_value = resolve_case_equity_value(case_cfg, equity=equity, profit=profit)
        if floor_equity_value is not None:
            floor_price = floor_equity_value / share_count

    floor_protection = floor_price / current_price if floor_price not in (None, 0) and current_price not in (None, 0) else None
    score = _banded_score(
        floor_protection,
        resolve_score_bands(discipline, "floor_protection", DEFAULT_FLOOR_PROTECTION_SCORE_BANDS),
    )
    availability = "full" if floor_protection is not None else "missing"
    confidence = "full" if floor_protection is not None else "degraded"
    return _component_payload(
        score,
        availability=availability,
        confidence=confidence,
        reason=f"route={route or 'unknown'}, anchor={anchor}, floor_protection={floor_protection}",
        inputs_used={"current_price": current_price, "total_equity": equity, "net_profit": profit, "share_count": share_count},
        extra={"floor_price": floor_price, "floor_protection": floor_protection, "anchor": anchor},
    )


def _approx_altman_z(total_assets: float | None, total_equity: float | None, net_profit: float | None, cash: float | None, short_debt: float | None) -> float | None:
    if total_assets in (None, 0) or total_equity is None or net_profit is None:
        return None
    working_capital_ratio = ((cash or 0.0) - (short_debt or 0.0)) / total_assets
    equity_ratio = total_equity / total_assets
    profit_ratio = net_profit / total_assets
    return 3.25 + 6.56 * working_capital_ratio + 3.26 * equity_ratio + 6.72 * profit_ratio


def _extract_interest_coverage(income_row: dict[str, Any]) -> float | None:
    if not income_row:
        return None
    earnings_before_interest = None
    for key in (
        "EBIT",
        "ebit",
        "息税前利润",
        "营业利润",
        "operate_profit",
        "利润总额",
        "total_profit",
        "利润总额(元)",
    ):
        earnings_before_interest = safe_float(income_row.get(key))
        if earnings_before_interest is not None:
            break
    interest_expense = None
    for key in (
        "利息支出",
        "利息费用",
        "财务费用",
        "int_exp",
        "interest_expense",
        "fin_exp",
    ):
        interest_expense = safe_float(income_row.get(key))
        if interest_expense is not None:
            break
    if earnings_before_interest is None or interest_expense in (None, 0):
        return None
    return earnings_before_interest / abs(interest_expense)


def _survival_tripwire_threshold(driver_stack: dict[str, Any] | None, *, state_owned_support: bool) -> float:
    primary_type = normalize_text((driver_stack or {}).get("primary_type")).lower()
    if state_owned_support or primary_type == "cyclical":
        return 0.30
    return 0.80


def assess_survival_boundary(scan_data: dict[str, Any], driver_stack: dict[str, Any] | None = None) -> dict[str, Any]:
    cashflow = get_latest_cashflow_snapshot(scan_data.get("cashflow_statement", {}).get("data", [])).get("operating_cashflow")
    balance_snapshot = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
    income_snapshot = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    balance_row = balance_snapshot.get("raw", {})
    total_equity = balance_snapshot.get("total_equity")
    total_assets = safe_float(balance_row.get("资产总计") or balance_row.get("总资产"))
    debt_wall = extract_short_term_debt_wall(balance_row)
    cash = extract_cash_and_equivalents(balance_row)
    tradable_financial_assets = extract_trading_financial_assets(balance_row) or 0.0
    liquid_assets = (cash or 0.0) + tradable_financial_assets
    cash_coverage = None if debt_wall in (None, 0) else (liquid_assets / debt_wall)
    cfo_support = None if debt_wall in (None, 0) else (cashflow / debt_wall if cashflow is not None else None)
    if cash_coverage is None and liquid_assets > 0 and debt_wall in (None, 0):
        cash_coverage = 2.0
    if cfo_support is None and cashflow is not None and debt_wall in (None, 0):
        cfo_support = 1.5 if cashflow > 0 else 0.0
    net_cash_ratio = None
    if total_assets not in (None, 0) and debt_wall is not None:
        net_cash_ratio = (liquid_assets - debt_wall) / total_assets
    z_score = _approx_altman_z(total_assets, total_equity, income_snapshot.get("net_profit"), liquid_assets, debt_wall)
    interest_coverage = _extract_interest_coverage(income_snapshot.get("raw", {}))
    equity_positive = total_equity is not None and total_equity > 0
    controller_text = " ".join(
        normalize_text(value)
        for value in [
            scan_data.get("company_profile", {}).get("data", {}).get("实际控制人"),
            scan_data.get("company_profile", {}).get("data", {}).get("控股股东"),
        ]
        if value
    )
    state_owned_support = any(token in controller_text for token in ("国资", "国有", "国务院", "财政", "地方政府", "国资委", "SASAC"))
    tripwire_threshold = _survival_tripwire_threshold(driver_stack, state_owned_support=state_owned_support)
    interest_coverage_bypass = interest_coverage is not None and interest_coverage >= 1.50
    tripwire_reject = bool(
        debt_wall not in (None, 0)
        and cash_coverage is not None
        and cash_coverage < tripwire_threshold
        and not interest_coverage_bypass
    )

    score = (
        _banded_score(cash_coverage, [(0.40, 6), (0.80, 15), (1.00, 25), (1.50, 35)])
        + _banded_score(cfo_support, [(0.00, 5), (0.20, 10), (0.50, 15), (1.00, 20)])
        + _banded_score(net_cash_ratio, [(-0.10, 4), (0.00, 12), (0.10, 20)])
        + _banded_score(z_score, [(1.10, 8), (1.80, 15), (3.00, 20)])
        + (5 if equity_positive else 0)
    )
    if tripwire_reject:
        score = min(score, 20.0)
    availability = "full" if total_equity is not None and any(value is not None for value in (cash_coverage, cfo_support, z_score)) else "missing"
    confidence = "full" if availability == "full" else "degraded"
    return _component_payload(
        score,
        availability=availability,
        confidence=confidence,
        reason=(
            f"cash_coverage={cash_coverage}, cfo_support={cfo_support}, net_cash_ratio={net_cash_ratio}, "
            f"z_score={z_score}, interest_coverage={interest_coverage}, equity_positive={equity_positive}, "
            f"tripwire_threshold={tripwire_threshold}, tripwire_reject={tripwire_reject}"
        ),
        inputs_used={
            "operating_cashflow": cashflow,
            "short_term_debt_wall": debt_wall,
            "cash_and_equivalents": cash,
            "trading_financial_assets": tradable_financial_assets,
            "total_assets": total_assets,
            "total_equity": total_equity,
            "interest_coverage": interest_coverage,
        },
        extra={
            "coverage": cash_coverage,
            "cash_coverage": cash_coverage,
            "cfo_support": cfo_support,
            "net_cash_ratio": net_cash_ratio,
            "z_score": z_score,
            "interest_coverage": interest_coverage,
            "equity_positive": equity_positive,
            "state_owned_support": state_owned_support,
            "tripwire_threshold": tripwire_threshold,
            "interest_coverage_bypass": interest_coverage_bypass,
            "tripwire_reject": tripwire_reject,
        },
    )


def assess_governance_anti_fraud(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    profile = scan_data.get("company_profile", {}).get("data", {})
    text = " ".join(
        value
        for value in [
            normalize_text(profile.get("主营业务")),
            normalize_text(profile.get("经营范围")),
            normalize_text(profile.get("实际控制人") or profile.get("控股股东")),
        ]
        if value
    )
    balance_snapshot = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
    balance_row = balance_snapshot.get("raw", {})
    total_assets = safe_float(balance_row.get("资产总计") or balance_row.get("总资产"))
    cash = extract_cash_and_equivalents(balance_row)
    short_debt = extract_short_term_debt_wall(balance_row)

    score = 100.0
    if any(token in text for token in ("保留意见", "无法表示意见", "否定意见")):
        score -= 35
    if any(token in text for token in ("更换会计师事务所", "审计机构变更")):
        score -= 15
    if total_assets not in (None, 0) and cash is not None and short_debt is not None:
        if cash / total_assets > 0.30 and short_debt / total_assets > 0.30:
            score -= 12
    if "关联交易" in text:
        score -= 10
    if any(token in text for token in ("处罚", "占用", "造假", "控制权纠纷")):
        score -= 25

    return _component_payload(
        score,
        availability="full",
        confidence="partial",
        reason="penalty-based governance screen",
        inputs_used={"profile_text": text, "cash": cash, "short_debt": short_debt, "total_assets": total_assets},
    )


def assess_business_or_asset_quality(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    revenue_records = scan_data.get("revenue_breakdown", {}).get("data", []) or []
    purity = assess_business_purity(revenue_records)
    moat = assess_moat_quality(scan_data.get("company_profile", {}).get("data", {}), revenue_records=revenue_records)
    route = normalize_text(driver_stack.get("sector_route")).lower()
    purity_score = clamp(safe_float(purity.get("top_ratio")) or 0.0, 0.0, 100.0)
    route_fit_score = 80.0 if route != "unknown" else 30.0
    verification_score = 70.0 if route in {"core_resource", "financial_asset"} else float(moat.get("score", 0)) * 10.0
    stability_score = 70.0 if revenue_records else 40.0
    score = purity_score * 0.35 + route_fit_score * 0.20 + verification_score * 0.25 + stability_score * 0.20
    return _component_payload(
        score,
        availability="full" if revenue_records else "partial",
        confidence="partial" if revenue_records else "degraded",
        reason=f"route={route}, purity={purity.get('top_ratio')}, moat={moat.get('verdict')}",
        inputs_used={"top_ratio": purity.get("top_ratio"), "route": route, "moat_score": moat.get("score")},
    )


def assess_normalized_earnings_power(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    discipline = load_valuation_discipline()
    route = normalize_text(driver_stack.get("sector_route")).lower()
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    current_price = extract_latest_price(quote, kline)
    share_count = _share_count(scan_data)
    equity = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", [])).get("total_equity")
    profit = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", [])).get("net_profit")

    route_cfg = resolve_route_case_overrides(discipline, route)
    normalized_cfg = route_cfg.get("normalized", {})
    normalized_equity_value = resolve_case_equity_value(normalized_cfg, equity=equity, profit=profit)
    route_methods = discipline.get("route_methods", {}) or {}
    anchor = normalize_text(
        (route_methods.get(route, {}) or {}).get("normalized_anchor")
        or normalized_cfg.get("anchor")
        or "conservative_fallback"
    )

    implied_price = normalized_equity_value / share_count if normalized_equity_value not in (None, 0) and share_count not in (None, 0) else None
    value_ratio = implied_price / current_price if implied_price not in (None, 0) and current_price not in (None, 0) else None
    score = _banded_score(
        value_ratio,
        resolve_score_bands(discipline, "normalized_value_ratio", DEFAULT_NORMALIZED_VALUE_RATIO_BANDS),
    )
    availability = "full" if implied_price is not None else "missing"
    confidence = "partial" if implied_price is not None else "degraded"
    return _component_payload(
        score,
        availability=availability,
        confidence=confidence,
        reason=f"route={route}, anchor={anchor}, value_ratio={value_ratio}",
        inputs_used={"current_price": current_price, "share_count": share_count, "net_profit": profit, "total_equity": equity},
        extra={"implied_price": implied_price, "value_ratio": value_ratio, "anchor": anchor},
    )


def detect_big_bath(financials: dict[str, Any]) -> dict[str, Any]:
    one_off_impairment_ratio = safe_float(financials.get("one_off_impairment_ratio"))
    ocf_vs_net_income_divergence = safe_float(financials.get("ocf_vs_net_income_divergence"))
    gross_margin_delta = safe_float(financials.get("gross_margin_delta"))

    if gross_margin_delta is None:
        trend = "inconclusive"
    elif gross_margin_delta > 0.0:
        trend = "recovering"
    elif gross_margin_delta >= -0.02:
        trend = "stable"
    else:
        trend = "declining"

    verdict = "inconclusive"
    confidence = "low"
    if (
        one_off_impairment_ratio is not None
        and one_off_impairment_ratio >= 0.8
        and (ocf_vs_net_income_divergence or 0.0) > 0
        and trend in {"stable", "recovering"}
    ):
        verdict = "big_bath"
        confidence = "medium"
    elif (
        one_off_impairment_ratio is not None
        and one_off_impairment_ratio < 0.5
        and (ocf_vs_net_income_divergence or 0.0) < 0
        and trend == "declining"
    ):
        verdict = "genuine_collapse"
        confidence = "medium"

    return {
        "verdict": verdict,
        "one_off_impairment_ratio": one_off_impairment_ratio,
        "core_gross_margin_trend": trend,
        "ocf_vs_net_income_divergence": ocf_vs_net_income_divergence,
        "confidence": confidence,
    }


def score_underwrite_axis(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]:
    components = {
        "intrinsic_value_floor": assess_intrinsic_value_floor(scan_data, driver_stack),
        "survival_boundary": assess_survival_boundary(scan_data, driver_stack),
        "governance_anti_fraud": assess_governance_anti_fraud(scan_data, driver_stack),
        "business_or_asset_quality": assess_business_or_asset_quality(scan_data, driver_stack),
        "normalized_earnings_power": assess_normalized_earnings_power(scan_data, driver_stack),
    }
    weights = resolve_vcrf_weight_template(driver_stack.get("primary_type", "compounder"), driver_stack.get("sector_route", "unknown"))["underwrite"]
    score = 0.0
    for name, component in components.items():
        score += component["score"] * float(weights.get(name, 0.0))
    confidence = "full"
    if any(component["availability"] == "missing" for component in components.values()):
        confidence = "degraded"
    elif any(component["availability"] == "partial" for component in components.values()):
        confidence = "partial"
    return {
        "score": round(score, 2),
        "confidence": confidence,
        "components": components,
        "weights_used": {name: round(float(value), 6) for name, value in weights.items()},
    }
