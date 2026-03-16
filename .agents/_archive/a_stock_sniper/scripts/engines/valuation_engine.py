"""Valuation engine for cyclicals and military names."""

from __future__ import annotations

from typing import Any

from utils.research_utils import (
    _pick_revenue_col,
    extract_market_cap,
    get_crocodile_mode_config,
    get_latest_income_snapshot,
    normalize_text,
    safe_float,
    select_latest_record,
)


GENERIC_NAME_TOKENS = ("其他", "合计", "国内", "国外", "补充")


def _autofill_map(tier0_autofill_result: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    items = (tier0_autofill_result or {}).get("items", [])
    return {item.get("field_name"): item for item in items if isinstance(item, dict) and item.get("field_name")}


def _annualization_factor(report_date: str) -> float:
    text = normalize_text(report_date).replace("-", "").replace("/", "")
    if len(text) < 8:
        return 1.0
    mmdd = text[4:8]
    return {
        "0331": 4.0,
        "0630": 2.0,
        "0930": 4.0 / 3.0,
        "1231": 1.0,
    }.get(mmdd, 1.0)


def _latest_spot_summary(commodity_data: dict[str, Any]) -> dict[str, Any]:
    spot_data = commodity_data.get("spot_price", {}).get("data", [])
    if isinstance(spot_data, list) and spot_data:
        latest = spot_data[-1]
        close_value = latest.get("收盘价") or latest.get("close")
        date_value = latest.get("日期") or latest.get("date")
        return {"latest_price": close_value, "latest_date": str(date_value), "status": commodity_data.get("spot_price", {}).get("status")}
    return {"latest_price": None, "latest_date": "", "status": commodity_data.get("spot_price", {}).get("status", "missing")}


def _pick_conservative_high(futures_data: dict[str, Any], spot_summary: dict[str, Any], eco_context: dict[str, Any]) -> tuple[float | None, str]:
    valuation_cfg = get_crocodile_mode_config(eco_context.get("four_signal_mode")).get("valuation", {}) or {}
    latest_price = safe_float(spot_summary.get("latest_price")) or safe_float(futures_data.get("latest_close"))
    latest_floor_multiplier = safe_float(valuation_cfg.get("latest_floor_multiplier")) or 1.0
    latest_floor = latest_price * latest_floor_multiplier if latest_price is not None else None

    for key, label, haircut_key in (
        ("high_750d", "futures_high_750d_x_haircut", "long_high_haircut"),
        ("high_250d", "futures_high_250d_x_haircut", "medium_high_haircut"),
        ("high_60d", "futures_high_60d_x_haircut", "short_high_haircut"),
    ):
        raw_value = safe_float(futures_data.get(key))
        haircut = safe_float(valuation_cfg.get(haircut_key))
        if raw_value is not None and haircut is not None:
            conservative_value = raw_value * haircut
            if latest_floor is not None:
                conservative_value = max(conservative_value, latest_floor)
            return conservative_value, f"{label}_{haircut:.2f}"

    if latest_price is not None:
        return latest_price, "spot_latest"
    return None, "missing"


def _pick_cycle_product_row(scan_data: dict[str, Any], commodity_keyword: str) -> dict[str, Any]:
    records = scan_data.get("revenue_breakdown", {}).get("data", [])
    if not records:
        return {}

    type_col = _pick_revenue_col(records, ("分类类型", "分类方向", "类型"), contains=("分类", "类型"))
    name_col = _pick_revenue_col(records, ("主营构成", "产品名称", "分类名称", "名称"), contains=("构成", "产品", "名称"))
    revenue_col = _pick_revenue_col(records, ("主营收入", "营业收入"), contains=("收入",))
    cost_col = _pick_revenue_col(records, ("主营成本", "营业成本"), contains=("成本",))
    date_col = _pick_revenue_col(records, ("报告日期", "报告期", "日期"), contains=("日期", "报告"))

    product_rows = records
    if type_col:
        selected = [row for row in records if "按产品" in normalize_text(row.get(type_col))]
        if selected:
            product_rows = selected

    ranked: list[tuple[int, str, float, dict[str, Any]]] = []
    for row in product_rows:
        name = normalize_text(row.get(name_col or ""))
        if not name or any(token in name for token in GENERIC_NAME_TOKENS):
            continue
        revenue = safe_float(row.get(revenue_col or "")) or 0.0
        has_cost = safe_float(row.get(cost_col or "")) is not None
        score = 2 if commodity_keyword and commodity_keyword in name else 1
        score += 1 if has_cost else 0
        date_text = normalize_text(row.get(date_col or "")).replace("-", "").replace("/", "")
        ranked.append((score, date_text, revenue, row))

    if not ranked:
        return {}

    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    row = ranked[0][3]
    revenue = safe_float(row.get(revenue_col or "")) or 0.0
    cost = safe_float(row.get(cost_col or ""))
    return {
        "report_date": normalize_text(row.get(date_col or "")),
        "product_name": normalize_text(row.get(name_col or "")),
        "revenue_yuan": revenue,
        "cost_yuan": cost,
        "cost_ratio": (cost / revenue) if cost is not None and revenue > 0 else None,
    }


def _extract_financial_revenue(scan_data: dict[str, Any]) -> dict[str, Any]:
    records = scan_data.get("financial_summary", {}).get("data", [])
    latest = select_latest_record(records, ("日期", "报告日", "报告日期", "报告期"))
    if not latest:
        return {"revenue_yuan": None, "report_date": "", "basis": ""}

    for key, value in latest.items():
        key_text = normalize_text(key)
        if not key_text:
            continue
        if "营业总收入" in key_text or ("营业收入" in key_text and "同比" not in key_text and "增长" not in key_text):
            revenue = safe_float(value)
            if revenue is not None:
                return {
                    "revenue_yuan": revenue,
                    "report_date": normalize_text(latest.get("日期") or latest.get("报告日") or latest.get("报告日期") or latest.get("报告期")),
                    "basis": f"financial_summary:{key_text}",
                }
    return {"revenue_yuan": None, "report_date": "", "basis": ""}


def estimate_sales_proxy(scan_data: dict[str, Any], tier0_autofill_result: dict[str, Any] | None, eco_context: dict[str, Any]) -> dict[str, Any]:
    autofill = _autofill_map(tier0_autofill_result)
    capacity_candidate = (autofill.get("capacity") or {}).get("candidate_value") or {}
    if isinstance(capacity_candidate, dict):
        proxy_revenue = safe_float(capacity_candidate.get("proxy_product_revenue_yuan"))
        if proxy_revenue is not None:
            report_date = normalize_text(capacity_candidate.get("report_date"))
            factor = _annualization_factor(report_date)
            return {
                "sales_proxy_revenue": proxy_revenue * factor,
                "report_date": report_date,
                "annualization_factor": factor,
                "basis": "tier0_capacity_proxy_revenue",
                "product_name": normalize_text(capacity_candidate.get("primary_product") or capacity_candidate.get("capacity_label")),
            }

    cycle_product = _pick_cycle_product_row(scan_data, eco_context.get("commodity", ""))
    revenue = safe_float(cycle_product.get("revenue_yuan"))
    if revenue is not None:
        report_date = normalize_text(cycle_product.get("report_date"))
        factor = _annualization_factor(report_date)
        return {
            "sales_proxy_revenue": revenue * factor,
            "report_date": report_date,
            "annualization_factor": factor,
            "basis": "revenue_breakdown_primary_segment",
            "product_name": normalize_text(cycle_product.get("product_name")),
        }

    financial_revenue = _extract_financial_revenue(scan_data)
    revenue = safe_float(financial_revenue.get("revenue_yuan"))
    if revenue is not None:
        report_date = normalize_text(financial_revenue.get("report_date"))
        factor = _annualization_factor(report_date)
        return {
            "sales_proxy_revenue": revenue * factor,
            "report_date": report_date,
            "annualization_factor": factor,
            "basis": normalize_text(financial_revenue.get("basis")) or "financial_summary_revenue",
            "product_name": normalize_text(eco_context.get("commodity")),
        }

    return {
        "sales_proxy_revenue": None,
        "report_date": "",
        "annualization_factor": 1.0,
        "basis": "",
        "product_name": normalize_text(eco_context.get("commodity")),
    }


def estimate_current_ps(
    quote: dict[str, Any],
    scan_data: dict[str, Any],
    tier0_autofill_result: dict[str, Any] | None,
    eco_context: dict[str, Any],
) -> dict[str, Any]:
    market_cap = extract_market_cap(quote)
    sales_proxy = estimate_sales_proxy(scan_data, tier0_autofill_result, eco_context)
    revenue = safe_float(sales_proxy.get("sales_proxy_revenue"))
    current_ps = None
    if market_cap is not None and revenue not in (None, 0):
        current_ps = market_cap / revenue
    return {
        **sales_proxy,
        "market_cap": market_cap,
        "current_ps": round(current_ps, 4) if current_ps is not None else None,
    }


def resolve_exit_prices(cycle_valuation: dict[str, Any]) -> dict[str, Any]:
    sell_key = normalize_text(cycle_valuation.get("sell_key")) or ""
    optimistic_key = normalize_text(cycle_valuation.get("optimistic_key")) or ""
    bubble_key = normalize_text(cycle_valuation.get("bubble_key")) or ""
    target_prices = cycle_valuation.get("target_prices", {}) or {}
    return {
        "sell_key": sell_key,
        "optimistic_key": optimistic_key,
        "bubble_key": bubble_key,
        "conservative_sell_price": safe_float(target_prices.get(sell_key)),
        "optimistic_sell_price": safe_float(target_prices.get(optimistic_key)),
        "bubble_warning_price": safe_float(target_prices.get(bubble_key)),
    }


def build_valuation_case(
    stock_code: str,
    current_price: float | None,
    quote: dict[str, Any],
    commodity_data: dict[str, Any],
    tier0_autofill_result: dict[str, Any] | None,
    scan_data: dict[str, Any],
    eco_context: dict[str, Any],
) -> dict[str, Any]:
    eco_circle = normalize_text(eco_context.get("eco_circle"))
    mode_cfg = get_crocodile_mode_config(eco_context.get("four_signal_mode"))
    valuation_cfg = mode_cfg.get("valuation", {}) or {}
    market_cap = extract_market_cap(quote)
    share_count = market_cap / current_price if market_cap is not None and current_price not in (None, 0) else None
    income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    latest_profit = safe_float(income.get("net_profit"))
    current_pb = safe_float(scan_data.get("valuation_history", {}).get("data", {}).get("pb"))
    current_pe = market_cap / latest_profit if market_cap is not None and latest_profit not in (None, 0) else None

    foam_alert_pb = safe_float(valuation_cfg.get("foam_alert_pb")) or 3.0
    foam_alert_pe = safe_float(valuation_cfg.get("foam_alert_pe")) or 30.0
    foam_alert_ps = safe_float(valuation_cfg.get("foam_alert_ps"))

    if eco_circle == "core_military":
        ps_policy = {
            "entry_ps_pass": safe_float(valuation_cfg.get("entry_ps_pass")) or 2.5,
            "entry_ps_caution": safe_float(valuation_cfg.get("entry_ps_caution")) or 4.0,
            "sell_ps": safe_float(valuation_cfg.get("sell_ps")) or 6.0,
            "optimistic_ps": safe_float(valuation_cfg.get("optimistic_ps")) or 8.0,
            "bubble_ps": safe_float(valuation_cfg.get("bubble_ps")) or 10.0,
        }
        ps_snapshot = estimate_current_ps(quote, scan_data, tier0_autofill_result, eco_context)
        current_ps = safe_float(ps_snapshot.get("current_ps"))
        revenue_proxy = safe_float(ps_snapshot.get("sales_proxy_revenue"))
        target_market_caps: dict[str, float] = {}
        target_prices: dict[str, float] = {}
        missing_inputs: list[str] = []
        if revenue_proxy is None:
            missing_inputs.append("sales_proxy_revenue")
        else:
            for multiple in (ps_policy["sell_ps"], ps_policy["optimistic_ps"], ps_policy["bubble_ps"]):
                label = f"{int(multiple)}x"
                target_market_caps[label] = revenue_proxy * multiple
                if share_count:
                    target_prices[label] = target_market_caps[label] / share_count

        foam_reasons: list[str] = []
        if current_pb is not None and current_pb > foam_alert_pb:
            foam_reasons.append(f"PB={current_pb:.2f}>{foam_alert_pb:.1f}")
        if current_pe is not None and current_pe > foam_alert_pe:
            foam_reasons.append(f"PE={current_pe:.2f}>{foam_alert_pe:.1f}")
        if current_ps is not None and foam_alert_ps is not None and current_ps > foam_alert_ps:
            foam_reasons.append(f"PS={current_ps:.2f}>{foam_alert_ps:.1f}")

        return {
            "ready": len(missing_inputs) == 0,
            "valuation_method": "sales_proxy_ps",
            "target_metric": "PS",
            "sales_proxy_revenue": revenue_proxy,
            "sales_proxy_basis": ps_snapshot.get("basis"),
            "proxy_report_date": ps_snapshot.get("report_date"),
            "annualization_factor": ps_snapshot.get("annualization_factor"),
            "matched_product": ps_snapshot.get("product_name"),
            "current_ps": current_ps,
            "current_pe": round(current_pe, 4) if current_pe is not None else None,
            "current_pb": current_pb,
            "target_market_caps": target_market_caps,
            "target_prices": target_prices,
            "ps_policy": ps_policy,
            "sell_key": f"{int(ps_policy['sell_ps'])}x",
            "optimistic_key": f"{int(ps_policy['optimistic_ps'])}x",
            "bubble_key": f"{int(ps_policy['bubble_ps'])}x",
            "missing_inputs": missing_inputs,
            "foam_alert": {
                "triggered": bool(foam_reasons),
                "reasons": foam_reasons,
                "action": "绝对清仓离场" if foam_reasons else "继续跟踪",
            },
            "note": f"{stock_code} 军工估值采用销售代理收入 × {int(ps_policy['sell_ps'])}/{int(ps_policy['optimistic_ps'])}/{int(ps_policy['bubble_ps'])}PS。",
        }

    autofill = _autofill_map(tier0_autofill_result)
    futures_data = commodity_data.get("futures", {}).get("data", {}) if isinstance(commodity_data.get("futures", {}), dict) else {}
    spot_summary = _latest_spot_summary(commodity_data)
    pe_policy = {
        "sell_pe": int(valuation_cfg.get("sell_pe", 15) or 15),
        "optimistic_pe": int(valuation_cfg.get("optimistic_pe", 20) or 20),
        "bubble_pe": int(valuation_cfg.get("bubble_pe", 30) or 30),
    }

    high_price, high_price_basis = _pick_conservative_high(futures_data, spot_summary, eco_context)
    latest_spot_price = safe_float(spot_summary.get("latest_price")) or safe_float(futures_data.get("latest_close"))
    cost_candidate = (autofill.get("cost_structure") or {}).get("candidate_value") or {}
    capacity_candidate = (autofill.get("capacity") or {}).get("candidate_value") or {}
    cycle_product = _pick_cycle_product_row(scan_data, eco_context.get("commodity", ""))
    matched_product = normalize_text(cycle_product.get("product_name"))
    if not matched_product and isinstance(cost_candidate, dict):
        matched_product = normalize_text(cost_candidate.get("primary_product"))
    if not matched_product and isinstance(capacity_candidate, dict):
        matched_product = normalize_text(capacity_candidate.get("primary_product")) or normalize_text(capacity_candidate.get("capacity_label"))
    matched_product = matched_product or normalize_text(eco_context.get("commodity"))

    base_unit_cost = safe_float(cost_candidate.get("unit_cost")) if isinstance(cost_candidate, dict) else None
    unit_cost_basis = "tier0_unit_cost"
    if base_unit_cost is None and isinstance(cost_candidate, dict):
        cost_ratio = safe_float(cost_candidate.get("cost_ratio"))
        if cost_ratio is None:
            cost_ratio = safe_float(cycle_product.get("cost_ratio"))
        if cost_ratio is not None and latest_spot_price is not None:
            base_unit_cost = latest_spot_price * cost_ratio
            unit_cost_basis = "latest_spot_x_cost_ratio"

    cost_stress_pct = safe_float(mode_cfg.get("cost_stress_pct")) or 0.15
    stressed_unit_cost = base_unit_cost * (1 + cost_stress_pct) if base_unit_cost is not None else None

    quantity_value = safe_float(capacity_candidate.get("capacity_ton")) if isinstance(capacity_candidate, dict) else None
    quantity_basis = "tier0_capacity"
    annualization_factor = 1.0
    proxy_report_date = ""
    if quantity_value is None:
        proxy_revenue = safe_float(capacity_candidate.get("proxy_product_revenue_yuan")) if isinstance(capacity_candidate, dict) else None
        if proxy_revenue is None:
            proxy_revenue = safe_float(cycle_product.get("revenue_yuan"))
            proxy_report_date = normalize_text(cycle_product.get("report_date"))
        else:
            proxy_report_date = normalize_text(capacity_candidate.get("report_date"))
            if not proxy_report_date:
                proxy_report_date = normalize_text(cycle_product.get("report_date"))
        annualization_factor = _annualization_factor(proxy_report_date)
        if proxy_revenue is not None and annualization_factor > 1:
            proxy_revenue = proxy_revenue * annualization_factor
        if proxy_revenue is not None and latest_spot_price not in (None, 0):
            quantity_value = proxy_revenue / latest_spot_price
            if annualization_factor > 1:
                quantity_basis = f"annualized_{annualization_factor:.2f}x_revenue_div_latest_spot"
            else:
                quantity_basis = "latest_revenue_div_latest_spot"

    target_market_caps: dict[str, float] = {}
    target_prices: dict[str, float] = {}
    peak_profit = None
    per_unit_profit = None
    missing_inputs: list[str] = []
    if high_price is None:
        missing_inputs.append("conservative_high_price")
    if stressed_unit_cost is None:
        missing_inputs.append("stressed_unit_cost")
    if quantity_value is None:
        missing_inputs.append("capacity_or_volume_proxy")
    if not missing_inputs:
        per_unit_profit = max((high_price or 0) - (stressed_unit_cost or 0), 0)
        peak_profit = per_unit_profit * quantity_value
        for pe in sorted({pe_policy["sell_pe"], pe_policy["optimistic_pe"], pe_policy["bubble_pe"]}):
            label = f"{pe}x"
            target_market_caps[label] = peak_profit * pe
            if share_count:
                target_prices[label] = target_market_caps[label] / share_count

    foam_reasons: list[str] = []
    if current_pb is not None and current_pb > foam_alert_pb:
        foam_reasons.append(f"PB={current_pb:.2f}>{foam_alert_pb:.1f}")
    if current_pe is not None and current_pe > foam_alert_pe:
        foam_reasons.append(f"PE={current_pe:.2f}>{foam_alert_pe:.1f}")

    return {
        "ready": len(missing_inputs) == 0,
        "valuation_method": "peak_profit_pe",
        "target_metric": "PE",
        "high_price": high_price,
        "high_price_basis": high_price_basis,
        "unit_cost": base_unit_cost,
        "stressed_unit_cost": stressed_unit_cost,
        "unit_cost_basis": unit_cost_basis,
        "cost_stress_pct": cost_stress_pct * 100,
        "quantity_value": quantity_value,
        "quantity_basis": quantity_basis,
        "quantity_label": "产能(吨/年)" if quantity_basis == "tier0_capacity" else "销量代理(吨)",
        "proxy_report_date": proxy_report_date,
        "annualization_factor": annualization_factor,
        "matched_product": matched_product,
        "per_unit_profit": per_unit_profit,
        "peak_profit": peak_profit,
        "target_market_caps": target_market_caps,
        "target_prices": target_prices,
        "pe_policy": pe_policy,
        "sell_key": f"{pe_policy['sell_pe']}x",
        "optimistic_key": f"{pe_policy['optimistic_pe']}x",
        "bubble_key": f"{pe_policy['bubble_pe']}x",
        "current_pb": current_pb,
        "current_pe": round(current_pe, 4) if current_pe is not None else None,
        "missing_inputs": missing_inputs,
        "foam_alert": {
            "triggered": bool(foam_reasons),
            "reasons": foam_reasons,
            "action": "绝对清仓离场" if foam_reasons else "继续跟踪",
        },
        "note": (
            f"{stock_code} 周期估值按“保守高位价 - 上行情景成本（上浮 {cost_stress_pct * 100:.0f}%）”"
            f" × 数量 × {pe_policy['sell_pe']}/{pe_policy['optimistic_pe']}/{pe_policy['bubble_pe']}PE 生成"
        ),
    }
