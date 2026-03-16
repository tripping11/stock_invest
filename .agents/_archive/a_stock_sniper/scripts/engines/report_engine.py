"""Generate the final deep sniper markdown report from collected artifacts."""
from __future__ import annotations

import datetime
import json
import os
import re
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(SCRIPTS_DIR))

from utils.research_utils import (  # noqa: E402
    _pick_revenue_col,
    assess_price_trigger,
    assess_business_purity,
    classify_state_ownership,
    detect_data_freshness,
    extract_price_series,
    extract_latest_revenue_snapshot,
    extract_market_cap,
    get_manifest_field_entry,
    get_crocodile_mode_config,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
    get_tier0_item,
    load_industry_mapping,
    load_yaml_config,
    normalize_text,
    safe_float,
)
from engines.synthesis_engine import build_synthesis  # noqa: E402
from engines.valuation_engine import build_valuation_case, estimate_current_ps, resolve_exit_prices  # noqa: E402
from utils.signal_health_utils import evaluate_signal_health_v2  # noqa: E402


def _fmt_num(value, digits: int = 2) -> str:
    num = safe_float(value)
    if num is None:
        return "N/A"
    return f"{num:.{digits}f}"


def _fmt_pct(value, digits: int = 1) -> str:
    num = safe_float(value)
    if num is None:
        return "N/A"
    return f"{num:.{digits}f}%"


def _fmt_yi(value) -> str:
    num = safe_float(value)
    if num is None:
        return "N/A"
    return f"{num / 1e8:.2f}亿"


def _fmt_price(value) -> str:
    num = safe_float(value)
    if num is None:
        return "N/A"
    return f"{num:.2f}元"


def _compact_text(value, limit: int = 120) -> str:
    text = normalize_text(value)
    if not text:
        return "N/A"
    return text if len(text) <= limit else f"{text[:limit]}..."


def _compact_json(value, limit: int = 160) -> str:
    if value in (None, "", [], {}):
        return "N/A"
    if isinstance(value, (str, int, float)):
        return _compact_text(value, limit=limit)
    try:
        return _compact_text(json.dumps(value, ensure_ascii=False, default=str), limit=limit)
    except TypeError:
        return _compact_text(str(value), limit=limit)


def _manifest_source_label(entry: dict) -> str:
    source_path = normalize_text(entry.get("source_path"))
    status = normalize_text(entry.get("status")) or "N/A"
    if source_path:
        return f"`{source_path}` [{status}]"
    return f"`source_manifest.json` [{status}]"


def _manifest_tier_label(entry: dict, fallback: str) -> str:
    actual_tier = entry.get("actual_tier") if isinstance(entry, dict) else None
    if actual_tier in (0, 1, 2):
        return f"Tier {actual_tier}"
    return fallback


def _manifest_detail(entry: dict, *, fallback: str = "N/A") -> str:
    if not isinstance(entry, dict):
        return fallback

    value = entry.get("value")
    snippet = entry.get("snippet")
    pages = entry.get("pages")
    parts = []
    if value not in (None, "", [], {}):
        parts.append(f"`value={_compact_json(value, limit=100)}`")
    if pages not in (None, "", []):
        parts.append(f"`pages={_compact_json(pages, limit=40)}`")
    if snippet:
        parts.append(f"`snippet={_compact_text(snippet, limit=100)}`")
    return " ".join(parts) if parts else fallback


def _extract_quote_field(quote: dict, *, contains: tuple[str, ...]) -> float | None:
    for key, value in quote.items():
        key_text = normalize_text(key)
        if all(token in key_text for token in contains):
            num = safe_float(value)
            if num is not None:
                return num
    return None


def _extract_latest_quote_price(quote: dict) -> float | None:
    for key, value in quote.items():
        key_text = normalize_text(key)
        if "最新" in key_text and "价" in key_text:
            num = safe_float(value)
            if num is not None:
                return num
    return None


def _pick_verdict(score: float, scoring_rules: dict) -> dict:
    for item in scoring_rules.get("verdict", []):
        start, end = item["range"].split("-")
        if float(start) <= score <= float(end):
            return item
    return {"label": "未定义", "action": "待补规则"}


def _autofill_map(tier0_autofill_result: dict) -> dict:
    return {item["field_name"]: item for item in tier0_autofill_result.get("items", [])}


def _extract_candidate_value(autofill: dict, field_name: str):
    return autofill.get(field_name, {}).get("candidate_value")


def _extract_number_from_text(value, keywords: tuple[str, ...] = ()) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = ""
    if isinstance(value, dict):
        for key in keywords:
            if key in value and safe_float(value.get(key)) is not None:
                return safe_float(value.get(key))
        text = json.dumps(value, ensure_ascii=False, default=str)
    elif value is not None:
        text = str(value)

    if not text:
        return None

    pattern = re.compile(r"(-?\d+(?:\.\d+)?)")
    for match in pattern.finditer(text):
        try:
            return float(match.group(1))
        except ValueError:
            continue
    return None


def _latest_spot_summary(commodity_data: dict) -> dict:
    spot_data = commodity_data.get("spot_price", {}).get("data", [])
    if isinstance(spot_data, list) and spot_data:
        latest = spot_data[-1]
        close_value = latest.get("收盘价") or latest.get("close")
        date_value = latest.get("日期") or latest.get("date")
        return {"latest_price": close_value, "latest_date": str(date_value), "status": commodity_data.get("spot_price", {}).get("status")}
    return {"latest_price": None, "latest_date": "", "status": commodity_data.get("spot_price", {}).get("status", "missing")}


def _inventory_summary(commodity_data: dict) -> dict:
    inventory_entry = commodity_data.get("inventory", {}) if isinstance(commodity_data.get("inventory", {}), dict) else {}
    inventory_data = inventory_entry.get("data", {}) if isinstance(inventory_entry.get("data", {}), dict) else {}
    exchange_entry = commodity_data.get("exchange_inventory", {}) if isinstance(commodity_data.get("exchange_inventory", {}), dict) else {}
    exchange_data = exchange_entry.get("data", {}) if isinstance(exchange_entry.get("data", {}), dict) else {}
    social_entry = commodity_data.get("social_inventory", {}) if isinstance(commodity_data.get("social_inventory", {}), dict) else {}
    social_data = social_entry.get("data", {}) if isinstance(social_entry.get("data", {}), dict) else {}

    latest_exchange = exchange_data.get("latest_record", {}) if isinstance(exchange_data.get("latest_record", {}), dict) else {}
    coverage = normalize_text(inventory_data.get("coverage")) or "missing"
    exchange_text = "N/A"
    if latest_exchange:
        exchange_text = f"{latest_exchange.get('date', 'N/A')} 库存 {latest_exchange.get('inventory', 'N/A')}"
        if latest_exchange.get("change") not in (None, ""):
            exchange_text += f" / 增减 {latest_exchange.get('change')}"

    social_guide = social_data.get("manual_guide", {}) if isinstance(social_data.get("manual_guide", {}), dict) else {}
    social_text = social_guide.get("primary", "N/A") if social_guide else "N/A"

    headline_parts = []
    if exchange_text != "N/A":
        headline_parts.append(f"交易所: {exchange_text}")
    if coverage != "exchange_and_social":
        headline_parts.append(f"社会库存: {social_text}")

    return {
        "status": inventory_entry.get("status", "missing"),
        "coverage": coverage,
        "headline": " | ".join(headline_parts) or "N/A",
        "exchange_status": exchange_entry.get("status", "missing"),
        "social_status": social_entry.get("status", "missing"),
    }


def _latest_macro_record(macro_entry: dict) -> dict:
    data = macro_entry.get("data", {})
    if isinstance(data, dict) and isinstance(data.get("records"), list) and data["records"]:
        return data["records"][0]
    if isinstance(data, list) and data:
        return data[-1]
    return {}


def _score_simplicity(top_ratio: float, eco_circle: str) -> tuple[int, str]:
    business_purity = 0
    if top_ratio >= 0.9:
        business_purity = 8
    elif top_ratio >= 0.8:
        business_purity = 6
    elif top_ratio >= 0.7:
        business_purity = 4

    profit_clarity = 7 if eco_circle in {"core_resource", "rigid_shovel", "core_military"} else 1
    return business_purity + profit_clarity, f"主营占比={top_ratio:.1%}, eco_circle={eco_circle}"


def _gate_status_label(check: dict) -> str:
    if check.get("normalized_status"):
        return str(check["normalized_status"])
    verdict = str(check.get("verdict", "")).upper()
    if "PASS" in verdict:
        return "通过"
    if "KILL" in verdict:
        return "不通过"
    return "待补证"


def _pick_cycle_product_row(scan_data: dict, commodity_keyword: str) -> dict:
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

    ranked: list[tuple[int, str, float, dict]] = []
    for row in product_rows:
        name = normalize_text(row.get(name_col or ""))
        if not name or any(token in name for token in ("其他", "合计", "国内", "国外", "补充")):
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


def _pick_conservative_high(futures_data: dict, spot_summary: dict, eco_context: dict) -> tuple[float | None, str]:
    commodity_name = eco_context.get("commodity", "")
    if commodity_name:
        cmd_profiles = load_yaml_config("commodity_profiles.yaml").get("profiles", {})
        profile = cmd_profiles.get(commodity_name, {})
        static_anchor = safe_float(profile.get("conservative_high_anchor"))
        if static_anchor is not None:
            return static_anchor, "profile_conservative_anchor"

    valuation_cfg = get_crocodile_mode_config(eco_context.get("four_signal_mode")).get("valuation", {}) or {}
    latest_price = safe_float(spot_summary.get("latest_price")) or safe_float(futures_data.get("latest_close"))
    latest_floor_multiplier = safe_float(valuation_cfg.get("latest_floor_multiplier")) or 1.0
    latest_floor = latest_price * latest_floor_multiplier if latest_price is not None else None

    # Priority matching: Long term high > Medium term high > Short term high
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


def _resolve_ownership_snapshot(
    stock_code: str,
    scan_data: dict,
    tier0_prep: dict | None,
    tier0_autofill_result: dict,
) -> dict:
    profile = scan_data.get("company_profile", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    controller_text = normalize_text(profile.get("实际控制人") or profile.get("控股股东"))
    autofill = _autofill_map(tier0_autofill_result)
    if not controller_text:
        controller_text = normalize_text(autofill.get("actual_controller", {}).get("candidate_value"))
    company_name_hints = []
    for dataset in (profile, quote):
        if not isinstance(dataset, dict):
            continue
        for key, value in dataset.items():
            if any(token in normalize_text(key) for token in ("名称", "简称", "公司")):
                text = normalize_text(value)
                if text:
                    company_name_hints.append(text)
    return classify_state_ownership(
        stock_code,
        controller_text,
        tier0_item=get_tier0_item(tier0_prep, "actual_controller"),
        company_name_hints=[item for item in dict.fromkeys(company_name_hints) if item],
    )


def _score_ownership(ownership: dict) -> tuple[int, str]:
    score = int(ownership.get("score", 0) or 0)
    reason = normalize_text(ownership.get("reason")) or "未提取到明确控制关系"
    return min(score, 25), reason


def _score_signal_mismatch(
    pb: float | None,
    pb_percentile: float | None,
    current_vs_high: float | None,
    price_trigger: dict,
    signal_health: dict,
) -> tuple[int, str]:
    pb_score = 0
    if pb is not None:
        if pb <= 0.6:
            pb_score = 12
        elif pb <= 0.8:
            pb_score = 10
        elif pb <= 1.0:
            pb_score = 7
        elif pb <= 1.2:
            pb_score = 5
        elif pb <= 1.4:
            pb_score = 2

    percentile_score = 0
    if pb_percentile is not None:
        if pb_percentile <= 10:
            percentile_score = 5
        elif pb_percentile <= 25:
            percentile_score = 3
        elif pb_percentile <= 40:
            percentile_score = 2
        elif pb_percentile <= 60:
            percentile_score = 1

    mismatch_score = 0
    trigger_verdict = normalize_text(price_trigger.get("verdict")).lower()
    if trigger_verdict == "pass":
        if current_vs_high is not None and current_vs_high <= 45:
            mismatch_score = 8
        elif current_vs_high is not None and current_vs_high <= 55:
            mismatch_score = 6
        elif current_vs_high is not None and current_vs_high <= 65:
            mismatch_score = 4
        else:
            mismatch_score = 2
    elif trigger_verdict == "caution":
        if current_vs_high is not None and current_vs_high <= 50:
            mismatch_score = 4
        elif current_vs_high is not None and current_vs_high <= 60:
            mismatch_score = 3
        else:
            mismatch_score = 1
    elif signal_health.get("core_ready") and current_vs_high is not None and current_vs_high <= 55:
        mismatch_score = 2

    total = min(pb_score + percentile_score + mismatch_score, 25)
    reason = (
        f"PB={pb if pb is not None else 'N/A'}, PB分位={pb_percentile if pb_percentile is not None else 'N/A'}, "
        f"距高点={current_vs_high if current_vs_high is not None else 'N/A'}%, trigger={trigger_verdict or 'N/A'}"
    )
    return total, reason


def _score_profit_elasticity(
    eco_circle: str,
    autofill: dict,
    market_cap_yi: float | None,
    latest_net_profit: float | None,
    trigger_verdict: str | None = None,
) -> tuple[int, str]:
    autofill_fields = set(autofill.keys())
    score = 1
    if eco_circle == "core_resource":
        score = 6
    elif eco_circle == "rigid_shovel":
        score = 4
    elif eco_circle == "core_military":
        score = 5

    if market_cap_yi is not None:
        if market_cap_yi <= 200:
            score += 2
        elif market_cap_yi <= 500:
            score += 1

    if {"mineral_rights", "cost_structure"} <= autofill_fields:
        score += 4
    elif "cost_structure" in autofill_fields or "mineral_rights" in autofill_fields:
        score += 2

    capacity_candidate = _extract_candidate_value(autofill, "capacity") or {}
    capex_candidate = _extract_candidate_value(autofill, "capex_investment") or {}
    capacity_ton = safe_float(capacity_candidate.get("capacity_ton")) if isinstance(capacity_candidate, dict) else None
    capacity_hits = capacity_candidate.get("explicit_capacity_hits", []) if isinstance(capacity_candidate, dict) else []
    if capacity_ton is not None or capacity_hits:
        score += 4
    elif "capacity" in autofill_fields:
        score += 2

    capex_summary = normalize_text(capex_candidate.get("summary") if isinstance(capex_candidate, dict) else capex_candidate)
    if "capex_investment" in autofill_fields:
        if any(token in capex_summary for token in ("项目", "在建", "扩建", "新增投资", "采矿权", "建设")):
            score += 4
        else:
            score += 2

    if latest_net_profit is None:
        score += 2
    elif latest_net_profit < 1e8:
        score += 4
    elif latest_net_profit < 5e8:
        score += 3
    elif latest_net_profit < 1e9:
        score += 2
    else:
        score += 1

    penalty = 0
    if trigger_verdict == "pending":
        penalty = 4
    elif trigger_verdict == "caution":
        penalty = 2
    score = max(min(score - penalty, 25), 0)

    reason = (
        f"eco_circle={eco_circle}, autofill={sorted(autofill_fields)}, market_cap={market_cap_yi if market_cap_yi is not None else 'N/A'}亿, "
        f"net_profit={latest_net_profit if latest_net_profit is not None else 'N/A'}"
    )
    if penalty:
        reason += f" (timing_penalty={penalty})"
    return score, reason


def _score_bottom(current_vs_high: float | None, pb: float | None, pb_percentile: float | None) -> tuple[int, str]:
    if current_vs_high is None or pb is None or pb_percentile is None:
        return 0, "缺少位置或 PB 分位数据"
    if current_vs_high <= 35 and pb <= 0.8 and pb_percentile <= 20:
        return 10, f"距高点{current_vs_high:.1f}%, PB={pb:.3f}, PB分位{pb_percentile:.1f}%"
    if current_vs_high <= 45 and pb <= 0.95 and pb_percentile <= 40:
        return 7, f"距高点{current_vs_high:.1f}%, PB={pb:.3f}, PB分位{pb_percentile:.1f}%"
    if current_vs_high <= 55 and (pb <= 1.2 or pb_percentile <= 65):
        return 5, f"距高点{current_vs_high:.1f}%, PB={pb:.3f}, PB分位{pb_percentile:.1f}%"
    if current_vs_high <= 65 or pb <= 1.0 or pb_percentile <= 50:
        return 3, f"距高点{current_vs_high:.1f}%, PB={pb:.3f}, PB分位{pb_percentile:.1f}%"
    return 0, f"距高点{current_vs_high:.1f}%, PB={pb:.3f}, PB分位{pb_percentile:.1f}%"


def build_scorecard(
    scan_data: dict,
    source_manifest: dict,
    eco_context: dict,
    tier0_autofill_result: dict,
    *,
    tier0_prep: dict | None = None,
    commodity_data: dict | None = None,
) -> dict:
    scoring_rules = load_yaml_config("scoring_rules.yaml")
    autofill = _autofill_map(tier0_autofill_result)
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    price_trigger = assess_price_trigger(commodity_data or {}, eco_context)
    signal_health = evaluate_signal_health_v2(eco_context, source_manifest, commodity_data or {}, {})
    ownership = _resolve_ownership_snapshot(
        str(scan_data.get("company_profile", {}).get("data", {}).get("股票代码") or quote.get("代码") or ""),
        scan_data,
        tier0_prep,
        tier0_autofill_result,
    )

    ownership_score, ownership_reason = _score_ownership(ownership)
    signal_score, signal_reason = _score_signal_mismatch(
        safe_float(valuation.get("pb")),
        safe_float(valuation.get("pb_percentile")),
        safe_float(kline.get("current_vs_high")),
        price_trigger,
        signal_health,
    )
    market_cap_yi = None
    market_cap_yi = extract_market_cap(quote)
    if market_cap_yi is not None:
        market_cap_yi /= 1e8

    elasticity_score, elasticity_reason = _score_profit_elasticity(
        eco_context.get("eco_circle", ""),
        autofill,
        market_cap_yi,
        income.get("net_profit"),
        normalize_text(price_trigger.get("verdict")).lower(),
    )
    simplicity_score, simplicity_reason = _score_simplicity(purity.get("top_ratio", 0.0), eco_context.get("eco_circle", ""))
    bottom_score, bottom_reason = _score_bottom(
        safe_float(kline.get("current_vs_high")),
        safe_float(valuation.get("pb")),
        safe_float(valuation.get("pb_percentile")),
    )

    dimensions = [
        {"label": "1. 国资底线", "max_score": 25, "score": ownership_score, "reason": ownership_reason},
        {"label": "2. 四维错配与破净", "max_score": 25, "score": signal_score, "reason": signal_reason},
        {"label": "3. 宏观利润弹性巨大", "max_score": 25, "score": elasticity_score, "reason": elasticity_reason},
        {"label": "4. 极简纯粹", "max_score": 15, "score": simplicity_score, "reason": simplicity_reason},
        {"label": "5. 底部长牛皮市", "max_score": 10, "score": bottom_score, "reason": bottom_reason},
    ]
    raw_total = sum(item["score"] for item in dimensions)

    # Tier 0 scoring cap: prevent inflated scores on unverified data
    tier0_gate = scoring_rules.get("tier0_gate", {})
    tier0_cap = tier0_gate.get("tier0_unverified_cap", 74)
    tier0_threshold = tier0_gate.get("tier0_unverified_threshold", 3)

    tier0_missing = source_manifest.get("summary", {}).get("tier0_required_missing", [])
    tier0_capped = False
    cap_reason = ""
    if len(tier0_missing) >= tier0_threshold and raw_total >= tier0_cap + 1:
        cap_reason = f"Tier0 未核验字段 {len(tier0_missing)} 个(≥{tier0_threshold})，原始分 {raw_total} 被封顶为 {tier0_cap}"
        raw_total = tier0_cap
        tier0_capped = True

    signal_capped = False
    signal_cap_reason = ""
    if not signal_health.get("core_ready") and raw_total > 69:
        signal_cap_reason = f"四维核心信号未齐备: {', '.join(signal_health.get('core_missing', []))}"
        raw_total = 69
        signal_capped = True

    discipline_capped = False
    discipline_cap_reason = ""
    pb_value = safe_float(valuation.get("pb"))
    trigger_verdict = normalize_text(price_trigger.get("verdict")).lower()
    if (trigger_verdict == "pending" or (pb_value is not None and pb_value > 1.25)) and raw_total > 54:
        raw_total = 54
        discipline_capped = True
        discipline_cap_reason = "价格质变尚未确认或估值仍偏贵，总分封顶为 54"
    elif (trigger_verdict == "caution" or (pb_value is not None and pb_value > 1.0)) and raw_total > 69:
        raw_total = 69
        discipline_capped = True
        discipline_cap_reason = "周期信号仅初步改善或仍未进入破净区，总分封顶为 69"

    verdict = _pick_verdict(raw_total, scoring_rules)
    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dimensions": dimensions,
        "total_score": raw_total,
        "verdict": verdict,
        "ownership": ownership,
        "tier0_capped": tier0_capped,
        "tier0_cap_reason": cap_reason,
        "signal_capped": signal_capped,
        "signal_cap_reason": signal_cap_reason,
        "discipline_capped": discipline_capped,
        "discipline_cap_reason": discipline_cap_reason,
    }


def _legacy_build_scorecard_v2(
    scan_data: dict,
    source_manifest: dict,
    eco_context: dict,
    tier0_autofill_result: dict,
    *,
    tier0_prep: dict | None = None,
    commodity_data: dict | None = None,
    synthesis_result: dict | None = None,
) -> dict:
    """10-dimension weighted scoring aligned with reference research reports.

    Each dimension is scored on a 10-point scale, then weighted to produce
    a 100-point total.  Weights sum to 100 (= percentage points).
    """
    scoring_rules = load_yaml_config("scoring_rules.yaml")
    autofill = _autofill_map(tier0_autofill_result)
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    price_trigger = assess_price_trigger(commodity_data or {}, eco_context)
    signal_health = evaluate_signal_health_v2(eco_context, source_manifest, commodity_data or {}, {})
    ownership = _resolve_ownership_snapshot(
        str(scan_data.get("company_profile", {}).get("data", {}).get("股票代码") or quote.get("代码") or ""),
        scan_data,
        tier0_prep,
        tier0_autofill_result,
    )

    synth = synthesis_result or {}
    price_analysis = synth.get("price_analysis", {})
    profit_analysis = synth.get("profit_analysis", {})
    pb = safe_float(valuation.get("pb"))
    pb_pct = safe_float(valuation.get("pb_percentile"))
    current_vs_high = safe_float(kline.get("current_vs_5yr_high")) or safe_float(kline.get("current_vs_high"))
    consolidation_months = safe_float(kline.get("consolidation_months"))
    volume_ratio = safe_float(kline.get("volume_ratio_20_vs_120"))
    market_cap = extract_market_cap(quote)
    market_cap_yi = market_cap / 1e8 if market_cap is not None else None
    eco_circle = eco_context.get("eco_circle", "")
    valuation_cfg = get_crocodile_mode_config(eco_context.get("four_signal_mode")).get("valuation", {}) or {}
    ps_snapshot = estimate_current_ps(quote, scan_data, tier0_autofill_result, eco_context) if eco_circle == "core_military" else {}
    current_ps = safe_float(ps_snapshot.get("current_ps")) if isinstance(ps_snapshot, dict) else None

    # ── Dimension 1: 央企/省国资委 (weight 10) ──
    owner_score, owner_reason = _score_ownership(ownership)
    # Remap from 25-scale to 10-scale
    owner_map = {"central_soe": 10, "provincial_soe": 9, "local_soe": 6, "state_backed_unclear": 2, "platform_unknown": 2}
    d1_score = owner_map.get(ownership.get("category", ""), 0)
    d1_reason = owner_reason

    # ── Dimension 2: 独一无二，资本无法选择 (weight 14) ──
    # Heuristic: check eco_context + gate_result uniqueness cues
    d2_score = 5  # default mid
    d2_reason = "需人工判断行业地位"
    if eco_circle in ("core_resource", "rigid_shovel", "core_military"):
        d2_score = 7
        d2_reason = f"属于{eco_circle}圈层，具备一定稀缺性"
    if eco_circle == "core_military" and current_ps is not None and current_ps <= (safe_float(valuation_cfg.get("entry_ps_pass")) or 2.5):
        d2_score = 9
        d2_reason = f"军工低 PS 稀缺平台，PS={current_ps:.2f}"
    if eco_circle == "unknown":
        d2_score = 3
        d2_reason = "生态圈层未识别，稀缺性不明"

    # ── Dimension 3: 产品持续大涨>100% (weight 13) ──
    price_phase = price_analysis.get("phase", "")
    yoy_change = price_analysis.get("yoy_change_pct")
    drawdown = price_analysis.get("drawdown_from_high_pct")
    rebound = price_analysis.get("rebound_from_low_pct")
    if yoy_change is not None and yoy_change >= 100:
        d3_score, d3_reason = 10, f"产品同比涨{yoy_change:.0f}%"
    elif yoy_change is not None and yoy_change >= 50:
        d3_score, d3_reason = 8, f"产品同比涨{yoy_change:.0f}%"
    elif yoy_change is not None and yoy_change >= 10:
        d3_score, d3_reason = 6, f"产品同比涨{yoy_change:.0f}%"
    elif rebound is not None and rebound >= 20:
        d3_score, d3_reason = 5, f"底部反弹{rebound:.0f}%"
    elif drawdown is not None and drawdown >= 50:
        d3_score, d3_reason = 4, f"距高点跌{drawdown:.0f}%，处于深度底部"
    elif drawdown is not None and drawdown >= 30:
        d3_score, d3_reason = 2, f"距高点跌{drawdown:.0f}%，下行中"
    elif price_phase:
        d3_score, d3_reason = 2, f"{price_phase}"
    else:
        d3_score, d3_reason = 0, "无产品价格数据"

    # ── Dimension 4: 扣非净利润持续大涨>60% (weight 14) ──
    profit_change = profit_analysis.get("change_pct")
    profit_phase = profit_analysis.get("phase", "")
    latest_profit = profit_analysis.get("latest_profit")
    if profit_change is not None and profit_change >= 100:
        d4_score, d4_reason = 10, f"扣非净利同比+{profit_change:.0f}%"
    elif profit_change is not None and profit_change >= 60:
        d4_score, d4_reason = 8, f"扣非净利同比+{profit_change:.0f}%"
    elif profit_change is not None and profit_change >= 20:
        d4_score, d4_reason = 6, f"扣非净利同比+{profit_change:.0f}%"
    elif profit_change is not None and profit_change <= -30 and latest_profit is not None and latest_profit > 0:
        d4_score, d4_reason = 5, f"利润深跌{profit_change:.0f}%但未亏损，周期底部特征"
    elif profit_change is not None and profit_change >= 0:
        d4_score, d4_reason = 3, f"利润平稳(+{profit_change:.0f}%)"
    elif profit_change is not None:
        d4_score, d4_reason = 2, f"利润下滑({profit_change:.0f}%)"
    else:
        d4_score, d4_reason = 0, "利润数据不足"

    # ── Dimension 5: 主营集中>75% (weight 9) ──
    top_ratio = purity.get("top_ratio", 0.0)
    if top_ratio >= 0.90:
        d5_score, d5_reason = 10, f"主营占比{top_ratio:.0%}"
    elif top_ratio >= 0.80:
        d5_score, d5_reason = 8, f"主营占比{top_ratio:.0%}"
    elif top_ratio >= 0.70:
        d5_score, d5_reason = 6, f"主营占比{top_ratio:.0%}"
    else:
        d5_score, d5_reason = 0, f"主营占比{top_ratio:.0%} < 70%"

    # ── Dimension 6: 盈利模式简单清晰 (weight 8) ──
    d6_score = 7  # default: moderately clear
    d6_reason = "需人工判断"
    if eco_circle == "core_resource":
        d6_score, d6_reason = 10, "售价-成本=利润，商业模式极简"
    elif eco_circle == "rigid_shovel":
        d6_score, d6_reason = 9, "订单×单价-固定成本=利润"
    elif eco_circle == "core_military":
        d6_score, d6_reason = 8, "配套单价-研制成本=利润"

    # ── Dimension 7: 总市值<200亿 (weight 8) ──
    if market_cap_yi is not None:
        if market_cap_yi < 100:
            d7_score, d7_reason = 10, f"市值{market_cap_yi:.0f}亿 < 100亿"
        elif market_cap_yi < 150:
            d7_score, d7_reason = 8, f"市值{market_cap_yi:.0f}亿"
        elif market_cap_yi < 200:
            d7_score, d7_reason = 6, f"市值{market_cap_yi:.0f}亿"
        elif market_cap_yi < 300:
            d7_score, d7_reason = 4, f"市值{market_cap_yi:.0f}亿"
        else:
            d7_score, d7_reason = 2, f"市值{market_cap_yi:.0f}亿 > 300亿"
    else:
        d7_score, d7_reason = 0, "无市值数据"

    # ── Dimension 8: 股价盘整2-3年充分吸收 (weight 9) ──
    if consolidation_months is not None:
        if consolidation_months >= 36:
            d8_score, d8_reason = 10, f"横盘约{consolidation_months:.0f}个月"
        elif consolidation_months >= 24:
            d8_score, d8_reason = 8, f"横盘约{consolidation_months:.0f}个月"
        elif consolidation_months >= 12:
            d8_score, d8_reason = 5, f"横盘约{consolidation_months:.0f}个月"
        elif consolidation_months >= 6:
            d8_score, d8_reason = 2, f"横盘约{consolidation_months:.0f}个月，尚未充分"
        else:
            d8_score, d8_reason = 0, f"横盘仅{consolidation_months:.0f}个月"
    elif current_vs_high is not None and pb_pct is not None:
        if current_vs_high <= 45 and pb_pct <= 30:
            d8_score, d8_reason = 5, f"缺少横盘时长，仅见低位特征：距高点{current_vs_high:.0f}%，PB分位{pb_pct:.0f}%"
        elif current_vs_high <= 60:
            d8_score, d8_reason = 2, f"缺少横盘时长，仅见位置偏低：距高点{current_vs_high:.0f}%"
        else:
            d8_score, d8_reason = 0, f"缺少横盘时长且位置不低：距高点{current_vs_high:.0f}%"
    else:
        d8_score, d8_reason = 0, "K线数据不足"

    # ── Dimension 9: 股价位于月线底部区域 (weight 9) ──
    if eco_circle == "core_military" and current_ps is not None:
        if current_ps <= (safe_float(valuation_cfg.get("entry_ps_pass")) or 2.5) and current_vs_high is not None and current_vs_high <= 55:
            d9_score, d9_reason = 10, f"PS={current_ps:.2f}，距高点{100 - current_vs_high:.0f}%"
        elif current_ps <= (safe_float(valuation_cfg.get("entry_ps_caution")) or 4.0) and current_vs_high is not None and current_vs_high <= 70:
            d9_score, d9_reason = 7, f"PS={current_ps:.2f}，位置已明显回落"
        elif current_ps <= (safe_float(valuation_cfg.get("entry_ps_caution")) or 4.0):
            d9_score, d9_reason = 5, f"PS={current_ps:.2f}"
        else:
            d9_score, d9_reason = 0, f"PS={current_ps:.2f}，军工估值偏热"
    elif pb is not None and current_vs_high is not None:
        if pb <= 0.8 and current_vs_high <= 40:
            d9_score, d9_reason = 10, f"PB={pb:.2f}≤0.8，距高点跌{100 - current_vs_high:.0f}%"
        elif pb <= 1.0 and current_vs_high <= 50:
            d9_score, d9_reason = 8, f"PB={pb:.2f}≤1.0，距高点跌{100 - current_vs_high:.0f}%"
        elif pb <= 1.2 and current_vs_high <= 60:
            d9_score, d9_reason = 6, f"PB={pb:.2f}≤1.2"
        elif pb <= 1.5:
            d9_score, d9_reason = 4, f"PB={pb:.2f}"
        else:
            d9_score, d9_reason = 0, f"PB={pb:.2f}，估值偏高"
    elif pb is not None:
        if pb <= 1.0:
            d9_score, d9_reason = 7, f"PB={pb:.2f}≤1.0"
        else:
            d9_score, d9_reason = 2, f"PB={pb:.2f}"
    else:
        d9_score, d9_reason = 0, "无估值数据"

    # ── Dimension 10: 底部成交量显著放大 (weight 6) ──
    if volume_ratio is not None:
        if volume_ratio >= 3:
            d10_score, d10_reason = 10, f"近20日量能/120日均量={volume_ratio:.2f}x"
        elif volume_ratio >= 2:
            d10_score, d10_reason = 7, f"近20日量能/120日均量={volume_ratio:.2f}x"
        elif volume_ratio >= 1.2:
            d10_score, d10_reason = 4, f"近20日量能/120日均量={volume_ratio:.2f}x"
        else:
            d10_score, d10_reason = 2, f"近20日量能/120日均量={volume_ratio:.2f}x"
    else:
        d10_score, d10_reason = 0, "成交量数据不足"

    # ── Assemble ──
    dim_cfg = scoring_rules.get("dimensions", {})
    dimensions = [
        {"label": dim_cfg.get("state_ownership", {}).get("label", "1. 央企/省国资委"), "weight": 10, "raw_score": d1_score, "reason": d1_reason},
        {"label": dim_cfg.get("uniqueness", {}).get("label", "2. 独一无二"), "weight": 14, "raw_score": d2_score, "reason": d2_reason},
        {"label": dim_cfg.get("product_price_surge", {}).get("label", "3. 产品大涨"), "weight": 13, "raw_score": d3_score, "reason": d3_reason},
        {"label": dim_cfg.get("profit_surge", {}).get("label", "4. 利润大涨"), "weight": 14, "raw_score": d4_score, "reason": d4_reason},
        {"label": dim_cfg.get("business_concentration", {}).get("label", "5. 主营集中"), "weight": 9, "raw_score": d5_score, "reason": d5_reason},
        {"label": dim_cfg.get("profit_model_clarity", {}).get("label", "6. 盈利模式"), "weight": 8, "raw_score": d6_score, "reason": d6_reason},
        {"label": dim_cfg.get("market_cap_small", {}).get("label", "7. 市值<200亿"), "weight": 8, "raw_score": d7_score, "reason": d7_reason},
        {"label": dim_cfg.get("consolidation_time", {}).get("label", "8. 盘整时间"), "weight": 9, "raw_score": d8_score, "reason": d8_reason},
        {"label": dim_cfg.get("price_at_bottom", {}).get("label", "9. 月线底部"), "weight": 9, "raw_score": d9_score, "reason": d9_reason},
        {"label": dim_cfg.get("volume_breakout", {}).get("label", "10. 底部放量"), "weight": 6, "raw_score": d10_score, "reason": d10_reason},
    ]
    # Weighted score: (raw_score / 10) * weight, sum to 100
    for d in dimensions:
        d["weighted_score"] = round(d["raw_score"] / 10.0 * d["weight"], 2)
    raw_total = round(sum(d["weighted_score"] for d in dimensions), 1)

    # Tier 0 cap
    tier0_gate = scoring_rules.get("tier0_gate", {})
    tier0_cap = tier0_gate.get("tier0_unverified_cap", 74)
    tier0_threshold = tier0_gate.get("tier0_unverified_threshold", 3)
    tier0_missing = source_manifest.get("summary", {}).get("tier0_required_missing", [])
    tier0_capped = False
    cap_reason = ""
    if len(tier0_missing) >= tier0_threshold and raw_total >= tier0_cap + 1:
        cap_reason = f"Tier0 未核验字段 {len(tier0_missing)} 个(≥{tier0_threshold})，原始分 {raw_total} 被封顶为 {tier0_cap}"
        raw_total = tier0_cap
        tier0_capped = True

    signal_capped = False
    signal_cap_reason = ""
    if not signal_health.get("core_ready") and raw_total > 69:
        signal_cap_reason = f"四维核心信号未齐备: {', '.join(signal_health.get('core_missing', []))}"
        raw_total = 69
        signal_capped = True

    trigger_verdict = normalize_text(price_trigger.get("verdict")).lower()
    discipline_capped = False
    discipline_cap_reason = ""
    if eco_circle == "core_military":
        hard_ps = (safe_float(valuation_cfg.get("entry_ps_caution")) or 4.0) * 1.25
        caution_ps = safe_float(valuation_cfg.get("entry_ps_caution")) or 4.0
        if current_ps is None and raw_total > 54:
            discipline_cap_reason = "军工路径缺少 PS 代理收入，总分封顶为 54"
            raw_total = 54
            discipline_capped = True
        elif current_ps is not None and current_ps > hard_ps and raw_total > 54:
            discipline_cap_reason = f"军工 PS={current_ps:.2f} 仍显著偏贵，总分封顶为 54"
            raw_total = 54
            discipline_capped = True
        elif current_ps is not None and current_ps > caution_ps and raw_total > 69:
            discipline_cap_reason = f"军工 PS={current_ps:.2f} 未回到极低区，总分封顶为 69"
            raw_total = 69
            discipline_capped = True
    else:
        if (trigger_verdict == "pending" or (pb is not None and pb > 1.25)) and raw_total > 54:
            discipline_cap_reason = "价格质变尚未确认或估值仍偏贵，总分封顶为 54"
            raw_total = 54
            discipline_capped = True
        elif (trigger_verdict == "caution" or (pb is not None and pb > 1.0)) and raw_total > 69:
            discipline_cap_reason = "周期信号仅初步改善或仍未进入破净区，总分封顶为 69"
            raw_total = 69
            discipline_capped = True

    verdict = _pick_verdict(raw_total, scoring_rules)
    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "v2_10dim",
        "dimensions": dimensions,
        "total_score": raw_total,
        "verdict": verdict,
        "ownership": ownership,
        "tier0_capped": tier0_capped,
        "tier0_cap_reason": cap_reason,
        "signal_capped": signal_capped,
        "signal_cap_reason": signal_cap_reason,
        "discipline_capped": discipline_capped,
        "discipline_cap_reason": discipline_cap_reason,
    }


def _obsolete_build_scorecard_v2(
    scan_data: dict,
    source_manifest: dict,
    eco_context: dict,
    tier0_autofill_result: dict,
    *,
    tier0_prep: dict | None = None,
    commodity_data: dict | None = None,
    synthesis_result: dict | None = None,
) -> dict:
    """鳄鱼原教旨左侧 5 维打分表。"""
    autofill = _autofill_map(tier0_autofill_result)
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    price_trigger = assess_price_trigger(commodity_data or {}, eco_context)
    ownership = _resolve_ownership_snapshot(
        str(scan_data.get("company_profile", {}).get("data", {}).get("鑲＄エ浠ｇ爜") or quote.get("浠ｇ爜") or ""),
        scan_data,
        tier0_prep,
        tier0_autofill_result,
    )

    pb = safe_float(valuation.get("pb"))
    consolidation_months = safe_float(kline.get("consolidation_months"))
    market_cap = extract_market_cap(quote)
    market_cap_yi = market_cap / 1e8 if market_cap is not None else None
    eco_circle = eco_context.get("eco_circle", "")
    ps_snapshot = estimate_current_ps(quote, scan_data, tier0_autofill_result, eco_context) if eco_circle == "core_military" else {}
    current_ps = safe_float(ps_snapshot.get("current_ps")) if isinstance(ps_snapshot, dict) else None

    owner_cat = ownership.get("category", "")
    autofill_keys = set(autofill.keys())
    trigger_verdict = normalize_text(price_trigger.get("verdict")).lower()
    top_ratio = safe_float(purity.get("top_ratio")) or 0.0
    consolidation_label = "N/A" if consolidation_months is None else f"{consolidation_months:.0f}"

    d1_score = 15 if owner_cat == "central_soe" else 10 if owner_cat == "provincial_soe" else 0
    if "mineral_rights" in autofill_keys or "license_moat" in autofill_keys or eco_circle == "core_military":
        d1_score += 10

    if eco_circle == "core_military":
        if current_ps is not None and current_ps <= 2.5:
            d2_score = 25
        elif current_ps is not None and current_ps <= 4.0:
            d2_score = 15
        else:
            d2_score = 0
    else:
        if pb is not None and pb <= 0.8:
            d2_score = 15
        elif pb is not None and pb <= 1.0:
            d2_score = 5
        else:
            d2_score = 0
        if trigger_verdict == "pass":
            d2_score += 10

    d3_score = 0
    if market_cap_yi is not None:
        if market_cap_yi < 100:
            d3_score += 10
        elif market_cap_yi < 200:
            d3_score += 5
    if "capacity" in autofill_keys or "capex_investment" in autofill_keys or eco_circle == "rigid_shovel":
        d3_score += 15

    d4_score = 15 if top_ratio >= 0.70 else 0
    if consolidation_months is not None and consolidation_months >= 24:
        d5_score = 10
    elif consolidation_months is not None and consolidation_months >= 12:
        d5_score = 5
    else:
        d5_score = 0

    raw_total = min(100, d1_score + d2_score + d3_score + d4_score + d5_score)
    discipline_capped = False
    discipline_cap_reason = ""
    if eco_circle != "core_military" and ((pb is not None and pb > 1.0) or trigger_verdict != "pass"):
        capped_total = min(raw_total, 84)
        if capped_total != raw_total:
            raw_total = capped_total
            discipline_capped = True
            discipline_cap_reason = "不破净或价格触发未贯通，总分强制压至 85 以下。"

    verdict_label = "🔴 绝佳出击 (启动深度狙击)" if raw_total >= 85 else "🟢 赔率不足 (抛弃不看)"
    dimensions = [
        {
            "label": "1. 生存确定性 (国资+护城河)",
            "weight": 25,
            "raw_score": d1_score,
            "weighted_score": d1_score,
            "reason": f"ownership={owner_cat or 'unknown'} / moat={bool({'mineral_rights', 'license_moat'} & autofill_keys) or eco_circle == 'core_military'}",
        },
        {
            "label": "2. 四维错配与破净 (极寒估值)",
            "weight": 25,
            "raw_score": d2_score,
            "weighted_score": d2_score,
            "reason": f"PB={pb if pb is not None else 'N/A'} / PS={current_ps if current_ps is not None else 'N/A'} / trigger={trigger_verdict or 'N/A'}",
        },
        {
            "label": "3. 宏观利润弹性 (产能杠杆)",
            "weight": 25,
            "raw_score": d3_score,
            "weighted_score": d3_score,
            "reason": f"market_cap_yi={market_cap_yi if market_cap_yi is not None else 'N/A'} / capacity={'capacity' in autofill_keys} / capex={'capex_investment' in autofill_keys}",
        },
        {
            "label": "4. 极简业务纯粹度 (主营>70%)",
            "weight": 15,
            "raw_score": d4_score,
            "weighted_score": d4_score,
            "reason": f"纯度={top_ratio:.1%}",
        },
        {
            "label": "5. 底部长牛皮市 (散户出清)",
            "weight": 10,
            "raw_score": d5_score,
            "weighted_score": d5_score,
            "reason": f"横盘{consolidation_label}月",
        },
    ]

    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "v2_crocodile_5dim",
        "dimensions": dimensions,
        "total_score": raw_total,
        "verdict": {"label": verdict_label, "action": verdict_label},
        "ownership": ownership,
        "tier0_capped": False,
        "tier0_cap_reason": "",
        "signal_capped": False,
        "signal_cap_reason": "",
        "discipline_capped": discipline_capped,
        "discipline_cap_reason": discipline_cap_reason,
    }


def _intermediate_build_scorecard_v2(
    scan_data: dict,
    source_manifest: dict,
    eco_context: dict,
    tier0_autofill_result: dict,
    *,
    tier0_prep: dict | None = None,
    commodity_data: dict | None = None,
    synthesis_result: dict | None = None,
) -> dict:
    """鳄鱼原教旨左侧 5 维打分表 (满分100，彻底删除右侧动量指标)"""
    scoring_rules = load_yaml_config("scoring_rules.yaml")
    autofill = _autofill_map(tier0_autofill_result)
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    price_trigger = assess_price_trigger(commodity_data or {}, eco_context)
    signal_health = evaluate_signal_health_v2(eco_context, source_manifest, commodity_data or {}, {})
    profile_data = scan_data.get("company_profile", {}).get("data", {})
    ownership = _resolve_ownership_snapshot(
        str(
            profile_data.get("股票代码")
            or profile_data.get("鑲＄エ浠ｇ爜")
            or quote.get("代码")
            or quote.get("浠ｇ爜")
            or ""
        ),
        scan_data,
        tier0_prep,
        tier0_autofill_result,
    )

    pb = safe_float(valuation.get("pb"))
    consolidation_months = safe_float(kline.get("consolidation_months"))
    market_cap = extract_market_cap(quote)
    market_cap_yi = market_cap / 1e8 if market_cap is not None else None
    eco_circle = eco_context.get("eco_circle", "")

    # 1. 生存确定性 (满分25): 国资底线 + 核心军工/护城河
    owner_cat = ownership.get("category", "")
    d1_score = 15 if owner_cat == "central_soe" else (10 if owner_cat == "provincial_soe" else 0)
    autofill_keys = set(autofill.keys())
    if "mineral_rights" in autofill_keys or "license_moat" in autofill_keys or eco_circle == "core_military":
        d1_score += 10

    # 2. 四维错配与破净 (满分25): 极寒估值折价
    d2_score = 0
    if eco_circle == "core_military":
        current_ps = safe_float(
            estimate_current_ps(quote, scan_data, tier0_autofill_result, eco_context).get("current_ps")
        )
        if current_ps is not None:
            d2_score = 25 if current_ps <= 2.5 else (15 if current_ps <= 4.0 else 0)
    else:
        if pb is not None:
            d2_score = 15 if pb <= 0.8 else (5 if pb <= 1.0 else 0)
        if normalize_text(price_trigger.get("verdict")).lower() == "pass":
            d2_score += 10

    # 3. 宏观利润弹性巨大 (满分25): 市值推演与产能杠杆
    d3_score = 0
    if market_cap_yi is not None:
        d3_score += 10 if market_cap_yi < 100 else (5 if market_cap_yi < 200 else 0)
    if "capacity" in autofill_keys or "capex_investment" in autofill_keys or eco_circle == "rigid_shovel":
        d3_score += 15

    # 4. 极简纯粹 (满分15): 主营>70%
    top_ratio = purity.get("top_ratio", 0.0)
    d4_score = 15 if top_ratio >= 0.70 else 0

    # 5. 底部长牛皮市 (满分10): 散户出清
    d5_score = 0
    if consolidation_months is not None:
        d5_score = 10 if consolidation_months >= 24 else (5 if consolidation_months >= 12 else 0)

    raw_total = min(100, d1_score + d2_score + d3_score + d4_score + d5_score)

    # 纪律封顶：不破净直接压死在出击线以下
    discipline_capped, discipline_cap_reason = False, ""
    if eco_circle != "core_military" and pb is not None and pb > 1.0:
        raw_total = min(raw_total, 84)
        discipline_capped, discipline_cap_reason = True, "未破净(PB>1.0)，最高物理封顶84分"

    verdict_rules = scoring_rules.get("verdict", [])
    high_verdict = next(
        (item for item in verdict_rules if normalize_text(item.get("range")) == "85-100"),
        {"label": "🔴 绝佳出击 (启动深度狙击)", "action": "满足极高赔率，准备建仓"},
    )
    low_verdict = next(
        (item for item in verdict_rules if normalize_text(item.get("range")) == "0-84"),
        {"label": "🟢 赔率不足 (抛弃不看)", "action": "纪律否决，空仓等待"},
    )
    verdict = high_verdict if raw_total >= 85 else low_verdict
    verdict_label = normalize_text(verdict.get("label"))

    dimensions = [
        {
            "label": "1. 生存确定性",
            "weight": 25,
            "raw_score": d1_score,
            "weighted_score": d1_score,
            "reason": f"国资={owner_cat}",
        },
        {
            "label": "2. 四维错配与破净",
            "weight": 25,
            "raw_score": d2_score,
            "weighted_score": d2_score,
            "reason": "极寒折价得分",
        },
        {
            "label": "3. 宏观利润弹性",
            "weight": 25,
            "raw_score": d3_score,
            "weighted_score": d3_score,
            "reason": f"市值={market_cap_yi}亿",
        },
        {
            "label": "4. 极简纯粹",
            "weight": 15,
            "raw_score": d4_score,
            "weighted_score": d4_score,
            "reason": f"核心占比={top_ratio:.1%}",
        },
        {
            "label": "5. 底部长牛皮市",
            "weight": 10,
            "raw_score": d5_score,
            "weighted_score": d5_score,
            "reason": f"横盘{consolidation_months}月",
        },
    ]

    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "v3_crocodile_5dim",
        "dimensions": dimensions,
        "total_score": raw_total,
        "verdict": {"label": verdict_label, "action": normalize_text(verdict.get("action")) or verdict_label},
        "ownership": ownership,
        "tier0_cap_reason": "",
        "discipline_capped": discipline_capped,
        "discipline_cap_reason": discipline_cap_reason,
        "signal_capped": not signal_health.get("core_ready"),
        "signal_cap_reason": "",
        "tier0_capped": False,
    }


def build_scorecard_v2(
    scan_data: dict, source_manifest: dict, eco_context: dict,
    tier0_autofill_result: dict, *, tier0_prep: dict | None = None,
    commodity_data: dict | None = None, synthesis_result: dict | None = None,
) -> dict:
    """鳄鱼原教旨左侧 5 维打分表 (满分100，彻底删除右侧动量指标)"""
    scoring_rules = load_yaml_config("scoring_rules.yaml")
    autofill = {item.get("field_name"): item for item in tier0_autofill_result.get("items", [])}
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    price_trigger = assess_price_trigger(commodity_data or {}, eco_context)
    signal_health = evaluate_signal_health_v2(eco_context, source_manifest, commodity_data or {}, {})
    ownership = _resolve_ownership_snapshot(
        str(scan_data.get("company_profile", {}).get("data", {}).get("股票代码") or quote.get("代码") or ""),
        scan_data, tier0_prep, tier0_autofill_result
    )

    _ = scoring_rules
    pb = safe_float(valuation.get("pb"))
    consolidation_months = safe_float(kline.get("consolidation_months"))
    market_cap = extract_market_cap(quote)
    market_cap_yi = market_cap / 1e8 if market_cap is not None else None
    eco_circle = eco_context.get("eco_circle", "")

    # 1. 生存确定性 (满分25): 国资底线 + 核心军工/护城河
    owner_cat = ownership.get("category", "")
    d1_score = 15 if owner_cat == "central_soe" else (10 if owner_cat == "provincial_soe" else 0)
    autofill_keys = set(autofill.keys())
    if "mineral_rights" in autofill_keys or "license_moat" in autofill_keys or eco_circle == "core_military":
        d1_score += 10

    # 2. 四维错配与破净 (满分25): 极寒估值折价
    d2_score = 0
    if eco_circle == "core_military":
        current_ps = safe_float(estimate_current_ps(quote, scan_data, tier0_autofill_result, eco_context).get("current_ps"))
        if current_ps is not None:
            d2_score = 25 if current_ps <= 2.5 else (15 if current_ps <= 4.0 else 0)
    else:
        if pb is not None:
            d2_score = 15 if pb <= 0.8 else (5 if pb <= 1.0 else 0)
        if normalize_text(price_trigger.get("verdict")).lower() == "pass":
            d2_score += 10

    # 3. 宏观利润弹性 (满分25): 市值推演与产能杠杆
    d3_score = 0
    if market_cap_yi is not None:
        d3_score += 10 if market_cap_yi < 100 else (5 if market_cap_yi < 200 else 0)
    if "capacity" in autofill_keys or "capex_investment" in autofill_keys or eco_circle == "rigid_shovel":
        d3_score += 15

    # 4. 极简纯粹 (满分15): 主营>70%
    top_ratio = purity.get("top_ratio", 0.0)
    d4_score = 15 if top_ratio >= 0.70 else 0

    # 5. 底部长牛皮市 (满分10): 散户出清
    d5_score = 0
    if consolidation_months is not None:
        d5_score = 10 if consolidation_months >= 24 else (5 if consolidation_months >= 12 else 0)

    raw_total = min(100, d1_score + d2_score + d3_score + d4_score + d5_score)

    # 纪律封顶：不破净直接物理压死在出击线以下
    discipline_capped, discipline_cap_reason = False, ""
    if eco_circle != "core_military" and pb is not None and pb > 1.0:
        raw_total = min(raw_total, 84)
        discipline_capped, discipline_cap_reason = True, "未破净(PB>1.0)，最高物理封顶84分"

    verdict_label = "🔴 绝佳出击" if raw_total >= 85 else "🟢 赔率不足"

    dimensions = [
        {"label": "1. 生存确定性", "weight": 25, "raw_score": d1_score, "weighted_score": d1_score, "reason": f"国资={owner_cat}"},
        {"label": "2. 四维错配与破净", "weight": 25, "raw_score": d2_score, "weighted_score": d2_score, "reason": "极寒折价"},
        {"label": "3. 宏观利润弹性", "weight": 25, "raw_score": d3_score, "weighted_score": d3_score, "reason": f"市值={market_cap_yi}亿"},
        {"label": "4. 极简纯粹", "weight": 15, "raw_score": d4_score, "weighted_score": d4_score, "reason": f"核心占比={top_ratio:.1%}"},
        {"label": "5. 底部长牛皮市", "weight": 10, "raw_score": d5_score, "weighted_score": d5_score, "reason": f"横盘{consolidation_months}月"},
    ]

    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "version": "v3_crocodile_5dim",
        "dimensions": dimensions, "total_score": raw_total, "verdict": {"label": verdict_label, "action": verdict_label},
        "ownership": ownership, "discipline_capped": discipline_capped, "discipline_cap_reason": discipline_cap_reason,
        "signal_capped": not signal_health.get("core_ready"), "tier0_capped": False,
        "tier0_cap_reason": "", "signal_cap_reason": "核心信号未就绪" if not signal_health.get("core_ready") else ""
    }


def build_cycle_valuation(
    stock_code: str,
    current_price: float | None,
    quote: dict,
    commodity_data: dict,
    tier0_autofill_result: dict,
    scan_data: dict,
    eco_context: dict,
) -> dict:
    return build_valuation_case(
        stock_code,
        current_price,
        quote,
        commodity_data,
        tier0_autofill_result,
        scan_data,
        eco_context,
    )


def _render_data_health_section(scan_data: dict, commodity_data: dict, macro_data: dict, source_manifest: dict) -> list[str]:
    """Generate data health summary table + stale warnings."""
    lines = [
        "## 零、数据健康度",
        "",
    ]

    # Count statuses across all sources
    sources = [
        ("股票基本面 (akshare)", scan_data),
        ("大宗商品", commodity_data),
        ("宏观数据", macro_data),
    ]
    lines.extend([
        "| 数据源 | OK | Stale | Error | Manual | 健康度 |",
        "|---|---|---|---|---|---|",
    ])
    for source_name, dataset in sources:
        ok = stale = error = manual = 0
        for k, v in dataset.items():
            if k.startswith("_") or not isinstance(v, dict):
                continue
            s = str(v.get("status", "")).lower()
            if s.startswith("ok"):
                ok += 1
            elif "stale" in s:
                stale += 1
            elif "manual" in s or "partial" in s:
                manual += 1
            elif s.startswith("error"):
                error += 1
        total = ok + stale + error + manual
        health = f"{ok}/{total}" if total else "N/A"
        lines.append(f"| {source_name} | {ok} | {stale} | {error} | {manual} | {health} |")

    # Stale warnings
    stale_fields = source_manifest.get("summary", {}).get("stale_fields", [])
    if stale_fields:
        lines.extend([
            "",
            "> **⚠️ 数据新鲜度警告**: 以下字段使用了过期缓存（超过 24 小时），报告结论可能不准确：",
            "> " + ", ".join(f"`{f}`" for f in stale_fields),
        ])

    # Freshness check on scan_data itself
    freshness = detect_data_freshness(scan_data)
    if freshness.get("stale_count", 0) > 0 and not stale_fields:
        lines.extend([
            "",
            f"> **⚠️ akshare 数据新鲜度**: {freshness['stale_count']} 个字段超期: {', '.join(freshness['stale_fields'])}",
        ])

    lines.append("")
    return lines


def _render_human_action_checklist(actions: list[dict]) -> list[str]:
    """Render human-action-needed items as a table."""
    if not actions:
        return ["无需人工补充。", ""]
    priority_emoji = {"red": "🔴", "yellow": "🟡", "green": "🟢"}
    lines = [
        "| 优先级 | 来源 | 字段 | 所需操作 |",
        "|---|---|---|---|",
    ]
    for a in sorted(actions, key=lambda x: (0 if x.get("priority") == "red" else 1 if x.get("priority") == "yellow" else 2)):
        emoji = priority_emoji.get(a.get("priority", ""), "❓")
        lines.append(f"| {emoji} {a.get('priority', 'N/A')} | {a.get('source', '')} | {a.get('field', '')} | {a.get('action', '')} |")
    lines.append("")
    return lines


def _has_hard_stop(gate_result: dict) -> bool:
    hard_rules = {"0. 生态归属", "0. 极限破净闸门", "1. 国资底线", "3. 主营纯粹度", "4. 业务极简"}
    checks = list(gate_result.get("prechecks") or []) + list(gate_result.get("principle_checks") or [])
    if not checks:
        checks = gate_result.get("checks", [])
    for check in checks:
        if check.get("rule") in hard_rules and _gate_status_label(check) == "不通过":
            return True
    return False


def _build_investment_judgment(
    *,
    gate_result: dict,
    scorecard: dict,
    price_trigger: dict,
    valuation: dict,
    kline: dict,
    suggested_build_price: float | None,
    current_price: float | None,
    eco_context: dict | None = None,
) -> dict:
    eco_context = eco_context or {}
    eco_circle = normalize_text(eco_context.get("eco_circle"))
    valuation_cfg = get_crocodile_mode_config(eco_context.get("four_signal_mode")).get("valuation", {}) or {}
    foam_alert = valuation.get("foam_alert", {}) if isinstance(valuation.get("foam_alert"), dict) else {}
    current_ps = safe_float(valuation.get("current_ps"))
    current_vs_high = safe_float(kline.get("current_vs_5yr_high")) or safe_float(kline.get("current_vs_high"))
    total_score = int(scorecard.get("total_score", 0) or 0)

    prechecks = gate_result.get("prechecks") or [
        item for item in gate_result.get("checks", []) if item.get("rule", "").startswith("0.")
    ]
    principle_checks = gate_result.get("principle_checks") or [
        item for item in gate_result.get("checks", []) if item.get("rule", "").split(".", 1)[0] in {str(i) for i in range(1, 9)}
    ]
    pending_or_failed = [check for check in [*prechecks, *principle_checks] if _gate_status_label(check) != "通过"]
    key_blocker = pending_or_failed[0]["reason"] if pending_or_failed else "无明显阻断项"
    hard_stop = _has_hard_stop(gate_result)

    if hard_stop:
        return {
            "zone": "红色否决区",
            "action": "纪律否决",
            "summary": "硬纪律未通过，当前不进入估值与仓位讨论。",
            "key_blocker": key_blocker,
            "entry_condition": "先修复硬纪律问题，再重新评估。",
            "hard_stop": True,
        }

    if foam_alert.get("triggered"):
        foam_reasons = foam_alert.get("reasons", []) if isinstance(foam_alert.get("reasons"), list) else []
        foam_reason = " / ".join(str(item) for item in foam_reasons if item) or "触发泡沫警报"
        return {
            "zone": "红色否决区",
            "action": normalize_text(foam_alert.get("action")) or "触发泡沫警报",
            "summary": f"{foam_reason}，情绪与估值已进入极端区间。",
            "key_blocker": foam_reason,
            "entry_condition": "等待泡沫出清后，再重新评估建仓条件。",
            "hard_stop": True,
        }

    if eco_circle == "core_military":
        pass_ps = safe_float(valuation_cfg.get("entry_ps_pass")) or 2.5
        caution_ps = safe_float(valuation_cfg.get("entry_ps_caution")) or 4.0
        hard_ps = caution_ps * 1.25

        if current_ps is None:
            return {
                "zone": "绿色休息区",
                "action": "等待PS代理收入补证",
                "summary": "军工路径缺少PS代理收入或订单代理口径，暂时无法评估战略溢价。",
                "key_blocker": key_blocker if key_blocker != "无明显阻断项" else "缺少 sales proxy revenue",
                "entry_condition": "补齐代理收入/订单口径后，重新评估PS是否回落到2.5以下。",
                "hard_stop": False,
            }

        if current_ps > hard_ps or (current_vs_high is not None and current_vs_high >= 70):
            reasons = []
            if current_ps > hard_ps:
                reasons.append(f"PS={current_ps:.2f}>{hard_ps:.2f}")
            if current_vs_high is not None and current_vs_high >= 70:
                reasons.append(f"距5年高点仅回撤{100 - current_vs_high:.1f}%")
            return {
                "zone": "红色否决区",
                "action": "军工估值仍偏贵",
                "summary": " / ".join(reasons) + "，军工PS与位置尚未回到左侧极低区。",
                "key_blocker": key_blocker if key_blocker != "无明显阻断项" else "军工估值仍偏热",
                "entry_condition": f"等待PS回落到{pass_ps:.2f}以下，且股价重新进入低位回撤区。",
                "hard_stop": True,
            }

        price_gap = None
        if suggested_build_price not in (None, 0) and current_price not in (None, 0):
            price_gap = (current_price / suggested_build_price - 1.0) * 100

        if total_score >= 85 and current_ps <= pass_ps and (current_vs_high is None or current_vs_high <= 55):
            return {
                "zone": "红色狩猎区",
                "action": "满足出击条件",
                "summary": "军工PS已回到极低区且评分达标，进入优先狙击名单。",
                "key_blocker": "无",
                "entry_condition": "沿建仓价附近分批布局，并持续跟踪订单、资产注入与估值扩张节奏。",
                "hard_stop": False,
            }

        if total_score >= 55 and current_ps <= caution_ps:
            trigger_text = f"等待PS继续回落至{pass_ps:.2f}以下"
            if price_gap is not None and price_gap > 0:
                trigger_text = f"等待回落至建仓价附近（高于目标价约 {price_gap:.1f}%）"
            return {
                "zone": "黄色观察区",
                "action": "等待PS继续回落",
                "summary": "军工PS已回到观察区，但尚未进入极低区。",
                "key_blocker": key_blocker,
                "entry_condition": trigger_text,
                "hard_stop": False,
            }

        return {
            "zone": "绿色休息区",
            "action": "继续等待",
            "summary": "军工PS仍偏高，继续等待更深回撤与估值压缩。",
            "key_blocker": key_blocker,
            "entry_condition": f"等待军工PS<={caution_ps:.2f}，理想状态是进一步回到{pass_ps:.2f}以下。",
            "hard_stop": False,
        }

    pb_value = safe_float(valuation.get("current_pb")) or safe_float(valuation.get("pb"))
    bvps = safe_float(valuation.get("bvps"))
    trigger_verdict = normalize_text(price_trigger.get("verdict")).lower()

    if (pb_value is not None and pb_value > 1.0) or (current_vs_high is not None and current_vs_high >= 70):
        reason_parts = []
        if pb_value is not None and pb_value > 1.0:
            reason_parts.append(f"PB={pb_value:.2f}>1.0")
        if current_vs_high is not None and current_vs_high >= 70:
            reason_parts.append(f"距5年高点仅回撤{100 - current_vs_high:.1f}%")
        return {
            "zone": "红色否决区",
            "action": "铁律否决",
            "summary": " / ".join(reason_parts) + "，尚未进入破净深折区，不进入左侧建仓讨论。",
            "key_blocker": key_blocker if key_blocker != "无明显阻断项" else "未进入深折破净区",
            "entry_condition": "等待PB回到1.0以下，最好接近0.8，并观察商品周期反转确认。",
            "hard_stop": True,
        }

    if total_score >= 85 and trigger_verdict == "pass" and (pb_value is None or pb_value <= 0.8):
        return {
            "zone": "红色狩猎区",
            "action": "满足出击条件",
            "summary": "纪律与时机同时满足，进入可执行区。",
            "key_blocker": "无",
            "entry_condition": "按建仓价和仓位纪律执行。",
            "hard_stop": False,
        }

    price_gap = None
    if suggested_build_price not in (None, 0) and current_price not in (None, 0):
        price_gap = (current_price / suggested_build_price - 1.0) * 100

    if total_score >= 55 and (pb_value is None or pb_value <= 1.0):
        trigger_text = "等待价格/景气进一步确认" if trigger_verdict != "pass" else "等待回到更优估值区"
        if price_gap is not None and price_gap > 0:
            trigger_text = f"等待回落至建仓价附近（高于目标价约 {price_gap:.1f}%）"
        return {
            "zone": "黄色观察区",
            "action": "等待触发",
            "summary": "估值已接近纪律线，但尚未进入最优左侧出击区。",
            "key_blocker": key_blocker,
            "entry_condition": trigger_text,
            "hard_stop": False,
        }

    reentry_condition = "等待PB<0.8且商品周期反转信号。"
    if bvps not in (None, 0):
        reentry_condition = f"等待股价重新接近{bvps * 0.8:.2f}元（对应PB≈0.8）且商品周期反转。"
    return {
        "zone": "绿色休息区",
        "action": "继续等待",
        "summary": "当前估值或基本面不满足建仓条件。",
        "key_blocker": key_blocker,
        "entry_condition": reentry_condition,
        "hard_stop": False,
    }


def generate_report(
    stock_code: str,
    company_name: str,
    *,
    scan_data: dict,
    gate_result: dict,
    source_manifest: dict,
    eco_context: dict,
    commodity_data: dict,
    macro_data: dict,
    tier0_autofill_result: dict,
    tier0_verification_result: dict,
    annual_report_result: dict,
    execution_log: dict,
    report_dir: str,
    processed_dir: str,
    evidence_dir: str,
    human_required_actions: list[dict] | None = None,
) -> dict:
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
    balance = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    autofill = _autofill_map(tier0_autofill_result)
    verification = tier0_verification_result or {}
    ownership = _resolve_ownership_snapshot(
        str(scan_data.get("company_profile", {}).get("data", {}).get("股票代码") or quote.get("代码") or ""),
        scan_data,
        verification.get("updated_tier0_prep", {}),
        tier0_autofill_result,
    )

    current_price = safe_float(quote.get("最新价")) or safe_float(valuation.get("latest_close")) or safe_float(kline.get("latest_close"))
    current_price = _extract_latest_quote_price(quote) or current_price
    bvps = safe_float(valuation.get("bvps"))
    defensive_anchor = bvps if bvps is not None else None
    deep_value_anchor = bvps * 0.8 if bvps is not None else None
    value_ratio = (current_price / bvps) if current_price and bvps else None

    spot_summary = _latest_spot_summary(commodity_data)
    inventory_summary = _inventory_summary(commodity_data)
    macro_fai = _latest_macro_record(macro_data.get("fixed_asset_investment", {}))
    ppi_latest = _latest_macro_record(macro_data.get("ppi", {}))
    industrial_latest = _latest_macro_record(macro_data.get("industrial_value_added", {}))
    signal_health = gate_result.get("signal_health") or evaluate_signal_health_v2(eco_context, source_manifest, commodity_data, macro_data)
    cycle_valuation = build_cycle_valuation(
        stock_code, current_price, quote, commodity_data,
        tier0_autofill_result, scan_data, eco_context,
    )

    target_metric = normalize_text(cycle_valuation.get("target_metric")) or "PE"
    exit_prices = resolve_exit_prices(cycle_valuation)
    sell_key = normalize_text(exit_prices.get("sell_key")) or normalize_text(cycle_valuation.get("sell_key")) or "N/A"
    optimistic_key = normalize_text(exit_prices.get("optimistic_key")) or normalize_text(cycle_valuation.get("optimistic_key")) or "N/A"
    bubble_key = normalize_text(exit_prices.get("bubble_key")) or normalize_text(cycle_valuation.get("bubble_key")) or "N/A"
    conservative_sell_price = safe_float(exit_prices.get("conservative_sell_price"))
    optimistic_sell_price = safe_float(exit_prices.get("optimistic_sell_price"))
    bubble_warning_price = safe_float(exit_prices.get("bubble_warning_price"))
    if eco_context.get("eco_circle") == "core_military":
        current_ps = safe_float(cycle_valuation.get("current_ps"))
        entry_ps_pass = safe_float(cycle_valuation.get("ps_policy", {}).get("entry_ps_pass")) or 2.5
        if current_price not in (None, 0) and current_ps not in (None, 0):
            suggested_build_price = current_price * min(1.0, entry_ps_pass / current_ps)
        else:
            suggested_build_price = current_price
    else:
        suggested_build_price = deep_value_anchor or defensive_anchor or current_price
        if suggested_build_price is not None and current_price is not None and current_price < suggested_build_price:
            suggested_build_price = current_price
    price_trigger = assess_price_trigger(commodity_data, eco_context)

    pe_policy = cycle_valuation.get("pe_policy", {}) or {}
    sell_key = normalize_text(exit_prices.get("sell_key")) or f"{int(pe_policy.get('sell_pe', 15))}x"
    optimistic_key = normalize_text(exit_prices.get("optimistic_key")) or f"{int(pe_policy.get('optimistic_pe', 20))}x"
    bubble_key = normalize_text(exit_prices.get("bubble_key")) or f"{int(pe_policy.get('bubble_pe', 30))}x"
    conservative_sell_price = safe_float(exit_prices.get("conservative_sell_price"))
    optimistic_sell_price = safe_float(exit_prices.get("optimistic_sell_price"))
    pe_bubble_price = safe_float(exit_prices.get("bubble_warning_price"))

    # ── PB-based sell anchors (PRIMARY for cyclicals) ──
    pb_bubble_price = bvps * 3.0 if bvps else None

    # Take the LOWER of PB-anchor and PE-anchor (conservative discipline)
    def _min_valid(a, b):
        if a is None:
            return b
        if b is None:
            return a
        return min(a, b)

    bubble_warning_price = _min_valid(pb_bubble_price, pe_bubble_price)


    expected_return = None
    if suggested_build_price not in (None, 0) and conservative_sell_price is not None:
        expected_return = (conservative_sell_price / suggested_build_price - 1) * 100

    # Build old v1 scorecard (still needed for gate judgment backward compat)
    scorecard_v1 = build_scorecard(
        scan_data, source_manifest, eco_context, tier0_autofill_result,
        tier0_prep=verification.get("updated_tier0_prep", {}),
        commodity_data=commodity_data,
    )

    # Build synthesis — the missing "thinking layer"
    synthesis = build_synthesis(
        stock_code, company_name, scan_data, commodity_data, macro_data,
        eco_context, gate_result, tier0_autofill_result, scorecard_v1, cycle_valuation,
    )

    # Build new v2 scorecard with synthesis data
    scorecard = build_scorecard_v2(
        scan_data, source_manifest, eco_context, tier0_autofill_result,
        tier0_prep=verification.get("updated_tier0_prep", {}),
        commodity_data=commodity_data,
        synthesis_result=synthesis,
    )

    valuation_snapshot = dict(valuation)
    valuation_snapshot.update(
        {
            "current_pb": cycle_valuation.get("current_pb"),
            "current_pe": cycle_valuation.get("current_pe"),
            "current_ps": cycle_valuation.get("current_ps"),
            "foam_alert": cycle_valuation.get("foam_alert", {}),
        }
    )

    judgment = _build_investment_judgment(
        gate_result=gate_result, scorecard=scorecard, price_trigger=price_trigger,
        valuation=valuation_snapshot, kline=kline, suggested_build_price=suggested_build_price,
        current_price=current_price,
        eco_context=eco_context,
    )

    industry_mapping = load_industry_mapping()
    override_cfg = industry_mapping.get("company_overrides", {}).get(stock_code, {})
    cost_items = override_cfg.get("cost_items", [])
    if eco_context.get("eco_circle") == "core_resource":
        business_formula = f"{eco_context.get('commodity', '产品')}售价 - {' / '.join(cost_items) if cost_items else '刚性成本'} = 周期利润"
    elif eco_context.get("eco_circle") == "rigid_shovel":
        business_formula = "订单/服务费 - 固定制造成本 = 利润弹性"
    elif eco_context.get("eco_circle") == "core_military":
        business_formula = "核心配套单价 - 固定研发/制造成本 = 利润释放"
    else:
        business_formula = "生态位未明，暂无法写出一句话利润公式"

    report_path = os.path.join(report_dir, f"{company_name}_{stock_code}_深度狙击报告.md")
    score_path = os.path.join(processed_dir, "scorecard.json")
    with open(score_path, "w", encoding="utf-8") as f:
        json.dump(scorecard, f, ensure_ascii=False, indent=2, default=str)

    tier0_missing = source_manifest.get("summary", {}).get("tier0_required_missing", [])
    is_killed = judgment["hard_stop"]

    # ══════════════════════════════════════════════════════════════════
    # REPORT RENDERING — 6-chapter structure per reference guide
    # ══════════════════════════════════════════════════════════════════
    lines = [
        f"# {company_name}（{stock_code}）深度狙击报告",
        "",
        f"> 生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 终端版本：v3.0",
        f"> 生态位：{eco_context.get('eco_circle', 'unknown')} / {eco_context.get('commodity', 'N/A')}",
        f"> 最高信仰：绝不炒股！股票=免仓储费凭证",
        "",
        "---",
        "",
    ]
    if execution_log.get("final_verdict") == "KILLED_AT_GATE" or is_killed:
        lines.extend(
            [
                "> [!WARNING]",
                f"> 当前标的已被纪律否决：{judgment.get('summary', '硬纪律未通过')}",
                f"> 阻断原因：{judgment.get('key_blocker', '无')}",
                "> 仅保留本报告供人工复核，不构成建仓讨论。",
                "",
            ]
        )

    # ━━ Chapter 1: 投资概要与核心结论 ━━
    synth_vg = synthesis.get("valuation_gap", {})
    lines.extend([
        "## 第一章：投资概要与核心结论",
        "",
        "### 核心判断（定价错误在哪）",
        "",
        f"> {synthesis.get('core_mispricing', '数据不足，无法产出核心判断')}",
        "",
        "### 核心逻辑",
        "",
    ])
    for i, logic in enumerate(synthesis.get("core_logic", []), 1):
        lines.append(f"{i}. {logic}")
    lines.append("")

    if is_killed:
        lines.extend([
            "### 估值区间与目标空间",
            "",
            f"> 🔴 **因核心铁律不通过（{judgment.get('key_blocker', '')}），估值模型失效，不予展示。**",
            f"> 若未来股价跌至破净以下（<{_fmt_price(bvps)}）且商品周期反转，可重新评估。",
            "",
            "### 一句话买入本质",
            "",
            f"> **{synthesis.get('one_liner_thesis', '数据不足')}**",
            "",
            f"**当前区间**：{judgment['zone']} / {judgment['action']}",
            "",
            "---",
            "",
        ])
    else:
        lines.extend([
            "### 估值区间与目标空间",
            "",
            "| 项目 | 数值 |",
            "|---|---|",
            f"| 当前市值 | {_fmt_yi(extract_market_cap(quote))} |",
            f"| 建议建仓价 | {_fmt_price(suggested_build_price)} |",
            f"| Conservative Exit ({target_metric} {sell_key.upper()}) | {_fmt_price(conservative_sell_price)} |",
            f"| Optimistic Exit ({target_metric} {optimistic_key.upper()}) | {_fmt_price(optimistic_sell_price)} |",
            f"| Foam Alert ({target_metric} {bubble_key.upper()}) | {_fmt_price(bubble_warning_price)} |",
            f"| 预期获利 | {_fmt_pct(expected_return)} |",
            "",
            "### 一句话买入本质",
            "",
            f"> **{synthesis.get('one_liner_thesis', '数据不足')}**",
            "",
            f"**赔率来源**：{synthesis.get('odds_source', '待补充')}",
            "",
            f"**当前区间**：{judgment['zone']} / {judgment['action']}",
            "",
            "---",
            "",
        ])

    # ━━ Chapter 2: 公司基本盘 ━━
    prechecks = gate_result.get("prechecks") or [
        item for item in gate_result.get("checks", []) if item.get("rule", "").startswith("0.")
    ]
    principle_checks = gate_result.get("principle_checks") or [
        item for item in gate_result.get("checks", []) if item.get("rule", "").split(".", 1)[0] in {str(i) for i in range(1, 9)}
    ]
    ps = gate_result.get("principle_summary") or {
        "total_rules": len(principle_checks),
        "pass_count": sum(1 for c in principle_checks if _gate_status_label(c) == "通过"),
        "fail_count": sum(1 for c in principle_checks if _gate_status_label(c) == "不通过"),
        "pending_count": sum(1 for c in principle_checks if _gate_status_label(c) == "待补证"),
    }
    lines.extend([
        "## 第二章：公司基本盘（八大原则）",
        "",
        f"> 通过 {ps.get('pass_count',0)}/{ps.get('total_rules',0)} | "
        f"不通过 {ps.get('fail_count',0)} | 待补证 {ps.get('pending_count',0)}",
        "",
        "| 原则 | 状态 | 说明 |",
        "|---|---|---|",
    ])
    for check in (prechecks + principle_checks):
        lines.append(f"| {check.get('rule','')} | {_gate_status_label(check)} | {check.get('reason','')} |")
    if not prechecks and not principle_checks:
        lines.append("| - | - | - |")
    lines.extend([
        "",
        f"**主营纯度**：{purity.get('top_segment', 'N/A')} / {purity.get('top_ratio', 0):.1%}",
        f"**业务公式**：{business_formula}",
        "",
        "---",
        "",
    ])

    # ━━ Chapter 3: 核心变量与估值 ━━
    lines.extend([
        "## 第三章：核心变量分析与估值模型",
        "",
        "### 三要素（成本 × 数量 × 卖价）",
        "",
        "| 要素 | 数值 | 来源 |",
        "|---|---|---|",
        f"| 保守高位价 | {_fmt_price(cycle_valuation.get('high_price'))} | {cycle_valuation.get('high_price_basis', 'N/A')} |",
        f"| 刚性成本 | {_fmt_price(cycle_valuation.get('unit_cost'))} | {cycle_valuation.get('unit_cost_basis', 'N/A')} |",
        f"| 上行成本(+{_fmt_pct(cycle_valuation.get('cost_stress_pct'))}) | {_fmt_price(cycle_valuation.get('stressed_unit_cost'))} | 压力测试 |",
        f"| {cycle_valuation.get('quantity_label', '产能')} | {_fmt_num(cycle_valuation.get('quantity_value'))} | {cycle_valuation.get('quantity_basis', 'N/A')} |",
        "",
    ])
    if not is_killed:
        lines.extend([
            "### 目标市值推演",
            "",
            "| 场景 | 市值 | 目标价 |",
            "|---|---|---|",
            f"| 保守({sell_key.upper()}) | {_fmt_yi(cycle_valuation.get('target_market_caps', {}).get(sell_key))} | {_fmt_price(cycle_valuation.get('target_prices', {}).get(sell_key))} |",
            f"| 乐观({optimistic_key.upper()}) | {_fmt_yi(cycle_valuation.get('target_market_caps', {}).get(optimistic_key))} | {_fmt_price(cycle_valuation.get('target_prices', {}).get(optimistic_key))} |",
            f"| 泡沫({bubble_key.upper()}) | {_fmt_yi(cycle_valuation.get('target_market_caps', {}).get(bubble_key))} | {_fmt_price(cycle_valuation.get('target_prices', {}).get(bubble_key))} |",
            "",
            f"> 峰值利润: {_fmt_yi(cycle_valuation.get('peak_profit'))}",
            "",
        ])
    else:
        lines.extend([
            "> 🔴 硬纪律未通过，估值推演仅供参考。",
            "",
        ])

    lines.extend([
        "### 估值水位",
        "",
        "| 指标 | 数值 |",
        "|---|---|",
        f"| 当前股价 | {_fmt_price(current_price)} |",
        f"| BVPS | {_fmt_price(bvps)} |",
        f"| PB | {_fmt_num(valuation.get('pb'), 3)} |",
        f"| PB 分位 | {_fmt_pct(valuation.get('pb_percentile'))} |",
        f"| 总市值 | {_fmt_yi(extract_market_cap(quote))} |",
        f"| 0.5元买1元比率 | {_fmt_num(value_ratio, 3)} |",
        "",
        "---",
        "",
    ])

    # ━━ Chapter 4: 周期性分析 ━━
    if target_metric == "PS" and not is_killed:
        lines.extend(
            [
                "### PS Skeleton",
                "",
                f"- sales_proxy_revenue: {_fmt_yi(cycle_valuation.get('sales_proxy_revenue'))}",
                f"- current_ps: {_fmt_num(cycle_valuation.get('current_ps'), 3)}",
                f"- matched_product: {_compact_text(cycle_valuation.get('matched_product'))}",
                f"- proxy_report_date: {cycle_valuation.get('proxy_report_date', 'N/A')}",
                "",
            ]
        )
    tl = synthesis.get("time_lag_analysis", {})
    lines.extend([
        "## 第四章：周期性分析",
        "",
        "### 四维信号",
        "",
        "| 维度 | 数据 | 状态 |",
        "|---|---|---|",
        f"| 现货/期货 | {spot_summary.get('latest_date','')} 收盘 {spot_summary.get('latest_price','N/A')} | {spot_summary.get('status','N/A')} |",
        f"| 库存 | {inventory_summary.get('headline', 'N/A')} | {inventory_summary.get('status', 'N/A')} |",
        f"| 固投 | {json.dumps(macro_fai, ensure_ascii=False, default=str)[:80]} | {macro_data.get('fixed_asset_investment', {}).get('status', 'N/A')} |",
        f"| PPI | {json.dumps(ppi_latest, ensure_ascii=False, default=str)[:80]} | {macro_data.get('ppi', {}).get('status', 'N/A')} |",
        "",
        f"> 核心信号就绪: {'是' if signal_health.get('core_ready') else '否'} | 缺口: {', '.join(signal_health.get('core_missing', [])) or '无'}",
        "",
        "### 时滞错配",
        "",
        f"- **上游**：{tl.get('upstream_signal', 'N/A')}",
        f"- **下游传导**：{tl.get('downstream_response', 'N/A')}",
        f"- **股价vs基本面**：{tl.get('stock_vs_fundamental', 'N/A')}",
        "",
        "---",
        "",
    ])

    # ━━ Chapter 5: 风险提示 ━━
    lines.extend([
        "## 第五章：风险提示与反证条件",
        "",
    ])
    if is_killed:
        lines.append(f"> 🔴 硬纪律否决：{judgment.get('key_blocker', '')}")
        lines.append("")
    lines.append("**投资逻辑证伪条件**（任一触发则重新评估）：")
    lines.append("")
    for i, cond in enumerate(synthesis.get("falsification_conditions", []), 1):
        lines.append(f"{i}. {cond}")
    lines.extend(["", "---", ""])

    # ━━ Chapter 6: 操作策略与评分 ━━
    lines.extend([
        "## 第六章：操作策略与评分",
        "",
        "### 10维评分表",
        "",
        "| 维度 | 权重 | 分(10分制) | 加权 | 说明 |",
        "|---|---|---|---|---|",
    ])
    for item in scorecard["dimensions"]:
        lines.append(
            f"| {item['label']} | {item['weight']}% | {item['raw_score']} | {item['weighted_score']} | {item['reason']} |"
        )
    lines.append(f"| **总分** | **100%** | | **{scorecard['total_score']}** | **{scorecard['verdict']['label']}** |")
    if scorecard.get("tier0_capped"):
        lines.append(f"\n> ⚠️ {scorecard['tier0_cap_reason']}")
    if scorecard.get("signal_capped"):
        lines.append(f"\n> ⚠️ {scorecard['signal_cap_reason']}")
    if scorecard.get("discipline_capped"):
        lines.append(f"\n> 🔴 **铁律封顶：{scorecard['discipline_cap_reason']}**")
    lines.extend([
        "",
        "> ≥90 绝佳 | 80-89 稀有 | 70-79 合理 | <70 等待",
        "",
        "### 综合研判",
        "",
        f"- **区间**：{judgment['zone']} / {judgment['action']}",
        f"- **判断**：{judgment['summary']}",
        f"- **阻断**：{judgment['key_blocker']}",
        f"- **触发**：{judgment['entry_condition']}",
        "",
        "### 操作建议",
        "",
        "| 项目 | 建议 |",
        "|---|---|",
        f"| 建仓价 | {_fmt_price(suggested_build_price)} |",
        f"| 重仓价 | {_fmt_price(deep_value_anchor)} |",
        f"| 卖出价 | {_fmt_price(conservative_sell_price)} ~ {_fmt_price(optimistic_sell_price)} |",
        f"| 预期获利 | {_fmt_pct(expected_return)} |",
        f"| 持有周期 | 3-5年 |",
        "",
        "---",
        "",
    ])

    # ━━ Appendix ━━
    lines.extend(["## 附录：数据健康度与审计", ""])
    lines.extend(_render_data_health_section(scan_data, commodity_data, macro_data, source_manifest))
    human_actions = human_required_actions or []
    if human_actions:
        lines.extend([f"### 人工补充清单（{len(human_actions)} 项）", ""])
        lines.extend(_render_human_action_checklist(human_actions))
    # ━━ Evidence Citations ━━
    lines.extend(["### 证据引用索引", ""])
    lines.extend([
        "以下表格列出报告中每项关键数据的具体来源与原文摘录，供审阅核实：",
        "",
        "| 数据项 | 来源层级 | 来源文件/字段 | 原文摘录 |",
        "|--------|---------|--------------|---------|",
    ])
    # -- Price / Quote citations --
    q = scan_data.get("realtime_quote", {})
    q_data = q.get("data", {})
    q_status = q.get("status", "N/A")
    lines.append(
        f"| 当前股价 {_fmt_price(current_price)} | Tier 1 (akshare) | "
        f"`akshare_scan.json → realtime_quote` [{q_status}] | "
        f"`最新价={q_data.get('最新价', 'N/A')}` `总市值={q_data.get('总市值', 'N/A')}` |"
    )
    # -- Valuation citations --
    v = scan_data.get("valuation_history", {})
    v_data = v.get("data", {})
    lines.append(
        f"| PB={_fmt_num(safe_float(valuation.get('pb')))} | Tier 1 (akshare) | "
        f"`akshare_scan.json → valuation_history` [{v.get('status', 'N/A')}] | "
        f"`pb={v_data.get('pb', 'N/A')}` `bvps={v_data.get('bvps', 'N/A')}` `pb_percentile={v_data.get('pb_percentile', 'N/A')}` |"
    )
    revenue_manifest = get_manifest_field_entry(source_manifest, "revenue_breakdown")
    valuation_manifest = get_manifest_field_entry(source_manifest, "pb_ratio")
    ownership_manifest = get_manifest_field_entry(source_manifest, "actual_controller")
    spot_manifest = get_manifest_field_entry(source_manifest, "spot_price")
    cost_manifest = get_manifest_field_entry(source_manifest, "cost_structure")

    # -- Revenue / purity citations --
    top_ratio = safe_float(purity.get("top_ratio"))
    purity_detail = (
        f"`报告期={purity.get('latest_report_date') or 'N/A'}` "
        f"`分项={purity.get('top_segment') or 'N/A'}` "
        f"`占比={f'{top_ratio:.1%}' if top_ratio is not None else 'N/A'}`"
    )
    if revenue_manifest:
        manifest_detail = _manifest_detail(revenue_manifest, fallback="")
        if manifest_detail and manifest_detail != "N/A":
            purity_detail = f"{purity_detail} {manifest_detail}"
    lines.append(
        f"| 主营纯度 {f'{top_ratio:.1%}' if top_ratio is not None else 'N/A'} | "
        f"{_manifest_tier_label(revenue_manifest, 'Tier 1')} | "
        f"{_manifest_source_label(revenue_manifest) if revenue_manifest else '`akshare_scan.json → revenue_breakdown`'} | "
        f"{purity_detail} |"
    )
    # -- Income / profit citations --
    inc = scan_data.get("income_statement", {})
    lines.append(
        f"| 净利润 {_fmt_yi(income.get('net_profit'))} | Tier 1 (akshare) | "
        f"`akshare_scan.json → income_statement` [{inc.get('status', 'N/A')}] | "
        f"`报告期={income.get('report_date', 'N/A')}` `归母净利润={income.get('net_profit', 'N/A')}` |"
    )
    # -- BVPS citations --
    lines.append(
        f"| BVPS={_fmt_price(bvps)} | "
        f"{_manifest_tier_label(valuation_manifest, 'Tier 1')} | "
        f"{_manifest_source_label(valuation_manifest) if valuation_manifest else '`akshare_scan.json → valuation_history`'} | "
        f"`报告期={balance.get('report_date', 'N/A')}` `每股净资产={valuation.get('bvps', 'N/A')}` "
        f"`PB={valuation.get('pb', 'N/A')}` |"
    )
    # -- Ownership / Tier 0 citations --
    owner_value = ownership.get("label") or ownership.get("category") or "N/A"
    owner_evidence = f"`归类={owner_value}` {_manifest_detail(ownership_manifest)}"
    lines.append(
        f"| 国资属性 | {_manifest_tier_label(ownership_manifest, 'Tier 0/1')} | "
        f"{_manifest_source_label(ownership_manifest) if ownership_manifest else '`source_manifest.json → actual_controller`'} | "
        f"{owner_evidence or 'N/A'} |"
    )
    # -- Commodity / spot price citations --
    spot_source_label = _manifest_source_label(spot_manifest) if spot_manifest else "`commodity_scan.json → spot_price`"
    spot_detail = (
        f"`latest={spot_summary.get('latest_price', 'N/A')}` "
        f"`date={spot_summary.get('latest_date', 'N/A')}` "
        f"{_manifest_detail(spot_manifest, fallback='')}"
    )
    lines.append(
        f"| 现货/期货价格 | {_manifest_tier_label(spot_manifest, 'Tier 2')} | "
        f"{spot_source_label} | "
        f"{spot_detail} |"
    )
    # -- Cost structure / Tier 0 citations --
    lines.append(
        f"| 成本结构 | {_manifest_tier_label(cost_manifest, 'Tier 0')} | "
        f"{_manifest_source_label(cost_manifest) if cost_manifest else '`tier0_autofill.json → cost_structure`'} | "
        f"{_manifest_detail(cost_manifest, fallback='N/A')} |"
    )
    # -- Macro citations --
    fai = macro_data.get("fixed_asset_investment", {})
    fai_data = fai.get("data", [])
    fai_latest = fai_data[-1] if isinstance(fai_data, list) and fai_data else {}
    lines.append(
        f"| 固投数据 | Tier 0 (统计局) | "
        f"`macro_scan.json → fixed_asset_investment` [{fai.get('status', 'N/A')}] | "
        f"`{json.dumps(fai_latest, ensure_ascii=False, default=str)[:120]}` |"
    )
    ppi = macro_data.get("ppi", {})
    ppi_data = ppi.get("data", [])
    ppi_latest = ppi_data[-1] if isinstance(ppi_data, list) and ppi_data else {}
    lines.append(
        f"| PPI数据 | Tier 0 (统计局) | "
        f"`macro_scan.json → ppi` [{ppi.get('status', 'N/A')}] | "
        f"`{json.dumps(ppi_latest, ensure_ascii=False, default=str)[:120]}` |"
    )
    # -- Cycle valuation citations --
    lines.append(
        f"| 估值模型参数 | 计算 | "
        f"`crocodile_discipline.yaml → {eco_context.get('four_signal_mode', 'defaults')}` | "
        f"`sell_pb={pe_policy.get('sell_pb', 'N/A')}` `sell_pe={pe_policy.get('sell_pe', 'N/A')}` "
        f"`high_price={cycle_valuation.get('high_price', 'N/A')}` `cost={cycle_valuation.get('unit_cost', 'N/A')}` |"
    )
    valuation_param_parts = [f"`target_metric={target_metric}`", f"`sell_key={sell_key}`", f"`optimistic_key={optimistic_key}`"]
    if target_metric == "PS":
        ps_policy = cycle_valuation.get("ps_policy", {}) or {}
        valuation_param_parts.extend(
            [
                f"`sell_ps={ps_policy.get('sell_ps', 'N/A')}`",
                f"`current_ps={cycle_valuation.get('current_ps', 'N/A')}`",
                f"`proxy_revenue={cycle_valuation.get('sales_proxy_revenue', 'N/A')}`",
            ]
        )
    else:
        pe_policy = cycle_valuation.get("pe_policy", {}) or {}
        valuation_param_parts.extend(
            [
                f"`sell_pe={pe_policy.get('sell_pe', 'N/A')}`",
                f"`high_price={cycle_valuation.get('high_price', 'N/A')}`",
                f"`cost={cycle_valuation.get('unit_cost', 'N/A')}`",
            ]
        )
    lines.append(f"| 浼板€兼ā鍨嬭ˉ鍏呭弬鏁?| 璁＄畻 | `valuation_engine.py` | {' '.join(valuation_param_parts)} |")
    lines.extend(["", ""])

    lines.extend([
        "### 审计索引",
        "",
        f"- source_manifest: `{os.path.join(evidence_dir, 'source_manifest.json')}`",
        f"- scorecard: `{score_path}`",
        f"- tier0_autofill: `{os.path.join(evidence_dir, 'tier0_autofill.json')}`",
        f"- annual_reports: `{annual_report_result.get('annual_reports_dir', '')}`",
        "",
    ])

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return {
        "status": "ok",
        "report_path": report_path,
        "scorecard_path": score_path,
        "scorecard": scorecard,
        "synthesis": synthesis,
    }


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    name = sys.argv[2] if len(sys.argv) > 2 else "中盐化工"
    base_dir = Path(os.environ.get("A_STOCK_BASE", str(Path(__file__).resolve().parents[5])))
    raw = json.load(open(base_dir / "data" / "raw" / code / "akshare_scan.json", "r", encoding="utf-8"))
    gate = json.load(open(base_dir / "data" / "processed" / code / "redcard_gate.json", "r", encoding="utf-8"))
    manifest = json.load(open(base_dir / "evidence" / code / "source_manifest.json", "r", encoding="utf-8"))
    autofill = json.load(open(base_dir / "evidence" / code / "tier0_autofill.json", "r", encoding="utf-8"))
    verification = json.load(open(base_dir / "evidence" / code / "tier0_verification.json", "r", encoding="utf-8"))
    annual_reports = json.load(open(base_dir / "evidence" / code / "annual_reports" / "annual_reports_manifest.json", "r", encoding="utf-8"))
    execution_log = json.load(open(base_dir / "data" / "processed" / code / "execution_log.json", "r", encoding="utf-8"))
    commodity = json.load(open(base_dir / "data" / "raw" / code / "commodity" / "commodity_scan.json", "r", encoding="utf-8"))
    macro = json.load(open(base_dir / "data" / "raw" / code / "macro" / "macro_scan.json", "r", encoding="utf-8"))
    print(json.dumps(generate_report(code, name, scan_data=raw, gate_result=gate, source_manifest=manifest, eco_context=manifest.get("eco_context", {}), commodity_data=commodity, macro_data=macro, tier0_autofill_result=autofill, tier0_verification_result=verification, annual_report_result=annual_reports, execution_log=execution_log, report_dir=str(base_dir / "reports"), processed_dir=str(base_dir / "data" / "processed" / code), evidence_dir=str(base_dir / "evidence" / code)), ensure_ascii=False, indent=2))

