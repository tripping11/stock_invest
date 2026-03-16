"""
synthesis_engine.py — 研究判断引擎
接收全量采集数据，产出「证据→判断→估值」的结构化 synthesis，
弥补从数据闸门到打分之间缺失的"思考层"。

输出字段:
  core_mispricing      — 市场定价错误的本质描述
  core_logic           — 1-2 条核心投资逻辑
  mispricing_evidence  — 错杀点证据（price_fact / profit_fact / market_gap）
  time_lag_analysis    — 时滞错配（upstream / downstream / stock_vs_fundamental）
  falsification_conditions — 反证条件列表
  odds_source          — 赔率来源（0.5元买1元的本质）
  one_liner_thesis     — 一句话买入本质
  valuation_gap        — 当前市值 vs 应有市值
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from utils.research_utils import (
    assess_business_purity,
    assess_price_trigger,
    extract_price_series,
    extract_market_cap,
    get_latest_income_snapshot,
    normalize_text,
    safe_float,
)

MIN_COMPARABLE_PROFIT_BASE = 1e4

# ---------------------------------------------------------------------------
# Helper: classify product price change magnitude
# ---------------------------------------------------------------------------

def _classify_price_change(commodity_data: dict, eco_context: dict) -> dict:
    """Derive product/commodity price momentum from available spot & futures data."""
    prices = extract_price_series(commodity_data)
    futures = (
        commodity_data.get("futures", {}).get("data", {})
        if isinstance(commodity_data.get("futures", {}), dict)
        else {}
    )
    latest = prices[-1] if prices else safe_float(futures.get("latest_close"))
    high_250 = safe_float(futures.get("high_250d"))
    high_750 = safe_float(futures.get("high_750d"))
    low_60 = min(prices[-60:]) if len(prices) >= 60 else safe_float(futures.get("low_60d"))

    drawdown_from_high = None
    if latest is not None and high_750 not in (None, 0):
        drawdown_from_high = (1 - latest / high_750) * 100  # positive = fallen

    rebound_from_low = None
    if latest is not None and low_60 not in (None, 0):
        rebound_from_low = (latest / low_60 - 1) * 100

    yoy_change = None
    if len(prices) >= 250:
        old = safe_float(prices[-250])
        if old not in (None, 0):
            yoy_change = (latest / old - 1) * 100

    # Classify
    if yoy_change is not None and yoy_change >= 100:
        phase = "持续大涨(>100%)"
    elif yoy_change is not None and yoy_change >= 50:
        phase = "强势上涨(50-100%)"
    elif yoy_change is not None and yoy_change >= 10:
        phase = "温和上行(10-50%)"
    elif drawdown_from_high is not None and drawdown_from_high >= 50:
        phase = "深度下行(距高点跌>50%)"
    elif drawdown_from_high is not None and drawdown_from_high >= 30:
        phase = "回调下行(距高点跌30-50%)"
    elif rebound_from_low is not None and rebound_from_low >= 20:
        phase = "底部反弹(距低点涨>20%)"
    else:
        phase = "低位盘整"

    return {
        "latest_price": latest,
        "high_250d": high_250,
        "high_750d": high_750,
        "low_60d": low_60,
        "drawdown_from_high_pct": drawdown_from_high,
        "rebound_from_low_pct": rebound_from_low,
        "yoy_change_pct": yoy_change,
        "phase": phase,
    }


# ---------------------------------------------------------------------------
# Helper: classify profit trend
# ---------------------------------------------------------------------------

def _extract_report_date_text(record: dict) -> str:
    return normalize_text(record.get("报告日") or record.get("报告日期") or record.get("报告期") or "")


def _extract_profit_value(record: dict) -> float | None:
    return safe_float(
        record.get("归属于母公司所有者的净利润")
        or record.get("归属于母公司股东的净利润")
        or record.get("净利润")
    )


def _find_comparable_profit(records: list[dict], latest_report_date: str) -> tuple[float | None, str, str]:
    if not latest_report_date:
        return None, "", "no_latest_report_date"

    normalized = latest_report_date.replace("-", "").replace("/", "")
    period_suffix = normalized[-4:] if len(normalized) >= 8 else ""
    target_year = normalized[:4]
    comparison_basis = "same_period_last_year"

    comparable: list[tuple[str, float]] = []
    fallback: list[tuple[str, float]] = []
    for record in records:
        report_date = _extract_report_date_text(record).replace("-", "").replace("/", "")
        if not report_date or report_date == normalized:
            continue
        profit = _extract_profit_value(record)
        if profit is None:
            continue
        if period_suffix and report_date.endswith(period_suffix) and len(report_date) >= 8:
            year_text = report_date[:4]
            if target_year.isdigit() and year_text.isdigit() and int(year_text) == int(target_year) - 1:
                comparable.append((report_date, profit))
        fallback.append((report_date, profit))

    if comparable:
        comparable.sort(key=lambda item: item[0], reverse=True)
        return comparable[0][1], comparable[0][0], comparison_basis

    if fallback:
        fallback.sort(key=lambda item: item[0], reverse=True)
        return fallback[0][1], fallback[0][0], "latest_available_fallback"

    return None, "", "no_comparable_period"


def _classify_profit_trend(scan_data: dict) -> dict:
    """Analyze net profit trajectory from income statements."""
    records = scan_data.get("income_statement", {}).get("data", [])
    if not records:
        return {
            "phase": "数据不足",
            "latest_profit": None,
            "prev_profit": None,
            "change_pct": None,
            "prev_report_date": "",
            "comparison_basis": "no_data",
        }

    latest_income = get_latest_income_snapshot(records)
    latest_profit = latest_income.get("net_profit")
    latest_report_date = latest_income.get("report_date", "")
    prev_profit, prev_report_date, comparison_basis = _find_comparable_profit(records, latest_report_date)

    change_pct = None
    if latest_profit is not None and prev_profit is not None:
        if abs(prev_profit) <= MIN_COMPARABLE_PROFIT_BASE:
            comparison_basis = "base_too_small"
        elif latest_profit * prev_profit < 0:
            comparison_basis = "sign_flip"
        else:
            change_pct = (latest_profit / abs(prev_profit) - 1) * 100

    # Classify
    if comparison_basis == "base_too_small":
        phase = "利润基数过小，无法比较"
    elif comparison_basis == "sign_flip":
        phase = "利润由亏转盈" if latest_profit is not None and latest_profit > 0 else "利润由盈转亏"
    elif change_pct is not None and change_pct >= 60:
        phase = "利润大涨(>60%)"
    elif change_pct is not None and change_pct >= 20:
        phase = "利润增长(20-60%)"
    elif change_pct is not None and change_pct >= 0:
        phase = "利润平稳(0-20%)"
    elif change_pct is not None and change_pct >= -30:
        phase = "利润下滑(0~-30%)"
    elif change_pct is not None:
        phase = "利润深跌(>-30%)"
    else:
        phase = "数据不足"

    return {
        "phase": phase,
        "latest_profit": latest_profit,
        "prev_profit": prev_profit,
        "change_pct": change_pct,
        "report_date": latest_report_date,
        "prev_report_date": prev_report_date,
        "comparison_basis": comparison_basis,
    }


# ---------------------------------------------------------------------------
# Helper: determine mispricing evidence
# ---------------------------------------------------------------------------

def _build_mispricing_evidence(
    price_analysis: dict,
    profit_analysis: dict,
    kline: dict,
    valuation: dict,
    eco_context: dict,
) -> dict:
    """Construct structured evidence of market mispricing."""
    pb = safe_float(valuation.get("pb"))
    pb_pct = safe_float(valuation.get("pb_percentile"))
    current_vs_high = safe_float(kline.get("current_vs_high"))

    # Price fact
    price_phase = price_analysis.get("phase", "")
    commodity_name = eco_context.get("commodity", "产品")
    if "大涨" in price_phase or "强势" in price_phase:
        price_fact = f"{commodity_name}价格{price_phase}，产品景气已确认上行"
    elif "深度下行" in price_phase or "回调" in price_phase:
        price_fact = f"{commodity_name}价格{price_phase}，处于周期底部区域，高成本产能面临出清"
    elif "反弹" in price_phase:
        price_fact = f"{commodity_name}价格从底部反弹（距60日低点+{price_analysis.get('rebound_from_low_pct', 0):.1f}%），底部信号初现"
    else:
        price_fact = f"{commodity_name}价格处于低位盘整，等待底部确认"

    # Profit fact
    profit_phase = profit_analysis.get("phase", "")
    change_pct = profit_analysis.get("change_pct")
    if "大涨" in profit_phase:
        profit_fact = f"扣非净利润同比大涨{change_pct:.0f}%，业绩爆发已兑现"
    elif "增长" in profit_phase:
        profit_fact = f"扣非净利润同比增长{change_pct:.0f}%，业绩改善中"
    elif "深跌" in profit_phase:
        profit_fact = f"利润同比下滑{abs(change_pct):.0f}%，处于周期底部，但这正是'最差报表+最便宜估值'的错杀窗口"
    elif change_pct is not None:
        profit_fact = f"利润同比变化{change_pct:+.0f}%"
    else:
        profit_fact = "利润趋势数据不足，需补充财报验证"

    # Market gap — why hasn't the stock reacted?
    gap_reasons = []
    if pb is not None and pb <= 1.0:
        gap_reasons.append(f"PB={pb:.2f}≤1.0，市场以低于净资产对公司定价")
    if pb_pct is not None and pb_pct <= 25:
        gap_reasons.append(f"PB分位{pb_pct:.1f}%，处于历史极低区间")
    if current_vs_high is not None and current_vs_high <= 50:
        gap_reasons.append(f"股价仅为5年高点的{current_vs_high:.0f}%，市场恐慌仍在折价")

    eco_circle = eco_context.get("eco_circle", "")
    if eco_circle == "core_resource":
        if "底部" in price_phase or "下行" in price_phase:
            gap_reasons.append("行业在周期谷底，多数投资者已绝望出清")
        elif "大涨" in price_phase:
            gap_reasons.append("产品涨价的利润传导尚需1-2个季度体现在财报中")
    elif eco_circle == "rigid_shovel":
        gap_reasons.append("铲子股对下游Capex的响应存在6-12个月滞后")

    market_gap = "；".join(gap_reasons) if gap_reasons else "当前估值与基本面无显著脱离"

    return {
        "price_fact": price_fact,
        "profit_fact": profit_fact,
        "market_gap": market_gap,
    }


# ---------------------------------------------------------------------------
# Helper: time lag analysis
# ---------------------------------------------------------------------------

def _build_time_lag_analysis(
    price_analysis: dict,
    profit_analysis: dict,
    eco_context: dict,
    commodity_data: dict,
    macro_data: dict,
) -> dict:
    """Identify time-lag mismatch between upstream signal and stock price."""
    eco_circle = eco_context.get("eco_circle", "")
    commodity_name = eco_context.get("commodity", "产品")
    price_phase = price_analysis.get("phase", "")
    profit_phase = profit_analysis.get("phase", "")

    if eco_circle == "core_resource":
        # Resource body: spot price → company profit → stock price
        upstream = f"{commodity_name}现货：{price_phase}"
        if "大涨" in price_phase or "强势" in price_phase:
            downstream = "利润传导预计滞后1-2个季度，股价可能仍在消化前期悲观预期"
        elif "底部" in price_phase or "下行" in price_phase:
            downstream = "行业处于周期谷底，高成本产能承压出清中，底部确认需观察去库存和减产信号"
        elif "反弹" in price_phase:
            downstream = "价格初步企稳反弹，但需持续数月确认趋势，利润端尚未体现"
        else:
            downstream = "价格低位震荡，等待供给侧出清或需求脉冲"

        stock_status = _describe_stock_vs_cycle(price_phase, profit_phase)

    elif eco_circle == "rigid_shovel":
        # Shovel play: downstream capex → orders → shovel revenue → stock price
        fai = macro_data.get("fixed_asset_investment", {})
        fai_latest = fai.get("data", [{}])[-1] if isinstance(fai.get("data"), list) and fai.get("data") else {}
        fai_value = safe_float(fai_latest.get("value") or fai_latest.get("同比增长"))
        upstream = f"下游固投/Capex：{'同比+' + str(fai_value) + '%' if fai_value is not None else '数据待补充'}"
        downstream = "铲子股订单爆发通常滞后于下游Capex启动6-12个月"
        stock_status = _describe_stock_vs_cycle(price_phase, profit_phase)

    elif eco_circle == "core_military":
        upstream = "军工采购批次/型号列装计划"
        downstream = "军品交付确认→收入确认存在1-2年周期"
        stock_status = "军工标的估值受政策脉冲影响大，需关注军费预算和型号进展"
    else:
        upstream = "生态位未明确"
        downstream = "无法判断传导路径"
        stock_status = "需先确认生态归属"

    return {
        "upstream_signal": upstream,
        "downstream_response": downstream,
        "stock_vs_fundamental": stock_status,
    }


def _describe_stock_vs_cycle(price_phase: str, profit_phase: str) -> str:
    """One-line description of stock-vs-fundamental mismatch."""
    if ("大涨" in price_phase or "强势" in price_phase) and "深跌" in profit_phase:
        return "产品已强势上涨但利润尚在低谷——典型的时滞错配窗口（最佳介入期）"
    if ("大涨" in price_phase or "强势" in price_phase) and ("大涨" in profit_phase or "增长" in profit_phase):
        return "价格与利润同步上行——赚钱效应已开始显现，关注估值是否仍有安全边际"
    if ("底部" in price_phase or "下行" in price_phase) and "深跌" in profit_phase:
        return "价格与利润同处底部——至暗时刻，需等待向上拐点信号"
    if "反弹" in price_phase and "深跌" in profit_phase:
        return "价格初步反弹但利润仍差——早期信号，需确认价格反弹可持续"
    return "价格与利润的时滞关系需进一步观察"


# ---------------------------------------------------------------------------
# Helper: falsification conditions
# ---------------------------------------------------------------------------

def _build_falsification_conditions(eco_context: dict, price_analysis: dict) -> list[str]:
    """List the conditions that would invalidate the investment thesis."""
    eco_circle = eco_context.get("eco_circle", "")
    commodity_name = eco_context.get("commodity", "产品")
    conditions = []

    if eco_circle == "core_resource":
        conditions.extend([
            f"{commodity_name}现货价跌破行业成本线且持续超过6个月，行业无减产迹象",
            f"新增产能大规模释放超预期，导致供需格局恶化而非改善",
            "公司实际控制人变更或国资属性丧失",
            "核心矿权/产能出现环保或安全事故导致长期停产",
            f"公司主营纯粹度大幅下降（因投入与{commodity_name}无关的多元化项目）",
        ])
    elif eco_circle == "rigid_shovel":
        conditions.extend([
            "下游行业Capex增速转负且持续2个季度以上",
            "核心客户的资本开支计划大幅缩减或延后",
            "竞争对手获得同等资质/牌照，打破产能垄断",
            "公司实际控制人变更或国资属性丧失",
            "在手订单大幅低于预期（同比下降>30%）",
        ])
    elif eco_circle == "core_military":
        conditions.extend([
            "国防预算增速大幅放缓（低于GDP增速）",
            "核心型号被替代或采购计划延后",
            "公司实际控制人变更或国资属性丧失",
            "民品占比大幅上升稀释军品利润",
        ])
    else:
        conditions.append("生态归属不明，无法列出针对性证伪条件")

    # Universal condition
    conditions.append("PB>3.0 或 PE>30（A股周期泡沫警戒线）时应考虑抛售而非继续持有")

    return conditions


# ---------------------------------------------------------------------------
# Helper: odds source
# ---------------------------------------------------------------------------

def _build_odds_source(
    valuation: dict,
    cycle_valuation: dict,
    eco_context: dict,
    scorecard: dict,
) -> str:
    """Describe the source of asymmetric odds in plain language."""
    pb = safe_float(valuation.get("pb"))
    bvps = safe_float(valuation.get("bvps"))
    eco_circle = eco_context.get("eco_circle", "")
    commodity_name = eco_context.get("commodity", "产品")

    target_caps = cycle_valuation.get("target_market_caps", {})
    market_cap = cycle_valuation.get("_current_market_cap")
    upside_text = ""
    if market_cap and target_caps:
        # Use sell_pe target
        pe_policy = cycle_valuation.get("pe_policy", {})
        sell_key = f"{int(pe_policy.get('sell_pe', 15))}x"
        target_cap = safe_float(target_caps.get(sell_key))
        if target_cap and market_cap > 0:
            multiple = target_cap / market_cap
            upside_text = f"保守目标对应{multiple:.1f}倍空间"

    if pb is not None and pb <= 0.8:
        core = f"以PB={pb:.2f}（低于净资产8折）买入"
    elif pb is not None and pb <= 1.0:
        core = f"以PB={pb:.2f}（接近破净）买入"
    else:
        core = "在周期底部估值区间买入"

    if eco_circle == "core_resource":
        asset_desc = f"底层{commodity_name}矿山/产能"
    elif eco_circle == "rigid_shovel":
        asset_desc = "垄断特许牌照"
    elif eco_circle == "core_military":
        asset_desc = "核心军工配套产能"
    else:
        asset_desc = "生产资料"

    ownership = scorecard.get("ownership", {})
    owner_label = ownership.get("label", "国资")

    parts = [f"{core}{asset_desc}，{owner_label}兜底"]
    if upside_text:
        parts.append(upside_text)
    parts.append("免仓储费凭证，持有等待周期回归均值")

    return "，".join(parts)


# ---------------------------------------------------------------------------
# Helper: one-liner thesis
# ---------------------------------------------------------------------------

def _build_one_liner(
    company_name: str,
    eco_context: dict,
    price_analysis: dict,
    valuation: dict,
    scorecard: dict,
) -> str:
    """Generate the one-line investment thesis."""
    eco_circle = eco_context.get("eco_circle", "")
    commodity_name = eco_context.get("commodity", "产品")
    pb = safe_float(valuation.get("pb"))
    ownership = scorecard.get("ownership", {})
    owner_label = ownership.get("label", "国资")
    price_phase = price_analysis.get("phase", "")

    if "底部" in price_phase or "下行" in price_phase or "盘整" in price_phase:
        timing_desc = f"{commodity_name}行业最寒冷的冬天"
    elif "反弹" in price_phase:
        timing_desc = f"{commodity_name}价格从底部初步反弹"
    else:
        timing_desc = f"{commodity_name}景气上行期"

    if eco_circle == "core_resource":
        asset_desc = f"以{owner_label}兜底的安全边际，低价买入'{commodity_name}'底层产能的免仓储费凭证"
    elif eco_circle == "rigid_shovel":
        asset_desc = f"以{owner_label}兜底的安全边际，低价买入垄断铲子/收费站的免仓储费凭证"
    elif eco_circle == "core_military":
        asset_desc = f"以{owner_label}兜底的安全边际，低价买入核心军工配套的免仓储费凭证"
    else:
        asset_desc = f"买入{owner_label}控制的生产资料凭证"

    pb_text = f"（PB={pb:.2f}）" if pb is not None else ""

    return f"买入{company_name} = 在{timing_desc}，{pb_text}{asset_desc}。"


# ---------------------------------------------------------------------------
# Helper: valuation gap
# ---------------------------------------------------------------------------

def _build_valuation_gap(
    quote: dict,
    cycle_valuation: dict,
) -> dict:
    """Calculate the gap between current market cap and fair value."""
    market_cap = extract_market_cap(quote)
    if market_cap is not None:
        market_cap_yi = market_cap / 1e8
    else:
        market_cap_yi = None

    target_caps = cycle_valuation.get("target_market_caps", {})
    pe_policy = cycle_valuation.get("pe_policy", {})
    sell_pe = int(pe_policy.get("sell_pe", 15))
    sell_key = f"{sell_pe}x"
    optimistic_pe = int(pe_policy.get("optimistic_pe", 20))
    optimistic_key = f"{optimistic_pe}x"

    conservative_cap = safe_float(target_caps.get(sell_key))
    optimistic_cap = safe_float(target_caps.get(optimistic_key))

    # Apply 7-discount safety margin per the guide
    conservative_cap_discounted = conservative_cap * 0.7 if conservative_cap else None

    upside_pct = None
    if market_cap_yi and conservative_cap:
        # `target_market_caps` is produced in yuan in valuation_engine; normalize once to 亿.
        conservative_cap_yi = conservative_cap / 1e8
        upside_pct = (conservative_cap_yi / market_cap_yi - 1) * 100

    safety_upside_pct = None
    if market_cap_yi and conservative_cap_discounted:
        discounted_yi = conservative_cap_discounted / 1e8
        safety_upside_pct = (discounted_yi / market_cap_yi - 1) * 100

    return {
        "current_cap_yi": market_cap_yi,
        "fair_value_cap_conservative": conservative_cap,
        "fair_value_cap_optimistic": optimistic_cap,
        "fair_value_cap_discounted_70pct": conservative_cap_discounted,
        "upside_pct": upside_pct,
        "safety_margin_upside_pct": safety_upside_pct,
    }


# ---------------------------------------------------------------------------
# Core mispricing narrative
# ---------------------------------------------------------------------------

def _build_core_mispricing(
    mispricing_evidence: dict,
    time_lag: dict,
    eco_context: dict,
) -> str:
    """Synthesize a paragraph describing the core mispricing thesis."""
    eco_circle = eco_context.get("eco_circle", "")
    commodity_name = eco_context.get("commodity", "产品")
    price_fact = mispricing_evidence.get("price_fact", "")
    profit_fact = mispricing_evidence.get("profit_fact", "")
    market_gap = mispricing_evidence.get("market_gap", "")
    stock_status = time_lag.get("stock_vs_fundamental", "")

    parts = []
    if eco_circle == "core_resource":
        parts.append(f"[{commodity_name}本体] {price_fact}。{profit_fact}。")
    elif eco_circle == "rigid_shovel":
        parts.append(f"[铲子股] {price_fact}。{profit_fact}。")
    elif eco_circle == "core_military":
        parts.append(f"[核心军工] {profit_fact}。")
    else:
        parts.append(f"{price_fact}。{profit_fact}。")

    if market_gap and market_gap != "当前估值与基本面无显著脱离":
        parts.append(f"市场定价偏差：{market_gap}。")

    if stock_status:
        parts.append(f"时滞判断：{stock_status}")

    return "".join(parts)


# ---------------------------------------------------------------------------
# Core logic items
# ---------------------------------------------------------------------------

def _build_core_logic(
    eco_context: dict,
    price_analysis: dict,
    profit_analysis: dict,
    valuation: dict,
    scorecard: dict,
) -> list[str]:
    """Return 1-2 core investment logic lines."""
    eco_circle = eco_context.get("eco_circle", "")
    commodity_name = eco_context.get("commodity", "产品")
    pb = safe_float(valuation.get("pb"))
    price_phase = price_analysis.get("phase", "")
    profit_phase = profit_analysis.get("phase", "")
    ownership = scorecard.get("ownership", {})
    logics = []

    # Logic 1: cycle position + price status
    if "底部" in price_phase or "下行" in price_phase:
        logics.append(
            f"{commodity_name}处于周期底部（{price_phase}），"
            f"高成本产能出清中，供给收缩驱动的价格反转是确定性最高的周期逻辑"
        )
    elif "大涨" in price_phase or "强势" in price_phase:
        logics.append(
            f"{commodity_name}价格已确认上行（{price_phase}），"
            f"利润弹性正在释放或即将释放"
        )
    elif "反弹" in price_phase:
        logics.append(
            f"{commodity_name}从底部初步反弹，"
            f"若确认供给出清+库存拐点，将进入主升通道"
        )
    else:
        logics.append(f"{commodity_name}价格{price_phase}，等待催化剂")

    # Logic 2: valuation + ownership safety
    owner_label = ownership.get("label", "国资")
    if pb is not None and pb <= 1.0:
        logics.append(
            f"当前PB={pb:.2f}（破净），{owner_label}兜底确保最差情景不亏本金，"
            f"安全边际充足"
        )
    elif pb is not None and pb <= 1.3:
        logics.append(
            f"当前PB={pb:.2f}，虽未破净但估值处于偏低区间，{owner_label}提供下行保护"
        )
    else:
        logics.append(f"{owner_label}控股提供底线保障，但当前估值需等待更深折价")

    return logics


# ===========================================================================
# Public API
# ===========================================================================

def build_synthesis(
    stock_code: str,
    company_name: str,
    scan_data: dict,
    commodity_data: dict,
    macro_data: dict,
    eco_context: dict,
    gate_result: dict,
    tier0_autofill_result: dict,
    scorecard: dict,
    cycle_valuation: dict,
) -> dict:
    """
    Core synthesis function — bridges evidence → judgment → valuation.

    Returns a structured dict with all synthesis fields.
    """
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})

    # Step 1: Analyze price and profit trends
    price_analysis = _classify_price_change(commodity_data, eco_context)
    profit_analysis = _classify_profit_trend(scan_data)

    # Step 2: Build mispricing evidence
    mispricing_evidence = _build_mispricing_evidence(
        price_analysis, profit_analysis, kline, valuation, eco_context
    )

    # Step 3: Time-lag analysis
    time_lag = _build_time_lag_analysis(
        price_analysis, profit_analysis, eco_context, commodity_data, macro_data
    )

    # Step 4: Falsification conditions
    falsification = _build_falsification_conditions(eco_context, price_analysis)

    # Step 5: Odds source
    odds = _build_odds_source(valuation, cycle_valuation, eco_context, scorecard)

    # Step 6: One-liner thesis
    one_liner = _build_one_liner(
        company_name, eco_context, price_analysis, valuation, scorecard
    )

    # Step 7: Valuation gap
    valuation_gap = _build_valuation_gap(quote, cycle_valuation)

    # Step 8: Core mispricing narrative
    core_mispricing = _build_core_mispricing(mispricing_evidence, time_lag, eco_context)

    # Step 9: Core logic
    core_logic = _build_core_logic(
        eco_context, price_analysis, profit_analysis, valuation, scorecard
    )

    return {
        "core_mispricing": core_mispricing,
        "core_logic": core_logic,
        "mispricing_evidence": mispricing_evidence,
        "time_lag_analysis": time_lag,
        "falsification_conditions": falsification,
        "odds_source": odds,
        "one_liner_thesis": one_liner,
        "valuation_gap": valuation_gap,
        "price_analysis": price_analysis,
        "profit_analysis": profit_analysis,
    }
