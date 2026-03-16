"""Markdown report builders for market scans and deep dives."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.config_loader import load_scoring_rules
from utils.value_utils import normalize_text


_DIMENSION_KEY_MAP = {
    "type_clarity": "opportunity_type_clarity",
    "business_quality": "business_quality",
    "survival": "survival_boundary",
    "management": "management_capital_allocation",
    "regime_cycle": "regime_cycle_position",
    "valuation": "valuation_margin_of_safety",
    "catalyst": "catalyst_value_realization",
    "market_structure": "market_structure_tradability",
}

_FALLBACK_DIMENSION_MAX = {
    "type_clarity": 5,
    "business_quality": 20,
    "survival": 15,
    "management": 10,
    "regime_cycle": 15,
    "valuation": 20,
    "catalyst": 10,
    "market_structure": 5,
}


def _load_dimension_max() -> dict[str, float]:
    rules = load_scoring_rules()
    dimensions = rules.get("dimensions", {}) if isinstance(rules, dict) else {}
    result = dict(_FALLBACK_DIMENSION_MAX)
    for internal_key, yaml_key in _DIMENSION_KEY_MAP.items():
        weight = dimensions.get(yaml_key, {}).get("weight")
        if isinstance(weight, (int, float)):
            result[internal_key] = float(weight)
    return result


def _fmt_price(value: Any) -> str:
    if value in (None, ""):
        return "N/A"
    return f"{float(value):.2f}"


def _fmt_pct(value: Any) -> str:
    if value in (None, ""):
        return "N/A"
    return f"{float(value) * 100:.1f}%"


def _fmt_text_list(values: list[Any]) -> str:
    rendered = [str(value) for value in values if value not in (None, "")]
    return ", ".join(rendered) if rendered else "N/A"


def _company_business_text(scan_data: dict[str, Any]) -> str:
    profile = scan_data.get("company_profile", {}).get("data", {}) or {}
    for key in ("主营业务", "经营范围", "涓昏惀涓氬姟", "缁忚惀鑼冨洿"):
        value = normalize_text(profile.get(key))
        if value:
            return value
    return "N/A"


def _modifier_summary(driver_stack: dict[str, Any], flow_stage: str) -> str:
    modifiers = driver_stack.get("modifiers", {}) or {}
    parts = []
    for label, value in (
        ("cycle", modifiers.get("cycle_state")),
        ("repair", modifiers.get("repair_state")),
        ("path", modifiers.get("realization_path")),
        ("flow", flow_stage or modifiers.get("flow_stage")),
        ("elasticity", modifiers.get("elasticity_bucket")),
    ):
        if value not in (None, ""):
            parts.append(f"{label}={value}")
    return ", ".join(parts) if parts else "No driver modifiers yet."


def _valuation_summary_aliases(valuation_result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    summary = valuation_result.get("summary", {}) or {}
    floor_case = valuation_result.get("floor_case", valuation_result.get("bear_case", {}))
    normalized_case = valuation_result.get("normalized_case", valuation_result.get("base_case", {}))
    recognition_case = valuation_result.get("recognition_case", valuation_result.get("bull_case", {}))
    return floor_case or {}, normalized_case or {}, recognition_case or {}, summary


def generate_deep_dive_report(
    stock_code: str,
    company_name: str,
    *,
    market: str,
    scan_data: dict[str, Any],
    gate_result: dict[str, Any],
    valuation_result: dict[str, Any],
    synthesis_result: dict[str, Any],
    report_dir: str,
) -> dict[str, Any]:
    dimension_max = _load_dimension_max()
    opportunity = gate_result.get("opportunity_context", {}) or {}
    driver_stack = gate_result.get("driver_stack", {}) or {}
    underwrite_axis = gate_result.get("underwrite_axis", {}) or {}
    realization_axis = gate_result.get("realization_axis", {}) or {}
    scorecard = gate_result.get("scorecard", {}) or {}
    signals = gate_result.get("signals", {}) or {}
    gates = gate_result.get("gates", {}) or {}
    current_price = valuation_result.get("current_price")
    hard_vetos = gate_result.get("hard_vetos", []) or []
    position_state = gate_result.get("position_state", "unknown")
    prev_state = gate_result.get("prev_state", "NEW")
    transition_reason = gate_result.get("transition_reason", "No transition metadata.")
    flow_stage = gate_result.get("flow_stage", (driver_stack.get("modifiers", {}) or {}).get("flow_stage", "unknown"))
    floor_case, normalized_case, recognition_case, valuation_summary = _valuation_summary_aliases(valuation_result)
    margin_of_safety = valuation_summary.get("margin_of_safety")
    if margin_of_safety is None and valuation_summary.get("floor_protection") is not None:
        margin_of_safety = valuation_summary.get("floor_protection") - 1
    priced_state = valuation_summary.get("priced_in", valuation_summary.get("priced_state", "unknown"))
    report_path = Path(report_dir) / f"{stock_code}_{company_name}_deep_dive.md"

    lines = [
        "# Single Stock Deep Dive",
        "",
        "## 1. Executive view",
        f"- company: {company_name}",
        f"- ticker: {stock_code}",
        f"- market: {market}",
        f"- primary type: {opportunity.get('primary_label', 'Unknown')}",
        f"- one-sentence thesis: {opportunity.get('sentence', 'No clean thesis yet.')}",
        f"- current conclusion: {scorecard.get('verdict', 'reject / no action')}",
        f"- sector route: {driver_stack.get('sector_route', opportunity.get('sector_route', 'unknown'))}",
        f"- position state: {position_state}",
        f"- state transition: {prev_state} -> {position_state} ({transition_reason})",
        "",
        "## 2. Why this stock may be mispriced",
        f"- what the market likely sees: {' '.join(synthesis_result.get('market_perception', []))}",
        f"- what the market may be missing: {' '.join(synthesis_result.get('what_market_misses', []))}",
        f"- why the gap may close: {synthesis_result.get('why_gap_may_close', 'No clear rerating path yet.')}",
        "",
        "## 3. Opportunity type",
        f"- primary type: {opportunity.get('primary_label', 'Unknown')}",
        f"- why this is the right type: {opportunity.get('reason', 'Type signal is weak.')}",
        f"- why the other types are secondary or not primary: {_fmt_text_list(opportunity.get('secondary_types', []))}",
        f"- VCRF driver modifiers: {_modifier_summary(driver_stack, flow_stage)}",
        "",
        "## 4. Business truth",
        f"- business model: {_company_business_text(scan_data)}",
        f"- revenue and profit engine: dominant segment is {signals.get('purity', {}).get('top_segment') or 'unclear'}",
        f"- key operating drivers: {signals.get('moat', {}).get('reason', 'No durable edge surfaced.')}",
        f"- what must go right: {gates.get('business_truth', {}).get('reason', 'Need clearer business evidence.')}",
        "",
        "## 5. Survival truth",
        f"- debt and liquidity: {gates.get('survival_truth', {}).get('reason', 'Need explicit balance-sheet evidence.')}",
        f"- downside resilience: status={gates.get('survival_truth', {}).get('status', 'unknown')}",
        f"- what can break first: {hard_vetos[0] if hard_vetos else 'Balance-sheet stress or failed catalyst execution.'}",
        "",
        "## 6. Quality truth",
        f"- moat / advantage: {signals.get('moat', {}).get('reason', 'No moat claim yet.')}",
        f"- management and capital allocation: {signals.get('management', {}).get('verdict', 'unknown')}",
        f"- governance issues: {_fmt_text_list(signals.get('management', {}).get('red_flags', []))}",
        f"- return and cash flow quality: {gates.get('quality_truth', {}).get('reason', 'Mixed quality signal.')}",
        "",
        "## 7. Regime / cycle truth",
        f"- macro / rates / policy: {gates.get('regime_cycle_truth', {}).get('reason', 'No strong regime read.')}",
        f"- industry cycle position: {signals.get('bottom_pattern', {}).get('signal', 'mixed')}",
        f"- company transmission point: {opportunity.get('primary_label', 'Unknown')} names transmit through their core driver first.",
        f"- timing implications: status={gates.get('regime_cycle_truth', {}).get('status', 'unknown')}",
        "",
        "## 8. Valuation truth",
        f"- dual-axis snapshot: underwrite={underwrite_axis.get('score', 'N/A')}, realization={realization_axis.get('score', 'N/A')}, flow_stage={flow_stage}",
        f"- VCRF floor protection: {_fmt_pct(valuation_summary.get('floor_protection'))}",
        f"- VCRF normalized upside: {_fmt_pct(valuation_summary.get('normalized_upside'))}",
        f"- VCRF recognition upside: {_fmt_pct(valuation_summary.get('recognition_upside'))}",
        f"- wind dependency: {_fmt_pct(valuation_summary.get('wind_dependency'))}",
        "",
        "### Bear case",
        "- VCRF alias: floor_case",
        f"- assumptions: {_fmt_text_list(floor_case.get('assumptions', []))}",
        f"- valuation method: {floor_case.get('valuation_method', 'N/A')}",
        f"- implied value range: {_fmt_price(floor_case.get('implied_price'))}",
        "",
        "### Base / normal case",
        "- VCRF alias: normalized_case",
        f"- assumptions: {_fmt_text_list(normalized_case.get('assumptions', []))}",
        f"- valuation method: {normalized_case.get('valuation_method', 'N/A')}",
        f"- implied value range: {_fmt_price(normalized_case.get('implied_price'))}",
        "",
        "### Bull case",
        "- VCRF alias: recognition_case",
        f"- assumptions: {_fmt_text_list(recognition_case.get('assumptions', []))}",
        f"- valuation method: {recognition_case.get('valuation_method', 'N/A')}",
        f"- implied value range: {_fmt_price(recognition_case.get('implied_price'))}",
        "",
        f"- current price versus conservative value: current={_fmt_price(current_price)}, base={_fmt_price(normalized_case.get('implied_price'))}",
        f"- margin of safety: {_fmt_pct(margin_of_safety)}",
        f"- what expectations are priced in: {priced_state}",
        "",
        "## 9. Catalyst truth",
        f"- near-term catalysts: {_fmt_text_list((signals.get('catalyst', {}) or {}).get('catalysts', [])[:2])}",
        f"- medium-term catalysts: {_fmt_text_list((signals.get('catalyst', {}) or {}).get('catalysts', [])[2:])}",
        f"- what can unlock value: {signals.get('catalyst', {}).get('reason', 'No concrete catalyst surfaced.')}",
        "- what can delay value realization: weak catalyst truth or failing survival/business gates",
        "",
        "## 10. Anti-thesis",
    ]
    lines.extend([f"- {item}" for item in synthesis_result.get("anti_thesis", [])])
    lines.extend(
        [
            "",
            "## 11. Falsification points",
            *[f"- {item}" for item in synthesis_result.get("falsification_points", [])],
            "",
            "## 12. Scorecard",
            f"- underwrite axis: {underwrite_axis.get('score', 'N/A')}/100",
            f"- realization axis: {realization_axis.get('score', 'N/A')}/100",
            f"- position state: {position_state}",
            f"- flow stage: {flow_stage}",
            f"- type clarity: {scorecard.get('type_clarity', 'N/A')}/{int(dimension_max['type_clarity'])}",
            f"- business quality: {scorecard.get('business_quality', 'N/A')}/{int(dimension_max['business_quality'])}",
            f"- survival: {scorecard.get('survival', 'N/A')}/{int(dimension_max['survival'])}",
            f"- management: {scorecard.get('management', 'N/A')}/{int(dimension_max['management'])}",
            f"- regime/cycle: {scorecard.get('regime_cycle', 'N/A')}/{int(dimension_max['regime_cycle'])}",
            f"- valuation: {scorecard.get('valuation', 'N/A')}/{int(dimension_max['valuation'])}",
            f"- catalyst: {scorecard.get('catalyst', 'N/A')}/{int(dimension_max['catalyst'])}",
            f"- market structure: {scorecard.get('market_structure', 'N/A')}/{int(dimension_max['market_structure'])}",
            f"- total: {scorecard.get('total', 'N/A')}/100",
            "",
            "## 13. Action plan",
            "- ideal buy zone: below bear/base midpoint if the thesis remains intact",
            f"- starter zone: {_fmt_price(normalized_case.get('implied_price'))} or below with improving evidence",
            "- add conditions: better survival evidence, clearer catalyst, or stronger valuation gap",
            "- trim conditions: price reaches or exceeds base-to-bull range without evidence upgrade",
            "- exit conditions: hard veto appears or falsification points trigger",
            "",
            "## 14. Bottom line",
            synthesis_result.get("bottom_line", "This stock is not yet actionable."),
            "",
        ]
    )

    content = "\n".join(lines)
    report_path.write_text(content, encoding="utf-8")
    return {"report_path": str(report_path), "content": content}


def generate_market_scan_report(
    *,
    market: str,
    scope_text: str,
    results_summary: str,
    priority_shortlist: list[dict[str, Any]],
    secondary_watchlist: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    report_dir: str,
) -> dict[str, Any]:
    report_path = Path(report_dir) / "market_opportunity_scan.md"
    ranking_rows = priority_shortlist + secondary_watchlist + rejected
    lines = [
        "# Market Opportunity Scan",
        "",
        "## 1. Scope",
        f"- market covered: {market}",
        f"- universe definition: {scope_text}",
        "- exclusions: illiquid or obviously incomplete cases were deprioritized",
        "- date / freshness note: built from current adapter outputs at runtime",
        "",
        "## 2. Market-level read",
        "- overall opportunity set: based on the ranked sample rather than a mechanical all-market promise",
        "- broad valuation or sentiment condition: mixed unless the shortlist is empty",
        "- favorable styles: names with clearer type, survivability, and catalysts rank first",
        "",
        "## 3. Results summary",
        f"- {results_summary}",
        "",
        "## 4. Priority shortlist",
    ]
    for item in priority_shortlist:
        lines.extend(
            [
                f"- {item['ticker']} / {item['company_name']} | {item['opportunity_type']} | {item['score']}/100",
                f"  thesis: {item['thesis']}",
                f"  mispricing: {item['mispricing']}",
                f"  catalysts: {', '.join(item['catalysts']) or 'N/A'}",
                f"  risks: {', '.join(item['risks']) or 'N/A'}",
                f"  why passed: {item['why_passed']}",
                f"  next step: {item['next_step']}",
            ]
        )
    lines.extend(["", "## 5. Secondary watchlist"])
    for item in secondary_watchlist:
        lines.append(f"- {item['ticker']} / {item['company_name']} | {item['opportunity_type']} | {item['score']}/100 | {item['thesis']}")
    lines.extend(["", "## 6. Rejected ideas"])
    for item in rejected:
        lines.append(f"- {item['ticker']} / {item['company_name']}: {item['reason']}")
    lines.extend(["", "## 7. Ranking table", "", "| Name | Type | Score | Edge summary | Next action |", "| --- | --- | --- | --- | --- |"])
    for item in ranking_rows:
        lines.append(f"| {item['company_name']} | {item['opportunity_type']} | {item['score']} | {item['thesis']} | {item.get('next_step', item.get('reason', 'watch'))} |")
    lines.extend(["", "## 8. Deep-dive queue"])
    for item in priority_shortlist[:3]:
        lines.append(f"- {item['ticker']} / {item['company_name']}: deep dive first because {item['why_passed']}")
    content = "\n".join(lines)
    report_path.write_text(content, encoding="utf-8")
    return {"report_path": str(report_path), "content": content}
