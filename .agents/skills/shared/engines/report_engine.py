"""Markdown report builders for market scans and deep dives."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.config_loader import load_scoring_rules
from utils.value_utils import normalize_text


_DIMENSION_KEY_MAP = {
    "type_clarity": ("opportunity_type_clarity", "thesis_clarity"),
    "business_quality": ("business_quality", "business_or_asset_quality"),
    "survival": ("survival_boundary",),
    "management": ("management_capital_allocation", "governance_anti_fraud"),
    "regime_cycle": ("regime_cycle_position",),
    "valuation": ("valuation_margin_of_safety", "intrinsic_value_floor"),
    "catalyst": ("catalyst_value_realization", "turnaround_catalyst"),
    "market_structure": ("market_structure_tradability", "flow_realization_and_elasticity"),
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
    for internal_key, yaml_keys in _DIMENSION_KEY_MAP.items():
        for yaml_key in yaml_keys:
            weight = dimensions.get(yaml_key, {}).get("weight")
            if isinstance(weight, (int, float)):
                result[internal_key] = float(weight)
                break
    return result


def _fmt_price(value: Any) -> str:
    if value in (None, ""):
        return "N/A"
    return f"{float(value):.2f}"


def _fmt_pct(value: Any) -> str:
    if value in (None, ""):
        return "N/A"
    return f"{float(value) * 100:.1f}%"


def _pick_gate(gates: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        gate = gates.get(key)
        if isinstance(gate, dict):
            return gate
    return {}


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
    opportunity = gate_result.get("opportunity_context", {})
    scorecard = gate_result.get("scorecard", {})
    signals = gate_result.get("signals", {})
    gates = gate_result.get("gates", {})
    current_price = valuation_result.get("current_price")
    hard_vetos = gate_result.get("hard_vetos", [])
    business_gate = _pick_gate(gates, "business_or_asset_truth", "business_truth")
    survival_gate = _pick_gate(gates, "survival_truth")
    governance_gate = _pick_gate(gates, "governance_truth", "quality_truth")
    regime_gate = _pick_gate(gates, "regime_cycle_truth")
    valuation_gate = _pick_gate(gates, "valuation_floor_truth", "valuation_truth")
    realization_gate = _pick_gate(gates, "realization_truth", "catalyst_truth")
    floor_case = valuation_result.get("floor_case", valuation_result.get("bear_case", {}))
    normalized_case = valuation_result.get("normalized_case", valuation_result.get("base_case", {}))
    recognition_case = valuation_result.get("recognition_case", valuation_result.get("bull_case", {}))
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
        f"- position state: {gate_result.get('position_state', 'reject')}",
        f"- flow stage: {gate_result.get('flow_stage', 'latent')}",
        "",
        "## 2. Why this stock may be mispriced",
        f"- what the market likely sees: {' '.join(synthesis_result.get('market_perception', []))}",
        f"- what the market may be missing: {' '.join(synthesis_result.get('what_market_misses', []))}",
        f"- why the gap may close: {synthesis_result.get('why_gap_may_close', 'No clear rerating path yet.')}",
        "",
        "## 3. Opportunity type",
        f"- primary type: {opportunity.get('primary_label', 'Unknown')}",
        f"- why this is the right type: {opportunity.get('reason', 'Type signal is weak.')}",
        f"- why the other types are secondary or not primary: {', '.join(opportunity.get('secondary_types', [])) or 'No strong secondary type surfaced.'}",
        "",
        "## 4. Business / asset truth",
        f"- business model: {normalize_text(scan_data.get('company_profile', {}).get('data', {}).get('主营业务')) or normalize_text(scan_data.get('company_profile', {}).get('data', {}).get('涓昏惀涓氬姟')) or 'N/A'}",
        f"- revenue and profit engine: dominant segment is {signals.get('purity', {}).get('top_segment') or 'unclear'}",
        f"- key operating drivers: {signals.get('moat', {}).get('reason', 'No durable edge surfaced.')}",
        f"- what must go right: {business_gate.get('reason', 'Need clearer business evidence.')}",
        "",
        "## 5. Survival truth",
        f"- debt and liquidity: {survival_gate.get('reason', 'Need explicit balance-sheet evidence.')}",
        f"- downside resilience: status={survival_gate.get('status', 'unknown')}",
        f"- what can break first: {hard_vetos[0] if hard_vetos else 'Balance-sheet stress or failed catalyst execution.'}",
        "",
        "## 6. Governance truth",
        f"- moat / advantage: {signals.get('moat', {}).get('reason', 'No moat claim yet.')}",
        f"- management and capital allocation: {signals.get('management', {}).get('verdict', 'unknown')}",
        f"- governance issues: {', '.join(signals.get('management', {}).get('red_flags', [])) or 'None surfaced from current text.'}",
        f"- return and cash flow quality: {governance_gate.get('reason', 'Mixed governance signal.')}",
        "",
        "## 7. Regime / cycle truth",
        f"- macro / rates / policy: {regime_gate.get('reason', 'No strong regime read.')}",
        f"- industry cycle position: {signals.get('bottom_pattern', {}).get('signal', 'mixed')}",
        f"- company transmission point: {opportunity.get('primary_label', 'Unknown')} names transmit through their core driver first.",
        f"- timing implications: status={regime_gate.get('status', 'unknown')}",
        "",
        "## 8. Valuation truth",
        "### Floor case",
        f"- assumptions: {', '.join(floor_case.get('assumptions', [])) or 'N/A'}",
        f"- valuation method: {floor_case.get('valuation_method', 'N/A')}",
        f"- implied value range: {_fmt_price(floor_case.get('implied_price'))}",
        "",
        "### Normalized case",
        f"- assumptions: {', '.join(normalized_case.get('assumptions', [])) or 'N/A'}",
        f"- valuation method: {normalized_case.get('valuation_method', 'N/A')}",
        f"- implied value range: {_fmt_price(normalized_case.get('implied_price'))}",
        "",
        "### Recognition case",
        f"- assumptions: {', '.join(recognition_case.get('assumptions', [])) or 'N/A'}",
        f"- valuation method: {recognition_case.get('valuation_method', 'N/A')}",
        f"- implied value range: {_fmt_price(recognition_case.get('implied_price'))}",
        "",
        f"- current price versus normalized value: current={_fmt_price(current_price)}, normalized={_fmt_price(normalized_case.get('implied_price'))}",
        f"- floor protection: {_fmt_pct(valuation_result.get('summary', {}).get('floor_protection'))}",
        f"- normalized upside: {_fmt_pct(valuation_result.get('summary', {}).get('normalized_upside'))}",
        f"- recognition upside: {_fmt_pct(valuation_result.get('summary', {}).get('recognition_upside'))}",
        f"- wind dependency: {_fmt_pct(valuation_result.get('summary', {}).get('wind_dependency'))}",
        f"- what expectations are priced in: {valuation_result.get('summary', {}).get('priced_in', 'unknown')}",
        f"- valuation floor gate: {valuation_gate.get('reason', 'Need cleaner floor evidence.')}",
        "",
        "## 9. Realization truth",
        f"- near-term catalysts: {', '.join(signals.get('catalyst', {}).get('catalysts', [])[:2]) or 'None explicit'}",
        f"- medium-term catalysts: {', '.join(signals.get('catalyst', {}).get('catalysts', [])[2:]) or 'Need stronger value-unlock path'}",
        f"- what can unlock value: {signals.get('catalyst', {}).get('reason', 'No concrete catalyst surfaced.')}",
        f"- flow setup: {signals.get('flow', {}).get('reason', 'No strong flow evidence yet')}",
        f"- realization status: {realization_gate.get('status', 'unknown')} | position={gate_result.get('position_state', 'reject')}",
        f"- what can delay value realization: weak flow setup or failing survival/business gates",
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
            f"- ideal buy zone: near the floor-to-normalized zone while position_state is {gate_result.get('position_state', 'reject')}",
            f"- starter zone: {_fmt_price(normalized_case.get('implied_price'))} or below with improving evidence",
            "- add conditions: move from cold_storage to ready, or from ready to attack with stronger flow confirmation",
            "- trim conditions: price reaches normalized-to-recognition range without evidence upgrade",
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
                f"  state: {item.get('position_state', 'reject')} | flow: {item.get('flow_stage', 'latent')}",
                f"  catalysts: {', '.join(item['catalysts']) or 'N/A'}",
                f"  risks: {', '.join(item['risks']) or 'N/A'}",
                f"  why passed: {item['why_passed']}",
                f"  next step: {item['next_step']}",
            ]
        )
    lines.extend(["", "## 5. Secondary watchlist"])
    for item in secondary_watchlist:
        lines.append(
            f"- {item['ticker']} / {item['company_name']} | {item['opportunity_type']} | "
            f"{item['score']}/100 | {item.get('position_state', 'reject')} | {item['thesis']}"
        )
    lines.extend(["", "## 6. Rejected ideas"])
    for item in rejected:
        lines.append(f"- {item['ticker']} / {item['company_name']}: {item['reason']}")
    lines.extend(["", "## 7. Ranking table", "", "| Name | Type | Score | Edge summary | Next action |", "| --- | --- | --- | --- | --- |"])
    for item in ranking_rows:
        lines.append(
            f"| {item['company_name']} | {item['opportunity_type']} | {item['score']} | "
            f"{item['thesis']} | {item.get('next_step', item.get('reason', 'watch'))} |"
        )
    lines.extend(["", "## 8. Deep-dive queue"])
    for item in priority_shortlist[:3]:
        lines.append(f"- {item['ticker']} / {item['company_name']}: deep dive first because {item['why_passed']}")
    content = "\n".join(lines)
    report_path.write_text(content, encoding="utf-8")
    return {"report_path": str(report_path), "content": content}
