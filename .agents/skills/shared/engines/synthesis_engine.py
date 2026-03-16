"""Narrative synthesis for the whole-market framework."""
from __future__ import annotations

from typing import Any

from utils.framework_utils import normalize_text


def build_investment_synthesis(
    stock_code: str,
    company_name: str,
    gate_result: dict[str, Any],
    valuation_result: dict[str, Any],
) -> dict[str, Any]:
    opportunity = gate_result.get("opportunity_context", {})
    signals = gate_result.get("signals", {})
    scorecard = gate_result.get("scorecard", {})
    hard_vetos = gate_result.get("hard_vetos", [])
    catalysts = signals.get("catalyst", {}).get("catalysts", [])
    moat_reason = signals.get("moat", {}).get("reason", "")
    purity = signals.get("purity", {})

    market_perception = [
        f"The market likely sees {company_name} as a {opportunity.get('primary_label', 'mixed')} story with incomplete evidence.",
        f"Current score is {scorecard.get('total', 'N/A')}/100, which keeps the thesis out of automatic high-conviction territory.",
    ]
    what_market_misses = [
        f"Type classification is {opportunity.get('confidence', 'low')} confidence because {opportunity.get('reason', 'classification is still noisy')}.",
        f"Business concentration currently points to {purity.get('top_segment') or 'no dominant segment'} as the main earnings driver.",
    ]
    if moat_reason:
        what_market_misses.append(f"Potential moat angle: {moat_reason}.")
    if catalysts:
        what_market_misses.append(f"Concrete rerating path exists through: {', '.join(catalysts)}.")

    why_gap_may_close = "Catalysts are still weak."
    if catalysts:
        why_gap_may_close = f"The gap may close if {', '.join(catalysts)} converts from expectation into filings or earnings."

    anti_thesis = list(hard_vetos)
    for gate_name, gate in (gate_result.get("gates", {}) or {}).items():
        if gate.get("status") == "fail":
            anti_thesis.append(f"{gate_name}: {gate.get('reason')}")
    anti_thesis = anti_thesis or ["No single fatal flaw was found, but the evidence set is still incomplete."]

    falsification_points = [
        "Core business economics become less understandable rather than clearer.",
        "Balance sheet resilience weakens or equity erosion accelerates.",
        "Expected catalysts fail to appear within the stated timeframe.",
        "Price fully discounts the base case before evidence improves.",
    ]
    if valuation_result.get("summary", {}).get("priced_in") == "optimistic":
        falsification_points.append("The stock continues trading above the base-case value range without stronger fundamentals.")

    return {
        "stock_code": stock_code,
        "company_name": company_name,
        "market_perception": market_perception,
        "what_market_misses": what_market_misses,
        "why_gap_may_close": why_gap_may_close,
        "anti_thesis": anti_thesis,
        "falsification_points": falsification_points,
        "bottom_line": (
            f"This is primarily a {opportunity.get('primary_label', 'mixed')} opportunity. "
            f"It is {'not attractive' if hard_vetos else 'potentially attractive'} because "
            f"{normalize_text(opportunity.get('reason')) or 'the available evidence is still mixed'}."
        ),
    }
