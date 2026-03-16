---
name: market-opportunity-scanner
description: Scan a stock market or watchlist to find high-potential investment candidates across the whole market, not limited to any one sector. Use when the user asks to scan the market, shortlist stocks, rank opportunities, build a watchlist, or find candidates that fit value investing, cycle analysis, and reversible turnaround logic. Do not use for deep analysis of one specific stock.
---

# Market Opportunity Scanner

## Purpose

This skill is for broad market scanning.

Its job is to search a market, sector set, or watchlist and produce a ranked shortlist of candidate stocks that deserve further deep research.

This skill must not behave like a single-stock deep-dive skill.
Its goal is not to fully value one company.
Its goal is to filter the market, reject weak ideas quickly, and surface only the best candidates for deeper work.

The investment framework combines:

1. Value investing
   - Treat stocks as business ownership.
   - Focus on intrinsic value, normal earning power, management quality, and margin of safety.
2. Cycle analysis
   - Judge macro, industry, inventory, pricing, credit, rate, policy, and capex position.
   - Use cycle position as a filter, not as a substitute for company analysis.
3. Reversible distress / turnaround logic
   - Distinguish temporary pain from permanent impairment.
   - Prefer situations where assets are intact, solvency is acceptable, and a repair path exists.

This skill covers the whole market.
It must not be restricted to resource stocks, state-owned companies, or low-PB names.
Those can be valid candidates, but they are only subsets of a larger framework.

## When to use

Use this skill when the user asks things like:

- Scan the market for investable opportunities
- Find 3 to 10 promising stocks
- Build a shortlist
- Rank current candidates
- Find stocks that match a value + cycle + turnaround framework
- Search A-shares / Hong Kong / US stocks / a sector / a watchlist for potential opportunities
- Build a radar map or action list
- Tell me whether there is anything worth doing right now

## When NOT to use

Do not use this skill when:

- The user wants a full deep dive on one specific stock
- The user wants a full valuation memo on one company
- The user asks mainly for portfolio allocation, tax, options, or trading execution
- The user only wants a rewrite, summary, or translation
- The user asks about one narrow event on one company

In those cases, use the dedicated single-stock deep-dive workflow instead.

## Core principle

The objective is not to always return candidates.
The objective is to return only candidates with real research value.

If the market is unattractive, overvalued, crowded, or lacking sufficient evidence, the correct output is:

- no-action
- waitlist only
- not enough edge

Never force a shortlist just to satisfy the prompt.

## Opportunity type system

Every candidate must first be classified into one primary type:

1. Compounder
2. Cyclical
3. Turnaround
4. Asset play
5. Special situation

If a stock cannot be clearly typed, confidence must be reduced.

## Hard veto rules

Reject or heavily penalize candidates if any of the following is true:

- business is not understandable
- normal earning power cannot be estimated at all
- balance sheet survival is doubtful
- thesis depends mostly on storytelling with weak evidence
- distress looks permanent rather than reversible
- management repeatedly destroys shareholder value
- valuation already prices in the optimistic case
- liquidity is too poor for the intended use case
- core thesis cannot be supported by recent, credible evidence

These vetoes matter more than superficial cheapness.

## Scanning workflow

Follow this sequence:

1. Define the search universe.
2. Gather fresh evidence from filings, exchange disclosures, company materials, market data, and sector/macro sources.
3. Classify each candidate into a primary opportunity type.
4. Apply the six gates: business truth, survival truth, quality truth, regime/cycle truth, valuation truth, catalyst truth.
5. Score candidates on the eight-dimensional 100-point framework.
6. Rank and filter into priority shortlist, secondary watchlist, and reject / no-action.
7. Produce a deep-dive queue for the names that deserve immediate follow-up.

## Output format

Use this structure unless the user explicitly requests another format:

1. Scope
2. Market-level read
3. Results summary
4. Priority shortlist
5. Secondary watchlist
6. Rejected ideas
7. Ranking table
8. Deep-dive queue

## Writing rules

- Be decisive.
- Be comparative.
- Focus on edge, not trivia.
- Do not pretend certainty where evidence is weak.
- Do not recommend action merely because a stock is cheap.
- If current market conditions are poor, say so clearly.
- Prefer a small truthful shortlist over a long low-quality list.

## Implementation resources

- Use `scripts/engines/radar_scan_engine.py` as the scanner entry point.
- Reuse shared adapters, validators, and config from `../shared/`.
- Treat `no-action` as a valid outcome.
