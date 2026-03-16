---
name: single-stock-deep-dive
description: Perform a full deep investment analysis on one specific stock using a whole-market investment framework: value investing, cycle/regime analysis, and reversible-turnaround logic. Use when the user asks for a deep dive, full evaluation, valuation memo, scoring, or action plan for a specific company or ticker. Do not use for broad market scanning.
---

# Single Stock Deep Dive

## Purpose

This skill is for deep analysis of one specific stock.

Its job is to transform a stock idea into a disciplined investment memo:

- what the asset is
- why the market may be mispricing it
- what the realistic valuation range is
- what can make the thesis work
- what can kill the thesis
- what action, if any, should be taken

This skill must not turn into a broad market screener.
It is for one company at a time.

The framework combines:

1. Value investing
2. Cycle / regime analysis
3. Reversible distress logic

## When to use

Use this skill when the user asks things like:

- Analyze this stock deeply
- Evaluate this company
- Tell me whether this stock is worth buying
- Build a full investment memo
- Score this company
- Give me buy zone / sell zone / action plan
- Analyze this turnaround / cyclical / value stock
- Judge whether this company fits a value + cycle + turnaround framework

## When NOT to use

Do not use this skill when:

- the user wants a market-wide scan
- the user wants a shortlist of many stocks
- the task is mainly about portfolio sizing across many names
- the task is about rewriting or summarizing non-investment text only

In those cases, use a market scanner or another workflow.

## Core principle

This skill must decide what kind of opportunity this stock is before choosing the valuation lens.

Never analyze all stocks with the same template.
Always begin with opportunity typing.

Primary types:

1. Compounder
2. Cyclical
3. Turnaround
4. Asset play
5. Special situation

## Universal six-gate framework

Every stock must be judged through these six gates:

1. Business truth
2. Survival truth
3. Quality truth
4. Regime / cycle truth
5. Valuation truth
6. Catalyst truth

## Hard veto rules

If any of these is strongly true, state it clearly and lean toward rejection:

- cannot understand the business sufficiently
- cannot estimate normal earning power at all
- balance sheet survival is questionable
- thesis depends mainly on hope or narrative
- distress looks permanent, not reversible
- management is chronically untrustworthy
- current price already discounts the optimistic scenario
- there is no plausible path to value realization

Do not hide behind neutrality.

## Required workflow

Follow this order:

1. Identify the company correctly
2. Opportunity typing
3. Business model
4. Quality and governance
5. Survival and downside
6. Regime and cycle position
7. Valuation: bear / base / bull
8. Catalyst map
9. Anti-thesis and falsification
10. Final action framework

## Scoring framework

Use this scoring system:

- Opportunity type clarity: 5
- Business quality: 20
- Survival boundary: 15
- Management and capital allocation: 10
- Regime / cycle position: 15
- Valuation and margin of safety: 20
- Catalyst and value realization path: 10
- Market structure and tradability: 5

Interpretation:

- 85 to 100: high conviction / strong candidate
- 75 to 84: reasonable candidate / starter possible
- 65 to 74: watchlist / incomplete edge
- below 65: reject or no action

## Required output format

Use this structure unless the user requests otherwise:

1. Executive view
2. Why this stock may be mispriced
3. Opportunity type
4. Business truth
5. Survival truth
6. Quality truth
7. Regime / cycle truth
8. Valuation truth
9. Catalyst truth
10. Anti-thesis
11. Falsification points
12. Scorecard
13. Action plan
14. Bottom line

## Writing rules

- Be analytical, not promotional.
- Separate facts from inference.
- Do not bury major risks.
- Do not use false precision in valuation.
- If evidence is mixed, say so.
- If the stock is not attractive, say so clearly.
- If timing is wrong but the business is interesting, say watch, not buy.

## Implementation resources

- Use `scripts/engines/deep_sniper_engine.py` as the deep-dive entry point.
- Reuse shared adapters, tier-0 utilities, valuation logic, and report builders from `../shared/`.
- Make bear / base / bull valuation and falsification points mandatory in the final memo.
