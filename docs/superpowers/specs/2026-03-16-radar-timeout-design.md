# Radar Timeout Optimization Design

**Date:** 2026-03-16

**Goal:** Reduce cold-start runtime for market radar scans without changing final shortlist semantics, final gate semantics, valuation semantics, or opportunity scoring logic.

## Context

The current market radar path in [radar_scan_engine.py](D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py) scans the selected universe serially and calls `run_full_scan()` for every stock. That forces each name through the full adapter stack before the scanner knows whether the stock has any realistic chance of clearing the `secondary_score_cutoff`.

This became the main practical bottleneck after the BaoStock fallback work:

- the scanner can now survive `akshare` universe snapshot failures
- fallback universe ordering is now meaningful
- but full live radar scans still time out under real provider latency

The current radar output only needs:

- exact final scores for `priority_shortlist`
- exact final scores for `secondary_watchlist`
- a defensible rejection reason for names that are not worth deepening

It does **not** require exact final scores for every rejected stock.

## Non-Goals

- Do not change the final scoring formula.
- Do not change the final hard-veto semantics.
- Do not change deep-dive behavior.
- Do not redesign the provider layer in this step.
- Do not add thread-pool concurrency or day-cache persistence in this step.

## Recommended Approach

Introduce a two-stage radar scan:

1. a cheap partial scan that fetches only the fields needed to compute a safe score upper bound
2. a selective completion pass that fetches the missing expensive fields only for survivors

This keeps final shortlist/watchlist scoring exact while allowing safe early rejection of names that cannot mathematically reach the watchlist cutoff.

## Why This Approach

- It attacks cold-start latency directly.
- It preserves current output semantics where precision matters.
- It only changes the radar path, not the single-stock deep-dive path.
- It creates a stable foundation for later thread-pool and cache work.

## Design

### 1. Split radar scanning into two stages

Stage 1 runs a partial scan for every stock in the radar universe.

Stage 2 runs targeted completion only for stocks that survive Stage 1.

The radar path will no longer call `run_full_scan()` for every stock by default.

Instead it will:

1. build the universe as it does now
2. fetch a partial `scan_data` payload
3. evaluate partial gate dimensions and a safe upper bound
4. reject only names whose upper bound is strictly below the watchlist cutoff
5. enrich only the survivors with missing fields
6. run the existing full gate and valuation flow on the enriched survivors

### 2. Stage 1 partial scan fields

Stage 1 fetches only the fields that are already enough to evaluate most radar dimensions:

- `company_profile`
- `revenue_breakdown`
- `valuation_history`
- `stock_kline`
- `realtime_quote`

Stage 1 does **not** fetch:

- `financial_summary`
- `income_statement`
- `balance_sheet`

Rationale:

- `financial_summary` is not used by current radar ranking output
- `income_statement` and `balance_sheet` are the current blockers for exact survival scoring and some hard-veto branches

### 3. Add a partial gate evaluator

Add a new helper in [universal_gate.py](D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py):

- `evaluate_partial_gate_dimensions(stock_code, scan_data, *, opportunity_context=None, extra_texts=None) -> dict[str, Any]`

This helper does **not** replace `evaluate_universal_gates()`.

Responsibilities:

- compute dimension-by-dimension partial scores using only the currently available fields
- mark each dimension as `full`, `partial`, or `none`
- declare which missing fields are required to make each dimension fully confident
- compute `known_total`
- compute `unknown_ceiling`
- compute `score_upper_bound`
- surface hard vetoes that are decidable from Stage 1 data
- surface hard vetoes that are blocked until Stage 2 data exists

`evaluate_universal_gates()` remains the source of truth for the exact final score and final veto list.

### 4. Partial dimension schema

Each dimension entry returned by `evaluate_partial_gate_dimensions()` should follow this shape:

```python
{
    "score": 8.0,
    "max": 20.0,
    "confidence": "partial",  # full | partial | none
    "requires": ["income_statement"],
    "reason": "short explanation of what is known and what is missing",
}
```

The `requires` field is part of the design, not optional. It lets the second-stage enrichment logic derive missing fields from the partial result rather than hard-coding them forever.

### 5. Upper-bound formula

Stage 1 rejection is based on a safe upper bound, not a guessed final score.

Definitions:

```python
known_total = sum(d["score"] for d in dimensions.values())
unknown_ceiling = sum(
    d["max"] - d["score"]
    for d in dimensions.values()
    if d["confidence"] != "full"
)
score_upper_bound = known_total + unknown_ceiling
```

Current implementation note:

- with the gate logic as of 2026-03-16, `income_statement` and `balance_sheet` only affect `survival` and two hard-veto branches
- so `unknown_ceiling` will often collapse to the remaining `survival` headroom in practice

The design still uses the general formula because it remains correct if later gate logic adds financial-data dependence to other dimensions.

### 6. Stage 1 rejection rules

A stock can be rejected at Stage 1 only if at least one of these is true:

- `decidable_hard_vetos` is non-empty
- `score_upper_bound < secondary_score_cutoff`

Boundary rule:

- `score_upper_bound == secondary_score_cutoff` must **not** be rejected
- the boundary is inclusive for advancement into Stage 2

Rejected names should carry an explicit radar reason such as:

- `safe_prefilter_reject: upper_bound 54.0 < 65`
- `safe_prefilter_reject: business is not understandable`

### 7. Stage 1 hard-veto subset

`evaluate_partial_gate_dimensions()` must distinguish between:

- `decidable_hard_vetos`
- `blocked_hard_vetos`

With the current implementation, Stage 1 can safely decide:

- `business is not understandable`
- `management credibility is materially impaired`

With the current implementation, Stage 1 cannot safely decide and must defer:

- `normal earning power cannot be estimated`
- `balance sheet survival is questionable`

If later gate logic adds more veto branches, their Stage 1 status must be explicit in the partial evaluator.

### 8. Stage 2 enrichment

Stage 2 runs only for stocks that survive Stage 1.

It should derive the missing fetch list from the partial result:

```python
fields_to_fetch = sorted(
    {
        field
        for dimension in partial_result["dimensions"].values()
        if dimension["confidence"] != "full"
        for field in dimension.get("requires", [])
    }
)
```

With the current implementation, this will usually resolve to:

- `income_statement`
- `balance_sheet`

Stage 2 then:

1. fetches the missing fields
2. merges them into the existing partial `scan_data`
3. calls the existing `evaluate_universal_gates()`
4. calls the existing `build_three_case_valuation()`
5. builds the final candidate payload with the existing radar logic

This preserves final shortlist and watchlist semantics.

### 9. Radar engine changes

Update [radar_scan_engine.py](D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py) to use the new flow.

Recommended structure:

- add a helper to fetch partial scan data for one stock
- add a helper to enrich partial scan data with missing fields
- add a helper to build a rejected payload from a partial rejection result
- keep `_candidate_payload()` as the full-score path for survivors

The engine should also stop fetching `financial_summary` for radar scans.

`financial_summary` can remain in `run_full_scan()` for other use cases, but the radar path should not pay for it if it does not use it.

### 10. Adapter-layer changes

The radar optimization should avoid forcing full-scan orchestration where it is not needed.

Recommended additions in [akshare_adapter.py](D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py):

- a small `RADAR_PARTIAL_STEPS` mapping for Stage 1 fetches
- a small `RADAR_EXPENSIVE_STEPS` mapping for Stage 2 enriches
- shared helper logic for retrying a named fetch function and applying cached fallback if available

This keeps the radar engine simple and avoids duplicating retry / fallback behavior in multiple places.

No change is required to the existing full-scan public contract used by other paths.

## Data Contracts

### Partial `scan_data`

Stage 1 `scan_data` should use the same top-level shape as full scans:

```python
{
    "company_profile": {...},
    "revenue_breakdown": {...},
    "valuation_history": {...},
    "stock_kline": {...},
    "realtime_quote": {...},
}
```

Missing fields are simply absent until Stage 2 enrichment.

No explicit `mode` flag should be added.

### Partial evaluation result

Suggested shape:

```python
{
    "opportunity_context": {...},
    "dimensions": {
        "type_clarity": {...},
        "business_quality": {...},
        "survival": {...},
        "management": {...},
        "regime_cycle": {...},
        "valuation": {...},
        "catalyst": {...},
        "market_structure": {...},
    },
    "known_total": 42.0,
    "unknown_ceiling": 23.0,
    "score_upper_bound": 65.0,
    "decidable_hard_vetos": [],
    "blocked_hard_vetos": [
        "normal earning power cannot be estimated",
        "balance sheet survival is questionable",
    ],
}
```

## Testing Strategy

Add focused tests in [test_investment_framework.py](D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py).

Required coverage:

- partial evaluator returns dimension metadata with `score`, `max`, `confidence`, and `requires`
- names with `score_upper_bound < 65` are rejected before Stage 2
- names with `score_upper_bound == 65` advance to Stage 2
- Stage 1 decidable hard vetoes reject immediately
- Stage 1 blocked hard vetoes do not reject until Stage 2 data exists
- Stage 2 fetch list is derived from `requires` rather than hard-coded logic
- radar path no longer fetches `financial_summary`
- a survivor’s final payload matches the existing full-score path
- BaoStock fallback still works through the two-stage radar path

## Verification

After implementation:

- run the shared test suite
- force a radar path where `ak.stock_zh_a_spot_em()` fails and BaoStock fallback is used
- run a small live radar sample with Stage 1 enabled
- confirm that Stage 1 rejections are labeled as safe prefilter rejects
- confirm that shortlist/watchlist names still have exact final scores

Success criteria:

- cold-start radar scans reject obvious non-candidates without fetching full expensive data
- shortlist and watchlist semantics remain unchanged
- no stock is rejected at Stage 1 when its safe upper bound still reaches the watchlist cutoff

## Risks

### Risk: partial evaluator drifts from full evaluator

If Stage 1 dimension logic diverges from the meaning of the full score, upper bounds become misleading.

Mitigation:

- keep Stage 1 logic limited and explicit
- keep `evaluate_universal_gates()` unchanged as the final source of truth
- add tests that compare survivor outputs against the current full path

### Risk: hidden data dependencies are added later

If future score dimensions start depending on additional fields, a hard-coded Stage 2 fetch list can become stale.

Mitigation:

- include `requires` on every partial dimension
- derive Stage 2 fetches from partial results

### Risk: rejection reasons become harder to interpret

Users need to distinguish exact rejection from safe prefilter rejection.

Mitigation:

- use explicit rejection reason strings
- keep shortlist/watchlist exact

## Files Expected To Change

- `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

## Decision

Proceed with a two-stage radar scan that uses partial dimension auditing and a safe upper-bound rejection rule, while preserving exact final scoring for all survivors.
