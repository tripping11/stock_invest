# VCRF OS 2.0 Design

**Date:** 2026-03-16

**Goal:** Replace the current one-dimensional scoring path with an A-share-first VCRF operating system built around `Underwrite x Realization`, a five-state machine, layered radar sampling, and route-aware valuation while preserving existing active entrypoints during migration.

## Context

The active pipeline already moved away from the archived crocodile stack and currently centers on:

- [`universal_gate.py`](D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py)
- [`valuation_engine.py`](D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py)
- [`report_engine.py`](D:/A价投+周期/.agents/skills/shared/engines/report_engine.py)
- [`radar_scan_engine.py`](D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py)
- [`deep_sniper_engine.py`](D:/A价投+周期/.agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py)

The archived files under [`.agents/_archive/a_stock_sniper`](D:/A价投+周期/.agents/_archive/a_stock_sniper) remain useful as historical references, but they must not be revived as the active architecture.

This design keeps A-shares as the first-class implementation target, preserves U.S. market interfaces where useful, and explicitly adds missing pieces that were previously only implied:

- route-aware sector data routing
- dynamic `primary_type` detection
- five-state transition discipline
- component-level degradation rules
- a spike gate for `detect_big_bath()`
- a lightweight monitor for attack-book harvest candidates

## Non-Goals

- Do not restore archived crocodile configs as active dependencies.
- Do not require full U.S. parity in the first implementation slice.
- Do not make `HARVEST` dependent on tick-level or intraday infrastructure.
- Do not force every missing data path to become blocking if a conservative downgrade is sufficient.

## Data Contracts

### 1. Driver Stack

Every name must resolve into one `Driver Stack` before scoring:

```json
{
  "market": "A-share|US",
  "sector_route": "core_resource|rigid_shovel|core_military|financial_asset|consumer|tech|unknown",
  "primary_type": "compounder|cyclical|turnaround|asset_play|special_situation",
  "primary_type_confidence": 0.0,
  "modifiers": {
    "cycle_state": "trough|repair|expansion|peak",
    "repair_state": "none|stabilizing|repairing|confirmed",
    "distress_source": "cyclical|operational|balance_sheet|governance|one_off",
    "realization_path": "repricing|asset_unlock|mna|buyback|policy|capital_return|institutional_entry",
    "flow_stage": "abandoned|latent|ignition|trend|crowded",
    "elasticity_bucket": "mega|large|mid|small|micro"
  },
  "special_tags": [
    "st",
    "star_st",
    "delisting_risk",
    "reorg_candidate",
    "state_owned"
  ]
}
```

Design rules:

- `sector_route` drives data routing and valuation anchor selection.
- `primary_type` drives weight templates and state-machine interpretation.
- `special_tags` do not bypass scoring, but they can alter hard-veto logic and route assignment.
- `ST` and `*ST` names are not hard-filtered out of the universe; they are tagged and routed.

Driver Stack fill order is mandatory:

1. resolve `sector_route` from `sector_classification.yaml`
2. compute route-only preliminary modifiers that do not depend on `primary_type`, especially `cycle_state`
3. determine `primary_type` from `sector_route + preliminary_cycle_state + financials + tags + events`
4. fill the remaining modifiers and special tags

This avoids a circular dependency where `primary_type` would otherwise depend on a modifier that itself depends on `primary_type`.

### 2. Dual-Axis Output

The core scoring contract is:

```json
{
  "underwrite_axis": {
    "score": 0.0,
    "confidence": "full|partial|degraded",
    "components": {
      "intrinsic_value_floor": {},
      "survival_boundary": {},
      "governance_anti_fraud": {},
      "business_or_asset_quality": {},
      "normalized_earnings_power": {}
    }
  },
  "realization_axis": {
    "score": 0.0,
    "confidence": "full|partial|degraded",
    "components": {
      "repair_state": {},
      "regime_cycle_position": {},
      "marginal_buyer_probability": {},
      "flow_confirmation": {},
      "elasticity": {},
      "catalyst_quality": {}
    }
  }
}
```

Each component returns:

```json
{
  "score": 0.0,
  "confidence": "full|partial|degraded",
  "availability": "full|partial|missing",
  "reason": "short explanation",
  "inputs_used": ["field_a", "field_b"]
}
```

### 3. State Machine Output

Legal states are fixed:

- `REJECT`
- `COLD_STORAGE`
- `READY`
- `ATTACK`
- `HARVEST`

The full state contract is:

```json
{
  "prev_state": "NEW|REJECT|COLD_STORAGE|READY|ATTACK|HARVEST",
  "state": "REJECT|COLD_STORAGE|READY|ATTACK|HARVEST",
  "transition_allowed": true,
  "transition_reason": "why the transition happened or was capped",
  "harvest_candidate": false
}
```

`NEW` is a required pseudo-state for first-seen names. It prevents accidental blocking of first-pass `READY` or `ATTACK` classifications.

### 4. State History Record

State history is append-only JSONL:

`data/processed/vcrf_state_history.jsonl`

Each line stores:

```json
{
  "date": "2026-03-16",
  "code": "600348",
  "prev_state": "READY",
  "next_state": "ATTACK",
  "underwrite_score": 78.0,
  "realization_score": 72.0,
  "reason": "flow ignition confirmed after repair evidence"
}
```

## Configuration Files

### 1. `sector_classification.yaml`

Extend the active file to support:

- `sector_routes`
- `primary_type_hints`
- `realization_path_keywords`
- `special_tag_rules`
- company overrides where needed

This file becomes the first routing layer for `sector_route`, not the full scoring source of truth.

### 2. `vcrf_weights.yaml`

The weight file uses inheritance, not a full `sector_route x primary_type` Cartesian product.

Structure:

```yaml
_meta:
  enforce_normalization: true
  tolerance: 0.001

base_templates:
  compounder:
    underwrite:
      intrinsic_value_floor: 0.18
      survival_boundary: 0.22
      governance_anti_fraud: 0.18
      business_or_asset_quality: 0.20
      normalized_earnings_power: 0.22
    realization:
      repair_state: 0.10
      regime_cycle_position: 0.10
      marginal_buyer_probability: 0.15
      flow_confirmation: 0.15
      elasticity: 0.10
      catalyst_quality: 0.40
  cyclical:
    underwrite:
      intrinsic_value_floor: 0.20
      survival_boundary: 0.25
      governance_anti_fraud: 0.10
      business_or_asset_quality: 0.20
      normalized_earnings_power: 0.25
    realization:
      repair_state: 0.20
      regime_cycle_position: 0.30
      marginal_buyer_probability: 0.05
      flow_confirmation: 0.15
      elasticity: 0.20
      catalyst_quality: 0.10
  turnaround:
    underwrite:
      intrinsic_value_floor: 0.18
      survival_boundary: 0.34
      governance_anti_fraud: 0.18
      business_or_asset_quality: 0.10
      normalized_earnings_power: 0.20
    realization:
      repair_state: 0.30
      regime_cycle_position: 0.10
      marginal_buyer_probability: 0.10
      flow_confirmation: 0.15
      elasticity: 0.10
      catalyst_quality: 0.25
  asset_play:
    underwrite:
      intrinsic_value_floor: 0.30
      survival_boundary: 0.18
      governance_anti_fraud: 0.18
      business_or_asset_quality: 0.14
      normalized_earnings_power: 0.20
    realization:
      repair_state: 0.10
      regime_cycle_position: 0.10
      marginal_buyer_probability: 0.15
      flow_confirmation: 0.10
      elasticity: 0.15
      catalyst_quality: 0.40
  special_situation:
    underwrite:
      intrinsic_value_floor: 0.24
      survival_boundary: 0.22
      governance_anti_fraud: 0.22
      business_or_asset_quality: 0.12
      normalized_earnings_power: 0.20
    realization:
      repair_state: 0.12
      regime_cycle_position: 0.08
      marginal_buyer_probability: 0.20
      flow_confirmation: 0.10
      elasticity: 0.10
      catalyst_quality: 0.40

sector_overrides:
  core_resource:
    underwrite:
      normalized_earnings_power: -0.05
      intrinsic_value_floor: +0.05
  rigid_shovel:
    realization:
      catalyst_quality: +0.05
      elasticity: -0.05
  core_military:
    underwrite:
      business_or_asset_quality: +0.05
      normalized_earnings_power: -0.05
  financial_asset:
    underwrite:
      intrinsic_value_floor: +0.05
      business_or_asset_quality: -0.05
  consumer:
    realization:
      catalyst_quality: +0.05
      elasticity: -0.05
  tech:
    realization:
      marginal_buyer_probability: +0.05
      catalyst_quality: -0.05
```

Rules:

- `base_templates` are keyed only by `primary_type`.
- `sector_overrides` apply signed deltas after base weights are loaded.
- After overlay application, each axis must still sum to `1.0 +/- tolerance`.
- Loader behavior on failed normalization: raise an exception. Do not silently renormalize.
- The five `base_templates` above are initial calibration values and must be treated as explicit defaults, not placeholders.

### 3. `vcrf_state_machine.yaml`

This file owns:

- score thresholds
- `flow_stage_order`
- legal transition matrix
- harvest candidate rules

Required fields:

```yaml
allowed_transitions:
  NEW: [REJECT, COLD_STORAGE, READY, ATTACK, HARVEST]
  REJECT: [COLD_STORAGE]
  COLD_STORAGE: [READY, REJECT]
  READY: [ATTACK, COLD_STORAGE]
  ATTACK: [HARVEST, READY]
  HARVEST: [COLD_STORAGE]

harvest_candidate:
  consecutive_closes_above_recognition: 3
  breakout_day_return_pct: 0.10
  breakout_close_to_recognition_ratio: 0.95
  require_flow_stage_deterioration_to: crowded
```

### 4. `vcrf_degradation.yaml`

Degradation is component-specific.

Each component declares:

- `on_missing`
- optional `fallback_score`
- optional `cap_state`
- optional diagnostic flag

Hard-risk components like `survival_boundary` can cap the maximum state; soft-upside components like Level-2 flow evidence may only degrade confidence.

### 5. `valuation_discipline.yaml`

Keep the active file, but evolve it to add:

- route-aware floor anchors
- route-aware normalized anchors
- recognition-case methods
- state-machine threshold defaults if needed by valuation consumers

`normalized_case` is never chosen by `primary_type` alone. It must depend on `sector_route`.

## Underwrite Axis Pseudocode

### 1. `intrinsic_value_floor`

Purpose: quantify the downside floor if the wind never comes.

Route-aware anchor selection:

- `core_resource` / `rigid_shovel`: stressed book or replacement-cost proxy
- `financial_asset`: `min(stressed_nav, stressed_book)`
- `consumer` / `tech`: no-growth owner-earnings anchor
- `unknown`: conservative stressed book / NCAV fallback

Mapping:

```python
floor_protection = floor_price / current_price
score = map_linear(
    floor_protection,
    bands=[
        (0.60, 20),
        (0.75, 45),
        (0.85, 65),
        (1.00, 85),
        (1.20, 100),
    ],
)
```

### 2. `survival_boundary`

Purpose: detect whether the business can survive stress.

Inputs:

- `cashflow_statement` (new required scan step)
- `balance_sheet`
- `income_statement`

Formula:

```python
coverage = ocf_ttm / short_term_interest_bearing_debt
net_cash_ratio = (cash_and_equivalents - short_term_interest_bearing_debt) / total_assets
z_score = altman_z(...)
equity_positive = total_equity > 0

score = weighted_sum(
    coverage_band(coverage),
    liquidity_band(net_cash_ratio),
    z_band(z_score),
    20 if equity_positive else 0,
)
```

Hard veto examples:

- negative equity with no recap path
- very weak cash coverage plus visible debt wall

### 3. `governance_anti_fraud`

Purpose: penalize names that look optically cheap but are operationally untrustworthy.

Signals:

- audit opinion
- frequent auditor changes
- cash/debt mismatch
- related-party intensity
- CNINFO event flags for penalties, occupation, fraud, or control disputes

Scoring style:

```python
score = 100
score -= penalty_for_audit_opinion(...)
score -= penalty_for_auditor_turnover(...)
score -= penalty_for_cash_debt_mismatch(...)
score -= penalty_for_related_party_ratio(...)
score -= penalty_for_regulatory_events(...)
score = clamp(score, 0, 100)
```

### 4. `business_or_asset_quality`

Purpose: verify that the company owns understandable economics or real assets.

Signals:

- dominant segment purity
- asset verifiability
- route fit
- moat / license / mineral-right evidence

Formula:

```python
score = weighted_sum(
    purity_score,
    route_fit_score,
    moat_or_asset_verification_score,
    segment_stability_score,
)
```

### 5. `normalized_earnings_power`

Purpose: estimate mid-cycle earnings power without using current reported earnings blindly.

Route logic:

- `core_resource`: commodity median or 7-year profit median
- `rigid_shovel`: capex-mid-cycle anchor
- `core_military`: 3-year average revenue times median margin
- `consumer` / `tech`: 7-10 year ROE median or owner earnings
- `financial_asset`: mid-cycle ROE / ROA on current equity

Scoring maps normalized value versus current price after route-specific haircuts.

## Realization Axis Pseudocode

### 1. `repair_state`

Inputs:

- 3-year financial trend
- `detect_big_bath()` output if available
- margin stabilization
- OCF trend

Output states:

- `none`
- `stabilizing`
- `repairing`
- `confirmed`

State-to-score mapping:

```python
REPAIR_STATE_SCORE = {
    "none": 20,
    "stabilizing": 45,
    "repairing": 70,
    "confirmed": 90,
}

score = REPAIR_STATE_SCORE[repair_state]
if detect_big_bath_verdict == "big_bath":
    score += 5
if ocf_turn_positive and gross_margin_recovering:
    score += 5
score = clamp(score, 0, 100)
```

### 2. `regime_cycle_position`

Inputs:

- sector route
- commodity / capex / defense / policy context
- price drawdown and base-building pattern

`core_resource` and `rigid_shovel` depend heavily on cycle context; `consumer` and `tech` depend more on repair and demand normalization.

Scoring structure:

```python
score = weighted_sum(
    route_cycle_score,        # commodity / capex / policy / order-cycle context
    price_position_score,     # deep drawdown, base-building, rebound profile
    inventory_or_demand_score # route-specific demand tightening / destocking / recovery
)
```

Example mapping:

- `trough`: `70-85`
- `repair`: `55-75`
- `expansion`: `40-65`
- `peak`: `10-35`

### 3. `marginal_buyer_probability`

Inputs differ by market.

A-share first implementation:

- shareholder-count trend
- ownership concentration trend when available
- northbound / institutional proxies when available
- CNINFO event flags

U.S. implementation remains interface-compatible but can degrade to partial data.

Scoring structure:

```python
score = weighted_sum(
    shareholder_trend_score,
    ownership_concentration_score,
    institutional_flow_proxy_score,
    event_buyer_signal_score,
)
```

For A-shares:

- shareholder count falling consistently: positive
- concentration improving: positive
- northbound / institutional accumulation proxy: positive
- insider reduction or risk-disposal events: negative

Map final score into `0-100`.

### 4. `flow_confirmation`

Level 1:

- `volume_ratio_20_vs_120`
- drawdown / rebound profile
- recent turnover expansion

Level 2:

- buyback cancellation signals
- shareholder increase announcements
- asset injection / restructuring / approval signals

Missing Level 2 does not block the axis. It degrades confidence and may reduce the score.

Scoring structure:

```python
level1_score = weighted_sum(
    volume_ratio_score,      # 0.40
    drawdown_rebound_score,  # 0.35
    turnover_expansion_score # 0.25
)

level2_bonus = weighted_sum(
    buyback_or_cancellation_score,
    shareholder_increase_score,
    asset_injection_or_approval_score,
)

score = level1_score + level2_bonus
score = clamp(score, 0, 100)
```

Suggested Level 1 mappings:

- `volume_ratio_20_vs_120 < 0.8`: `15`
- `0.8-1.1`: `35`
- `1.1-1.5`: `60`
- `1.5-2.0`: `80`
- `>2.0`: `90`

### 5. `elasticity`

Use free-float size, turnover, and crowding sensitivity. Small size is a multiplier, not a hard gate.

Scoring structure:

```python
size_score = banded_free_float_cap_score(...)
turnover_score = banded_turnover_score(...)
crowding_penalty = banded_crowding_penalty(...)

score = clamp(0.60 * size_score + 0.25 * turnover_score - 0.15 * crowding_penalty, 0, 100)
```

Suggested free-float-cap mapping for A-shares:

- `micro`: `95`
- `small`: `80`
- `mid`: `60`
- `large`: `35`
- `mega`: `20`

### 6. `catalyst_quality`

Explicitly scored by realization path:

- `asset_unlock`
- `mna`
- `buyback`
- `policy`
- `capital_return`
- `institutional_entry`

Scoring structure:

```python
score = max_path_score(realization_paths_detected)
if catalyst_is_signed_or_approved:
    score += 10
if catalyst_is_only_keyword_level:
    score -= 10
score = clamp(score, 0, 100)
```

Default path-quality anchors:

- `asset_unlock`, `mna`: `75-90`
- `buyback`, `capital_return`: `60-80`
- `policy`: `50-75`
- `institutional_entry`: `45-70`

## Primary Type Detection

`primary_type` is dynamic, not a static label lookup.

```python
def determine_primary_type(
    sector_route,
    preliminary_cycle_state,
    financials_3y,
    tags,
    events,
    big_bath_result,
) -> tuple[str, float]:
    if "st" in tags or "star_st" in tags:
        return "special_situation", 0.90
    if losses_2y and (
        big_bath_result["verdict"] == "big_bath"
        or repair_evidence_from_financial_trend
    ):
        return "turnaround", 0.80
    if sector_route in {"core_resource", "rigid_shovel"} and preliminary_cycle_state in {"trough", "repair"}:
        return "cyclical", 0.75
    if deep_discount_to_nav and asset_unlock_path:
        return "asset_play", 0.75
    return "compounder", 0.60
```

If `big_bath_result["verdict"] == "inconclusive"`, the turnaround branch may still trigger via repair evidence from margins, OCF, and one-off-loss proxies. `detect_big_bath()` is informative, not a single point of failure.

Priority ordering is deliberate:

- distress-special cases first
- then turnaround
- then cyclical
- then asset-play rerating
- then default compounder

## Harvest Semantics

`HARVEST` is a state, but harvest candidate generation is separate.

### Full-state `HARVEST`

Produced during normal scans when:

- recognition value is exhausted, or
- `flow_stage == crowded`, or
- upside asymmetry has collapsed

### `HARVEST_CANDIDATE`

Produced by a lightweight `attack_book_monitor` for active `ATTACK` names only.

Trigger:

- close above recognition case for `N` consecutive days, or
- a breakout day above the configured recognition ratio after a large daily move

And:

- `flow_stage` deterioration must point to `crowded`

This keeps low-frequency scan semantics intact while still giving the operator timely sell alerts.

## Radar Design

Radar runs in two stages.

### Stage 1: coarse universe reduction

Use batch-friendly fields only:

- free-float or fallback market cap
- liquidity
- ST tags
- PB / valuation snapshot where available

Goal: reduce the universe to roughly `200-400` names before expensive scoring.

### Stage 2: full VCRF scoring

Only Stage 1 survivors receive:

- full `Driver Stack`
- dual-axis scoring
- three-case valuation
- state-machine classification

This is required to keep AkShare and CNINFO usage tractable.

## Module Change List

### Create

- `D:/A价投+周期/.agents/skills/shared/engines/flow_realization_engine.py`
- `D:/A价投+周期/.agents/skills/shared/engines/state_transition_tracker.py`
- `D:/A价投+周期/.agents/skills/shared/engines/vcrf_calibrator.py`
- `D:/A价投+周期/.agents/skills/shared/engines/attack_book_monitor.py`
- `D:/A价投+周期/.agents/skills/shared/utils/vcrf_probes.py`
- `D:/A价投+周期/.agents/skills/shared/utils/primary_type_router.py`
- `D:/A价投+周期/.agents/skills/shared/config/vcrf_weights.yaml`
- `D:/A价投+周期/.agents/skills/shared/config/vcrf_state_machine.yaml`
- `D:/A价投+周期/.agents/skills/shared/config/vcrf_degradation.yaml`

### Modify

- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- `D:/A价投+周期/.agents/skills/shared/adapters/cninfo_adapter.py`
- `D:/A价投+周期/.agents/skills/shared/config/sector_classification.yaml`
- `D:/A价投+周期/.agents/skills/shared/config/valuation_discipline.yaml`
- `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
- `D:/A价投+周期/.agents/skills/shared/engines/report_engine.py`
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- `D:/A价投+周期/.agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py`
- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

### Keep as facades

- [`research_utils.py`](D:/A价投+周期/.agents/skills/shared/utils/research_utils.py)
- [`framework_utils.py`](D:/A价投+周期/.agents/skills/shared/utils/framework_utils.py)

New logic should live in focused modules and only be re-exported where compatibility requires it.

## Migration Compatibility Table

| Active surface | Current behavior | New behavior | Migration rule |
| --- | --- | --- | --- |
| `evaluate_universal_gates()` | one-dimensional gate output | dual-axis + state output | keep legacy aliases for one transition phase |
| `build_three_case_valuation()` | bear/base/bull naming | floor/normalized/recognition | keep bear/base/bull aliases |
| `generate_deep_dive_report()` | old scorecard language | VCRF vocabulary | render both old alias names and new labels during transition |
| radar payload | score-heavy ranking | state-aware ranking | add new fields without removing old shortlist keys |
| `scorecard.verdict` | old textual verdict | kept temporarily | preserve one version while `position_state` becomes the canonical action field |

Legacy alias exit condition:

- remove compatibility aliases after the first successful full-market radar scan under the new pipeline produces state-consistent output for more than `90%` of names and all active report consumers have been updated to read the new canonical fields
- until that point, legacy fields remain read-only compatibility shims and must not become the source of truth

## `detect_big_bath()` Spike Plan

### Goal

Verify whether the full Tier-0 extraction path is reliable enough to support a multi-factor `detect_big_bath()` implementation.

### Sample

Use `3-5` known A-share cases with visible impairment / cleanup events.

### Path

1. CNINFO query
2. annual-report PDF retrieval
3. Docling extraction
4. impairment-table parsing
5. `detect_big_bath()` prototype scoring

### Pass criteria

- impairment rows are extracted correctly in most sample cases
- OCF and margin deltas can be aligned to the same fiscal periods
- at least a medium-confidence verdict is possible on the sample set

### Fallback if spike fails

Implement a conservative version:

- non-recurring PnL ratio
- OCF versus net income divergence
- gross-margin change

Return `inconclusive` when evidence is not strong enough.

## Verification

Before implementation planning:

- confirm all config schemas are internally consistent
- confirm weight overlays preserve exact axis normalization
- confirm `NEW` state handling avoids illegal first-run transitions
- confirm `HARVEST_CANDIDATE` parameters are owned by config, not hard-coded

## Test Contract

The implementation plan must include at least these five test groups:

1. `WeightNormalizationTests`
   - load every `base_template` combined with every applicable `sector_override`
   - assert underwrite and realization sums are `1.0 +/- tolerance`
   - assert invalid overlays raise immediately

2. `StateTransitionMatrixTests`
   - feed `(prev_state, underwrite_score, realization_score, valuation_summary)` combinations
   - assert legal transitions pass and forbidden transitions are downgraded to the nearest legal state

3. `DegradationBehaviorTests`
   - simulate missing `survival_boundary`
   - assert max state is capped at `COLD_STORAGE`
   - simulate missing Level-2 `flow_confirmation`
   - assert state remains legal but confidence degrades

4. `PrimaryTypeRoutingTests`
   - use representative route-aware fixtures
   - assert `sector_route + preliminary_cycle_state + financials + tags` maps to the expected `primary_type`

5. `VCRFEndToEndSmokeTests`
   - run one real or high-fidelity synthetic name through `Driver Stack -> dual-axis -> valuation -> state machine`
   - assert output shape matches the published contracts

## Decision

Proceed with a full VCRF OS 2.0 spec and implementation plan using:

- inheritance-based weights
- component-level degradation
- legal state-transition enforcement
- two-stage radar execution
- route-aware valuation
- spike-first validation of `detect_big_bath()`
