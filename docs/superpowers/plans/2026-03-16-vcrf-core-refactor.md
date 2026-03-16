# VCRF Core Refactor Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the trading logic around `value floor -> cycle/repair -> realization path -> flow confirmation` while preserving the current scanner/deep-dive entry points and keeping the first implementation slice limited to the core scoring engines.

**Architecture:** Add a dedicated flow/realization engine, evolve valuation from `bear/base/bull` into `floor/normalized/recognition`, then replace the current six-gate heuristic with a VCRF-style underwriting gate that outputs both exact scores and a hidden position state. Update radar universe loading and ranking to prefer layered, tradable mid-cap opportunities instead of pure market-cap-descending samples, but defer taxonomy modifiers and ST sandboxing to a second phase.

**Tech Stack:** Python 3.13, `unittest`, YAML config, `akshare`, existing shared adapters/utilities, PowerShell

**Design basis:** User brief in thread + `D:/A价投+周期/专家修改建议/vcrf_refactor_blueprint.md` + `D:/A价投+周期/专家修改建议/flow_realization_engine.py`

**Environment note:** `D:/A价投+周期` is not a git repository, so commit steps below should be treated as checkpoint notes.

---

## Scope For This Phase

### In scope
- `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- `D:/A价投+周期/.agents/skills/shared/config/scoring_rules.yaml`
- `D:/A价投+周期/.agents/skills/shared/config/valuation_discipline.yaml`
- `D:/A价投+周期/.agents/skills/shared/engines/report_engine.py`
- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
- New helper module for flow/realization scoring

### Explicitly out of scope
- `sector_classification.yaml` modifier expansion (`cycle_state`, `repair_state`, `distress_source`, etc.)
- ST / `*ST` distress sandbox universe
- New adapter fields requiring extra vendor/API work
- Full configuration externalization of gate thresholds and scoring segment constants

### Compatibility rules
- Keep `priority_shortlist`, `secondary_watchlist`, and `rejected` as the scanner’s public buckets.
- Add hidden/internal state fields such as `position_state`, `flow_stage`, and `realization_path` without requiring report consumers to change immediately.
- Keep backward-compatible valuation aliases (`bear_case`, `base_case`, `bull_case`) for one phase, mapping to `floor_case`, `normalized_case`, and `recognition_case`.
- Keep backward-compatible gate aliases where needed so existing report builders do not break during the migration.
- Preserve `signals.catalyst` as a compatibility bridge in phase 1 so `synthesis_engine.py` can remain unchanged while `realization_truth` is introduced.

### Phase-1 flow data mode
- Phase 1 runs the flow engine in a deliberately data-degraded mode.
- Active inputs must come only from fields already derivable from the current `stock_kline` summary:
  - `volume_ratio_20_vs_120`
  - `drawdown_from_5yr_high_pct` and/or a derived rebound proxy from `latest_close / low_5y / high_5y`
  - optional observability fields such as `avg_turnover_1y`
- The following inputs are explicitly allowed to remain `None` / `False` in phase 1 without blocking rollout:
  - `shareholder_concentration_delta`
  - `institutional_holding_delta`
  - `buyback_flag`
  - `insider_buy_flag`
  - `activist_flag`
  - `mna_flag`
- Tests must include an all-`None` degraded-input case and assert that the flow stage safely falls back to `latent` or `abandoned`, not a false-positive attack state.
- `secondary_watch` from the expert prototype is intentionally collapsed into `cold_storage` for phase 1. The legal state set remains exactly five states:
  - `cold_storage`
  - `ready`
  - `attack`
  - `harvest`
  - `reject`

---

## File Map

- **Create:** `D:/A价投+周期/.agents/skills/shared/engines/flow_realization_engine.py`
  - Own the marginal-buyer / flow-stage scoring and `cold_storage / ready / attack / harvest / reject` state machine.
- **Modify:** `D:/A价投+周期/.agents/skills/shared/config/scoring_rules.yaml`
  - Replace the old evenly weighted lens with VCRF-oriented dimensions and type-specific weight templates.
- **Modify:** `D:/A价投+周期/.agents/skills/shared/config/valuation_discipline.yaml`
  - Replace `bear/base/bull` semantics with `floor_case / normalized_case / recognition_case` config and add state thresholds.
- **Modify:** `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
  - Produce floor/normalized/recognition outputs, summary protection/upside metrics, and compatibility aliases.
- **Modify:** `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
  - Replace current gate names and scoring logic with VCRF underwriting gates plus hidden position state.
- **Modify:** `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - Replace market-cap-descending universe truncation with layered sampling and incorporate state-aware ranking.
- **Modify:** `D:/A价投+周期/.agents/skills/shared/engines/report_engine.py`
  - Surface the new valuation vocabulary and the hidden position-state summary without breaking report generation.
- **Modify:** `D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml`
  - Hold phase-1 layered market-cap bucket thresholds and target sample weights so radar sampling is not hardcoded in Python.
- **Modify:** `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
  - Add regression tests for flow stages, VCRF valuation outputs, gate/state outputs, and radar layered sampling behavior.

---

## Non-Negotiable Behavior Changes

- Scanner should no longer systematically prefer only the largest market-cap names when `limit` is small.
- Valuation should answer:
  - `floor_protection = floor_value / current_price`
  - `normalized_upside = normalized_value / current_price - 1`
  - `recognition_upside = recognition_value / current_price - 1`
  - `wind_dependency = recognition_value / normalized_value - 1` or a safe equivalent
- Gates should no longer treat “has some catalyst text” as sufficient realization evidence.
- Scanner payloads should distinguish “cheap but wind not here yet” from “wind has started and attack is allowed.”

---

## Chunk 1: Lock The New VCRF Contracts With Failing Tests

### Task 1: Add failing tests for the new flow-state engine

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test for turnover/relative-strength flow ignition**

```python
def test_flow_engine_classifies_ignition_when_turnover_and_relative_strength_improve(self) -> None:
    result = score_flow_setup(
        FlowInputs(
            current_price=10.0,
            avg20_turnover=1.8,
            avg120_turnover=1.0,
            rel_strength_20d=0.09,
            rel_strength_60d=0.12,
            rebound_from_low_pct=0.18,
            shareholder_concentration_delta=0.0,
            institutional_holding_delta=0.0,
            buyback_flag=False,
            mna_flag=False,
        )
    )
    self.assertEqual(result["stage"], "ignition")
```

- [ ] **Step 2: Add a failing test for `cold_storage` classification**

```python
def test_position_state_is_cold_storage_when_floor_is_strong_but_flow_is_latent(self) -> None:
    state = classify_position_state(
        floor_protection=0.92,
        normalized_upside=0.45,
        recognition_upside=0.70,
        repair_state="stabilizing",
        flow_stage="latent",
    )
    self.assertEqual(state, "cold_storage")
```

- [ ] **Step 3: Add a failing test for `attack` classification**

```python
def test_position_state_is_attack_when_flow_trends_and_recognition_upside_remains(self) -> None:
    ...
    self.assertEqual(state, "attack")
```

- [ ] **Step 4: Run only the new flow tests to verify they fail because the module does not yet exist**

Run:

```powershell
python -m unittest test_investment_framework.<new_flow_test_class> -v
```

Expected: import or attribute failure for the missing flow engine.

- [ ] **Step 5: Add a failing degraded-data test**

```python
def test_flow_engine_degrades_to_latent_when_optional_inputs_are_missing(self) -> None:
    result = score_flow_setup(
        FlowInputs(
            current_price=10.0,
            avg20_turnover=None,
            avg120_turnover=None,
            rel_strength_20d=None,
            rel_strength_60d=None,
            rebound_from_low_pct=None,
            shareholder_concentration_delta=None,
            institutional_holding_delta=None,
        )
    )
    self.assertIn(result["stage"], {"abandoned", "latent"})
```

### Task 2: Add failing tests for valuation contract changes

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test for `floor_case / normalized_case / recognition_case` output keys**

```python
def test_valuation_outputs_vcrf_case_names_and_summary_metrics(self) -> None:
    valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
    self.assertIn("floor_case", valuation)
    self.assertIn("normalized_case", valuation)
    self.assertIn("recognition_case", valuation)
    self.assertIn("floor_protection", valuation["summary"])
    self.assertIn("normalized_upside", valuation["summary"])
    self.assertIn("recognition_upside", valuation["summary"])
    self.assertIn("wind_dependency", valuation["summary"])
```

- [ ] **Step 2: Add a failing test for cyclical normalized earnings using historical income rather than `equity * 0.06` as the first fallback**

```python
def test_cyclical_normalized_value_prefers_history_based_anchor(self) -> None:
    ...
    self.assertGreater(valuation["normalized_case"]["implied_equity_value"], valuation["floor_case"]["implied_equity_value"])
```

- [ ] **Step 3: Add a failing compatibility test that `base_case` still exists and mirrors `normalized_case`**

```python
def test_valuation_keeps_base_case_alias_during_transition(self) -> None:
    self.assertEqual(valuation["base_case"]["implied_price"], valuation["normalized_case"]["implied_price"])
```

- [ ] **Step 4: Run the new valuation tests and confirm they fail for missing keys/old semantics**

Run:

```powershell
python -m unittest test_investment_framework.ValuationAndReportTests -v
```

Expected: failures on missing VCRF case names or summary metrics.

### Task 3: Add failing tests for gate-state and radar behavior

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test for new gate names**

```python
def test_universal_gate_outputs_vcrf_gate_names(self) -> None:
    gate = evaluate_universal_gates("600348", scan_data)
    self.assertIn("business_or_asset_truth", gate["gates"])
    self.assertIn("governance_truth", gate["gates"])
    self.assertIn("valuation_floor_truth", gate["gates"])
    self.assertIn("realization_truth", gate["gates"])
```

- [ ] **Step 2: Add a failing test for hidden `position_state`**

```python
def test_universal_gate_includes_hidden_position_state(self) -> None:
    gate = evaluate_universal_gates("600348", scan_data)
    self.assertIn(gate["position_state"], {"cold_storage", "ready", "attack", "harvest", "reject"})
```

- [ ] **Step 3: Add a failing test for layered universe sampling**

```python
def test_load_universe_prefers_layered_sample_over_top_market_cap_slice(self) -> None:
    ...
    self.assertIn("mid-cap candidate", sampled_names)
    self.assertNotEqual(sampled_codes, top_cap_only_codes)
```

- [ ] **Step 4: Add a failing test that `cold_storage` names do not auto-promote into the priority bucket purely by value score**

```python
def test_radar_priority_bucket_requires_ready_or_attack_state(self) -> None:
    ...
    self.assertFalse(any(item["position_state"] == "cold_storage" for item in result["priority_shortlist"]))
```

- [ ] **Step 5: Run the relevant tests and verify they fail for the right reasons**

Run:

```powershell
python -m unittest test_investment_framework.UniversalGateTests test_investment_framework.PartialRadarFlowTests test_investment_framework.RadarParallelExecutionTests -v
```

Expected: failures due to old gate names, missing states, and old universe/ranking behavior.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 1 tests added and failing as intended.`

---

## Chunk 2: Add Config And Flow Foundations

### Task 4: Create the flow/realization helper module

**Files:**
- Create: `D:/A价投+周期/.agents/skills/shared/engines/flow_realization_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Port the prototype skeleton into a production helper using shared `safe_float`**

Implement:

```python
@dataclass
class FlowInputs:
    ...

def score_flow_setup(inputs: FlowInputs, *, market: str = "A-share") -> dict[str, Any]:
    ...

def classify_position_state(... ) -> str:
    ...
```

- [ ] **Step 2: Keep stage names stable**

Supported stages:

```python
FLOW_STAGE_ORDER = {
    "abandoned": 0,
    "latent": 1,
    "ignition": 2,
    "trend": 3,
    "crowded": 4,
}
```

- [ ] **Step 3: Run the new flow tests and verify they pass**

Run:

```powershell
python -m unittest test_investment_framework.<new_flow_test_class> -v
```

Expected: PASS.

### Task 5: Replace scoring config with VCRF-oriented dimensions and templates

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/config/scoring_rules.yaml`

- [ ] **Step 1: Rename dimensions to the new underwriting vocabulary**

Required dimensions:

```yaml
dimensions:
  thesis_clarity:
    weight: 5
  intrinsic_value_floor:
    weight: 20
  survival_boundary:
    weight: 15
  governance_anti_fraud:
    weight: 10
  business_or_asset_quality:
    weight: 10
  regime_cycle_position:
    weight: 15
  turnaround_catalyst:
    weight: 10
  flow_realization_and_elasticity:
    weight: 15
```

- [ ] **Step 2: Add `weight_templates` per primary type**

At minimum:

```yaml
weight_templates:
  cyclical: ...
  turnaround: ...
  asset_play: ...
  compounder: ...
  special_situation: ...
```

- [ ] **Step 3: Treat `thesis_clarity` as a fixed 5-point dimension outside the templates**

Implementation note:

```yaml
dimensions:
  thesis_clarity:
    weight: 5
```

The template sum should intentionally be `95`, with `thesis_clarity` contributing the final fixed `5`.

- [ ] **Step 4: Keep `verdict` ranges for now unless a gate-state-specific override is needed**

- [ ] **Step 5: Re-run any config-loading tests that depend on `load_scoring_rules()`**

### Task 6: Replace valuation config with floor/normalized/recognition structure

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/config/valuation_discipline.yaml`

- [ ] **Step 1: Convert each type to explicit VCRF case names**

Example:

```yaml
opportunity_types:
  cyclical:
    methods:
      floor_case: tangible_book_or_replacement_cost
      normalized_case: mid_cycle_earnings
      recognition_case: rerated_mid_cycle_earnings
```

- [ ] **Step 2: Add thresholds needed by `classify_position_state()`**

```yaml
state_thresholds:
  cold_storage_min_floor_protection: 0.85
  cold_storage_min_normalized_upside: 0.40
  ready_min_normalized_upside: 0.25
  attack_min_recognition_upside: 0.20
```

- [ ] **Step 3: Preserve a compatibility block if existing helpers still read old keys**

- [ ] **Step 4: Checkpoint note**

Record: `Chunk 2 config and flow foundations landed.`

---

## Chunk 3: Rebuild The Valuation Engine Around Floor / Normalized / Recognition

### Task 7: Refactor valuation outputs without breaking current consumers

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Stop importing through the facade**

Use direct imports from:
- `utils.config_loader`
- `utils.financial_snapshot`
- `utils.value_utils`

- [ ] **Step 2: Add helpers for normalized anchors**

Add minimal helpers such as:

```python
def _historical_profit_anchor(records: list[dict[str, Any]]) -> float | None:
    ...

def _floor_value_for_type(primary_type: str, equity: float | None, profit_anchor: float | None, cfg: dict[str, Any]) -> float | None:
    ...
```

- [ ] **Step 3: Replace the current `bear/base/bull` assembly with VCRF cases**

Required output shape:

```python
{
  "floor_case": {...},
  "normalized_case": {...},
  "recognition_case": {...},
  "summary": {
      "floor_protection": ...,
      "normalized_upside": ...,
      "recognition_upside": ...,
      "wind_dependency": ...,
  },
  "bear_case": <alias of floor_case>,
  "base_case": <alias of normalized_case>,
  "bull_case": <alias of recognition_case>,
}
```

- [ ] **Step 4: Keep the implementation minimal per type**

Phase-1 valuation semantics:
- `compounder`: floor from no-growth owner earnings / normalized from current earnings / recognition from premium multiple
- `cyclical`: floor from tangible-book-like anchor / normalized from historical or mid-cycle profit anchor / recognition from rerated normalized earnings
- `turnaround`: floor from survival value / normalized from repaired earnings + equity / recognition from rerating after repair
- `asset_play`: floor from stressed book/NAV / normalized from book/NAV / recognition from discount close
- `special_situation`: floor/normalized/recognition from probability-weighted outcomes

- [ ] **Step 5: Run the valuation tests until green**

Run:

```powershell
python -m unittest test_investment_framework.ValuationAndReportTests -v
```

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 3 valuation engine migrated to VCRF cases with compatibility aliases.`

---

## Chunk 4: Replace Universal Gate With VCRF Underwriting Gates

### Task 8: Rename and rebuild gate semantics

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- Modify: `D:/A价投+周期/.agents/skills/shared/engines/report_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Swap YAML/internal key mapping to the new dimension names**

At minimum:

```python
_YAML_TO_INTERNAL = {
    "thesis_clarity": "thesis_clarity",
    "intrinsic_value_floor": "intrinsic_value_floor",
    "survival_boundary": "survival_boundary",
    "governance_anti_fraud": "governance_anti_fraud",
    "business_or_asset_quality": "business_or_asset_quality",
    "regime_cycle_position": "regime_cycle_position",
    "turnaround_catalyst": "turnaround_catalyst",
    "flow_realization_and_elasticity": "flow_realization_and_elasticity",
}
```

Also define a constant or equivalent comment-level contract:

```python
THESIS_CLARITY_FIXED = 5.0
```

Template-driven weighting must apply only to the remaining seven underwriting dimensions.

- [ ] **Step 2: Add a resolved context block for flow and valuation summaries**

The gate context should carry:
- `valuation_summary`
- `flow_setup`
- `flow_stage`
- `position_state`
- `repair_state` (phase-1 heuristic)
- `realization_path` (phase-1 heuristic)

- [ ] **Step 3: Replace current gates with**

```python
gates = {
    "business_or_asset_truth": ...,
    "survival_truth": ...,
    "governance_truth": ...,
    "regime_cycle_truth": ...,
    "valuation_floor_truth": ...,
    "realization_truth": ...,
}
```

- [ ] **Step 4: Add or tighten hard vetoes**

Minimum veto coverage for phase 1:
- negative equity with no repair path
- governance/fraud-style red flags in profile/text
- unintelligible business/asset truth

- [ ] **Step 5: Add hidden state to the returned payload**

Required fields:

```python
{
  "position_state": "cold_storage|ready|attack|harvest|reject",
  "flow_stage": "...",
  "realization_path": "...",
}
```

- [ ] **Step 6: Preserve one-phase compatibility aliases**

Map legacy gate keys where necessary:
- `business_truth -> business_or_asset_truth`
- `quality_truth -> governance_truth` or a documented transitional equivalent
- `catalyst truth` callers should read `realization_truth`

- [ ] **Step 7: Update `report_engine.py` to show `floor/normalized/recognition` and `position_state`**

Do not redesign the whole report. Minimal changes only:
- rename valuation section labels
- add one line for `position_state`
- avoid requiring any caller changes

- [ ] **Step 8: Run gate/report tests until green**

Run:

```powershell
python -m unittest test_investment_framework.UniversalGateTests test_investment_framework.ValuationAndReportTests -v
```

- [ ] **Step 9: Checkpoint note**

Record: `Chunk 4 universal gate and report vocabulary migrated.`

---

## Chunk 5: Rebuild Radar Selection Around Layered Sampling And State-Aware Ranking

### Task 9: Replace top-cap slicing with layered sampling

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add phase-1 market-cap bucket thresholds and target weights to config**

Add to `scan_defaults.yaml`:

```yaml
layered_sampling:
  market_cap_buckets:
    micro: [2000000000, 5000000000]
    small: [5000000000, 15000000000]
    mid: [15000000000, 50000000000]
    large: [50000000000, null]
  target_mix_pct:
    micro: 25
    small: 40
    mid: 25
    large: 10
```

These values come directly from the expert blueprint and are the phase-1 defaults.

- [ ] **Step 2: Add a helper that builds size buckets from the snapshot universe**

Expected buckets:
- `large`
- `mid`
- `small`

Optional derived bucket:
- `mega` only if implementation chooses to split an ultra-large tail out of `large`

Use existing market-cap and liquidity columns only. Do not add new adapter fetches in this phase.

- [ ] **Step 3: Normalize blueprint buckets into the runtime bucket set used by radar**

Phase-1 runtime rule:
- `micro` and `small` blueprint buckets may be merged into runtime `small`
- `mid` stays `mid`
- `large` stays `large`
- `mega` remains optional only if existing snapshot names naturally spill beyond the `large` bucket

- [ ] **Step 4: Add a layered sampler**

Minimal phase-1 behavior:
- bias toward `mid` and `small`
- keep some `large` names for asset-play/policy cases
- preserve industry diversity where the snapshot exposes industry/name clues
- still exclude `ST` in this phase

- [ ] **Step 5: Make `_load_universe()` return the layered sample instead of the top-cap slice**

- [ ] **Step 6: Run the new universe test and verify green**

### Task 10: Make ranking state-aware while preserving public buckets

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Extend `_candidate_payload()` to include**

```python
{
  "position_state": ...,
  "flow_stage": ...,
  "realization_path": ...,
  "floor_protection": ...,
  "normalized_upside": ...,
  "recognition_upside": ...,
}
```

- [ ] **Step 2: Add a state-priority ordering**

Example:

```python
STATE_PRIORITY = {
    "attack": 0,
    "ready": 1,
    "cold_storage": 2,
    "harvest": 3,
    "reject": 4,
}
```

- [ ] **Step 3: Apply bucket rules**

Phase-1 scanner bucketing:
- `priority_shortlist`: only `ready` or `attack`, no hard veto, above priority cutoff
- `secondary_watchlist`: `cold_storage`, `ready`, or `attack`, no hard veto, above secondary cutoff
- `rejected`: everything else

- [ ] **Step 4: Keep `cold_storage` names visible but not attack-ranked**

This is the key behavior change: value floor alone is enough for watchlist placement, not for attack placement.

- [ ] **Step 5: Run radar tests until green**

Run:

```powershell
python -m unittest test_investment_framework.PartialRadarFlowTests test_investment_framework.RadarParallelExecutionTests -v
```

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 5 radar scanner migrated to layered sampling and state-aware ranking.`

---

## Chunk 6: Full Verification

### Task 11: Run the complete shared suite

**Files:**
- Verify only

- [ ] **Step 1: Run the full shared test suite**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS.

- [ ] **Step 2: If `discover` is flaky in this harness, rerun by class groups and record the grouped output**

Run:

```powershell
python -m unittest test_investment_framework.OpportunityTypeTests
python -m unittest test_investment_framework.BaoStockFallbackTests
python -m unittest test_investment_framework.UniversalGateTests test_investment_framework.PartialRadarFlowTests test_investment_framework.RadarDayCacheTests
python -m unittest test_investment_framework.RadarParallelExecutionTests test_investment_framework.ValuationAndReportTests test_investment_framework.LegacyCleanupTests
```

- [ ] **Step 3: Run one small live radar smoke test**

Suggested:

```powershell
python ".agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py" --scope "600328,600348,000731" --limit 3
```

Verify:
- output contains `position_state`
- at least one name can land in `cold_storage` or `ready`
- report generation still succeeds

- [ ] **Step 4: Record final checkpoint note**

Record: `VCRF core slice verified; phase-2 items (sector modifiers, ST sandbox, richer adapters) intentionally deferred.`

---

## Phase 2 Backlog (Do Not Pull Into This Plan)

- Expand `sector_classification.yaml` into `primary_type + modifiers`
- Add `distress sandbox` for ST / `*ST`
- Add richer buyer-path data sources (northbound changes, shareholder concentration, US insider/13D adapters)
- Externalize more scoring constants if a later calibration pass justifies it
