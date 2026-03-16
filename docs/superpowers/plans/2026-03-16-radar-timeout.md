# Radar Timeout Optimization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut cold-start radar scan time by introducing a safe two-stage scan that rejects obvious non-candidates early while preserving exact final scores for all survivors.

**Architecture:** Keep the existing full-score path intact and add a new partial-evaluation path for the radar scanner only. Stage 1 fetches cheap fields, computes per-dimension confidence and a safe score upper bound, and only Stage 2 survivors fetch the missing expensive fields before reusing the current full gate and valuation flow.

**Tech Stack:** Python 3.13, `unittest`, `akshare`, `baostock`, PowerShell

**Spec:** `D:/A价投+周期/docs/superpowers/specs/2026-03-16-radar-timeout-design.md`

**Environment note:** `D:/A价投+周期` is not a git repository, so commit steps in this plan should be treated as checkpoint notes rather than executable git commands.

---

## File Map

- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
  - Add regression tests for partial dimensions, upper-bound gating, `requires`-driven enrichment, radar orchestration, and BaoStock fallback through the new path.
- `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
  - Add `evaluate_partial_gate_dimensions()` and keep `evaluate_universal_gates()` unchanged as the exact final scorer.
- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
  - Add reusable radar fetch-step maps and retry helpers for partial fetches and enrichment fetches.
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - Replace the one-shot full-scan loop with the two-stage radar flow and explicit `safe_prefilter_reject` payload generation.

## Constraints To Preserve

- Final shortlist and watchlist scores must still come from the existing exact path.
- Deep-dive flow must remain unchanged.
- `financial_summary` must not be fetched by the radar path.
- `score_upper_bound < 65` is reject; `score_upper_bound == 65` must advance.
- `RADAR_PARTIAL_STEPS` and `RADAR_EXPENSIVE_STEPS` keys must match the top-level `scan_data` keys exactly so `fields_to_fetch` can be dispatched without translation.

## Chunk 1: Lock In Behavior With Tests

### Task 1: Add failing tests for partial-dimension scoring and upper-bound rules

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test for partial dimension metadata**

```python
def test_partial_gate_dimensions_include_confidence_and_requires(self) -> None:
    result = evaluate_partial_gate_dimensions("600348", scan_data_without_financials)
    self.assertEqual(result["dimensions"]["survival"]["confidence"], "none")
    self.assertIn("balance_sheet", result["dimensions"]["survival"]["requires"])
```

- [ ] **Step 2: Run the targeted test and verify it fails because the helper does not exist yet**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: FAIL with missing `evaluate_partial_gate_dimensions` or missing dimension metadata.

- [ ] **Step 3: Add a failing test for strict `< 65` rejection**

```python
def test_prefilter_rejects_only_when_upper_bound_is_below_cutoff(self) -> None:
    ...
```

- [ ] **Step 4: Add a failing test for the inclusive `== 65` advancement boundary**

```python
def test_prefilter_advances_when_upper_bound_equals_cutoff(self) -> None:
    ...
```

- [ ] **Step 5: Add a failing test for decidable vs blocked hard veto separation**

```python
def test_partial_gate_separates_decidable_and_blocked_hard_vetos(self) -> None:
    ...
```

- [ ] **Step 6: Re-run the shared test file and verify the new tests fail for the expected reasons**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: new partial-gate tests fail; unrelated existing tests still pass.

### Task 2: Add failing radar orchestration tests before touching scanner code

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test that Stage 2 fetch fields are derived from `requires`**

```python
def test_radar_enrichment_fetches_fields_from_requires(self) -> None:
    ...
```

- [ ] **Step 2: Add a failing test that radar no longer fetches `financial_summary`**

```python
def test_radar_path_does_not_fetch_financial_summary(self) -> None:
    ...
```

- [ ] **Step 3: Add a failing test that a survivor's final payload matches the existing exact path**

```python
def test_two_stage_radar_matches_existing_full_payload_for_survivor(self) -> None:
    ...
```

- [ ] **Step 4: Add a failing test that BaoStock fallback still works through the two-stage radar path**

```python
def test_two_stage_radar_preserves_baostock_universe_fallback(self) -> None:
    ...
```

- [ ] **Step 5: Re-run the shared test file and verify the new radar tests fail**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: orchestration tests fail because the radar engine still uses the old serial `run_full_scan()` loop.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 1 tests added and failing as intended.`

## Chunk 2: Implement Partial Gate Dimensions

### Task 3: Add a partial evaluator without changing the final scorer

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Extract shared intermediate values needed by both partial and full evaluation**

```python
opportunity_context = opportunity_context or determine_opportunity_type(...)
profile = scan_data.get("company_profile", {}).get("data", {})
quote = scan_data.get("realtime_quote", {}).get("data", {})
valuation = scan_data.get("valuation_history", {}).get("data", {})
kline = scan_data.get("stock_kline", {}).get("data", {})
```

- [ ] **Step 2: Implement `evaluate_partial_gate_dimensions()` with `score`, `max`, `confidence`, `requires`, and `reason` for each dimension**

```python
dimensions["survival"] = {
    "score": 0.0,
    "max": 15.0,
    "confidence": "none",
    "requires": ["income_statement", "balance_sheet"],
    "reason": "survival cannot be scored without financial statements",
}
```

- [ ] **Step 3: Implement `known_total`, `unknown_ceiling`, and `score_upper_bound` using the generic formula**

```python
unknown_ceiling = sum(
    item["max"] - item["score"]
    for item in dimensions.values()
    if item["confidence"] != "full"
)
```

- [ ] **Step 4: Implement `decidable_hard_vetos` and `blocked_hard_vetos` explicitly**

```python
decidable_hard_vetos = []
blocked_hard_vetos = [
    "normal earning power cannot be estimated",
    "balance sheet survival is questionable",
]
```

- [ ] **Step 5: Run the shared test file and make the new partial-gate tests pass**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: partial-dimension tests pass; radar orchestration tests still fail.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 2 partial evaluator implemented; full evaluator unchanged.`

## Chunk 3: Add Radar Fetch Maps And Reusable Fetch Helpers

### Task 4: Add step maps that align with `scan_data` keys

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Define `RADAR_PARTIAL_STEPS` keyed exactly by top-level `scan_data` names**

```python
RADAR_PARTIAL_STEPS = {
    "company_profile": get_company_profile,
    "revenue_breakdown": get_revenue_breakdown,
    "valuation_history": get_valuation_history,
    "stock_kline": get_stock_kline,
    "realtime_quote": get_realtime_quote,
}
```

- [ ] **Step 2: Define `RADAR_EXPENSIVE_STEPS` keyed exactly by top-level `scan_data` names**

```python
RADAR_EXPENSIVE_STEPS = {
    "income_statement": get_income_statement,
    "balance_sheet": get_balance_sheet,
}
```

- [ ] **Step 3: Extract a small retry-and-cache helper for named fetches so radar code does not duplicate `run_full_scan()` logic**

```python
def run_named_scan_steps(stock_code: str, step_map: dict[str, Any], *, cached_results: dict[str, Any] | None = None) -> dict[str, Any]:
    ...
```

- [ ] **Step 4: Ensure radar-oriented helpers never include `financial_summary`**

```python
assert "financial_summary" not in RADAR_PARTIAL_STEPS
assert "financial_summary" not in RADAR_EXPENSIVE_STEPS
```

- [ ] **Step 5: Run the shared test file and verify key-alignment and no-`financial_summary` tests pass**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: helper and key-alignment tests pass; radar orchestration still fails.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 3 adapter step maps implemented with direct scan_data key alignment.`

## Chunk 4: Replace The Radar Loop With Two-Stage Orchestration

### Task 5: Add partial fetch and enrichment helpers to the radar engine

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Replace the direct `run_full_scan()` dependency with radar-specific partial fetch helpers**

```python
partial_scan_data = run_named_scan_steps(item["code"], RADAR_PARTIAL_STEPS)
partial_gate = evaluate_partial_gate_dimensions(item["code"], partial_scan_data)
```

- [ ] **Step 2: Add a helper that converts a Stage 1 rejection into a report payload**

```python
def _prefilter_rejected_payload(stock_code: str, company_name: str, partial_gate: dict[str, Any]) -> dict[str, Any]:
    ...
```

- [ ] **Step 3: Derive `fields_to_fetch` from `requires` and enrich only survivors**

```python
fields_to_fetch = sorted(
    {
        field
        for dimension in partial_gate["dimensions"].values()
        if dimension["confidence"] != "full"
        for field in dimension.get("requires", [])
    }
)
```

- [ ] **Step 4: Merge enriched fields into partial `scan_data` and reuse existing `_candidate_payload()`**

```python
enriched = dict(partial_scan_data)
enriched.update(run_named_scan_steps(item["code"], selected_step_map))
ranked.append(_candidate_payload(item["code"], item["name"], enriched))
```

- [ ] **Step 5: Enforce the boundary rule exactly**

```python
if partial_gate["decidable_hard_vetos"] or partial_gate["score_upper_bound"] < secondary_cutoff:
    rejected.append(...)
else:
    ...
```

- [ ] **Step 6: Run the shared test file and make the two-stage radar tests pass**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: two-stage radar orchestration tests pass and legacy passing tests remain green.

- [ ] **Step 7: Checkpoint note**

Record: `Chunk 4 radar engine now uses Stage 1 prefilter plus Stage 2 enrichment.`

## Chunk 5: Verification

### Task 6: Verify behavior with the shared suite and live fallback checks

**Files:**
- Verify only:
  - `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
  - `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
  - `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Run the shared test suite**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS.

- [ ] **Step 2: Run a forced radar-orchestration check with mocked or patched Stage 1 inputs**

Run:

```powershell
@'
import importlib.util
from pathlib import Path

path = Path(r"D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py")
spec = importlib.util.spec_from_file_location("radar_scan_engine_check", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
print(hasattr(module, "run_radar_scan"))
'@ | python -X utf8 -
```

Expected: `True`, confirming the module still imports cleanly after the orchestration refactor.

- [ ] **Step 3: Run a small live radar sample**

Run:

```powershell
python "D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py" "A-share" --limit 6
```

Expected: completes, writes market-scan artifacts, and can emit `safe_prefilter_reject` reasons without breaking report generation.

- [ ] **Step 4: Record verification findings in the close-out**

Record:
- shared test count and pass status
- whether `safe_prefilter_reject` appeared as expected
- whether BaoStock fallback still worked when primary universe loading failed

- [ ] **Step 5: Checkpoint note**

Record: `Verification complete. Workspace has no git metadata, so no commit was created.`
