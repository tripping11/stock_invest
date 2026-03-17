# VCRF OS 2.0 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved VCRF OS 2.0 architecture across the active A-share pipeline, including dual-axis scoring, five-state transitions, route-aware valuation, two-stage radar selection, and the supporting config, probe, and monitoring layers.

**Architecture:** Start with config contracts and data-source foundations so every later module can load validated weights, thresholds, and degradation rules. Then build reusable route/probe modules, layer in realization/state engines, upgrade valuation and gating, and finally integrate radar, reporting, calibration, and spike validation with compatibility aliases preserved until the new pipeline proves stable.

**Tech Stack:** Python 3.13, `unittest`, YAML config, AkShare, CNINFO HTTP adapter, Docling/Tier-0 PDF workflow, PowerShell, append-only JSONL state history

**Spec basis:** `D:/A价投+周期/docs/superpowers/specs/2026-03-16-vcrf-os-2-design.md`

---

## File Map

### Create

- `D:/A价投+周期/.agents/skills/shared/config/vcrf_weights.yaml`
  - Primary-type base weights plus sector-route overlays and normalization metadata.
- `D:/A价投+周期/.agents/skills/shared/config/vcrf_state_machine.yaml`
  - Five-state thresholds, allowed transitions, flow-stage order, and harvest-candidate rules.
- `D:/A价投+周期/.agents/skills/shared/config/vcrf_degradation.yaml`
  - Component-level missing-data policies and state caps.
- `D:/A价投+周期/.agents/skills/shared/utils/vcrf_probes.py`
  - Underwrite and realization probe functions, including the conservative `detect_big_bath()` path.
- `D:/A价投+周期/.agents/skills/shared/utils/primary_type_router.py`
  - `sector_route` resolution, preliminary cycle-state inference, and `primary_type` routing.
- `D:/A价投+周期/.agents/skills/shared/engines/flow_realization_engine.py`
  - Realization-axis composition, flow-stage scoring, and harvest-candidate helpers.
- `D:/A价投+周期/.agents/skills/shared/engines/state_transition_tracker.py`
  - Append-only JSONL read/write helpers and legal transition enforcement.
- `D:/A价投+周期/.agents/skills/shared/engines/attack_book_monitor.py`
  - Low-frequency monitor for `ATTACK` names and `HARVEST_CANDIDATE` alerts.
- `D:/A价投+周期/.agents/skills/shared/engines/vcrf_calibrator.py`
  - Score-distribution statistics and threshold calibration report generation.

### Modify

- `D:/A价投+周期/.agents/skills/shared/utils/config_loader.py`
  - Load new VCRF config files and enforce weight normalization.
- `D:/A价投+周期/.agents/skills/shared/utils/financial_snapshot.py`
  - Add latest cash-flow, debt-wall, and shareholder-count snapshot helpers.
- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
  - Add `cashflow_statement`, `shareholder_count`, and route-friendly snapshot fields.
- `D:/A价投+周期/.agents/skills/shared/adapters/cninfo_adapter.py`
  - Add event-query helpers for buyback,增持, asset injection, and restructuring signals.
- `D:/A价投+周期/.agents/skills/shared/config/sector_classification.yaml`
  - Add `sector_route`, `primary_type_hints`, `realization_path_keywords`, and special-tag rules.
- `D:/A价投+周期/.agents/skills/shared/config/valuation_discipline.yaml`
  - Add route-aware floor/normalized/recognition valuation methods and thresholds.
- `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
  - Replace static bear/base/bull logic with route-aware `floor/normalized/recognition` outputs and aliases.
- `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
  - Replace one-dimensional scoring with Driver Stack, dual-axis output, degradation handling, and state transitions.
- `D:/A价投+周期/.agents/skills/shared/engines/report_engine.py`
  - Render dual-axis, state, valuation triad, and compatibility fields.
- `D:/A价投+周期/.agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py`
  - Persist new outputs, consume route-aware valuation/gate payloads, and expose canonical `position_state`.
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - Two-stage coarse/fine radar, ST tagging, state-aware ranking, and candidate payload expansion.
- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
  - Contract tests for configs, probes, routing, valuation, state transitions, degradation, radar integration, and end-to-end smoke coverage.

### Facades To Preserve

- `D:/A价投+周期/.agents/skills/shared/utils/framework_utils.py`
- `D:/A价投+周期/.agents/skills/shared/utils/research_utils.py`

These should remain re-export shims where compatibility is required. New implementation code should live in focused modules.

---

## Chunk 1: Lock Config Contracts First

### Task 1: Add failing tests for VCRF config loading and normalization

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
- Test target: new `VCRFConfigContractTests`

- [ ] **Step 1: Add a failing loader test for new config files**

```python
class VCRFConfigContractTests(unittest.TestCase):
    def test_loaders_expose_vcrf_config_files(self) -> None:
        from utils.config_loader import (
            load_vcrf_weights,
            load_vcrf_state_machine,
            load_vcrf_degradation,
        )

        self.assertIn("base_templates", load_vcrf_weights())
        self.assertIn("allowed_transitions", load_vcrf_state_machine())
        self.assertIn("degradation_rules", load_vcrf_degradation())
```

- [ ] **Step 2: Add a failing normalization test**

```python
    def test_all_weight_templates_normalize_after_sector_overrides(self) -> None:
        from utils.config_loader import resolve_vcrf_weight_template

        primary_types = ["compounder", "cyclical", "turnaround", "asset_play", "special_situation"]
        routes = ["core_resource", "rigid_shovel", "core_military", "financial_asset", "consumer", "tech", "unknown"]
        for primary_type in primary_types:
            for route in routes:
                template = resolve_vcrf_weight_template(primary_type, route)
                self.assertAlmostEqual(sum(template["underwrite"].values()), 1.0, places=3)
                self.assertAlmostEqual(sum(template["realization"].values()), 1.0, places=3)
```

- [ ] **Step 3: Run only the new config tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFConfigContractTests -v
```

Expected: import failures or missing-key failures because the config files and loader functions do not exist yet.

- [ ] **Step 4: Add a failing transition-config test for `NEW` and harvest rules**

```python
    def test_state_machine_config_includes_new_state_and_harvest_candidate_rules(self) -> None:
        from utils.config_loader import load_vcrf_state_machine

        cfg = load_vcrf_state_machine()
        self.assertIn("NEW", cfg["allowed_transitions"])
        self.assertIn("harvest_candidate", cfg)
        self.assertEqual(cfg["harvest_candidate"]["consecutive_closes_above_recognition"], 3)
```

- [ ] **Step 5: Checkpoint note**

Record: `Chunk 1 config tests added and failing for the expected reasons.`

### Task 2: Create the VCRF config files and loader helpers

**Files:**
- Create: `D:/A价投+周期/.agents/skills/shared/config/vcrf_weights.yaml`
- Create: `D:/A价投+周期/.agents/skills/shared/config/vcrf_state_machine.yaml`
- Create: `D:/A价投+周期/.agents/skills/shared/config/vcrf_degradation.yaml`
- Modify: `D:/A价投+周期/.agents/skills/shared/utils/config_loader.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add the config files using the spec values**

Key content to include:

```yaml
_meta:
  enforce_normalization: true
  tolerance: 0.001
base_templates:
  compounder: ...
  cyclical: ...
  turnaround: ...
  asset_play: ...
  special_situation: ...
sector_overrides:
  core_resource: ...
  rigid_shovel: ...
  core_military: ...
  financial_asset: ...
  consumer: ...
  tech: ...
```

- [ ] **Step 2: Add loader functions to `config_loader.py`**

Implement:

```python
def load_vcrf_weights() -> dict[str, Any]: ...
def load_vcrf_state_machine() -> dict[str, Any]: ...
def load_vcrf_degradation() -> dict[str, Any]: ...
def resolve_vcrf_weight_template(primary_type: str, sector_route: str) -> dict[str, dict[str, float]]: ...
```

- [ ] **Step 3: Enforce normalization by raising on invalid overlays**

Implementation note:

```python
def _assert_axis_normalized(weights: dict[str, float], *, tolerance: float) -> None:
    total = sum(float(value) for value in weights.values())
    if abs(total - 1.0) > tolerance:
        raise ValueError(f"VCRF axis weights must sum to 1.0, got {total:.6f}")
```

- [ ] **Step 4: Run config tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFConfigContractTests -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add .agents/skills/shared/config/vcrf_weights.yaml .agents/skills/shared/config/vcrf_state_machine.yaml .agents/skills/shared/config/vcrf_degradation.yaml .agents/skills/shared/utils/config_loader.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add VCRF config contracts and loaders"
```

---

## Chunk 2: Expand Data Sources And Persistence Primitives

**Parallelization note:** Keep `Task 3` first so the AkShare path and the CNINFO/state-history path both start from explicit failing tests. After `Task 3` lands, `Task 4` and `Task 5` may be executed in parallel because they touch separate implementation files and independent test groups, then rejoin on the next full regression gate.

### Task 3: Add failing tests for cash-flow, shareholder-count, and CNINFO event helpers

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing cash-flow adapter contract test**

```python
class VCRFDataSourceTests(unittest.TestCase):
    def test_akshare_adapter_exposes_cashflow_statement_step(self) -> None:
        from adapters.akshare_adapter import RADAR_ALL_STEPS

        self.assertIn("cashflow_statement", RADAR_ALL_STEPS)
```

- [ ] **Step 2: Add a failing shareholder-count helper test**

```python
    def test_akshare_adapter_exposes_shareholder_count_step(self) -> None:
        from adapters.akshare_adapter import RADAR_PARTIAL_STEPS

        self.assertIn("shareholder_count", RADAR_PARTIAL_STEPS)
```

- [ ] **Step 3: Add a failing CNINFO event-query helper test**

```python
    def test_cninfo_adapter_exposes_vcrf_event_query(self) -> None:
        from adapters.cninfo_adapter import fetch_vcrf_event_signals

        result = fetch_vcrf_event_signals("600348", start_date="20240101", end_date="20241231")
        self.assertIn("events", result)
```

- [ ] **Step 4: Run the new data-source tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFDataSourceTests -v
```

Expected: missing step names or missing helper imports.

- [ ] **Step 5: Add a failing state-history contract test**

```python
class VCRFStateHistoryTests(unittest.TestCase):
    def test_missing_history_resolves_to_new_state(self) -> None:
        from engines.state_transition_tracker import load_latest_state

        self.assertEqual(load_latest_state("600348", history_path="missing.jsonl"), "NEW")
```

### Task 4: Expand AkShare and financial snapshots for survival inputs

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- Modify: `D:/A价投+周期/.agents/skills/shared/utils/financial_snapshot.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add `get_cashflow_statement()` to `akshare_adapter.py`**

Implementation target:

```python
def get_cashflow_statement(stock_code: str) -> dict[str, Any]:
    df = ak.stock_financial_report_sina(stock=_stock_symbol(stock_code), symbol="现金流量表")
    records = _recent_records(df, 8)
    ...
```

- [ ] **Step 2: Add `get_shareholder_count()` to `akshare_adapter.py`**

Implementation target:

```python
def get_shareholder_count(stock_code: str) -> dict[str, Any]:
    df = ak.stock_zh_a_gdhs(symbol=stock_code)
    ...
```

- [ ] **Step 3: Register both fields in radar/full-scan step maps**

Required updates:

```python
RADAR_PARTIAL_STEPS["shareholder_count"] = get_shareholder_count
RADAR_EXPENSIVE_STEPS["cashflow_statement"] = get_cashflow_statement
FULL_SCAN_STEPS.append(("cashflow_statement", get_cashflow_statement))
FULL_SCAN_STEPS.append(("shareholder_count", get_shareholder_count))
```

- [ ] **Step 4: Add financial snapshot helpers for OCF and debt-wall extraction**

Implement in `financial_snapshot.py`:

```python
def get_latest_cashflow_snapshot(records: list[dict[str, Any]]) -> dict[str, Any]: ...
def extract_short_term_interest_bearing_debt(balance_row: dict[str, Any]) -> float | None: ...
def extract_cash_and_equivalents(balance_row: dict[str, Any]) -> float | None: ...
```

- [ ] **Step 5: Run data-source tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFDataSourceTests -v
```

- [ ] **Step 6: Commit**

```powershell
git add .agents/skills/shared/adapters/akshare_adapter.py .agents/skills/shared/utils/financial_snapshot.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add VCRF survival data inputs"
```

### Task 5: Add CNINFO event parsing and state-history primitives

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/adapters/cninfo_adapter.py`
- Create: `D:/A价投+周期/.agents/skills/shared/engines/state_transition_tracker.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add `fetch_vcrf_event_signals()` to `cninfo_adapter.py`**

Implementation target:

```python
def fetch_vcrf_event_signals(stock_code: str, *, start_date: str, end_date: str) -> dict[str, Any]:
    keyword_groups = {
        "buyback": ["回购", "回购注销"],
        "shareholder_support": ["增持", "兜底增持"],
        "asset_unlock": ["资产注入", "分拆上市", "REIT", "出售资产"],
        "restructuring": ["重大资产重组", "债务重组", "破产重整"],
    }
    ...
```

- [ ] **Step 2: Add `state_transition_tracker.py` with append-only helpers**

Implement:

```python
def load_latest_state(stock_code: str, *, history_path: str | Path) -> str: ...
def enforce_transition(prev_state: str, proposed_state: str, *, cfg: dict[str, Any]) -> tuple[str, bool, str]: ...
def append_state_record(record: dict[str, Any], *, history_path: str | Path) -> None: ...
```

- [ ] **Step 3: Add a transition-matrix test for forbidden downgrades**

```python
    def test_forbidden_transition_is_downgraded(self) -> None:
        from engines.state_transition_tracker import enforce_transition
        from utils.config_loader import load_vcrf_state_machine

        state, allowed, reason = enforce_transition("HARVEST", "ATTACK", cfg=load_vcrf_state_machine())
        self.assertEqual(state, "COLD_STORAGE")
        self.assertFalse(allowed)
```

- [ ] **Step 4: Run state-history tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFStateHistoryTests -v
```

- [ ] **Step 5: Commit**

```powershell
git add .agents/skills/shared/adapters/cninfo_adapter.py .agents/skills/shared/engines/state_transition_tracker.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add VCRF event signals and state tracking"
```

---

## Chunk 3: Build Driver Stack And Probe Layer

### Task 6: Add failing tests for route resolution and primary-type routing

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing `sector_route` resolution test**

```python
class VCRFDriverStackTests(unittest.TestCase):
    def test_sector_route_resolves_from_active_sector_classification(self) -> None:
        from utils.primary_type_router import resolve_sector_route

        route = resolve_sector_route(
            "600348",
            {"行业": "煤炭", "主营业务": "煤炭开采与销售"},
            revenue_records=[{"主营构成": "煤炭", "主营收入": 85}],
        )
        self.assertEqual(route["sector_route"], "core_resource")
```

- [ ] **Step 2: Add a failing `primary_type` routing test**

```python
    def test_turnaround_routing_wins_when_losses_and_repair_evidence_exist(self) -> None:
        from utils.primary_type_router import determine_primary_type

        primary_type, confidence = determine_primary_type(
            sector_route="core_resource",
            preliminary_cycle_state="repair",
            financials_3y={"losses_2y": True, "repair_evidence": True},
            tags=[],
            events={},
            big_bath_result={"verdict": "inconclusive"},
        )
        self.assertEqual(primary_type, "turnaround")
        self.assertGreaterEqual(confidence, 0.75)
```

- [ ] **Step 3: Run the new routing tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFDriverStackTests -v
```

Expected: import failures because `primary_type_router.py` does not exist.

- [ ] **Step 4: Add a failing degradation-cap test**

```python
    def test_missing_survival_boundary_caps_state_at_cold_storage(self) -> None:
        from validators.universal_gate import _apply_degradation_caps

        adjusted = _apply_degradation_caps(
            proposed_state="ATTACK",
            component_availability={"survival_boundary": "missing"},
        )
        self.assertEqual(adjusted, "COLD_STORAGE")
```

### Task 7a: Implement route resolution and Driver Stack routing

**Files:**
- Create: `D:/A价投+周期/.agents/skills/shared/utils/primary_type_router.py`
- Modify: `D:/A价投+周期/.agents/skills/shared/config/sector_classification.yaml`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Extend `sector_classification.yaml` with routing and hint vocabularies**

Required top-level sections:

```yaml
sector_routes:
  core_resource: ...
  rigid_shovel: ...
  core_military: ...
primary_type_hints:
  turnaround: ...
  asset_play: ...
realization_path_keywords:
  buyback: ...
  asset_unlock: ...
special_tag_rules:
  st: ...
  star_st: ...
```

- [ ] **Step 2: Implement `primary_type_router.py`**

Implement:

```python
def resolve_sector_route(stock_code: str, profile: dict[str, Any], revenue_records: list[dict[str, Any]]) -> dict[str, Any]: ...
def infer_preliminary_cycle_state(sector_route: str, scan_data: dict[str, Any]) -> str: ...
def determine_primary_type(... ) -> tuple[str, float]: ...
def build_driver_stack(stock_code: str, scan_data: dict[str, Any], *, extra_texts: list[str] | None = None) -> dict[str, Any]: ...
```

- [ ] **Step 3: Run driver-stack routing tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFDriverStackTests -v
```

- [ ] **Step 4: Run the full shared test suite to catch compatibility regressions early**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. If legacy tests fail here, fix the router/config compatibility in this task instead of carrying the regression forward.

- [ ] **Step 5: Commit**

```powershell
git add .agents/skills/shared/config/sector_classification.yaml .agents/skills/shared/utils/primary_type_router.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add VCRF driver stack routing"
```

### Task 7b: Implement conservative VCRF probes and facade shims

**Files:**
- Create: `D:/A价投+周期/.agents/skills/shared/utils/vcrf_probes.py`
- Modify: `D:/A价投+周期/.agents/skills/shared/utils/framework_utils.py`
- Modify: `D:/A价投+周期/.agents/skills/shared/utils/research_utils.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Implement conservative `vcrf_probes.py` underwrite functions**

Required functions:

```python
def assess_intrinsic_value_floor(... ) -> dict[str, Any]: ...
def assess_survival_boundary(... ) -> dict[str, Any]: ...
def assess_governance_anti_fraud(... ) -> dict[str, Any]: ...
def assess_business_or_asset_quality(... ) -> dict[str, Any]: ...
def assess_normalized_earnings_power(... ) -> dict[str, Any]: ...
def detect_big_bath(... ) -> dict[str, Any]: ...
```

Conservative `detect_big_bath()` path for the first implementation:

```python
return {
    "verdict": "big_bath" | "genuine_collapse" | "inconclusive",
    "one_off_impairment_ratio": ...,
    "core_gross_margin_trend": ...,
    "ocf_vs_net_income_divergence": ...,
    "confidence": ...,
}
```

- [ ] **Step 2: Re-export the new helpers through facades only where compatibility requires it**

Implementation note:

```python
from utils.primary_type_router import build_driver_stack, determine_primary_type
from utils.vcrf_probes import assess_survival_boundary, detect_big_bath
```

- [ ] **Step 3: Run driver-stack and probe tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFDriverStackTests -v
```

- [ ] **Step 4: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. Any facade/import breakage must be fixed inside this task.

- [ ] **Step 5: Commit**

```powershell
git add .agents/skills/shared/utils/vcrf_probes.py .agents/skills/shared/utils/framework_utils.py .agents/skills/shared/utils/research_utils.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add VCRF underwrite probes"
```

---

## Chunk 4: Realization Engine, Monitor, And Valuation Triad

### Task 8: Add failing tests for realization scoring and harvest monitoring

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing realization-axis score-shape test**

```python
class VCRFRealizationEngineTests(unittest.TestCase):
    def test_realization_axis_returns_all_six_components(self) -> None:
        from engines.flow_realization_engine import score_realization_axis

        result = score_realization_axis(scan_data={}, driver_stack={})
        self.assertEqual(
            set(result["components"].keys()),
            {
                "repair_state",
                "regime_cycle_position",
                "marginal_buyer_probability",
                "flow_confirmation",
                "elasticity",
                "catalyst_quality",
            },
        )
```

- [ ] **Step 2: Add a failing harvest-candidate test**

```python
    def test_attack_book_monitor_requires_price_and_flow_confirmation(self) -> None:
        from engines.attack_book_monitor import evaluate_harvest_candidate

        result = evaluate_harvest_candidate(
            closes=[10.1, 10.2, 10.3],
            recognition_price=10.0,
            daily_returns=[0.01, 0.01, 0.01],
            flow_stage="trend",
            cfg={"consecutive_closes_above_recognition": 3, "require_flow_stage_deterioration_to": "crowded"},
        )
        self.assertFalse(result["harvest_candidate"])
```

- [ ] **Step 3: Run realization tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFRealizationEngineTests -v
```

### Task 9: Implement realization-axis composition and attack-book monitor

**Files:**
- Create: `D:/A价投+周期/.agents/skills/shared/engines/flow_realization_engine.py`
- Create: `D:/A价投+周期/.agents/skills/shared/engines/attack_book_monitor.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Implement the six realization components with the spec mappings**

Required functions:

```python
def score_repair_state(... ) -> dict[str, Any]: ...
def score_regime_cycle_position(... ) -> dict[str, Any]: ...
def score_marginal_buyer_probability(... ) -> dict[str, Any]: ...
def score_flow_confirmation(... ) -> dict[str, Any]: ...
def score_elasticity(... ) -> dict[str, Any]: ...
def score_catalyst_quality(... ) -> dict[str, Any]: ...
def score_realization_axis(scan_data: dict[str, Any], driver_stack: dict[str, Any]) -> dict[str, Any]: ...
```

- [ ] **Step 2: Preserve raw L1/L2 detail in `flow_confirmation.reason`**

Implementation note:

```python
reason = f"l1_raw={level1_score:.1f}, l2_raw={level2_bonus:.1f}, clamped={score:.1f}"
```

- [ ] **Step 3: Implement `attack_book_monitor.py`**

Implement:

```python
def evaluate_harvest_candidate(
    *,
    closes: list[float],
    recognition_price: float,
    daily_returns: list[float],
    flow_stage: str,
    cfg: dict[str, Any],
) -> dict[str, Any]: ...
```

- [ ] **Step 4: Run realization tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFRealizationEngineTests -v
```

- [ ] **Step 5: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. Any compatibility regression introduced by realization helpers gets fixed here.

- [ ] **Step 6: Commit**

```powershell
git add .agents/skills/shared/engines/flow_realization_engine.py .agents/skills/shared/engines/attack_book_monitor.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add VCRF realization scoring and harvest monitor"
```

### Task 10: Upgrade `valuation_engine.py` to route-aware floor/normalized/recognition outputs

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/config/valuation_discipline.yaml`
- Modify: `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add failing valuation-route tests**

```python
class VCRFValuationRouteTests(unittest.TestCase):
    def test_normalized_case_depends_on_sector_route_not_primary_type_only(self) -> None:
        valuation = build_three_case_valuation(
            "600348",
            scan_data,
            {"sector_route": "core_resource", "primary_type": "cyclical"},
        )
        self.assertEqual(valuation["route_anchor"], "core_resource_mid_cycle")
```

- [ ] **Step 2: Run valuation tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFValuationRouteTests -v
```

- [ ] **Step 3: Update `valuation_discipline.yaml` with route-aware methods and thresholds**

Required additions:

```yaml
route_methods:
  core_resource:
    normalized_anchor: core_resource_mid_cycle
  rigid_shovel:
    normalized_anchor: rigid_shovel_capex_mid_cycle
  core_military:
    normalized_anchor: core_military_margin_anchor
```

- [ ] **Step 4: Refactor `valuation_engine.py`**

Implement or add:

```python
def _resolve_route_anchor(... ) -> tuple[str, float | None]: ...
def _build_floor_case(... ) -> dict[str, Any]: ...
def _build_normalized_case(... ) -> dict[str, Any]: ...
def _build_recognition_case(... ) -> dict[str, Any]: ...
```

Required output keys:

```python
{
    "floor_case": ...,
    "normalized_case": ...,
    "recognition_case": ...,
    "summary": {
        "floor_protection": ...,
        "normalized_upside": ...,
        "recognition_upside": ...,
        "wind_dependency": ...,
    },
    "bear_case": <alias>,
    "base_case": <alias>,
    "bull_case": <alias>,
}
```

- [ ] **Step 5: Run valuation tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFValuationRouteTests test_investment_framework.VCRFValuationContractTests -v
```

- [ ] **Step 6: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. Do not carry valuation compatibility regressions into Chunk 5.

- [ ] **Step 7: Commit**

```powershell
git add .agents/skills/shared/config/valuation_discipline.yaml .agents/skills/shared/engines/valuation_engine.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: add route-aware VCRF valuation triad"
```

---

## Chunk 5: Replace Universal Gate With Driver Stack + Dual Axis

### Task 11: Add failing tests for degradation caps, compatibility aliases, and state enforcement

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing gate-shape test**

```python
class VCRFUniversalGateTests(unittest.TestCase):
    def test_universal_gate_returns_driver_stack_and_dual_axes(self) -> None:
        gate = evaluate_universal_gates("600348", scan_data)
        self.assertIn("driver_stack", gate)
        self.assertIn("underwrite_axis", gate)
        self.assertIn("realization_axis", gate)
```

- [ ] **Step 2: Add a failing compatibility test**

```python
    def test_universal_gate_keeps_legacy_scorecard_aliases_during_transition(self) -> None:
        gate = evaluate_universal_gates("600348", scan_data)
        self.assertIn("scorecard", gate)
        self.assertIn("business_truth", gate["gates"])
```

- [ ] **Step 3: Add a failing transition-enforcement test**

```python
    def test_harvest_to_attack_is_downgraded(self) -> None:
        gate = evaluate_universal_gates("600348", scan_data, prior_state="HARVEST")
        self.assertNotEqual(gate["position_state"], "ATTACK")
```

- [ ] **Step 4: Run the gate tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFUniversalGateTests -v
```

### Task 12a: Rewrite `universal_gate.py` and deep-dive integration around the new contracts

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- Modify: `D:/A价投+周期/.agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add Driver Stack resolution to `evaluate_universal_gates()`**

Implementation target:

```python
driver_stack = build_driver_stack(stock_code, scan_data, extra_texts=extra_texts)
underwrite_axis = score_underwrite_axis(scan_data, driver_stack)
realization_axis = score_realization_axis(scan_data, driver_stack)
```

- [ ] **Step 2: Apply component-level degradation and legal state transitions**

Implement helper structure:

```python
proposed_state = classify_position_state(...)
proposed_state = _apply_degradation_caps(...)
position_state, transition_allowed, transition_reason = enforce_transition(...)
```

- [ ] **Step 3: Preserve compatibility aliases**

Required bridge fields:

```python
gate_result["scorecard"] = legacy_scorecard_alias(...)
gate_result["gates"]["business_truth"] = gate_result["gates"]["business_or_asset_truth"]
gate_result["gates"]["quality_truth"] = gate_result["gates"]["governance_truth"]
gate_result["signals"]["catalyst"] = legacy_catalyst_bridge(...)
```

- [ ] **Step 4: Update `deep_sniper_engine.py` to persist canonical `position_state` and transition metadata**

Required output additions:

- `driver_stack`
- `underwrite_axis`
- `realization_axis`
- `position_state`
- `prev_state`
- `transition_reason`

- [ ] **Step 5: Run gate and deep-dive tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFUniversalGateTests test_investment_framework.VCRFGateAndRadarTests test_investment_framework.VCRFValuationContractTests -v
```

- [ ] **Step 6: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. Any compatibility regression stays in this task.

- [ ] **Step 7: Commit**

```powershell
git add .agents/skills/shared/validators/universal_gate.py .agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: migrate universal gate to VCRF dual-axis scoring"
```

### Task 12b: Adapt `report_engine.py` to the VCRF rendering contract

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/engines/report_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a Driver Stack and state-machine summary section**

Render:

- `sector_route`
- `primary_type`
- modifiers summary
- `position_state`
- `prev_state`
- `transition_reason`

- [ ] **Step 2: Replace scorecard-centric rendering with dual-axis and valuation-triad sections**

Render:

- underwrite score and component detail
- realization score and component detail
- `floor_case`, `normalized_case`, `recognition_case`
- `wind_dependency`

- [ ] **Step 3: Preserve one-phase legacy readability bridges**

Keep:

- legacy scorecard alias display where still referenced
- legacy bear/base/bull labels as aliases to the new valuation triad
- section ordering stable enough that old consumers do not break during transition

- [ ] **Step 4: Run gate/report/deep-dive tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFUniversalGateTests test_investment_framework.VCRFGateAndRadarTests test_investment_framework.VCRFValuationContractTests -v
```

- [ ] **Step 5: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. Rendering regressions must be fixed here, not deferred.

- [ ] **Step 6: Commit**

```powershell
git add .agents/skills/shared/engines/report_engine.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: adapt reports to VCRF state and valuation outputs"
```

---

## Chunk 6: Integrate Radar, Calibration, Smoke Coverage, And Spike Validation

### Task 13: Add failing tests for two-stage radar and calibrator outputs

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing radar coarse/fine split test**

```python
class VCRFRadarIntegrationTests(unittest.TestCase):
    def test_radar_coarse_stage_limits_fine_stage_candidate_count(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        result = radar_scan_engine.run_radar_scan("A-share", limit=24)
        self.assertIn("coarse_candidate_count", result)
        self.assertLessEqual(result["fine_candidate_count"], result["coarse_candidate_count"])
```

- [ ] **Step 2: Add a failing calibrator test**

```python
    def test_vcrf_calibrator_reports_axis_quantiles(self) -> None:
        from engines.vcrf_calibrator import summarize_axis_distribution

        report = summarize_axis_distribution([10, 20, 30, 40, 50])
        self.assertIn("p50", report)
        self.assertIn("histogram", report)
```

- [ ] **Step 3: Run the new radar/calibrator tests and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFRadarIntegrationTests -v
```

### Task 14: Rebuild radar around two-stage filtering and state-aware ranking

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Create: `D:/A价投+周期/.agents/skills/shared/engines/vcrf_calibrator.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Implement a cheap Stage-1 universe reducer**

Required behavior:

```python
def _coarse_filter_universe(... ) -> list[dict[str, Any]]:
    # route/lightweight tags only
    # liquidity + size buckets + ST tagging
    # return roughly 200-400 names
```

- [ ] **Step 2: Make Stage 2 call the full Driver Stack and dual-axis path only for survivors**

Required payload fields:

```python
{
    "position_state": ...,
    "prev_state": ...,
    "flow_stage": ...,
    "sector_route": ...,
    "primary_type": ...,
    "floor_protection": ...,
    "normalized_upside": ...,
    "recognition_upside": ...,
}
```

- [ ] **Step 3: Add `vcrf_calibrator.py`**

Implement:

```python
def summarize_axis_distribution(scores: list[float]) -> dict[str, Any]: ...
def build_calibration_report(records: list[dict[str, Any]]) -> dict[str, Any]: ...
```

- [ ] **Step 4: Apply state-aware shortlist rules**

Required bucket logic:

- `priority_shortlist`: only `READY` and `ATTACK`
- `secondary_watchlist`: `COLD_STORAGE`, `READY`, `ATTACK`
- `rejected`: everything else

- [ ] **Step 5: Run radar/calibrator tests until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFRadarIntegrationTests test_investment_framework.VCRFGateAndRadarTests -v
```

- [ ] **Step 6: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. Radar integration should not break earlier gate or valuation contracts.

- [ ] **Step 7: Commit**

```powershell
git add .agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py .agents/skills/shared/engines/vcrf_calibrator.py .agents/skills/shared/tests/test_investment_framework.py
git commit -m "feat: integrate VCRF radar and calibration workflow"
```

### Task 15: Add end-to-end smoke coverage for the full VCRF contract

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
- Modify as needed: `D:/A价投+周期/.agents/skills/shared/validators/universal_gate.py`
- Modify as needed: `D:/A价投+周期/.agents/skills/shared/engines/valuation_engine.py`
- Modify as needed: `D:/A价投+周期/.agents/skills/shared/engines/flow_realization_engine.py`

- [ ] **Step 1: Add a failing synthetic end-to-end smoke test**

```python
class VCRFEndToEndSmokeTests(unittest.TestCase):
    def test_synthetic_candidate_flows_through_driver_stack_gate_and_valuation_contract(self) -> None:
        result = evaluate_universal_gates("600348", SYNTHETIC_SCAN_DATA, prior_state="NEW")
        valuation = build_three_case_valuation("600348", SYNTHETIC_SCAN_DATA, result["driver_stack"])

        self.assertIn("driver_stack", result)
        self.assertIn("underwrite_axis", result)
        self.assertIn("realization_axis", result)
        self.assertIn("position_state", result)
        self.assertIn("floor_case", valuation)
        self.assertIn("normalized_case", valuation)
        self.assertIn("recognition_case", valuation)
```

- [ ] **Step 2: Run the end-to-end smoke test and verify failure**

Run:

```powershell
python -m unittest test_investment_framework.VCRFEndToEndSmokeTests -v
```

Expected: FAIL until the synthetic-path glue and output contracts are fully aligned.

- [ ] **Step 3: Fix any remaining in-memory contract gaps without introducing network dependencies**

Implementation notes:

- use synthetic/local fixtures only
- no AkShare or CNINFO calls inside this test path
- patch missing default handling in gate / realization / valuation helpers as required

- [ ] **Step 4: Run the end-to-end smoke test until green**

Run:

```powershell
python -m unittest test_investment_framework.VCRFEndToEndSmokeTests -v
```

- [ ] **Step 5: Run the full shared test suite before commit**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS. The smoke coverage task should tighten contracts, not introduce new regressions.

- [ ] **Step 6: Commit**

```powershell
git add .agents/skills/shared/tests/test_investment_framework.py .agents/skills/shared/validators/universal_gate.py .agents/skills/shared/engines/valuation_engine.py .agents/skills/shared/engines/flow_realization_engine.py
git commit -m "test: add VCRF end-to-end smoke coverage"
```

### Task 16: Run the `detect_big_bath()` spike before final turnaround calibration

**Files:**
- Verify only, plus optional notes file if desired

- [ ] **Step 1: Select 3-5 known A-share impairment / cleanup names**

Record the sample set with dates and expected verdicts.

- [ ] **Step 2: Run the CNINFO -> PDF -> Docling path for each sample**

Suggested commands:

```powershell
python ".agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py" 600XXX "SampleCo"
```

- [ ] **Step 3: Evaluate whether impairment rows, OCF trend, and margin trend are recoverable**

Acceptance criteria:

- impairment evidence extracted in most cases
- period alignment is possible
- medium-confidence verdict exists for most samples

- [ ] **Step 4: If the spike fails, lock `detect_big_bath()` to the conservative fallback**

Fallback rule:

```python
verdict = "inconclusive" if impairment_extraction_unreliable else computed_verdict
```

- [ ] **Step 5: Checkpoint note**

Record: `big_bath spike completed; final turnaround calibration mode decided.`

---

## Chunk 7: Full Verification And Execution Handoff

### Task 17: Run the complete verification suite

**Files:**
- Verify only

- [ ] **Step 1: Run the shared test suite**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS.

- [ ] **Step 2: If discover is noisy, rerun by VCRF test groups**

Run:

```powershell
python -m unittest test_investment_framework.VCRFConfigContractTests -v
python -m unittest test_investment_framework.VCRFDataSourceTests test_investment_framework.VCRFStateHistoryTests -v
python -m unittest test_investment_framework.VCRFDriverStackTests test_investment_framework.VCRFRealizationEngineTests -v
python -m unittest test_investment_framework.VCRFValuationRouteTests test_investment_framework.VCRFUniversalGateTests -v
python -m unittest test_investment_framework.VCRFRadarIntegrationTests test_investment_framework.VCRFEndToEndSmokeTests -v
```

- [ ] **Step 3: Run one deep-dive smoke test**

Run:

```powershell
python ".agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py" 600348 "华阳股份"
```

Verify:

- output includes `driver_stack`
- output includes `underwrite_axis` and `realization_axis`
- output includes canonical `position_state`

- [ ] **Step 4: Run one radar smoke test**

Run:

```powershell
python ".agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py" "600328,600348,000731" --limit 3
```

Verify:

- stage-1 and stage-2 counts are present
- `priority_shortlist` contains only `READY` or `ATTACK`
- at least one payload includes `sector_route` and `primary_type`

- [ ] **Step 5: Record final checkpoint note**

Record: `VCRF OS 2.0 core implementation verified; compatibility aliases remain until exit criteria are met.`
