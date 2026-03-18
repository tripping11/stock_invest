# Tushare Provider Routing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Route market scan and deep-dive entrypoints through a shared Tushare-first provider layer while preserving existing cache paths and fallback behavior.

**Architecture:** Add a shared provider router in `shared/adapters` that exports the scan contracts (`RADAR_*`, `run_named_scan_steps`, `run_full_scan`, `get_all_a_share_stocks`). The router selects `tushare_adapter` as primary and reuses generic cache/retry logic from the existing scan infrastructure, while keeping `akshare` and `baostock` as fallback providers behind adapter boundaries.

**Tech Stack:** Python 3.11, `unittest`, existing shared adapters, current cache JSON conventions.

---

### Task 1: Add provider routing contract tests

**Files:**
- Create: `.agents/skills/shared/tests/test_provider_routing.py`
- Modify: none

**Step 1: Write the failing test**

Add tests that:
- load `radar_scan_engine.py` and assert its imported `RADAR_PARTIAL_STEPS`, `RADAR_ALL_STEPS`, `run_named_scan_steps`, `resolve_radar_trade_date`, and `get_all_a_share_stocks` come from the new shared router module
- load `deep_sniper_engine.py` and assert its imported `run_full_scan` comes from the new shared router module
- assert the router selects `tushare` by default

**Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_provider_routing.py'`

Expected: FAIL because the router module does not exist yet and the engines still import from `akshare_adapter`.

**Step 3: Write minimal implementation**

Create the test file with focused import-contract assertions only.

**Step 4: Run test to verify it fails**

Run the same command again and confirm the failure is due to missing router wiring rather than test syntax.

### Task 2: Implement the shared provider router

**Files:**
- Create: `.agents/skills/shared/adapters/provider_router.py`
- Modify: `.agents/skills/shared/adapters/tushare_adapter.py` only if a missing export blocks wiring

**Step 1: Write the failing test**

Use the Task 1 tests as the RED state for the router contract.

**Step 2: Run test to verify it fails**

Confirm import/routing failures are still present.

**Step 3: Write minimal implementation**

Implement `provider_router.py` with:
- a default provider name of `tushare`
- exported `RADAR_PARTIAL_STEPS`, `RADAR_ALL_STEPS`, and `FULL_SCAN_STEPS` from the selected adapter
- exported `get_all_a_share_stocks` from the selected adapter when available
- exported `run_named_scan_steps` and `resolve_radar_trade_date` by reusing the generic existing scan helpers
- a router-owned `run_full_scan` that uses the selected adapter’s `FULL_SCAN_STEPS` and keeps writing `akshare_scan.json` / evidence JSON for compatibility

**Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_provider_routing.py'`

Expected: PASS

### Task 3: Rewire market scan and deep dive engines

**Files:**
- Modify: `.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Modify: `.agents/skills/single-stock-deep-dive/scripts/engines/deep_sniper_engine.py`
- Test: `.agents/skills/shared/tests/test_provider_routing.py`

**Step 1: Write the failing test**

Extend provider routing tests to assert the engine imports now resolve to router exports.

**Step 2: Run test to verify it fails**

Run the same targeted test command and confirm the failure references direct adapter imports.

**Step 3: Write minimal implementation**

Change the engines to import from `adapters.provider_router` instead of direct `akshare_adapter` / `baostock_adapter` scan contracts.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_provider_routing.py'`

Expected: PASS

### Task 4: Regression verification

**Files:**
- Test: `.agents/skills/shared/tests/test_tushare_primary_adapter.py`
- Test: `.agents/skills/shared/tests/test_tushare_backtest_dataset.py`
- Test: `.agents/skills/shared/tests/test_public_backtest_dataset.py`

**Step 1: Run focused regression tests**

Run:
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_provider_routing.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_tushare_primary_adapter.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_tushare_backtest_dataset.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_public_backtest_dataset.py'`

Expected: all PASS

**Step 2: Run a live smoke verification**

Run a short `.venv/bin/python` snippet that imports the router and checks:
- `get_scan_adapter_name()` returns `tushare`
- `get_company_profile('600328')` via the selected adapter works with the configured token
- `run_full_scan('600328', temp_dir)` produces a compatible cache file

**Step 3: Review compatibility output**

Confirm `data/raw/<code>/akshare_scan.json` shape remains readable by the existing local-cache consumers.
