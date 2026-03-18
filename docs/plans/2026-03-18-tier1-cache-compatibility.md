# Tier1 Cache Compatibility Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace hard-coded `akshare_scan.json` naming assumptions with a provider-agnostic Tier1 cache convention while preserving backward compatibility.

**Architecture:** Centralize cache path resolution and read/write behavior in `provider_router.py`. Writers will emit canonical `tier1_scan.json` and `tier1_evidence.json`, plus legacy `akshare_*` mirrors for compatibility. Readers will prefer canonical files and fall back to legacy files when needed.

**Tech Stack:** Python 3.11, `unittest`, existing shared adapters and engines.

---

### Task 1: Add failing cache compatibility tests

**Files:**
- Create: `.agents/skills/shared/tests/test_tier1_cache_compatibility.py`

**Step 1: Write the failing test**

Add tests for:
- `load_scan_cache()` preferring `tier1_scan.json` over `akshare_scan.json`
- `load_scan_cache()` falling back to `akshare_scan.json` if canonical file is missing
- `run_full_scan()` writing both canonical and legacy cache/evidence files

**Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_tier1_cache_compatibility.py'`

Expected: FAIL because these helpers do not exist yet and the writer still emits legacy names only.

### Task 2: Implement canonical cache helpers in the router

**Files:**
- Modify: `.agents/skills/shared/adapters/provider_router.py`

**Step 1: Use the failing tests from Task 1**

Keep the tests red while implementing only the minimal cache helpers.

**Step 2: Write minimal implementation**

Add:
- canonical and legacy filename constants
- helper functions returning cache/evidence paths
- `load_scan_cache()` that prefers canonical and falls back to legacy
- `write_scan_cache()` that writes both canonical and legacy files
- update `run_full_scan()` to use these helpers

**Step 3: Run test to verify it passes**

Run the same targeted test command.

### Task 3: Migrate local readers to the shared cache loader

**Files:**
- Modify: `.agents/skills/shared/engines/public_backtest_dataset_engine.py`
- Modify: `.agents/skills/shared/validators/tier0_autofill.py`
- Modify: `scripts/build_public_backtest_inputs.py`

**Step 1: Write the failing test**

Extend tests to cover public-cache loading through the router helper, using canonical cache only.

**Step 2: Run test to verify it fails**

Confirm the loader still looks for `akshare_scan.json` directly.

**Step 3: Write minimal implementation**

Replace hard-coded local cache reads with `provider_router.load_scan_cache(...)`. Update user-facing help text from `akshare_scan.json` to provider-agnostic wording.

**Step 4: Run tests to verify they pass**

Run:
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_tier1_cache_compatibility.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_public_backtest_dataset.py'`

### Task 4: Regression and live verification

**Files:**
- Test only

**Step 1: Run focused regressions**

Run:
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_provider_routing.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_tushare_primary_adapter.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_public_backtest_dataset.py'`
- `.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_investment_framework.py' -k base_dir`

**Step 2: Run a live smoke**

Use `.venv/bin/python` to call `provider_router.run_full_scan('600328', temp_dir)` and verify:
- `tier1_scan.json` exists
- `akshare_scan.json` still exists
- `_scan_provider == "tushare"`
- public loader can read the canonical file
