# BaoStock Fallback Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the A-share scanner operational when `akshare` market snapshot endpoints fail by adding `baostock` as an explicit day-level fallback for universe, kline, valuation, and quote snapshot retrieval.

**Architecture:** Add one narrow `baostock` adapter, then wire it into the existing `akshare`-centric flow only at the failure boundaries. Preserve `akshare` as the primary source, and mark every `baostock` path as a latest-trading-day snapshot rather than a realtime feed.

**Tech Stack:** Python 3.13, `unittest`, `akshare`, `baostock`, PowerShell

**Environment note:** `D:/A价投+周期` is not a git repository, so normal commit steps are not available in this workspace.

---

## Chunk 1: Regression Tests First

### Task 1: Add fallback behavior tests before implementation

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Write the failing universe fallback test**

```python
def test_radar_universe_falls_back_to_baostock_when_akshare_snapshot_fails(self) -> None:
    ...
```

- [ ] **Step 2: Run the targeted test and verify it fails**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: failure because `_load_universe()` does not yet use `baostock`.

- [ ] **Step 3: Write the failing quote snapshot fallback test**

```python
def test_realtime_quote_uses_baostock_daily_snapshot_when_primary_sources_fail(self) -> None:
    ...
```

- [ ] **Step 4: Write the failing valuation-history fallback test**

```python
def test_valuation_history_uses_baostock_daily_pb_series(self) -> None:
    ...
```

- [ ] **Step 5: Write the failing stock-kline fallback test**

```python
def test_stock_kline_uses_baostock_history_when_akshare_history_fails(self) -> None:
    ...
```

- [ ] **Step 6: Re-run the targeted test file and verify the new tests fail for the expected reasons**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: new fallback tests fail because `baostock` wiring is still missing.

## Chunk 2: Add the BaoStock Adapter

### Task 2: Create a dedicated adapter for normalized BaoStock access

**Files:**
- Create: `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py`

- [ ] **Step 1: Create `_load_baostock()` and import guards**
- [ ] **Step 2: Implement login-scoped helpers for query execution**
- [ ] **Step 3: Implement `get_all_a_share_stocks()` returning normalized code/name rows**
- [ ] **Step 4: Implement `get_stock_basic()` returning normalized basic metadata**
- [ ] **Step 5: Implement `get_daily_history()` returning normalized record lists**
- [ ] **Step 6: Add explicit `source_type="baostock"` evidence payloads**
- [ ] **Step 7: Run the test file again and keep it red only for the integration points**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: adapter-only pieces are in place, but integration tests still fail.

## Chunk 3: Wire the Fallback into Existing Adapters

### Task 3: Add `baostock` fallback to `akshare_adapter.py`

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- Modify: `D:/A价投+周期/requirements.txt`

- [ ] **Step 1: Add lazy loading for the new `baostock_adapter`**
- [ ] **Step 2: Update `get_realtime_quote()` to return `ok_fallback_baostock_daily_snapshot` when primary quote paths fail**
- [ ] **Step 3: Update `get_valuation_history()` to derive PB stats from `baostock` daily rows when `akshare` valuation fetch fails**
- [ ] **Step 4: Update `get_stock_kline()` to derive summary metrics from `baostock` history when `akshare` history fetch fails**
- [ ] **Step 5: Add `baostock` to `requirements.txt`**
- [ ] **Step 6: Re-run the targeted tests and verify the adapter fallback tests pass**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: quote / kline / valuation fallback tests pass.

## Chunk 4: Wire the Fallback into Universe Scanning

### Task 4: Make radar universe loading survive `akshare` market-snapshot failure

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`

- [ ] **Step 1: Import the new `baostock` fallback helper**
- [ ] **Step 2: Wrap the current `ak.stock_zh_a_spot_em()` universe path**
- [ ] **Step 3: On failure, call `get_all_a_share_stocks()` and preserve deterministic filtering**
- [ ] **Step 4: Re-run the test file and verify the universe fallback test passes**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: all newly added fallback tests pass.

## Chunk 5: End-to-End Verification

### Task 5: Verify forced-fallback execution paths

**Files:**
- Verify only:
  - `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py`
  - `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
  - `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
  - `D:/A价投+周期/requirements.txt`

- [ ] **Step 1: Run the full targeted shared test file**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS.

- [ ] **Step 2: Force a single-stock fallback path for `600328`**

Run:

```powershell
@'
from adapters.akshare_adapter import get_realtime_quote, get_stock_kline, get_valuation_history
print(get_realtime_quote("600328")["status"])
print(get_stock_kline("600328")["status"])
print(get_valuation_history("600328")["status"])
'@ | python -X utf8 -
```

Expected: at least one path demonstrates `ok_fallback_baostock_*` status when the primary source is disabled or failing.

- [ ] **Step 3: Run a forced-fallback market scan**

Run:

```powershell
python "D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py" "A-share" --limit 12
```

Expected: scan completes without crashing on `stock_zh_a_spot_em()` failure and writes report artifacts.

- [ ] **Step 4: Record verification findings in the close-out**
- [ ] **Step 5: Skip commit because this workspace is not a git repository**
