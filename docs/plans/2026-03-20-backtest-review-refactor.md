# Backtest Review Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Align the VCRF backtest and candidate-selection stack with the review memo by replacing legacy pulse ranking and global stops with route-aware flow, sleeve-aware ranking, dynamic valuation refresh, refill behavior, and richer diagnostics.

**Architecture:** Keep the existing research pipeline and VCRF gate contracts, but upgrade the execution-facing layers. The flow engine will emit a cleaner fundamental-momentum-aware confirmation payload; the selection layer will rank candidates cross-sectionally with sleeve isolation; the backtest engine will refresh thesis prices from later signals, apply type-aware stop rules, refill empty slots from a same-sleeve waitlist, and publish the new KPI set.

**Tech Stack:** Python 3.11, `unittest`, pandas, YAML config, existing shared engines/utils

---

### Task 1: Lock New Selection And Exit Behavior With Tests

**Files:**
- Modify: `.agents/skills/shared/tests/test_backtest_pipeline.py`
- Test: `.agents/skills/shared/tests/test_backtest_pipeline.py`

**Step 1: Write failing tests for expectation-error ranking and sleeve isolation**

Add tests that assert:
- selection no longer sorts by `effective_date`
- each round prefers distinct sleeves
- a high-score same-sleeve duplicate is pushed to waitlist when another sleeve is available

**Step 2: Run only the new ranking tests and verify failure**

Run: `/.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_backtest_pipeline.py' -v`

Expected: failures around old sort order and missing sleeve fields.

**Step 3: Write failing tests for type-aware stops, monthly thesis refresh, and waitlist refill**

Add tests that assert:
- `turnaround` names do not trigger `max_loss_stop`
- `compounder` names still do
- a later monthly signal updates `floor_price` / `recognition_price`
- a vacancy can be refilled from same-sleeve waitlist on the next effective date

**Step 4: Run the targeted backtest tests again and verify failure**

Run: `/.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_backtest_pipeline.py' -v`

Expected: failures around missing dynamic refresh and refill behavior.

### Task 2: Lock New Flow Confirmation Contract

**Files:**
- Modify: `.agents/skills/shared/tests/test_backtest_pipeline.py`
- Modify: `.agents/skills/shared/tests/test_investment_framework.py`

**Step 1: Write failing tests for fundamental-momentum-aware flow confirmation**

Add tests that assert:
- left-side absorption is capped at ignition without fundamental or breakout confirmation
- strong margin/cashflow/ROA improvement can promote flow into trend
- legacy pulse-only setup no longer auto-scores into the 90s

**Step 2: Run the targeted flow tests and verify failure**

Run: `/.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_backtest_pipeline.py' -v`

Expected: failures because current flow logic still overweights pulse-volume events.

### Task 3: Implement Minimal Production Changes

**Files:**
- Modify: `.agents/skills/shared/engines/flow_realization_engine.py`
- Modify: `.agents/skills/shared/engines/backtest_engine.py`
- Modify: `.agents/skills/shared/config/backtest_protocol.yaml`
- Modify: `scripts/run_vcrf_backtest.py`

**Step 1: Implement fundamental-momentum-aware flow confirmation**

Add:
- constrained absorption logic
- fundamental momentum score from existing PIT factor columns
- promotion rules that require either fundamentals, breakout, or catalyst evidence

**Step 2: Implement cross-sectional candidate ranking with sleeve isolation**

Add:
- z-score helper
- expectation-error score
- sleeve mapping
- same-route concentration cap
- round-level waitlist bookkeeping

**Step 3: Implement type-aware backtest exits and thesis refresh**

Add:
- per-type stop lookup
- monthly refresh from later `signal_daily` rows
- same-sleeve waitlist refill
- enhanced trade-level diagnostics (`mfe`, `mae`, `days_to_target`)

**Step 4: Implement richer summary/report output**

Add:
- `median_trade_irr`
- `expectancy`
- `mfe_mae_ratio`
- `median_days_to_target`
- `peak_gross_exposure_ratio`
- report emphasis on `portfolio_cagr` over `avg_stock_cagr`

### Task 4: Verify End To End

**Files:**
- No new files

**Step 1: Run the focused backtest test suite**

Run: `/.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_backtest_pipeline.py' -v`

Expected: PASS.

**Step 2: Run the focused framework regression suite**

Run: `/.venv/bin/python -m unittest discover -s .agents/skills/shared/tests -p 'test_investment_framework.py' -v`

Expected: PASS.

**Step 3: Run a smoke backtest**

Run: `/.venv/bin/python scripts/run_vcrf_backtest.py --signals-month-end reports/backtests/smoke_tushare_inputs/signals_month_end.csv --daily-bars reports/backtests/smoke_tushare_inputs/daily_bars.csv --out-dir reports/backtests/smoke_latest_review_refactor`

Expected: outputs written successfully with the new summary columns.
