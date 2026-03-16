# Legacy Cleanup Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the remaining non-active crocodile-era entry points from shared utilities and physically isolate the legacy skill tree.

**Architecture:** Keep the active shared framework intact, then clean the dormant surface in two steps: normalize `signal_health_utils.py` around opportunity-type naming and move the old `a_stock_sniper/` tree out of `.agents/skills`. Guard both changes with explicit regression tests.

**Tech Stack:** Python 3, unittest, PowerShell filesystem moves

---

## Chunk 1: Signal Health Naming

### Task 1: Cover primary-type routing in tests

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
- Modify: `D:/A价投+周期/.agents/skills/shared/utils/signal_health_utils.py`

- [ ] **Step 1: Write the failing test**
- [ ] **Step 2: Run the targeted test and verify it fails**
- [ ] **Step 3: Update `signal_health_utils.py` to prefer `primary_type`/`opportunity_type` over `four_signal_mode`**
- [ ] **Step 4: Re-run the targeted test and verify it passes**

## Chunk 2: Legacy Skill Isolation

### Task 2: Archive the old skill tree

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
- Modify: `D:/A价投+周期/CLAUDE.md`
- Move: `D:/A价投+周期/.agents/skills/a_stock_sniper` -> `D:/A价投+周期/.agents/_archive/a_stock_sniper`

- [ ] **Step 1: Write the failing archive-path test**
- [ ] **Step 2: Run the targeted test and verify it fails**
- [ ] **Step 3: Move the directory and update docs**
- [ ] **Step 4: Run the full shared test suite and syntax checks**
