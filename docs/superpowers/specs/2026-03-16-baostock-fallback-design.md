# BaoStock Fallback Design

**Date:** 2026-03-16

**Goal:** Add `baostock` as a fallback data source for A-share market scanning so the system can still build a tradable universe and fetch day-level quote/valuation inputs when `akshare` market snapshot APIs fail.

## Context

The current A-share scanning path depends heavily on `akshare`, especially:

- whole-market universe enumeration in `radar_scan_engine.py`
- quote and market-cap retrieval in `akshare_adapter.py`
- price / valuation-derived signals used by downstream scoring

This failed in the current environment because `ak.stock_zh_a_spot_em()` and the underlying Eastmoney endpoint were being disconnected remotely. The failure blocks market-wide scanning even though other data paths still work.

`baostock` was validated locally on 2026-03-16 and can successfully:

- `login()`
- `query_all_stock(day=...)`
- `query_stock_basic(code=...)`
- `query_history_k_data_plus(...)`

`baostock` does **not** provide a direct whole-market real-time snapshot equivalent to `stock_zh_a_spot_em()`. Therefore the fallback must be explicitly modeled as a latest-trading-day snapshot, not a real-time feed.

## Non-Goals

- Do not replace `akshare` as the primary source.
- Do not redesign the whole provider architecture.
- Do not pretend `baostock` data is intraday real-time.
- Do not widen scope into unrelated data adapters.

## Recommended Approach

Use `baostock` as a targeted fallback only when the relevant `akshare` path fails.

### Why this approach

- Minimal surface-area change.
- Solves the concrete failure mode seen in production-like use.
- Preserves current `akshare` behavior when it works.
- Keeps the fallback semantics explicit and auditable.

## Design

### 1. Add a dedicated BaoStock adapter

Create `shared/adapters/baostock_adapter.py` with a narrow responsibility:

- manage `baostock` import and login/logout lifecycle
- fetch tradable stock universe for a given day
- fetch stock basic info
- fetch day-level historical quote rows with valuation fields
- normalize returned values into shapes compatible with existing adapter consumers

The adapter should expose small functions rather than a large class. This matches the current codebase style.

Proposed functions:

- `_load_baostock()`
- `_latest_trade_day()`
- `_to_bs_code(stock_code)`
- `_from_bs_code(code)`
- `_resultset_to_records(rs)`
- `get_all_a_share_stocks(day: str | None = None) -> dict[str, Any]`
- `get_stock_basic(stock_code: str) -> dict[str, Any]`
- `get_daily_history(stock_code: str, start_date: str, end_date: str, fields: str) -> dict[str, Any]`

### 2. Use BaoStock fallback in market universe loading

In `market-opportunity-scanner/scripts/engines/radar_scan_engine.py`, `_load_universe()` currently calls `ak.stock_zh_a_spot_em()` directly. This is the most fragile point for market-wide scanning.

Change `_load_universe()` to:

1. try current `akshare` path first
2. if it fails, call `baostock_adapter.get_all_a_share_stocks()`
3. filter to A-share common stocks
4. exclude ST names
5. return the first `limit` records in deterministic order

Because `baostock query_all_stock` does not return market cap, fallback ordering cannot mimic the current “sort by cap descending” behavior exactly. The fallback should prefer deterministic and broad coverage over fake precision.

Recommended fallback ordering:

- preserve provider order after filtering
- no extra pseudo-ranking

This is acceptable because the fallback goal is continuity, not a new ranking methodology.

### 3. Use BaoStock fallback in quote and valuation-related paths

In `shared/adapters/akshare_adapter.py`, add `baostock` fallback to the places most affected by the current outage:

- `get_realtime_quote()`
- `get_valuation_history()`
- `get_stock_kline()`
- optionally `get_company_profile()` for stock name / listing status if needed

#### `get_realtime_quote()`

Fallback behavior:

- query recent daily history from `baostock`
- use the latest available daily row as a snapshot
- return normalized fields including:
  - code
  - latest price
  - PB / PE if available
  - latest trade date

Status must clearly indicate snapshot semantics, e.g.:

- `ok_fallback_baostock_daily_snapshot`

If market cap cannot be reliably derived from `baostock` alone, leave it `None` rather than fabricating it.

#### `get_valuation_history()`

Fallback behavior:

- use `query_history_k_data_plus(..., fields='date,close,pbMRQ,peTTM')`
- derive:
  - `pb`
  - `pb_percentile`
  - `pb_min`
  - `pb_max`
  - `pb_median`
  - `latest_close`

This is actually a strong fit for `baostock`, because it provides `pbMRQ` directly on daily rows.

#### `get_stock_kline()`

Fallback behavior:

- use `query_history_k_data_plus(..., fields='date,open,high,low,close,volume,amount,turn,pctChg')`
- derive the same summary fields currently used downstream:
  - `latest_close`
  - `latest_date`
  - `high_5y`
  - `low_5y`
  - `current_vs_high`
  - `drawdown_from_5yr_high_pct`
  - `consolidation_months`
  - `avg_vol_20d`
  - `avg_vol_120d`
  - `volume_ratio_20_vs_120`

### 4. Status and evidence semantics

Fallback status strings must remain explicit so reports and debugging can distinguish them from primary-source success.

Recommended status names:

- `ok_fallback_baostock_universe`
- `ok_fallback_baostock_stock_basic`
- `ok_fallback_baostock_daily_snapshot`
- `ok_fallback_baostock_history`

Evidence payloads should set:

- `source_type: "baostock"`
- `source_tier: 1`
- description including exact queried endpoint/function
- confidence: `medium`

For daily snapshot fallbacks, evidence description must mention “latest trading day snapshot, not realtime”.

### 5. Dependency strategy

Add `baostock` to `requirements.txt`.

Do not vendor it immediately. `baostock` is lightweight and installs cleanly in this environment. If future portability issues appear, vendoring can be evaluated separately.

### 6. Testing strategy

Use TDD. Add focused unit tests around behavior, not around third-party internals.

New tests should cover:

- `_load_universe()` falls back to `baostock` when `akshare` universe fetch raises
- `get_realtime_quote()` returns a normalized daily snapshot when `akshare` and `efinance` fail
- `get_valuation_history()` can build PB stats from `baostock` history rows
- `get_stock_kline()` can build expected summary fields from `baostock` history rows

Tests should stub provider functions rather than call the network.

### 7. Verification

After implementation, run:

- unit tests for the new fallback behavior
- a forced-fallback market scan
- a forced-fallback single-stock pull for `600328`

Success criteria:

- market universe can be built without `stock_zh_a_spot_em()`
- quote / kline / valuation paths produce non-empty structured output
- scanner completes and writes report artifacts

## Risks

### Risk: market-cap loss in fallback universe

`baostock query_all_stock` does not provide market cap. The fallback universe will lose the current “sort by cap descending” heuristic.

Mitigation:

- keep fallback deterministic
- document the reduced fidelity
- treat this as acceptable degradation during upstream outage

### Risk: semantic confusion between snapshot and realtime

If `baostock` daily close is treated as realtime, downstream decisions become misleading.

Mitigation:

- explicit status names
- explicit evidence wording
- no fake intraday fields

### Risk: login lifecycle leaks or repeated login cost

`baostock` requires login before queries.

Mitigation:

- centralize login/logout handling in the adapter
- keep calls short and stateless

## Files Expected To Change

- `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py` (new)
- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
- `D:/A价投+周期/requirements.txt`

## Decision

Proceed with targeted `baostock` fallback only, with explicit day-snapshot semantics and test coverage for the failure paths that blocked market scanning.
