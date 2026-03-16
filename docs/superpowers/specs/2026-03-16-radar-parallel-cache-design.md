# Radar Parallel Cache Design

**Date:** 2026-03-16

**Goal:** Reduce radar scan latency further by adding a radar-specific day cache and then parallelizing scans across stocks, without changing ranking semantics or interfering with the existing deep-dive cache path.

## Context

The current radar path already avoids unconditional full scans by using a two-stage flow:

1. Stage 1 fetches cheap fields and computes a safe score upper bound
2. Stage 2 enriches only the survivors with missing expensive fields

This fixed the worst inefficiency, but the radar path still has two large runtime bottlenecks:

- stock processing is still fully serial in [radar_scan_engine.py](D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py)
- the new radar path does not persist any field-level day cache, so repeated scans on the same trading day still refetch everything

The old hourly freshness cache in [akshare_adapter.py](D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py) only applies to `run_full_scan(output_dir=...)`. The radar path does not call that entrypoint anymore, so it currently gets none of that reuse.

## Non-Goals

- Do not change final radar ranking semantics.
- Do not change the two-stage safe-prefilter logic.
- Do not reuse the old hourly freshness rules for radar cache hits.
- Do not refactor deep-dive caching in this step.
- Do not introduce nested concurrency across both stocks and fields in the first pass.

## Recommended Approach

Implement throughput improvements in this order:

1. add a radar-specific day cache keyed by resolved trading day, stock code, and field name
2. add cross-stock thread-pool parallelism while keeping per-stock field fetches serial
3. expose worker count, retry delays, and cache toggle in scan config

This order keeps the design understandable and lets the cache pay down most repeated work before adding concurrency.

## Why This Approach

- Day cache helps both cold-start follow-up scans and repeated operator runs on the same trading day.
- Cross-stock parallelism attacks the largest remaining wall-clock bottleneck.
- Keeping field fetches serial inside each stock avoids a more complex concurrency model and reduces provider-rate-limit risk.
- Separating radar cache from deep-dive cache avoids semantic drift and accidental reuse of stale hourly logic.

## Design

### 1. Resolve one radar trade date per scan

The radar run should resolve a single `radar_trade_date` once at startup and use it everywhere:

- day cache path
- cache hit/miss decisions
- metadata written for the scan

This date must represent the current A-share trading day semantics:

- on an open trading day: that same trading date
- on weekends or exchange holidays: the latest prior trading day

Recommended helper:

- add a dedicated `resolve_radar_trade_date()` helper, likely in the shared adapter/util layer

This helper should not rely on local wall-clock date alone. It should use the exchange calendar or the existing BaoStock trading-date path so the cache key tracks market reality rather than calendar date.

### 2. Add a radar-specific day cache

The radar day cache is separate from `run_full_scan()` cache files.

Directory layout:

```text
data/processed/radar_cache/
  2026-03-16/
    600328.json
    600348.json
    _meta.json
```

Semantics:

- cache key = `(radar_trade_date, stock_code, field_name)`
- if a field is already cached for that trade date, reuse it directly
- no hourly freshness check applies
- when `radar_trade_date` changes, the old directory is naturally ignored

Each stock file should store field-level results incrementally:

```json
{
  "company_profile": { "...": "..." },
  "revenue_breakdown": { "...": "..." },
  "valuation_history": { "...": "..." },
  "stock_kline": { "...": "..." },
  "realtime_quote": { "...": "..." },
  "income_statement": { "...": "..." },
  "balance_sheet": { "...": "..." }
}
```

`_meta.json` should record at least:

- `trade_date`
- `created_at`
- optional cache schema/version marker

### 3. Radar day cache hit behavior

When `run_named_scan_steps()` is used by the radar path, it should accept an optional day-cache directory and check it before provider fetch.

Proposed signature extension:

```python
def run_named_scan_steps(
    stock_code: str,
    step_map: dict[str, Any],
    *,
    cached_results: dict[str, Any] | None = None,
    day_cache_dir: Path | None = None,
) -> dict[str, Any]:
    ...
```

Hit rule:

- if `day_cache_dir / f"{stock_code}.json"` exists and contains the requested field, return it immediately

Returned status for a hit:

- `ok_day_cache`

Evidence behavior:

- preserve original evidence payload if present
- annotate description with day-cache provenance
- keep the original source type (`akshare`, `baostock`, etc.) visible

This is intentionally different from:

- `ok_cached_fallback`
- `stale_cached_fallback`

because radar day cache is not a freshness-degraded fallback; it is the primary intra-day reuse mechanism.

### 4. Radar day cache miss behavior

On a miss:

1. call the existing `_resolve_scan_step()` logic
2. if the field fetch succeeds, write the field back to the stock cache file
3. return the freshly fetched result

Writes should be field-incremental so Stage 1 and Stage 2 naturally share the same stock cache file.

Recommended write behavior:

- read the existing stock cache file if present
- update only the requested fields
- write back atomically via temporary file + replace

### 5. Keep radar cache independent from hourly deep-dive cache

The old cache path in `run_full_scan()` stays as-is:

- still keyed by output directory
- still governed by `CACHE_STALE_HOURS`
- still used by non-radar workflows

The new radar day cache should not:

- reinterpret `CACHE_STALE_HOURS`
- overwrite `data/raw/{code}/akshare_scan.json`
- try to merge semantics with deep-dive caching

These are two different cache systems with different freshness guarantees.

### 6. Parallelize across stocks, not across fields

Add cross-stock parallelism in `run_radar_scan()` using `ThreadPoolExecutor`.

Recommended concurrency shape:

- one worker processes one stock at a time
- within a stock, Stage 1 and Stage 2 field fetches stay serial

Recommended structure:

```python
def _scan_one_stock(item, *, secondary_cutoff, day_cache_dir):
    partial_scan_data = run_named_scan_steps(
        item["code"],
        RADAR_PARTIAL_STEPS,
        day_cache_dir=day_cache_dir,
    )
    partial_gate = evaluate_partial_gate_dimensions(item["code"], partial_scan_data)
    ...
    return payload
```

Then:

```python
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = {
        pool.submit(
            _scan_one_stock,
            item,
            secondary_cutoff=secondary_cutoff,
            day_cache_dir=day_cache_dir,
        ): item
        for item in universe
    }
    for future in as_completed(futures):
        ranked.append(future.result())
```

This is the preferred first concurrency layer because:

- it gives the highest speedup
- it avoids per-stock write contention
- it keeps retry and fallback logic simple

### 7. BaoStock fallback must be serialized

`akshare` HTTP-style calls are generally safe to parallelize conservatively.

`baostock` should be treated as non-thread-safe for now because it uses an explicit login/session pattern and may hide shared global state.

Recommended protection:

- add a module-level `threading.Lock()` in [baostock_adapter.py](D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py)
- wrap each public query path or the `_session()` block with that lock

This should serialize BaoStock fallback calls while leaving `akshare` free to run in parallel.

Because BaoStock is only hit on fallback paths, the lock should not materially hurt normal-case throughput.

### 8. Keep per-stock cache writes contention-free

The design relies on a simple concurrency invariant:

- one stock is processed by one worker
- only that worker writes `day_cache_dir / f"{stock_code}.json"`

That means:

- no cross-thread writes to the same stock cache file
- Stage 1 and Stage 2 updates for a stock remain sequential inside the same worker

This is why the first concurrency layer should be cross-stock only.

### 9. Retry policy after cache lands

Current retry delays in `_resolve_scan_step()` are:

- `1.0`
- `2.0`

Once radar day cache exists, repeat runs will avoid most retries automatically because cache hits return before provider access.

Recommended adjustment for radar workloads:

- keep retry logic, but make it configurable
- default radar retry delays should be more conservative on wall-clock cost, e.g. `(0.5, 1.0)`

This should be configuration-driven rather than hard-coded into deep-dive behavior.

### 10. Add radar throughput config

Extend [scan_defaults.yaml](D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml) with radar throughput controls.

Suggested keys:

```yaml
defaults:
  radar_day_cache_enabled: true
  radar_max_workers: 4
  radar_retry_delays:
    - 0.5
    - 1.0
```

Behavior:

- `radar_day_cache_enabled=false` disables the new cache layer for debugging
- `radar_max_workers` controls cross-stock worker pool size
- `radar_retry_delays` applies to radar fetch calls only

These should not silently rewrite deep-dive defaults.

## Data Contracts

### Radar cache stock file

Each stock cache file should be a mapping from top-level `scan_data` keys to the normal field result envelope:

```python
{
    "company_profile": {
        "data": {...},
        "evidence": {...},
        "status": "ok",
        "fetch_timestamp": "...",
    },
    "revenue_breakdown": {...},
}
```

This lets `run_named_scan_steps()` reuse cached field results without translation.

### Cache dispatch keys

The keys in:

- `RADAR_PARTIAL_STEPS`
- `RADAR_EXPENSIVE_STEPS`
- radar cache stock files
- `fields_to_fetch`

must all match the top-level `scan_data` keys exactly.

No translation layer should be required between partial evaluation output and radar fetch dispatch.

## Testing Strategy

Add targeted tests in [test_investment_framework.py](D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py).

Required day-cache coverage:

- first radar fetch writes a stock cache file for the resolved trade date
- second same-day radar fetch reuses cached fields and returns `ok_day_cache`
- same-day Stage 2 enrich appends missing fields instead of rewriting unrelated ones
- different trade date results in a cache miss
- radar day cache does not use `CACHE_STALE_HOURS`

Required parallel coverage:

- parallel radar result set matches serial radar result set
- result ordering remains controlled by final sort, not completion order
- BaoStock fallback path still works under parallel radar execution
- BaoStock lock prevents concurrent fallback access from corrupting results

Required config coverage:

- disabling day cache forces fresh fetches
- lowering worker count still preserves correctness

## Verification

After implementation:

- run the shared test suite
- run a same-day radar scan twice and confirm the second run is materially faster
- verify the second run shows `ok_day_cache` on reused fields
- run a small parallel live radar sample and compare its ranked output with a serial run on the same sample
- force an `akshare` universe failure and confirm BaoStock fallback still works under the new execution model

Success criteria:

- second same-day radar scan reuses cached fields directly
- cross-stock parallelism reduces wall-clock time on cold-start radar scans
- outputs remain semantically identical to the serial path

## Risks

### Risk: wrong trade-date resolution causes bad cache hits

If the radar cache uses wall-clock date instead of resolved exchange trade date, weekend and holiday runs may read or write the wrong cache bucket.

Mitigation:

- resolve one `radar_trade_date` from market calendar logic at scan start
- use that same value everywhere in the radar run

### Risk: provider throttling under parallel load

Parallel `akshare` access may trigger slower responses or upstream throttling.

Mitigation:

- keep default `radar_max_workers` conservative
- start with cross-stock parallelism only
- keep fallback and retry policy configurable

### Risk: BaoStock global state is not thread-safe

Concurrent BaoStock fallback calls may race through login/session handling.

Mitigation:

- serialize BaoStock public query execution with a lock

### Risk: cache semantics drift from radar semantics

If radar day cache starts sharing the old hourly stale-cache logic, operators may get confusing results.

Mitigation:

- keep radar cache and deep-dive cache separate
- use explicit `ok_day_cache` status for radar cache hits

## Files Expected To Change

- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py`
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml`
- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

## Decision

Proceed with radar-specific day cache first, then add conservative cross-stock thread-pool parallelism, and keep both changes isolated from the existing deep-dive cache semantics.
