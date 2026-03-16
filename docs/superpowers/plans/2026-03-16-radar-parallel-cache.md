# Radar Parallel Cache Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce radar scan wall-clock time by adding a radar-only day cache and then conservative cross-stock parallelism, without changing ranking semantics or deep-dive cache behavior.

**Architecture:** Keep the current two-stage radar scorer intact and improve throughput around it. First add a radar-specific field cache keyed by resolved trading day, stock code, and `scan_data` field name; then wrap the per-stock two-stage scan in a `ThreadPoolExecutor`, while keeping field fetches serial inside each worker and serializing BaoStock fallback behind a global lock.

**Tech Stack:** Python 3.13, `unittest`, `akshare`, `baostock`, `concurrent.futures`, PowerShell

**Spec:** `D:/A价投+周期/docs/superpowers/specs/2026-03-16-radar-parallel-cache-design.md`

**Environment note:** `D:/A价投+周期` is not a git repository, so commit steps in this plan should be treated as checkpoint notes rather than executable git commands.

---

## File Map

- `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`
  - Add regression tests for radar day-cache semantics, trade-date fallback behavior, cache precedence, parallel result parity, and BaoStock locking/fallback coverage.
- `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
  - Add radar trade-date resolution, radar day-cache read/write helpers, optional radar retry-delay plumbing, and `run_named_scan_steps()` support for `day_cache_dir`.
- `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py`
  - Add a module-level lock around BaoStock query execution and expose any small helper needed by radar trade-date resolution.
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - Create the radar day-cache directory and `_meta.json`, pass cache/config into scan helpers, and replace the serial universe loop with a cross-stock worker pool.
- `D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml`
  - Add `radar_day_cache_enabled`, `radar_max_workers`, and `radar_retry_delays`.

## Constraints To Preserve

- Final shortlist and watchlist scores must still come from the existing exact Stage 2 path.
- Deep-dive cache behavior and `CACHE_STALE_HOURS` semantics must remain unchanged.
- Radar day-cache precedence must be `cached_results > day_cache_dir > provider fetch`.
- Radar day-cache keys must continue to match top-level `scan_data` keys exactly.
- `_meta.json` must be written once by the main radar thread before any workers start.
- Windows cache writes must use temp-file replacement with a short retry on `PermissionError`.
- `resolve_radar_trade_date()` must degrade gracefully if remote trading-day resolution fails.
- BaoStock fallback must remain correct under parallel radar execution by serializing BaoStock access.

## Chunk 1: Lock In Day-Cache Behavior With Tests

### Task 1: Add failing cache-semantic tests before touching adapters

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test for cache precedence**

```python
def test_run_named_scan_steps_prefers_memory_cache_before_day_cache(self) -> None:
    in_memory = {"company_profile": {"status": "ok", "data": {"公司名称": "内存版本"}}}
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "600328.json"
        cache_path.write_text(json.dumps({
            "company_profile": {"status": "ok_day_cache", "data": {"公司名称": "磁盘版本"}}
        }, ensure_ascii=False), encoding="utf-8")
        result = run_named_scan_steps(
            "600328",
            {"company_profile": lambda _: {"status": "ok", "data": {"公司名称": "网络版本"}}},
            cached_results=in_memory,
            day_cache_dir=Path(tmpdir),
        )
    self.assertEqual(result["company_profile"]["data"]["公司名称"], "内存版本")
```

- [ ] **Step 2: Add a failing test for same-day write then `ok_day_cache` hit**

```python
def test_run_named_scan_steps_writes_and_reuses_day_cache(self) -> None:
    ...
    self.assertEqual(first["company_profile"]["status"], "ok")
    self.assertEqual(second["company_profile"]["status"], "ok_day_cache")
```

- [ ] **Step 3: Add a failing test for Stage 2 incremental append**

```python
def test_day_cache_stage_two_append_preserves_stage_one_fields(self) -> None:
    ...
    self.assertIn("company_profile", cached_payload)
    self.assertIn("balance_sheet", cached_payload)
```

- [ ] **Step 4: Add a failing test for cross-day miss**

```python
def test_day_cache_misses_when_trade_date_changes(self) -> None:
    ...
    self.assertEqual(fetch_count["company_profile"], 2)
```

- [ ] **Step 5: Add a failing test that radar day cache ignores `CACHE_STALE_HOURS`**

```python
def test_day_cache_hit_does_not_consult_hourly_staleness(self) -> None:
    ...
```

- [ ] **Step 6: Run the shared suite and verify the new cache tests fail for expected reasons**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: the new day-cache tests fail because `run_named_scan_steps()` does not yet support `day_cache_dir`.

### Task 2: Add failing tests for trade-date fallback and parallel parity

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a failing test for degraded trade-date fallback**

```python
def test_resolve_radar_trade_date_falls_back_to_previous_weekday_when_remote_lookup_fails(self) -> None:
    with patch("adapters.akshare_adapter.get_all_a_share_stocks", side_effect=RuntimeError("boom")):
        self.assertEqual(resolve_radar_trade_date(date(2026, 3, 15)), "2026-03-13")
```

- [ ] **Step 2: Add a failing test that `run_radar_scan()` writes `_meta.json` before workers run**

```python
def test_run_radar_scan_initializes_day_cache_meta_once(self) -> None:
    ...
    self.assertTrue((day_cache_dir / "_meta.json").exists())
```

- [ ] **Step 3: Add a failing test for serial vs parallel result parity**

```python
def test_parallel_radar_matches_serial_result_set(self) -> None:
    serial = run_radar_scan("A-share", limit=3, max_workers_override=1)
    parallel = run_radar_scan("A-share", limit=3, max_workers_override=4)
    self.assertEqual(serial["ranked"], parallel["ranked"])
```

- [ ] **Step 4: Add a failing test that BaoStock fallback is serialized under parallel execution**

```python
def test_parallel_radar_serializes_baostock_fallback_calls(self) -> None:
    ...
```

- [ ] **Step 5: Re-run the shared suite and verify the new parallel/trade-date tests fail**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: new tests fail because neither trade-date helper nor worker-pool path exists yet.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 1 tests added and failing as intended.`

## Chunk 2: Implement Radar Day Cache In The Shared Adapter

### Task 3: Add trade-date resolution and field-level day-cache helpers

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add `resolve_radar_trade_date()` near the radar helper section**

```python
def resolve_radar_trade_date(reference_date: date | None = None) -> str:
    current = reference_date or date.today()
    try:
        result = get_all_a_share_stocks(current.isoformat())
        if result.get("status") == "ok" and result.get("day"):
            return str(result["day"])
    except Exception:
        pass
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.isoformat()
```

- [ ] **Step 2: Add small helpers for radar cache paths and JSON envelopes**

```python
def _radar_day_cache_file(day_cache_dir: Path, stock_code: str) -> Path:
    return day_cache_dir / f"{stock_code}.json"

def _load_radar_day_cache_fields(day_cache_dir: Path, stock_code: str) -> dict[str, Any]:
    ...
```

- [ ] **Step 3: Add atomic field-level writeback with short Windows-safe retry**

```python
def _write_radar_day_cache_fields(day_cache_dir: Path, stock_code: str, updates: dict[str, Any]) -> None:
    for attempt in range(3):
        try:
            os.replace(tmp_path, target_path)
            return
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.1 * (attempt + 1))
```

- [ ] **Step 4: Extend `run_named_scan_steps()` to accept `day_cache_dir` and optional radar retry delays**

```python
def run_named_scan_steps(
    stock_code: str,
    step_map: dict[str, Any],
    *,
    cached_results: dict[str, Any] | None = None,
    day_cache_dir: Path | None = None,
    retry_delays: tuple[float, ...] | None = None,
) -> dict[str, Any]:
    ...
```

- [ ] **Step 5: Enforce precedence `cached_results > day_cache_dir > provider fetch` and emit `ok_day_cache`**

```python
if cached_results and step_name in cached_results:
    results[step_name] = cached_results[step_name]
    continue
if day_cache_dir:
    cached_field = day_cache_fields.get(step_name)
    if cached_field:
        results[step_name] = {**cached_field, "status": "ok_day_cache"}
        continue
results[step_name] = _resolve_scan_step(..., retry_delays=retry_delays or (1.0, 2.0))
```

- [ ] **Step 6: Keep cache reads and writes inside `run_named_scan_steps()` only**

```python
# _resolve_scan_step remains unaware of disk cache and only fetches provider data.
```

- [ ] **Step 7: Run the shared suite and make day-cache tests pass while parallel tests still fail**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: cache-semantics and trade-date tests pass; worker-pool parity tests still fail.

- [ ] **Step 8: Checkpoint note**

Record: `Chunk 2 radar day cache implemented in adapter layer.`

## Chunk 3: Wire Radar Cache Through The Scanner And Config

### Task 4: Add radar throughput config and main-thread cache initialization

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml`
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Extend scan defaults with radar throughput knobs**

```yaml
defaults:
  market: "A-share"
  max_universe_size: 24
  priority_score_cutoff: 75
  secondary_score_cutoff: 65
  radar_day_cache_enabled: true
  radar_max_workers: 4
  radar_retry_delays:
    - 0.5
    - 1.0
```

- [ ] **Step 2: Add a small radar-cache bootstrap helper in `radar_scan_engine.py`**

```python
def _init_radar_day_cache_dir(trade_date: str, enabled: bool) -> Path | None:
    if not enabled:
        return None
    cache_dir = ROOT / "data" / "processed" / "radar_cache" / trade_date
    cache_dir.mkdir(parents=True, exist_ok=True)
    meta_path = cache_dir / "_meta.json"
    if not meta_path.exists():
        meta_path.write_text(json.dumps({"trade_date": trade_date, "created_at": now_iso}, ensure_ascii=False), encoding="utf-8")
    return cache_dir
```

- [ ] **Step 3: Resolve `radar_trade_date` once per run and initialize cache before the universe loop**

```python
trade_date = resolve_radar_trade_date()
day_cache_dir = _init_radar_day_cache_dir(
    trade_date,
    enabled=bool(DEFAULTS.get("radar_day_cache_enabled", True)),
)
retry_delays = tuple(float(x) for x in DEFAULTS.get("radar_retry_delays", [0.5, 1.0]))
```

- [ ] **Step 4: Thread the new cache/config parameters through the current serial radar flow**

```python
partial_scan_data = run_named_scan_steps(
    item["code"],
    RADAR_PARTIAL_STEPS,
    day_cache_dir=day_cache_dir,
    retry_delays=retry_delays,
)
...
enriched_scan_data.update(
    run_named_scan_steps(
        item["code"],
        selected_steps,
        cached_results=partial_scan_data,
        day_cache_dir=day_cache_dir,
        retry_delays=retry_delays,
    )
)
```

- [ ] **Step 5: Run the shared suite and make cache bootstrap/config tests pass**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: radar cache bootstrap tests pass; parallel parity tests still fail.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 3 scanner now uses radar trade date, main-thread _meta.json, and day cache.`

## Chunk 4: Add Conservative Cross-Stock Parallelism

### Task 5: Serialize BaoStock fallback and add worker-pool orchestration

**Files:**
- Modify: `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py`
- Modify: `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
- Test: `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Add a module-level lock around BaoStock session/query execution**

```python
_BAOSTOCK_LOCK = threading.Lock()

@contextmanager
def _baostock_session():
    with _BAOSTOCK_LOCK:
        bs.login()
        try:
            yield
        finally:
            bs.logout()
```

- [ ] **Step 2: Extract one-stock scan work into a helper that preserves current two-stage semantics**

```python
def _scan_one_stock(
    item: dict[str, Any],
    *,
    secondary_cutoff: float,
    day_cache_dir: Path | None,
    retry_delays: tuple[float, ...],
) -> dict[str, Any]:
    partial_scan_data = run_named_scan_steps(...)
    partial_gate = evaluate_partial_gate_dimensions(item["code"], partial_scan_data)
    if _should_prefilter_reject(partial_gate, secondary_cutoff):
        return {"kind": "rejected", "payload": _prefilter_rejected_payload(...)}
    ...
    return {"kind": "ranked", "payload": _candidate_payload(...)}
```

- [ ] **Step 3: Replace the serial universe loop with `ThreadPoolExecutor` and post-sort aggregation**

```python
max_workers = int(max_workers_override or DEFAULTS.get("radar_max_workers", 4))
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = [
        pool.submit(
            _scan_one_stock,
            item,
            secondary_cutoff=secondary_cutoff,
            day_cache_dir=day_cache_dir,
            retry_delays=retry_delays,
        )
        for item in universe
    ]
    for future in as_completed(futures):
        result = future.result()
        ...
ranked.sort(key=lambda item: item["score"], reverse=True)
```

- [ ] **Step 4: Keep a serial test mode by accepting `max_workers_override=1` in `run_radar_scan()`**

```python
def run_radar_scan(
    scope: str = "A-share",
    limit: int | None = None,
    *,
    max_workers_override: int | None = None,
) -> dict[str, Any]:
    ...
```

- [ ] **Step 5: Run the shared suite and make parallel parity and BaoStock-lock tests pass**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS, including cache, trade-date, parallel parity, and BaoStock serialization coverage.

- [ ] **Step 6: Checkpoint note**

Record: `Chunk 4 parallel radar path implemented with one-stock-one-worker semantics.`

## Chunk 5: Verification

### Task 6: Verify correctness, cache reuse, and live throughput behavior

**Files:**
- Verify only:
  - `D:/A价投+周期/.agents/skills/shared/adapters/akshare_adapter.py`
  - `D:/A价投+周期/.agents/skills/shared/adapters/baostock_adapter.py`
  - `D:/A价投+周期/.agents/skills/market-opportunity-scanner/scripts/engines/radar_scan_engine.py`
  - `D:/A价投+周期/.agents/skills/market-opportunity-scanner/config/scan_defaults.yaml`
  - `D:/A价投+周期/.agents/skills/shared/tests/test_investment_framework.py`

- [ ] **Step 1: Run the shared suite**

Run:

```powershell
python -m unittest discover -s "D:\A价投+周期\.agents\skills\shared\tests" -p "test_investment_framework.py" -v
```

Expected: PASS.

- [ ] **Step 2: Run a serial live radar sample to establish a baseline**

Run:

```powershell
@'
import importlib.util
from pathlib import Path

path = Path(r"D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py")
spec = importlib.util.spec_from_file_location("radar_scan_engine_check", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
result = module.run_radar_scan("A-share", limit=3, max_workers_override=1)
print(result.get("universe_size"))
print(len(result.get("ranked", [])))
'@ | python -X utf8 -
```

Expected: completes and returns a small result set without import/runtime errors.

- [ ] **Step 3: Run the same live sample again and confirm same-day cache reuse**

Run:

```powershell
@'
import importlib.util
from pathlib import Path

path = Path(r"D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py")
spec = importlib.util.spec_from_file_location("radar_scan_engine_check", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
result = module.run_radar_scan("A-share", limit=3, max_workers_override=1)
first = (result.get("ranked") or result.get("rejected") or [None])[0]
print(first.get("reason") if first else "no-results")
'@ | python -X utf8 -
```

Expected: completes materially faster than Step 2, with field results showing `ok_day_cache` in logs or inspected envelopes.

- [ ] **Step 4: Run a parallel live sample and compare output shape with the serial sample**

Run:

```powershell
@'
import importlib.util
from pathlib import Path

path = Path(r"D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py")
spec = importlib.util.spec_from_file_location("radar_scan_engine_check", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
result = module.run_radar_scan("A-share", limit=3, max_workers_override=4)
print(result.get("universe_size"))
print([item.get("ticker") for item in result.get("ranked", [])[:3]])
'@ | python -X utf8 -
```

Expected: completes successfully and preserves ranking semantics relative to the serial sample.

- [ ] **Step 5: Force primary-universe failure and verify BaoStock fallback still works under the new execution model**

Run:

```powershell
@'
import importlib.util
from pathlib import Path
from unittest.mock import patch

path = Path(r"D:\A价投+周期\.agents\skills\market-opportunity-scanner\scripts\engines\radar_scan_engine.py")
spec = importlib.util.spec_from_file_location("radar_scan_engine_check", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
with patch.object(module.ak, "stock_zh_a_spot_em", side_effect=RuntimeError("forced")):
    result = module.run_radar_scan("A-share", limit=1, max_workers_override=2)
    print(result.get("universe_size"))
    print((result.get("ranked") or result.get("rejected") or [{}])[0].get("ticker"))
'@ | python -X utf8 -
```

Expected: completes through BaoStock-backed fallback path instead of aborting.

- [ ] **Step 6: Record verification findings in the close-out**

Record:
- shared test count and pass status
- whether same-day second run hit day cache
- serial vs parallel sample parity outcome
- whether forced primary-universe failure still completed through BaoStock fallback

- [ ] **Step 7: Checkpoint note**

Record: `Verification complete. Workspace has no git metadata, so no commit was created.`
