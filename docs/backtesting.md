# VCRF Backtesting

## Inputs

The backtest layer expects two tabular inputs:

- `signals_month_end`
  Research-layer signals with at least:
  `signal_date`, `ticker`, `vcrf_state`, `floor_price`, `recognition_price`
- `daily_bars`
  Daily execution-layer bars with at least:
  `date`, `ticker`, `open`, `high`, `low`, `close`

Optional signal columns that will be preserved:

- `effective_date`
- `announcement_date`
- `total_score`
- `tradable_flag`
- `reject_reason`
- `v_score`, `c_score`, `r_score`, `f_score`
- `signal_version`

## Build Public-Source Inputs

If you do not already have a point-in-time historical warehouse, you can build an
approximate watchlist-scale dataset from public sources:

```bash
.venv/bin/python scripts/build_public_backtest_inputs.py \
  --tickers 600328,600348,600019 \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --out-dir /path/to/output/public_inputs
```

If `--tickers` is omitted, the script falls back to the repo's local covered universe
from `evidence/`, `data/raw/`, and `data/processed/`.

Outputs:

- `signals_month_end.parquet` or CSV fallback
- `daily_bars.parquet` or CSV fallback
- `manifest.json`

Limitations of this mode:

- It is `public_source_watchlist_v1`, not a strict full-market PIT warehouse
- Statement availability uses plausible announcement dates when available, otherwise
  statutory lag fallback
- Company profile and revenue text reuse latest public profile data
- Free-float market cap may be missing and then falls back to total market cap

## Build Tushare PIT Inputs

If you have `TUSHARE_TOKEN` or `TUSHARE_TOKENS`, prefer the Tushare-backed builder over the public-source
approximation:

```bash
.venv/bin/python scripts/build_tushare_backtest_inputs.py \
  --tickers 600328,600348,600019 \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --out-dir /path/to/output/tushare_inputs
```

You can also omit `--tickers` and let the script discover a universe from
`stock_basic`:

```bash
.venv/bin/python scripts/build_tushare_backtest_inputs.py \
  --list-statuses L,D,P \
  --limit 500 \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --out-dir /path/to/output/tushare_inputs
```

Token discovery order:

- `TUSHARE_TOKENS` from the shell environment, formatted as `token1,token2,token3`
- `TUSHARE_TOKEN` from the shell environment
- `TUSHARE_TOKENS=...` in repo-local [`.env`](/Users/hz/hz/A价投+周期/.env)
- `TUSHARE_TOKEN=...` in repo-local [`.env`](/Users/hz/hz/A价投+周期/.env)

When multiple tokens are configured, the adapter tries the last successful token first
and automatically fails over to the next token if a query errors.

Outputs:

- `signals_month_end.parquet` or CSV fallback
- `daily_bars.parquet` or CSV fallback
- `manifest.json`

This mode is intended for route-B self-hosted backtests:

- deterministic local execution engine
- month-end research signals expanded into daily execution signals
- limit/stop/time-exit handling stays in the local engine, not in a third-party framework

## Build Signal Library

```bash
.venv/bin/python scripts/build_vcrf_signal_library.py \
  --signals-month-end /path/to/signals_month_end.csv \
  --daily-bars /path/to/daily_bars.csv \
  --out-dir /path/to/output/signal_library
```

Outputs:

- `signal_month_end.parquet` or CSV fallback
- `signal_daily.parquet` or CSV fallback

`effective_date` is normalized to the next trading day when only `announcement_date`
or `signal_date` is present.

## Run Backtest

```bash
.venv/bin/python scripts/run_vcrf_backtest.py \
  --signals-month-end /path/to/signals_month_end.csv \
  --daily-bars /path/to/daily_bars.csv \
  --out-dir /path/to/output/backtest
```

Outputs per run:

- `selected_candidates.csv`
- `round_summary.csv`
- `round_01_trades.csv`
- `round_01_equity.csv`
- `round_01_report.md`
- `backtest_manifest.json`

## Execution Semantics

- Research signals become tradable on `effective_date`
- Entries execute at the next trading day's open
- Recognition exits use limit semantics at `recognition_price`
- Floor exits use stop semantics at `floor_price`
- Same-bar target/stop collisions default to `stop_first`
- A-share entries are rounded down to 100-share lots
- Rounds use the protocol in `.agents/skills/shared/config/backtest_protocol.yaml`
