"""Tushare Pro adapters for PIT-friendly A-share backtest inputs."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from utils.evidence_helpers import now_iso


REPO_ROOT = Path(__file__).resolve().parents[4]
_LAST_GOOD_TOKEN: str | None = None
TUSHARE_PRO_HTTPS_URL = "https://api.waditu.com/dataapi"


def _split_token_values(raw_value: str) -> tuple[str, ...]:
    tokens = tuple(part.strip().strip("'\"") for part in raw_value.split(",") if part.strip().strip("'\""))
    return tokens


def resolve_tushare_tokens(repo_root: Path | None = None) -> tuple[str, ...]:
    multi_env = (os.getenv("TUSHARE_TOKENS") or "").strip()
    if multi_env:
        tokens = _split_token_values(multi_env)
        if tokens:
            return tokens
    single_env = (os.getenv("TUSHARE_TOKEN") or "").strip().strip("'\"")
    if single_env:
        return (single_env,)
    env_path = (repo_root or REPO_ROOT) / ".env"
    if not env_path.exists():
        return ()

    parsed: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip().strip("'\"")

    multi_file = parsed.get("TUSHARE_TOKENS", "")
    if multi_file:
        tokens = _split_token_values(multi_file)
        if tokens:
            return tokens
    single_file = parsed.get("TUSHARE_TOKEN", "")
    if single_file:
        return (single_file,)
    return ()


def resolve_tushare_token(repo_root: Path | None = None) -> str | None:
    tokens = resolve_tushare_tokens(repo_root)
    return tokens[0] if tokens else None


@lru_cache(maxsize=1)
def _load_tushare():
    try:
        import tushare as ts  # type: ignore

        return ts
    except Exception as exc:  # pragma: no cover - import failure path
        raise RuntimeError(f"tushare import failed: {exc}") from exc


@lru_cache(maxsize=4)
def _pro_client(token: str):
    ts = _load_tushare()
    ts.set_token(token)
    return _force_https_transport(ts.pro_api(token))


def _force_https_transport(client: Any):
    # tushare 1.4.25 still defaults to the legacy HTTP endpoint, which now returns 502.
    if hasattr(client, "_DataApi__http_url"):
        setattr(client, "_DataApi__http_url", TUSHARE_PRO_HTTPS_URL)
    return client


def get_tushare_client(repo_root: Path | None = None):
    token = resolve_tushare_token(repo_root)
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is not configured")
    return _pro_client(token)


def _ordered_tokens(repo_root: Path | None = None) -> tuple[str, ...]:
    tokens = list(resolve_tushare_tokens(repo_root))
    if _LAST_GOOD_TOKEN and _LAST_GOOD_TOKEN in tokens:
        tokens.remove(_LAST_GOOD_TOKEN)
        tokens.insert(0, _LAST_GOOD_TOKEN)
    return tuple(tokens)


def _remember_good_token(token: str) -> None:
    global _LAST_GOOD_TOKEN
    _LAST_GOOD_TOKEN = token


def _frame_to_records(df: Any) -> list[dict[str, Any]]:
    if df is None:
        return []
    if isinstance(df, pd.DataFrame):
        if df.empty:
            return []
        return df.to_dict("records")
    return list(df or [])


def _query(api_name: str, *, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    tokens = _ordered_tokens(repo_root)
    if not tokens:
        return {"data": [], "status": "error: TUSHARE_TOKEN is not configured", "fetch_timestamp": now_iso()}

    errors: list[str] = []
    for token in tokens:
        try:
            pro = _pro_client(token)
            query_fn = getattr(pro, api_name)
            df = query_fn(**kwargs)
            _remember_good_token(token)
            return {"data": _frame_to_records(df), "status": "ok", "fetch_timestamp": now_iso()}
        except Exception as exc:
            errors.append(f"{token[:6]}...: {exc}")
            continue
    return {"data": [], "status": f"error: {' | '.join(errors)}", "fetch_timestamp": now_iso()}


def query_stock_basic(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("stock_basic", repo_root=repo_root, **kwargs)


def query_stock_company(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("stock_company", repo_root=repo_root, **kwargs)


def query_fina_mainbz(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("fina_mainbz", repo_root=repo_root, **kwargs)


def query_trade_cal(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("trade_cal", repo_root=repo_root, **kwargs)


def query_daily(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("daily", repo_root=repo_root, **kwargs)


def query_daily_basic(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("daily_basic", repo_root=repo_root, **kwargs)


def query_adj_factor(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("adj_factor", repo_root=repo_root, **kwargs)


def query_income(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("income", repo_root=repo_root, **kwargs)


def query_balancesheet(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("balancesheet", repo_root=repo_root, **kwargs)


def query_cashflow(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("cashflow", repo_root=repo_root, **kwargs)


def query_fina_indicator(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("fina_indicator", repo_root=repo_root, **kwargs)


def query_suspend_d(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("suspend_d", repo_root=repo_root, **kwargs)


def query_stk_limit(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("stk_limit", repo_root=repo_root, **kwargs)


def query_dividend(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("dividend", repo_root=repo_root, **kwargs)


def _normalize_symbol(record: dict[str, Any]) -> str:
    symbol = str(record.get("symbol") or "").strip()
    ts_code = str(record.get("ts_code") or "").strip()
    if symbol:
        return symbol.zfill(6)
    if "." in ts_code:
        return ts_code.split(".", 1)[0].zfill(6)
    return ts_code.zfill(6)


def discover_tushare_universe_tickers(
    *,
    list_statuses: tuple[str, ...] = ("L", "D", "P"),
    limit: int | None = None,
    repo_root: Path | None = None,
) -> list[str]:
    seen: set[str] = set()
    tickers: list[str] = []
    fields = "ts_code,symbol,name,industry,list_status,list_date,delist_date"
    for status in list_statuses:
        result = query_stock_basic(repo_root=repo_root, list_status=status, fields=fields)
        for row in result.get("data", []) or []:
            symbol = _normalize_symbol(row)
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            tickers.append(symbol)
            if limit is not None and limit > 0 and len(tickers) >= limit:
                return tickers
    return tickers
