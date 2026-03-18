"""Tushare Pro adapters for PIT-friendly A-share backtest inputs."""
from __future__ import annotations

import datetime
import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from utils.evidence_helpers import make_evidence, now_iso
from utils.market_utils import infer_market_from_stock_code, normalize_display_code, to_tushare_code
from utils.value_utils import normalize_text, safe_float


REPO_ROOT = Path(__file__).resolve().parents[4]
_LAST_GOOD_TOKEN: str | None = None
TUSHARE_PRO_HTTPS_URL = "https://api.waditu.com/dataapi"
_CACHEABLE_TUSHARE_APIS = {
    "hk_basic": 24 * 3600,
    "us_basic": 24 * 3600,
    "hk_daily": 12 * 3600,
    "us_daily": 12 * 3600,
}


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


def _cache_ttl_seconds(api_name: str) -> int | None:
    return _CACHEABLE_TUSHARE_APIS.get(api_name)


def _query_cache_dir(repo_root: Path | None = None) -> Path:
    return (repo_root or REPO_ROOT) / ".cache" / "tushare_pro"


def _query_cache_path(api_name: str, kwargs: dict[str, Any], repo_root: Path | None = None) -> Path:
    normalized_kwargs = json.dumps(kwargs, ensure_ascii=False, sort_keys=True, default=str)
    cache_key = hashlib.sha1(f"{api_name}:{normalized_kwargs}".encode("utf-8")).hexdigest()
    return _query_cache_dir(repo_root) / f"{api_name}_{cache_key}.json"


def _load_query_cache(api_name: str, kwargs: dict[str, Any], *, repo_root: Path | None = None, allow_stale: bool = False) -> dict[str, Any] | None:
    ttl_seconds = _cache_ttl_seconds(api_name)
    if ttl_seconds is None:
        return None
    path = _query_cache_path(api_name, kwargs, repo_root)
    if not path.exists():
        return None
    if not allow_stale:
        age_seconds = datetime.datetime.now().timestamp() - path.stat().st_mtime
        if age_seconds > ttl_seconds:
            return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict) or not payload.get("data"):
        return None
    return payload


def _save_query_cache(api_name: str, kwargs: dict[str, Any], response: dict[str, Any], *, repo_root: Path | None = None) -> None:
    if _cache_ttl_seconds(api_name) is None:
        return
    if not _is_ok_status(response.get("status")) or not response.get("data"):
        return
    path = _query_cache_path(api_name, kwargs, repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "data": response.get("data", []),
        "status": "ok",
        "fetch_timestamp": response.get("fetch_timestamp") or now_iso(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _is_rate_limited(status: Any) -> bool:
    text = normalize_text(status).lower()
    return "每分钟最多访问" in text or "每天最多访问" in text or "rate limit" in text or "too many requests" in text


def _query(api_name: str, *, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    fresh_cache = _load_query_cache(api_name, kwargs, repo_root=repo_root, allow_stale=False)
    if fresh_cache is not None:
        return {**fresh_cache, "status": "ok_cached"}

    stale_cache = _load_query_cache(api_name, kwargs, repo_root=repo_root, allow_stale=True)
    tokens = _ordered_tokens(repo_root)
    if not tokens:
        if stale_cache is not None:
            return {**stale_cache, "status": "ok_cached"}
        return {"data": [], "status": "error: TUSHARE_TOKEN is not configured", "fetch_timestamp": now_iso()}

    errors: list[str] = []
    rate_limited = False
    for token in tokens:
        try:
            pro = _pro_client(token)
            query_fn = getattr(pro, api_name)
            df = query_fn(**kwargs)
            _remember_good_token(token)
            response = {"data": _frame_to_records(df), "status": "ok", "fetch_timestamp": now_iso()}
            _save_query_cache(api_name, kwargs, response, repo_root=repo_root)
            return response
        except Exception as exc:
            error_text = f"{token[:6]}...: {exc}"
            errors.append(error_text)
            if _is_rate_limited(error_text):
                rate_limited = True
            continue
    if rate_limited and stale_cache is not None:
        return {**stale_cache, "status": "ok_cached_rate_limited"}
    return {"data": [], "status": f"error: {' | '.join(errors)}", "fetch_timestamp": now_iso()}


def _to_ts_code(stock_code: str) -> str:
    return to_tushare_code(stock_code)


def query_stock_basic(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("stock_basic", repo_root=repo_root, **kwargs)


def query_stock_company(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("stock_company", repo_root=repo_root, **kwargs)


def query_hk_basic(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("hk_basic", repo_root=repo_root, **kwargs)


def query_hk_daily(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("hk_daily", repo_root=repo_root, **kwargs)


def query_hk_income(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("hk_income", repo_root=repo_root, **kwargs)


def query_hk_balancesheet(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("hk_balancesheet", repo_root=repo_root, **kwargs)


def query_hk_cashflow(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("hk_cashflow", repo_root=repo_root, **kwargs)


def query_hk_fina_indicator(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("hk_fina_indicator", repo_root=repo_root, **kwargs)


def query_us_basic(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("us_basic", repo_root=repo_root, **kwargs)


def query_us_daily(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("us_daily", repo_root=repo_root, **kwargs)


def query_us_income(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("us_income", repo_root=repo_root, **kwargs)


def query_us_balancesheet(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("us_balancesheet", repo_root=repo_root, **kwargs)


def query_us_cashflow(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("us_cashflow", repo_root=repo_root, **kwargs)


def query_us_fina_indicator(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("us_fina_indicator", repo_root=repo_root, **kwargs)


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


def query_stk_holdernumber(*, repo_root: Path | None = None, **kwargs: Any) -> dict[str, Any]:
    return _query("stk_holdernumber", repo_root=repo_root, **kwargs)


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


def _make_evidence(field: str, value: Any, source_desc: str, confidence: str = "medium") -> dict[str, Any]:
    return make_evidence(field, value, source_desc, source_type="tushare", tier=1, confidence=confidence)


def _is_ok_status(status: Any) -> bool:
    return normalize_text(status).lower().startswith("ok")


def _is_permission_denied(status: Any) -> bool:
    text = normalize_text(status).lower()
    return "没有接口访问权限" in text or "permission" in text or "权限" in text


@lru_cache(maxsize=1)
def _load_akshare():
    try:
        import akshare as ak  # type: ignore

        return ak
    except Exception:
        return None


def _normalize_hk_basic_profile(stock_code: str, basic_rows: list[dict[str, Any]]) -> dict[str, Any]:
    basic = (basic_rows or [{}])[0]
    return {
        **basic,
        "股票代码": normalize_display_code(stock_code),
        "股票简称": normalize_text(basic.get("name") or stock_code),
        "公司名称": normalize_text(basic.get("fullname") or basic.get("name")),
        "英文名称": normalize_text(basic.get("enname")),
        "市场类型": normalize_text(basic.get("market")),
        "上市时间": normalize_text(basic.get("list_date")),
        "退市日期": normalize_text(basic.get("delist_date")),
        "交易单位": safe_float(basic.get("trade_unit")),
        "币种": normalize_text(basic.get("curr_type")),
        "行业": normalize_text(basic.get("market")),
    }


def _normalize_us_basic_profile(stock_code: str, basic_rows: list[dict[str, Any]]) -> dict[str, Any]:
    basic = (basic_rows or [{}])[0]
    return {
        **basic,
        "股票代码": normalize_display_code(stock_code),
        "股票简称": normalize_text(basic.get("name") or basic.get("enname") or stock_code),
        "公司名称": normalize_text(basic.get("name") or basic.get("enname") or stock_code),
        "英文名称": normalize_text(basic.get("enname") or basic.get("name")),
        "市场类型": "US",
        "上市时间": normalize_text(basic.get("list_date")),
        "退市日期": normalize_text(basic.get("delist_date")),
        "行业": normalize_text(basic.get("classify")),
        "资产类别": normalize_text(basic.get("classify")),
    }


def _hk_indicator_snapshot(stock_code: str) -> dict[str, Any]:
    ak = _load_akshare()
    if ak is None:
        return {}
    try:
        df = ak.stock_hk_financial_indicator_em(symbol=normalize_display_code(stock_code))
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    row = df.iloc[0].to_dict()
    market_cap = safe_float(row.get("总市值(港元)"))
    share_count = safe_float(row.get("已发行股本(股)") or row.get("已发行股本-H股(股)"))
    latest_price = market_cap / share_count if market_cap not in (None, 0) and share_count not in (None, 0) else None
    return {
        "latest_price": latest_price,
        "market_cap": market_cap,
        "float_market_cap": safe_float(row.get("港股市值(港元)")),
        "pb": safe_float(row.get("市净率")),
        "pe": safe_float(row.get("市盈率")),
        "share_count": share_count,
        "raw": row,
    }


def _pivot_hk_report_records(records: list[dict[str, Any]], aliases: dict[str, str]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in records or []:
        report_date = normalize_text(row.get("REPORT_DATE") or row.get("STD_REPORT_DATE"))
        if not report_date:
            continue
        payload = grouped.setdefault(report_date, {"报告日": report_date.replace("-", "")[:8], "报告日期": report_date, "公告日期": report_date})
        item_name = normalize_text(row.get("STD_ITEM_NAME"))
        if item_name:
            payload[item_name] = safe_float(row.get("AMOUNT"))
    normalized = list(grouped.values())
    for row in normalized:
        for src_key, target_key in aliases.items():
            value = safe_float(row.get(src_key))
            if value is not None and target_key not in row:
                row[target_key] = value
    normalized.sort(key=lambda row: normalize_text(row.get("报告日")))
    return normalized


def _akshare_hk_statement(stock_code: str, statement: str, aliases: dict[str, str]) -> dict[str, Any]:
    ak = _load_akshare()
    if ak is None:
        return {"data": [], "status": "error: akshare import failed", "fetch_timestamp": now_iso()}
    try:
        df = ak.stock_financial_hk_report_em(stock=normalize_display_code(stock_code), symbol=statement, indicator="年度")
        records = _pivot_hk_report_records(df.to_dict("records"), aliases)
        if records:
            return {"data": records[-8:], "status": "ok_fallback_akshare_hk", "fetch_timestamp": now_iso()}
        return {"data": [], "status": "error: empty akshare hk statement", "fetch_timestamp": now_iso()}
    except Exception as exc:
        return {"data": [], "status": f"error: {exc}", "fetch_timestamp": now_iso()}


def _normalize_hk_financial_summary_from_akshare(stock_code: str) -> dict[str, Any]:
    ak = _load_akshare()
    if ak is None:
        return {"data": [], "status": "error: akshare import failed", "fetch_timestamp": now_iso()}
    try:
        df = ak.stock_financial_hk_analysis_indicator_em(symbol=normalize_display_code(stock_code), indicator="年度")
    except TypeError:
        df = ak.stock_financial_hk_analysis_indicator_em(symbol=normalize_display_code(stock_code))
    except Exception as exc:
        return {"data": [], "status": f"error: {exc}", "fetch_timestamp": now_iso()}
    if df is None or df.empty:
        return {"data": [], "status": "error: empty akshare hk indicator", "fetch_timestamp": now_iso()}
    records: list[dict[str, Any]] = []
    for row in df.to_dict("records"):
        report_date = normalize_text(row.get("REPORT_DATE"))
        records.append(
            {
                **row,
                "报告日": report_date.replace("-", "")[:8] if report_date else "",
                "公告日期": report_date.replace("-", "")[:8] if report_date else "",
                "销售净利率(%)": safe_float(row.get("NET_PROFIT_RATIO")),
                "净资产收益率(%)": safe_float(row.get("ROE_YEARLY")),
                "加权平均净资产收益率(%)": safe_float(row.get("ROE_AVG")),
                "资产负债率(%)": safe_float(row.get("DEBT_ASSET_RATIO")),
                "每股收益(元)": safe_float(row.get("BASIC_EPS")),
            }
        )
    records.sort(key=lambda row: normalize_text(row.get("报告日")))
    return {"data": records[-10:], "status": "ok_fallback_akshare_hk", "fetch_timestamp": now_iso()}


def _to_ts_code(stock_code: str) -> str:
    return to_tushare_code(stock_code)


def _to_ts_date(value: str | datetime.date | datetime.datetime | None) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, datetime.date):
        return value.strftime("%Y%m%d")
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _to_display_date(value: Any) -> str:
    digits = _to_ts_date(value)
    if len(digits) != 8:
        return normalize_text(value)
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def _history_window(*, years: int = 5, days: int = 30) -> tuple[str, str]:
    end = datetime.date.today()
    start = end - datetime.timedelta(days=years * 365 + days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _latest_trade_date() -> str:
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=14)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    result = query_trade_cal(start_date=start, end_date=end, is_open="1")
    days = sorted(_to_ts_date(row.get("cal_date")) for row in result.get("data", []) or [] if _to_ts_date(row.get("cal_date")))
    return days[-1] if days else today.strftime("%Y%m%d")


def _recent_trade_dates(reference_day: str, lookback_days: int = 14) -> list[str]:
    reference_digits = _to_ts_date(reference_day)
    if len(reference_digits) != 8:
        return []
    reference_date = datetime.datetime.strptime(reference_digits, "%Y%m%d").date()
    start = (reference_date - datetime.timedelta(days=lookback_days)).strftime("%Y%m%d")
    result = query_trade_cal(start_date=start, end_date=reference_digits, is_open="1")
    days = sorted(_to_ts_date(row.get("cal_date")) for row in result.get("data", []) or [] if _to_ts_date(row.get("cal_date")))
    return [day for day in days if day <= reference_digits]


def _latest_record(records: list[dict[str, Any]], date_keys: tuple[str, ...]) -> dict[str, Any]:
    if not records:
        return {}
    return max(records, key=lambda row: tuple(_to_ts_date(row.get(key)) for key in date_keys))


def _records_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def _wrap_fallback(result: dict[str, Any], status: str) -> dict[str, Any]:
    wrapped = dict(result)
    if _is_ok_status(wrapped.get("status")):
        wrapped["status"] = status
    return wrapped


def _fallback_akshare(function_name: str, *args: Any) -> dict[str, Any]:
    from adapters import akshare_adapter

    result = getattr(akshare_adapter, function_name)(*args)
    return _wrap_fallback(result, "ok_fallback_akshare")


def _fallback_baostock(function_name: str, *args: Any) -> dict[str, Any]:
    from adapters import baostock_adapter

    result = getattr(baostock_adapter, function_name)(*args)
    return _wrap_fallback(result, "ok_fallback_baostock")


def _normalize_tushare_profile(
    stock_code: str,
    basic_rows: list[dict[str, Any]],
    company_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    basic = (basic_rows or [{}])[0]
    company = (company_rows or [{}])[0]
    return {
        **basic,
        **company,
        "股票代码": normalize_display_code(stock_code),
        "股票简称": normalize_text(basic.get("name") or company.get("fullname") or stock_code),
        "行业": normalize_text(basic.get("industry")),
        "上市时间": normalize_text(basic.get("list_date")),
        "退市日期": normalize_text(basic.get("delist_date")),
        "主营业务": normalize_text(company.get("main_business")),
        "经营范围": normalize_text(company.get("business_scope")),
        "公司介绍": normalize_text(company.get("introduction")),
        "地域": normalize_text(basic.get("area") or company.get("province")),
    }


def _normalize_tushare_revenue_breakdown(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        sales = safe_float(row.get("bz_sales"))
        cost = safe_float(row.get("bz_cost"))
        profit = safe_float(row.get("bz_profit"))
        gross_margin = None
        if sales not in (None, 0) and profit is not None:
            gross_margin = profit / sales
        elif sales not in (None, 0) and cost is not None:
            gross_margin = (sales - cost) / sales
        normalized.append(
            {
                **row,
                "报告期": normalize_text(row.get("end_date")),
                "主营构成": normalize_text(row.get("bz_item")),
                "产品名称": normalize_text(row.get("bz_item")),
                "主营收入": sales,
                "主营成本": cost,
                "主营利润": profit,
                "毛利率": gross_margin,
            }
        )
    normalized.sort(key=lambda row: (normalize_text(row.get("报告期")), normalize_text(row.get("主营构成"))))
    return normalized


def _normalize_tushare_income_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "报告日": normalize_text(row.get("end_date")),
                "公告日期": normalize_text(row.get("f_ann_date") or row.get("ann_date")),
                "营业总收入": safe_float(row.get("total_revenue") or row.get("revenue")),
                "营业收入": safe_float(row.get("revenue")),
                "归属于母公司所有者的净利润": safe_float(row.get("n_income_attr_p") or row.get("n_income")),
                "资产减值损失": safe_float(row.get("assets_impair_loss")),
                "信用减值损失": safe_float(row.get("credit_impa_loss")),
                "商誉减值": safe_float(row.get("goodwill")),
            }
        )
    normalized.sort(key=lambda row: normalize_text(row.get("报告日")))
    return normalized


def _normalize_tushare_balance_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "报告日": normalize_text(row.get("end_date")),
                "公告日期": normalize_text(row.get("f_ann_date") or row.get("ann_date")),
                "资产总计": safe_float(row.get("total_assets")),
                "归属于母公司所有者权益合计": safe_float(row.get("total_hldr_eqy_exc_min_int") or row.get("total_hldr_eqy_inc_min_int")),
                "货币资金": safe_float(row.get("money_cap")),
                "交易性金融资产": safe_float(row.get("trad_asset")),
                "短期借款": safe_float(row.get("st_borr")),
                "一年内到期的非流动负债": safe_float(row.get("non_cur_liab_due_1y")),
                "实收资本(或股本)": safe_float(row.get("total_share")),
            }
        )
    normalized.sort(key=lambda row: normalize_text(row.get("报告日")))
    return normalized


def _normalize_tushare_cashflow_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "报告日": normalize_text(row.get("end_date")),
                "公告日期": normalize_text(row.get("f_ann_date") or row.get("ann_date")),
                "经营活动产生的现金流量净额": safe_float(row.get("n_cashflow_act")),
            }
        )
    normalized.sort(key=lambda row: normalize_text(row.get("报告日")))
    return normalized


def _normalize_tushare_financial_summary(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "报告日": normalize_text(row.get("end_date")),
                "公告日期": normalize_text(row.get("ann_date")),
                "销售净利率(%)": safe_float(row.get("netprofit_margin")),
                "净资产收益率(%)": safe_float(row.get("roe")),
                "加权平均净资产收益率(%)": safe_float(row.get("roe_waa")),
                "资产负债率(%)": safe_float(row.get("debt_to_assets")),
                "每股收益(元)": safe_float(row.get("eps")),
            }
        )
    normalized.sort(key=lambda row: normalize_text(row.get("报告日")))
    return normalized


def _normalize_tushare_shareholder_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records or []:
        normalized.append(
            {
                **row,
                "公告日期": normalize_text(row.get("ann_date")),
                "截止日期": normalize_text(row.get("end_date")),
                "股东户数": safe_float(row.get("holder_num")),
            }
        )
    normalized.sort(key=lambda row: (normalize_text(row.get("截止日期")), normalize_text(row.get("公告日期"))))
    return normalized


def _load_tushare_daily_frame(stock_code: str, *, years: int = 5) -> pd.DataFrame:
    start_date, end_date = _history_window(years=years)
    market = infer_market_from_stock_code(stock_code)
    ts_code = _to_ts_code(stock_code)
    if market == "A-share":
        daily = query_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        adj_factor = query_adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        volume_multiplier = 100.0
        amount_multiplier = 1000.0
    elif market == "HK-share":
        daily = query_hk_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        adj_factor = {"data": [], "status": "not_applicable"}
        volume_multiplier = 1.0
        amount_multiplier = 1.0
    else:
        daily = query_us_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        adj_factor = {"data": [], "status": "not_applicable"}
        volume_multiplier = 1.0
        amount_multiplier = 1.0
    bars = _records_frame(daily.get("data", []) or [])
    if bars.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume", "amount"])

    bars = bars.rename(columns={"trade_date": "date", "vol": "volume"})
    bars["date"] = pd.to_datetime(bars["date"], format="%Y%m%d", errors="coerce").dt.normalize()
    bars["ticker"] = normalize_display_code(stock_code)
    for column in ("open", "high", "low", "close", "volume", "amount"):
        bars[column] = pd.to_numeric(bars.get(column), errors="coerce")

    adj = _records_frame(adj_factor.get("data", []) or [])
    if not adj.empty:
        adj = adj.rename(columns={"trade_date": "date"})
        adj["date"] = pd.to_datetime(adj["date"], format="%Y%m%d", errors="coerce").dt.normalize()
        adj["adj_factor"] = pd.to_numeric(adj.get("adj_factor"), errors="coerce")
        adj = adj.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        latest_values = adj["adj_factor"].dropna()
        latest_adj_factor = float(latest_values.iloc[-1]) if not latest_values.empty else None
        bars = bars.merge(adj[["date", "adj_factor"]], on="date", how="left")
        if latest_adj_factor not in (None, 0):
            price_scale = bars["adj_factor"].fillna(latest_adj_factor) / latest_adj_factor
            for column in ("open", "high", "low", "close"):
                bars[column] = bars[column] * price_scale

    bars["volume"] = bars["volume"] * volume_multiplier
    bars["amount"] = bars["amount"] * amount_multiplier
    bars = bars.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    return bars[["date", "ticker", "open", "high", "low", "close", "volume", "amount"]]


def get_company_profile(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    ts_code = _to_ts_code(stock_code)
    if market == "HK-share":
        basic = query_hk_basic(ts_code=ts_code)
        basic_rows = basic.get("data", []) or []
        if basic_rows:
            profile = _normalize_hk_basic_profile(stock_code, basic_rows)
            evidence = _make_evidence("company_profile", profile, f"tushare hk_basic ({stock_code})")
            return {"data": profile, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        return {"data": {}, "evidence": {}, "status": basic.get("status", "error: hk_basic unavailable"), "fetch_timestamp": now_iso()}

    if market == "US-share":
        basic = query_us_basic(ts_code=ts_code)
        basic_rows = basic.get("data", []) or []
        if basic_rows:
            profile = _normalize_us_basic_profile(stock_code, basic_rows)
            evidence = _make_evidence("company_profile", profile, f"tushare us_basic ({stock_code})")
            return {"data": profile, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        return {"data": {}, "evidence": {}, "status": basic.get("status", "error: us_basic unavailable"), "fetch_timestamp": now_iso()}

    basic = query_stock_basic(
        ts_code=ts_code,
        fields="ts_code,symbol,name,area,industry,market,list_status,list_date,delist_date",
    )
    company = query_stock_company(
        ts_code=ts_code,
        fields="ts_code,chairman,manager,secretary,province,city,introduction,website,email,office,business_scope,main_business",
    )
    basic_rows = basic.get("data", []) or []
    company_rows = company.get("data", []) or []
    if basic_rows or company_rows:
        profile = _normalize_tushare_profile(stock_code, basic_rows, company_rows)
        evidence = _make_evidence("company_profile", profile, f"tushare stock_basic + stock_company ({stock_code})")
        return {"data": profile, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_company_profile", stock_code)


def get_financial_summary(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    start_date, end_date = _history_window(years=8)
    if market == "HK-share":
        result = query_hk_fina_indicator(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_financial_summary(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("financial_summary", f"{len(records)} periods", f"tushare hk_fina_indicator ({stock_code})")
            return {"data": records[-10:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        fallback = _normalize_hk_financial_summary_from_akshare(stock_code)
        if fallback.get("data"):
            evidence = _make_evidence("financial_summary", f"{len(fallback['data'])} periods", f"akshare stock_financial_hk_analysis_indicator_em ({stock_code})")
            return {"data": fallback["data"], "evidence": evidence, "status": fallback["status"], "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", fallback.get("status")), "fetch_timestamp": now_iso()}

    if market == "US-share":
        result = query_us_fina_indicator(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_financial_summary(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("financial_summary", f"{len(records)} periods", f"tushare us_fina_indicator ({stock_code})")
            return {"data": records[-10:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", "error: us_fina_indicator unavailable"), "fetch_timestamp": now_iso()}

    result = query_fina_indicator(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
    records = _normalize_tushare_financial_summary(result.get("data", []) or [])
    if records:
        evidence = _make_evidence(
            "financial_summary",
            f"{len(records)} periods",
            f"tushare fina_indicator ({stock_code})",
        )
        return {"data": records[-10:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_financial_summary", stock_code)


def get_revenue_breakdown(stock_code: str) -> dict[str, Any]:
    if infer_market_from_stock_code(stock_code) != "A-share":
        return {"data": [], "evidence": {}, "status": "not_applicable_for_market", "fetch_timestamp": now_iso()}
    start_date, end_date = _history_window(years=5)
    result = query_fina_mainbz(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date, type="P")
    records = _normalize_tushare_revenue_breakdown(result.get("data", []) or [])
    if records:
        evidence = _make_evidence(
            "revenue_breakdown",
            f"{len(records)} segments",
            f"tushare fina_mainbz ({stock_code})",
        )
        return {"data": records[-50:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_revenue_breakdown", stock_code)


def get_income_statement(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    if market == "HK-share":
        start_date, end_date = _history_window(years=8)
        result = query_hk_income(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_income_records(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("income_statement", f"{len(records)} periods", f"tushare hk_income ({stock_code})")
            return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        fallback = _akshare_hk_statement(
            stock_code,
            "利润表",
            {
                "股东应占溢利": "归属于母公司所有者的净利润",
                "营业额": "营业总收入",
                "经营溢利": "营业利润",
                "融资成本": "利息支出",
                "除税前溢利": "利润总额",
            },
        )
        if fallback.get("data"):
            evidence = _make_evidence("income_statement", f"{len(fallback['data'])} periods", f"akshare stock_financial_hk_report_em income ({stock_code})")
            return {"data": fallback["data"], "evidence": evidence, "status": fallback["status"], "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", fallback.get("status")), "fetch_timestamp": now_iso()}

    if market == "US-share":
        start_date, end_date = _history_window(years=8)
        result = query_us_income(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_income_records(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("income_statement", f"{len(records)} periods", f"tushare us_income ({stock_code})")
            return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", "error: us_income unavailable"), "fetch_timestamp": now_iso()}

    start_date, end_date = _history_window(years=8)
    result = query_income(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
    records = _normalize_tushare_income_records(result.get("data", []) or [])
    if records:
        evidence = _make_evidence("income_statement", f"{len(records)} periods", f"tushare income ({stock_code})")
        return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_income_statement", stock_code)


def get_balance_sheet(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    if market == "HK-share":
        start_date, end_date = _history_window(years=8)
        result = query_hk_balancesheet(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_balance_records(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("balance_sheet", f"{len(records)} periods", f"tushare hk_balancesheet ({stock_code})")
            return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        fallback = _akshare_hk_statement(
            stock_code,
            "资产负债表",
            {
                "股东权益": "归属于母公司所有者权益合计",
                "总资产": "资产总计",
                "现金及等价物": "货币资金",
                "股本": "实收资本(或股本)",
            },
        )
        if fallback.get("data"):
            evidence = _make_evidence("balance_sheet", f"{len(fallback['data'])} periods", f"akshare stock_financial_hk_report_em balance ({stock_code})")
            return {"data": fallback["data"], "evidence": evidence, "status": fallback["status"], "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", fallback.get("status")), "fetch_timestamp": now_iso()}

    if market == "US-share":
        start_date, end_date = _history_window(years=8)
        result = query_us_balancesheet(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_balance_records(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("balance_sheet", f"{len(records)} periods", f"tushare us_balancesheet ({stock_code})")
            return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", "error: us_balancesheet unavailable"), "fetch_timestamp": now_iso()}

    start_date, end_date = _history_window(years=8)
    result = query_balancesheet(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
    records = _normalize_tushare_balance_records(result.get("data", []) or [])
    if records:
        evidence = _make_evidence("balance_sheet", f"{len(records)} periods", f"tushare balancesheet ({stock_code})")
        return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_balance_sheet", stock_code)


def get_cashflow_statement(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    if market == "HK-share":
        start_date, end_date = _history_window(years=8)
        result = query_hk_cashflow(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_cashflow_records(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("cashflow_statement", f"{len(records)} periods", f"tushare hk_cashflow ({stock_code})")
            return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        fallback = _akshare_hk_statement(
            stock_code,
            "现金流量表",
            {
                "经营业务现金净额": "经营活动产生的现金流量净额",
                "期末现金": "现金及现金等价物余额",
            },
        )
        if fallback.get("data"):
            evidence = _make_evidence("cashflow_statement", f"{len(fallback['data'])} periods", f"akshare stock_financial_hk_report_em cashflow ({stock_code})")
            return {"data": fallback["data"], "evidence": evidence, "status": fallback["status"], "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", fallback.get("status")), "fetch_timestamp": now_iso()}

    if market == "US-share":
        start_date, end_date = _history_window(years=8)
        result = query_us_cashflow(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        records = _normalize_tushare_cashflow_records(result.get("data", []) or [])
        if records:
            evidence = _make_evidence("cashflow_statement", f"{len(records)} periods", f"tushare us_cashflow ({stock_code})")
            return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
        return {"data": [], "evidence": {}, "status": result.get("status", "error: us_cashflow unavailable"), "fetch_timestamp": now_iso()}

    start_date, end_date = _history_window(years=8)
    result = query_cashflow(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
    records = _normalize_tushare_cashflow_records(result.get("data", []) or [])
    if records:
        evidence = _make_evidence("cashflow_statement", f"{len(records)} periods", f"tushare cashflow ({stock_code})")
        return {"data": records[-8:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_cashflow_statement", stock_code)


def get_shareholder_count(stock_code: str) -> dict[str, Any]:
    if infer_market_from_stock_code(stock_code) != "A-share":
        return {"data": [], "evidence": {}, "status": "not_applicable_for_market", "fetch_timestamp": now_iso()}
    start_date, end_date = _history_window(years=5)
    result = query_stk_holdernumber(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
    records = _normalize_tushare_shareholder_records(result.get("data", []) or [])
    if records:
        evidence = _make_evidence("shareholder_count", f"{len(records)} periods", f"tushare stk_holdernumber ({stock_code})")
        return {"data": records[-12:], "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}
    return _fallback_akshare("get_shareholder_count", stock_code)


def get_stock_kline(stock_code: str, period: str = "daily", years: int = 5) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    if normalize_text(period).lower() != "daily":
        if market != "A-share":
            return {"data": {}, "evidence": {}, "status": "error: non_a_share_only_supports_daily_kline", "fetch_timestamp": now_iso()}
        return _fallback_akshare("get_stock_kline", stock_code, period, years)

    bars = _load_tushare_daily_frame(stock_code, years=years)
    if bars.empty:
        if market != "A-share":
            return {"data": {}, "evidence": {}, "status": "error: empty market-aware daily bars", "fetch_timestamp": now_iso()}
        return _fallback_akshare("get_stock_kline", stock_code, period, years)

    ordered = bars.sort_values("date").reset_index(drop=True)
    latest_close = float(ordered["close"].iloc[-1])
    latest_date = pd.Timestamp(ordered["date"].iloc[-1]).normalize()
    rolling_window = ordered[ordered["date"] >= latest_date - pd.DateOffset(years=5)].copy()
    if rolling_window.empty:
        rolling_window = ordered
    high_5y = float(rolling_window["close"].max())
    low_5y = float(rolling_window["close"].min())
    monthly_closes = ordered.set_index("date")["close"].resample("ME").last().dropna()
    summary = {
        "latest_close": latest_close,
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "high_5y": high_5y,
        "low_5y": low_5y,
        "current_vs_high": round(latest_close / high_5y * 100, 1) if high_5y else None,
        "current_vs_5yr_high": round(latest_close / high_5y * 100, 1) if high_5y else None,
        "drawdown_from_5yr_high_pct": round(100 - latest_close / high_5y * 100, 1) if high_5y else None,
        "total_bars": len(ordered),
        "consolidation_months": int(monthly_closes.notna().sum()),
        "avg_vol_1y": float(ordered["volume"].tail(252).mean()) if ordered["volume"].tail(252).notna().any() else None,
        "avg_vol_20d": float(ordered["volume"].tail(20).mean()) if ordered["volume"].tail(20).notna().any() else None,
        "avg_vol_120d": float(ordered["volume"].tail(120).mean()) if ordered["volume"].tail(120).notna().any() else None,
        "avg_turnover_1y": float(ordered["amount"].tail(252).mean()) if ordered["amount"].tail(252).notna().any() else None,
        "avg_turnover_20d": float(ordered["amount"].tail(20).mean()) if ordered["amount"].tail(20).notna().any() else None,
        "avg_turnover_120d": float(ordered["amount"].tail(120).mean()) if ordered["amount"].tail(120).notna().any() else None,
    }
    if summary["avg_vol_20d"] not in (None, 0) and summary["avg_vol_120d"] not in (None, 0):
        summary["volume_ratio_20_vs_120"] = round(summary["avg_vol_20d"] / summary["avg_vol_120d"], 2)
    evidence = _make_evidence("stock_kline", f"close={summary.get('latest_close', 'N/A')}", f"tushare daily + adj_factor ({stock_code})")
    return {"data": summary, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}


def get_valuation_history(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    if market == "HK-share":
        quote = get_realtime_quote(stock_code)
        indicator = _hk_indicator_snapshot(stock_code)
        latest = {
            "pb": round(float(indicator["pb"]), 4) if indicator.get("pb") is not None else None,
            "latest_close": round(float((quote.get("data") or {}).get("最新价")), 4) if safe_float((quote.get("data") or {}).get("最新价")) is not None else None,
            "latest_trade_date": normalize_text((quote.get("data") or {}).get("最新交易日")),
        }
        if latest["pb"] is not None or latest["latest_close"] is not None:
            evidence = _make_evidence("valuation_history", f"pb={latest.get('pb', 'N/A')}", f"tushare hk_daily + akshare hk indicator ({stock_code})", confidence="medium")
            return {"data": latest, "evidence": evidence, "status": "ok_partial_hk_current_only", "fetch_timestamp": now_iso()}
        return {"data": {}, "evidence": {}, "status": "error: hk valuation unavailable", "fetch_timestamp": now_iso()}

    if market == "US-share":
        quote = get_realtime_quote(stock_code)
        latest_price = safe_float((quote.get("data") or {}).get("最新价"))
        if latest_price is not None:
            evidence = _make_evidence("valuation_history", f"price={latest_price}", f"tushare us_daily ({stock_code})", confidence="low")
            return {
                "data": {
                    "latest_close": round(latest_price, 4),
                    "latest_trade_date": normalize_text((quote.get("data") or {}).get("最新交易日")),
                },
                "evidence": evidence,
                "status": "ok_partial_us_price_only",
                "fetch_timestamp": now_iso(),
            }
        return {"data": {}, "evidence": {}, "status": "error: us valuation unavailable", "fetch_timestamp": now_iso()}

    start_date, end_date = _history_window(years=5)
    basic = query_daily_basic(
        ts_code=_to_ts_code(stock_code),
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,pb,total_mv,circ_mv",
    )
    daily = query_daily(
        ts_code=_to_ts_code(stock_code),
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,close",
    )
    basic_df = _records_frame(basic.get("data", []) or [])
    daily_df = _records_frame(daily.get("data", []) or [])
    if basic_df.empty or daily_df.empty:
        return _fallback_akshare("get_valuation_history", stock_code)

    basic_df["trade_date"] = basic_df["trade_date"].map(_to_ts_date)
    daily_df["trade_date"] = daily_df["trade_date"].map(_to_ts_date)
    basic_df["pb"] = pd.to_numeric(basic_df.get("pb"), errors="coerce")
    daily_df["close"] = pd.to_numeric(daily_df.get("close"), errors="coerce")
    merged = (
        basic_df[["trade_date", "pb"]]
        .merge(daily_df[["trade_date", "close"]], on="trade_date", how="inner")
        .dropna(subset=["pb", "close"])
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    if merged.empty:
        return _fallback_akshare("get_valuation_history", stock_code)

    pb_series = merged["pb"]
    current_pb = float(pb_series.iloc[-1])
    latest = {
        "pb": round(current_pb, 4),
        "pb_percentile": round(float((pb_series < current_pb).sum() / len(pb_series) * 100), 2),
        "pb_min": round(float(pb_series.min()), 4),
        "pb_max": round(float(pb_series.max()), 4),
        "pb_median": round(float(pb_series.median()), 4),
        "latest_close": round(float(merged["close"].iloc[-1]), 4),
        "latest_trade_date": _to_display_date(merged["trade_date"].iloc[-1]),
    }
    evidence = _make_evidence(
        "valuation_history",
        f"pb={latest.get('pb', 'N/A')}, percentile={latest.get('pb_percentile', 'N/A')}%",
        f"tushare daily_basic + daily ({stock_code})",
    )
    return {"data": latest, "evidence": evidence, "status": "ok", "fetch_timestamp": now_iso()}


def get_realtime_quote(stock_code: str) -> dict[str, Any]:
    market = infer_market_from_stock_code(stock_code)
    if market == "HK-share":
        start_date, end_date = _history_window(years=0, days=20)
        daily = query_hk_daily(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        daily_rows = daily.get("data", []) or []
        latest_daily = _latest_record(daily_rows, ("trade_date",))
        latest_close = safe_float(latest_daily.get("close"))
        profile = get_company_profile(stock_code)
        profile_data = profile.get("data", {}) or {}
        indicator = _hk_indicator_snapshot(stock_code)
        record = {
            "代码": normalize_display_code(stock_code),
            "股票简称": normalize_text(profile_data.get("股票简称") or stock_code),
            "行业": normalize_text(profile_data.get("行业")),
            "最新价": latest_close if latest_close is not None else indicator.get("latest_price"),
            "最新交易日": _to_display_date(latest_daily.get("trade_date")),
            "市净率MRQ": indicator.get("pb"),
            "滚动市盈率TTM": indicator.get("pe"),
            "总市值": indicator.get("market_cap"),
            "流通市值": indicator.get("float_market_cap"),
            "总股本": indicator.get("share_count"),
            "流通股": indicator.get("share_count"),
        }
        if record["最新价"] is not None:
            evidence = _make_evidence("realtime_quote", f"price={record.get('最新价', 'N/A')}, mktcap={record.get('总市值', 'N/A')}", f"tushare hk_daily snapshot ({stock_code})", confidence="medium")
            return {"data": record, "evidence": evidence, "status": "ok_tushare_daily_snapshot", "fetch_timestamp": now_iso()}
        return {"data": {}, "evidence": {}, "status": daily.get("status", "error: hk_daily unavailable"), "fetch_timestamp": now_iso()}

    if market == "US-share":
        start_date, end_date = _history_window(years=0, days=20)
        daily = query_us_daily(ts_code=_to_ts_code(stock_code), start_date=start_date, end_date=end_date)
        daily_rows = daily.get("data", []) or []
        latest_daily = _latest_record(daily_rows, ("trade_date",))
        latest_close = safe_float(latest_daily.get("close"))
        profile = get_company_profile(stock_code)
        profile_data = profile.get("data", {}) or {}
        if latest_close is not None:
            record = {
                "代码": normalize_display_code(stock_code),
                "股票简称": normalize_text(profile_data.get("股票简称") or stock_code),
                "行业": normalize_text(profile_data.get("行业")),
                "最新价": latest_close,
                "最新交易日": _to_display_date(latest_daily.get("trade_date")),
                "总市值": None,
                "流通市值": None,
            }
            evidence = _make_evidence("realtime_quote", f"price={record.get('最新价', 'N/A')}, mktcap={record.get('总市值', 'N/A')}", f"tushare us_daily snapshot ({stock_code})", confidence="medium")
            return {"data": record, "evidence": evidence, "status": "ok_tushare_daily_snapshot", "fetch_timestamp": now_iso()}
        return {"data": {}, "evidence": {}, "status": daily.get("status", "error: us_daily unavailable"), "fetch_timestamp": now_iso()}

    start_date, end_date = _history_window(years=0, days=20)
    daily = query_daily(
        ts_code=_to_ts_code(stock_code),
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,close",
    )
    basic = query_daily_basic(
        ts_code=_to_ts_code(stock_code),
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,total_mv,circ_mv,pb",
    )
    daily_rows = daily.get("data", []) or []
    basic_rows = basic.get("data", []) or []
    if not daily_rows or not basic_rows:
        return _fallback_akshare("get_realtime_quote", stock_code)

    latest_daily = _latest_record(daily_rows, ("trade_date",))
    latest_basic = _latest_record(basic_rows, ("trade_date",))
    latest_close = safe_float(latest_daily.get("close"))
    total_mv = safe_float(latest_basic.get("total_mv"))
    circ_mv = safe_float(latest_basic.get("circ_mv"))
    if latest_close in (None, 0):
        return _fallback_akshare("get_realtime_quote", stock_code)

    total_mv = total_mv * 10_000.0 if total_mv is not None else None
    circ_mv = circ_mv * 10_000.0 if circ_mv is not None else None
    profile = get_company_profile(stock_code)
    profile_data = profile.get("data", {}) or {}
    record = {
        "代码": normalize_display_code(stock_code),
        "股票简称": normalize_text(profile_data.get("股票简称") or stock_code),
        "行业": normalize_text(profile_data.get("行业")),
        "最新价": latest_close,
        "最新交易日": _to_display_date(latest_daily.get("trade_date")),
        "市净率MRQ": safe_float(latest_basic.get("pb")),
        "总市值": total_mv,
        "流通市值": circ_mv,
        "总股本": total_mv / latest_close if total_mv not in (None, 0) else None,
        "流通股": circ_mv / latest_close if circ_mv not in (None, 0) else None,
    }
    evidence = _make_evidence(
        "realtime_quote",
        f"price={record.get('最新价', 'N/A')}, mktcap={record.get('总市值', 'N/A')}",
        f"tushare daily snapshot ({stock_code}) [latest trading day snapshot, not realtime]",
    )
    return {"data": record, "evidence": evidence, "status": "ok_tushare_daily_snapshot", "fetch_timestamp": now_iso()}


def get_all_a_share_stocks(day: str | None = None) -> dict[str, Any]:
    requested_day = _to_ts_date(day) or _latest_trade_date()
    stock_basic = query_stock_basic(list_status="L", fields="ts_code,symbol,name,industry,list_date")
    if not (stock_basic.get("data") or []):
        return _fallback_baostock("get_all_a_share_stocks", _to_display_date(requested_day))

    trade_day = requested_day
    daily_basic = {"data": [], "status": "ok"}
    daily = {"data": [], "status": "ok"}
    candidate_days = list(reversed(_recent_trade_dates(requested_day))) or [requested_day]
    if requested_day not in candidate_days:
        candidate_days.insert(0, requested_day)
    for candidate_day in candidate_days:
        daily_basic = query_daily_basic(
            trade_date=candidate_day,
            fields="ts_code,trade_date,total_mv,circ_mv",
        )
        daily = query_daily(
            trade_date=candidate_day,
            fields="ts_code,trade_date,amount",
        )
        if (daily_basic.get("data") or []) or (daily.get("data") or []):
            trade_day = candidate_day
            break

    basic_map = {normalize_text(row.get("ts_code")): row for row in (daily_basic.get("data", []) or [])}
    daily_map = {normalize_text(row.get("ts_code")): row for row in (daily.get("data", []) or [])}

    records: list[dict[str, Any]] = []
    for row in stock_basic.get("data", []) or []:
        ts_code = normalize_text(row.get("ts_code"))
        snapshot = basic_map.get(ts_code, {})
        trade_row = daily_map.get(ts_code, {})
        name = normalize_text(row.get("name"))
        records.append(
            {
                "code": _normalize_symbol(row),
                "name": name,
                "industry": normalize_text(row.get("industry")) or "unknown",
                "market_cap": (safe_float(snapshot.get("total_mv")) or 0.0) * 10_000.0 if snapshot else None,
                "float_market_cap": (safe_float(snapshot.get("circ_mv")) or 0.0) * 10_000.0 if snapshot else None,
                "turnover": safe_float(trade_row.get("amount")) * 1000.0 if safe_float(trade_row.get("amount")) is not None else None,
                "special_tags": ["special_situation"] if "ST" in name.upper() else [],
            }
        )
    records.sort(
        key=lambda item: (
            -(safe_float(item.get("float_market_cap")) or 0.0),
            -(safe_float(item.get("market_cap")) or 0.0),
            item.get("code", ""),
        )
    )
    evidence = _make_evidence("stock_universe", f"{len(records)} tickers", f"tushare stock_basic + daily_basic + daily ({trade_day})")
    return {"data": records, "status": "ok", "day": _to_display_date(trade_day), "evidence": evidence, "fetch_timestamp": now_iso()}


RADAR_PARTIAL_STEPS = {
    "company_profile": get_company_profile,
    "revenue_breakdown": get_revenue_breakdown,
    "valuation_history": get_valuation_history,
    "stock_kline": get_stock_kline,
    "realtime_quote": get_realtime_quote,
    "shareholder_count": get_shareholder_count,
}

RADAR_EXPENSIVE_STEPS = {
    "income_statement": get_income_statement,
    "balance_sheet": get_balance_sheet,
    "cashflow_statement": get_cashflow_statement,
}

RADAR_ALL_STEPS = {**RADAR_PARTIAL_STEPS, **RADAR_EXPENSIVE_STEPS}

FULL_SCAN_STEPS = [
    ("company_profile", get_company_profile),
    ("financial_summary", get_financial_summary),
    ("revenue_breakdown", get_revenue_breakdown),
    ("valuation_history", get_valuation_history),
    ("stock_kline", get_stock_kline),
    ("realtime_quote", get_realtime_quote),
    ("shareholder_count", get_shareholder_count),
    ("income_statement", get_income_statement),
    ("balance_sheet", get_balance_sheet),
    ("cashflow_statement", get_cashflow_statement),
]
