"""
commodity_adapter.py - Tier 2 commodity validation layer.

This adapter is now profile-driven:
- signal profiles live in config/commodity_profiles.yaml
- different industries can declare price proxies, inventory applicability,
  futures mappings, and social inventory guides without changing code
"""

from __future__ import annotations

import datetime
import json
import os
import sys
from typing import Any

import akshare as ak

from utils.commodity_profile_utils import build_profile_maps, resolve_signal_profile
from utils.evidence_helpers import make_evidence as _shared_make_evidence, now_ts
from utils.vendor_support import ensure_vendor_path, get_vendor_env


PROFILE_MAPS = build_profile_maps()
FUTURES_SYMBOL_MAP = PROFILE_MAPS["futures_symbol_map"]
EXCHANGE_INVENTORY_SYMBOL_MAP = PROFILE_MAPS["exchange_inventory_symbol_map"]
TQSDK_MAIN_SYMBOL_MAP = PROFILE_MAPS["tqsdk_symbol_map"]
INVENTORY_SOURCES = PROFILE_MAPS["social_inventory_map"]




def _make_evidence(
    field: str,
    value: Any,
    source_desc: str,
    *,
    tier: int = 2,
    url: str = "",
    confidence: str = "medium",
) -> dict[str, Any]:
    return _shared_make_evidence(field, value, source_desc, source_type="commodity_data", tier=tier, url=url, confidence=confidence)


def _load_tqsdk():
    if not ensure_vendor_path("tqsdk"):
        return None
    try:
        from tqsdk import TqApi, TqAuth  # type: ignore

        return TqApi, TqAuth
    except Exception:
        return None


def _load_efinance():
    if not ensure_vendor_path("efinance"):
        return None
    try:
        import efinance as ef  # type: ignore

        return ef
    except Exception:
        return None


def _get_tqsdk_auth() -> tuple[str, str] | None:
    username = get_vendor_env("TQSDK_USERNAME")
    password = get_vendor_env("TQSDK_PASSWORD")
    auth_text = get_vendor_env("TQSDK_AUTH")
    if auth_text and "," in auth_text:
        user, pwd = auth_text.split(",", 1)
        return user.strip(), pwd.strip()
    if username and password:
        return username, password
    return None


def _as_records(df, tail: int = 60) -> list[dict[str, Any]]:
    if df is None or len(df) == 0:
        return []
    return df.tail(tail).to_dict("records")


def _profile(commodity_name: str) -> dict[str, Any]:
    return resolve_signal_profile(commodity_name)


def _status_ok(status: str) -> bool:
    return str(status or "").lower().startswith("ok")


def _status_na(status: str) -> bool:
    return str(status or "").lower().startswith("not_applicable")


def _price_target(profile: dict[str, Any], commodity_name: str) -> str:
    return str(profile.get("price_proxy") or profile.get("name") or commodity_name)


def _spot_symbol_candidates(profile: dict[str, Any], commodity_name: str) -> list[str]:
    target = _price_target(profile, commodity_name)
    raw = list(profile.get("spot_symbols", []) or [])
    raw.extend([target, commodity_name])
    ordered: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _futures_symbol(profile: dict[str, Any], commodity_name: str) -> str:
    target = _price_target(profile, commodity_name)
    return str(
        profile.get("futures_symbol")
        or FUTURES_SYMBOL_MAP.get(target)
        or FUTURES_SYMBOL_MAP.get(commodity_name)
        or ""
    )


def _exchange_inventory_candidates(profile: dict[str, Any], commodity_name: str) -> list[str]:
    target = _price_target(profile, commodity_name)
    mapped = str(profile.get("exchange_inventory_symbol") or EXCHANGE_INVENTORY_SYMBOL_MAP.get(target) or "")
    futures_symbol = _futures_symbol(profile, commodity_name)
    futures_code = futures_symbol[:-1] if futures_symbol.endswith("0") else futures_symbol
    raw = [mapped, target, commodity_name, futures_code, futures_code.lower()]
    ordered: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _social_inventory_guide(profile: dict[str, Any], commodity_name: str) -> dict[str, Any]:
    guide = profile.get("social_inventory", {}) or INVENTORY_SOURCES.get(commodity_name, {})
    return guide or {
        "enabled": True,
        "primary": "行业协会 / 生意社",
        "url": "https://www.100ppi.com/",
        "frequency": "周度",
        "fields": ["库存量"],
    }


def _latest_kline_snapshot(records: list[dict[str, Any]]) -> tuple[Any, Any]:
    if not records:
        return None, None
    latest = records[-1]
    return latest.get("close") or latest.get("收盘价") or latest.get("收盘"), latest.get("date") or latest.get("日期")


def _latest_inventory_snapshot(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {}
    latest = records[-1]
    return {
        "date": latest.get("日期") or latest.get("date"),
        "inventory": latest.get("库存"),
        "change": latest.get("增减"),
    }


def _get_tqsdk_main_contract(profile: dict[str, Any], commodity_name: str) -> dict[str, Any] | None:
    auth = _get_tqsdk_auth()
    sdk = _load_tqsdk()
    target = _price_target(profile, commodity_name)
    symbol = str(profile.get("tqsdk_symbol") or TQSDK_MAIN_SYMBOL_MAP.get(target) or "")
    if auth is None or sdk is None or not symbol:
        return None

    TqApi, TqAuth = sdk
    api = None
    try:
        api = TqApi(auth=TqAuth(auth[0], auth[1]))
        quote = api.get_quote(symbol)
        klines = api.get_kline_serial(symbol, duration_seconds=24 * 60 * 60, data_length=260)
        for _ in range(60):
            api.wait_update()
            if quote.datetime and len(klines) >= 20:
                break
        if not quote.datetime or len(klines) == 0:
            return None

        records = []
        for _, row in klines.tail(60).iterrows():
            records.append(
                {
                    "datetime": str(row["datetime"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                }
            )

        return {
            "symbol": symbol,
            "latest_close": float(klines.iloc[-1]["close"]),
            "latest_datetime": str(quote.datetime),
            "high_60d": float(klines.tail(60)["high"].max()),
            "low_60d": float(klines.tail(60)["low"].min()),
            "high_250d": float(klines.tail(250)["high"].max()) if len(klines) >= 120 else None,
            "high_750d": None,
            "records": records,
        }
    except Exception:
        return None
    finally:
        if api is not None:
            api.close()


def _get_efinance_futures_snapshot(profile: dict[str, Any], commodity_name: str) -> dict[str, Any] | None:
    ef = _load_efinance()
    target = _price_target(profile, commodity_name)
    futures_symbol = _futures_symbol(profile, commodity_name)
    product_code = futures_symbol[:-1] if futures_symbol.endswith("0") else futures_symbol
    if ef is None or not product_code:
        return None

    try:
        quotes = ef.futures.get_realtime_quotes()
        if quotes is None or len(quotes) == 0:
            return None
        code_col = "期货代码" if "期货代码" in quotes.columns else quotes.columns[0]
        name_col = "期货名称" if "期货名称" in quotes.columns else quotes.columns[1]
        filtered = quotes[
            quotes[code_col].astype(str).str.contains(product_code, regex=False, na=False)
            | quotes[name_col].astype(str).str.contains(target, regex=False, na=False)
        ]
        if len(filtered) == 0:
            return None
        row = filtered.iloc[0].to_dict()
        return {
            "symbol": row.get(code_col, product_code),
            "latest_close": row.get("最新价"),
            "latest_datetime": row.get("更新时间"),
            "high_60d": row.get("最高"),
            "low_60d": row.get("最低"),
            "realtime_row": row,
        }
    except Exception:
        return None


def get_spot_price(commodity_name: str) -> dict[str, Any]:
    profile = _profile(commodity_name)
    target = _price_target(profile, commodity_name)
    spot_candidates = _spot_symbol_candidates(profile, commodity_name)

    for symbol in spot_candidates:
        try:
            df = ak.spot_goods(symbol=symbol)
            records = _as_records(df, tail=30)
            if records:
                evidence = _make_evidence(
                    f"{commodity_name}_spot_price",
                    f"{len(records)} records",
                    f"akshare spot_goods ({symbol})",
                )
                return {"data": records, "evidence": evidence, "status": "ok"}
        except Exception:
            continue

    futures_symbol = _futures_symbol(profile, commodity_name)
    if not futures_symbol:
        return {
            "data": [],
            "evidence": _make_evidence(
                f"{commodity_name}_spot_price",
                "manual_required",
                f"{commodity_name} 缺少现货/期货映射，需人工补充",
                confidence="low",
            ),
            "status": "manual_required: no spot source or futures mapping",
            "human_action_needed": {
                "action": f"补充{commodity_name}价格信号",
                "commodity": commodity_name,
                "suggested_sources": ["生意社 100ppi.com", "行业协会报价"],
                "priority": "yellow",
            },
        }

    try:
        df = ak.futures_main_sina(
            symbol=futures_symbol,
            start_date="20240101",
            end_date=datetime.datetime.now().strftime("%Y%m%d"),
        )
        records = _as_records(df, tail=60)
        if records:
            evidence = _make_evidence(
                f"{commodity_name}_futures_price",
                f"{len(records)} records",
                f"akshare futures_main_sina ({futures_symbol})",
            )
            return {"data": records, "evidence": evidence, "status": "partial: futures_fallback"}
    except Exception:
        pass

    tqsdk_result = _get_tqsdk_main_contract(profile, commodity_name)
    if tqsdk_result:
        evidence = _make_evidence(
            f"{commodity_name}_tqsdk_main_contract",
            f"close={tqsdk_result['latest_close']}",
            f"TqSdk main contract fallback ({tqsdk_result['symbol']})",
        )
        return {"data": tqsdk_result["records"], "evidence": evidence, "status": "partial: tqsdk_main_contract"}

    efinance_snapshot = _get_efinance_futures_snapshot(profile, commodity_name)
    if efinance_snapshot:
        evidence = _make_evidence(
            f"{commodity_name}_efinance_futures",
            f"close={efinance_snapshot['latest_close']}",
            f"efinance futures.get_realtime_quotes ({efinance_snapshot['symbol']})",
        )
        return {"data": [efinance_snapshot["realtime_row"]], "evidence": evidence, "status": "partial: efinance_futures_snapshot"}

    return {
        "data": [],
        "evidence": {},
        "status": "error: all sources failed",
        "human_action_needed": {
            "action": f"补充{commodity_name}价格信号",
            "commodity": commodity_name,
            "price_proxy": target,
            "futures_symbol": futures_symbol,
            "priority": "red",
        },
    }


def get_exchange_inventory(commodity_name: str) -> dict[str, Any]:
    profile = _profile(commodity_name)
    if profile.get("exchange_inventory_enabled", True) is False:
        return {
            "data": {"commodity": commodity_name, "reason": "profile_disabled"},
            "evidence": _make_evidence(
                f"{commodity_name}_exchange_inventory",
                "not_applicable",
                f"{commodity_name} 交易所库存不适用当前信号配置",
                confidence="medium",
            ),
            "status": "not_applicable: exchange inventory disabled",
        }

    candidates = _exchange_inventory_candidates(profile, commodity_name)
    attempts: list[dict[str, str]] = []
    for symbol in candidates:
        try:
            df = ak.futures_inventory_em(symbol=symbol)
            records = _as_records(df, tail=60)
            if records:
                latest = _latest_inventory_snapshot(records)
                evidence = _make_evidence(
                    f"{commodity_name}_exchange_inventory",
                    f"{latest.get('date')} inventory={latest.get('inventory')}",
                    f"akshare futures_inventory_em ({symbol})",
                )
                return {
                    "data": {
                        "commodity": commodity_name,
                        "symbol": symbol,
                        "records": records,
                        "latest_record": latest,
                    },
                    "evidence": evidence,
                    "status": "ok",
                }
            attempts.append({"symbol": symbol, "status": "empty"})
        except Exception as exc:
            attempts.append({"symbol": symbol, "status": f"error: {exc}"})

    return {
        "data": {"commodity": commodity_name, "candidates_tried": candidates, "attempts": attempts},
        "evidence": _make_evidence(
            f"{commodity_name}_exchange_inventory",
            "manual_required",
            f"{commodity_name} 交易所库存自动源暂未命中",
            confidence="low",
        ),
        "status": "manual_required: exchange inventory unavailable",
        "human_action_needed": {
            "action": f"补充{commodity_name}交易所库存/仓单",
            "commodity": commodity_name,
            "priority": "yellow",
        },
    }


def get_social_inventory(commodity_name: str) -> dict[str, Any]:
    profile = _profile(commodity_name)
    guide = _social_inventory_guide(profile, commodity_name)
    if guide.get("enabled", True) is False:
        return {
            "data": {"commodity": commodity_name, "reason": "profile_disabled"},
            "evidence": _make_evidence(
                f"{commodity_name}_social_inventory",
                "not_applicable",
                f"{commodity_name} 社会库存不适用当前信号配置",
                confidence="medium",
            ),
            "status": "not_applicable: social inventory disabled",
        }

    return {
        "data": {"commodity": commodity_name, "manual_guide": guide},
        "evidence": _make_evidence(
            f"{commodity_name}_social_inventory",
            "manual_required",
            f"{commodity_name} 社会库存待补证: {guide.get('primary', '行业协会 / 生意社')}",
            url=guide.get("url", ""),
            confidence="low",
        ),
        "status": "manual_required: social inventory source required",
        "human_action_needed": {
            "action": f"补充{commodity_name}社会库存",
            "commodity": commodity_name,
            "data_source": guide.get("primary", "行业协会 / 生意社"),
            "url": guide.get("url", ""),
            "frequency": guide.get("frequency", ""),
            "required_fields": guide.get("fields", []),
            "priority": "yellow",
        },
    }


def _merge_inventory_layers(commodity_name: str, exchange_result: dict[str, Any], social_result: dict[str, Any]) -> dict[str, Any]:
    exchange_status = str(exchange_result.get("status", "")).lower()
    social_status = str(social_result.get("status", "")).lower()
    exchange_ready = _status_ok(exchange_status)
    social_ready = _status_ok(social_status)
    exchange_na = _status_na(exchange_status)
    social_na = _status_na(social_status)

    if exchange_na and social_na:
        coverage = "not_applicable"
        status = "not_applicable: inventory not required"
        base_evidence = exchange_result.get("evidence") or social_result.get("evidence") or {}
        description = "inventory not applicable for current signal profile"
    elif exchange_ready and social_ready:
        coverage = "exchange_and_social"
        status = "ok_exchange_and_social_inventory"
        base_evidence = social_result.get("evidence") or exchange_result.get("evidence") or {}
        description = str(base_evidence.get("description", ""))
    elif exchange_ready:
        coverage = "exchange_only"
        status = "ok_exchange_inventory_only"
        base_evidence = exchange_result.get("evidence") or {}
        guide = social_result.get("data", {}).get("manual_guide", {}) if isinstance(social_result.get("data", {}), dict) else {}
        description = (
            f"{base_evidence.get('description', '')}; social inventory pending via {guide.get('primary', '行业协会 / 生意社')}"
        ).strip("; ")
    elif social_ready:
        coverage = "social_only"
        status = "partial: social_inventory_only"
        base_evidence = social_result.get("evidence") or {}
        description = str(base_evidence.get("description", ""))
    else:
        coverage = "missing"
        status = "manual_required: no reliable inventory source"
        base_evidence = social_result.get("evidence") or exchange_result.get("evidence") or {}
        description = str(base_evidence.get("description", ""))

    evidence = {
        **base_evidence,
        "field_name": f"{commodity_name}_inventory",
        "description": description,
        "fetch_time": now_ts(),
    }
    return {
        "data": {
            "commodity": commodity_name,
            "coverage": coverage,
            "coverage_warning": coverage not in {"exchange_and_social", "not_applicable"},
            "exchange_inventory": exchange_result.get("data", {}),
            "social_inventory": social_result.get("data", {}),
        },
        "evidence": evidence,
        "status": status,
    }


def collect_inventory_layers(commodity_name: str) -> dict[str, dict[str, Any]]:
    exchange_result = get_exchange_inventory(commodity_name)
    social_result = get_social_inventory(commodity_name)
    return {
        "exchange_inventory": exchange_result,
        "social_inventory": social_result,
        "inventory": _merge_inventory_layers(commodity_name, exchange_result, social_result),
    }


def get_inventory(commodity_name: str) -> dict[str, Any]:
    return collect_inventory_layers(commodity_name)["inventory"]


def get_futures(commodity_name: str) -> dict[str, Any]:
    profile = _profile(commodity_name)
    futures_symbol = _futures_symbol(profile, commodity_name)

    tqsdk_result = _get_tqsdk_main_contract(profile, commodity_name)
    if tqsdk_result:
        summary = {
            "commodity": commodity_name,
            "futures_symbol": tqsdk_result["symbol"],
            "latest_close": tqsdk_result["latest_close"],
            "latest_date": tqsdk_result["latest_datetime"],
            "high_60d": tqsdk_result["high_60d"],
            "low_60d": tqsdk_result["low_60d"],
            "high_250d": tqsdk_result.get("high_250d"),
            "high_750d": tqsdk_result.get("high_750d"),
        }
        evidence = _make_evidence(
            f"{commodity_name}_futures",
            f"close={summary['latest_close']}",
            f"TqSdk main contract ({tqsdk_result['symbol']})",
        )
        return {"data": summary, "evidence": evidence, "status": "ok_tqsdk"}

    if not futures_symbol:
        return {
            "data": {},
            "evidence": _make_evidence(
                f"{commodity_name}_futures",
                "not_applicable",
                f"{commodity_name} 无期货主力映射",
                confidence="medium",
            ),
            "status": "not_applicable: futures mapping missing",
        }

    try:
        df = ak.futures_zh_daily_sina(symbol=futures_symbol)
        records = _as_records(df, tail=750)
        if records:
            latest_price, latest_date = _latest_kline_snapshot(records)
            close_values = [row.get("close") for row in records if isinstance(row.get("close"), (int, float))]
            summary = {
                "commodity": commodity_name,
                "futures_symbol": futures_symbol,
                "latest_close": latest_price,
                "latest_date": str(latest_date),
                "high_60d": max(close_values[-60:]) if len(close_values) >= 60 else None,
                "low_60d": min(close_values[-60:]) if len(close_values) >= 60 else None,
                "high_250d": max(close_values[-250:]) if len(close_values) >= 120 else None,
                "high_750d": max(close_values[-750:]) if len(close_values) >= 300 else None,
            }
            evidence = _make_evidence(
                f"{commodity_name}_futures",
                f"close={summary['latest_close']}",
                f"akshare futures_zh_daily_sina ({futures_symbol})",
            )
            return {"data": summary, "evidence": evidence, "status": "ok"}
    except Exception as exc:
        efinance_snapshot = _get_efinance_futures_snapshot(profile, commodity_name)
        if efinance_snapshot:
            summary = {
                "commodity": commodity_name,
                "futures_symbol": efinance_snapshot["symbol"],
                "latest_close": efinance_snapshot["latest_close"],
                "latest_date": efinance_snapshot["latest_datetime"],
                "high_60d": efinance_snapshot["high_60d"],
                "low_60d": efinance_snapshot["low_60d"],
                "high_250d": efinance_snapshot.get("high_250d"),
                "high_750d": efinance_snapshot.get("high_750d"),
            }
            evidence = _make_evidence(
                f"{commodity_name}_futures",
                f"close={summary['latest_close']}",
                f"efinance futures.get_realtime_quotes ({efinance_snapshot['symbol']})",
            )
            return {"data": summary, "evidence": evidence, "status": "ok_efinance_futures"}
        return {
            "data": {},
            "evidence": {},
            "status": f"error: {exc}",
            "human_action_needed": {
                "action": f"手动获取{commodity_name}期货数据",
                "commodity": commodity_name,
                "futures_symbol": futures_symbol,
                "priority": "yellow",
            },
        }

    return {"data": {}, "evidence": {}, "status": "error: no futures data"}


def run_commodity_scan(commodity: str = "纯碱", output_dir: str | None = None) -> dict[str, Any]:
    print(f"[commodity_adapter] 开始扫描 {commodity} 相关数据 ...")
    results: dict[str, Any] = {}

    for name, func in (
        ("spot_price", lambda: get_spot_price(commodity)),
        ("futures", lambda: get_futures(commodity)),
    ):
        print(f"  [{name}] ...", end=" ")
        result = func()
        results[name] = result
        print(result["status"])

    inventory_layers = collect_inventory_layers(commodity)
    for name in ("exchange_inventory", "social_inventory", "inventory"):
        print(f"  [{name}] ...", end=" ")
        results[name] = inventory_layers[name]
        print(results[name]["status"])

    human_actions: list[dict[str, Any]] = []
    for name, result in results.items():
        action = result.get("human_action_needed")
        if action:
            payload = dict(action)
            payload["field"] = name
            human_actions.append(payload)
    results["_human_actions_summary"] = human_actions

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "commodity_scan.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        print(f"[commodity_adapter] 结果已保存到 {output_path}")
        if human_actions:
            print(f"[commodity_adapter] [WARNING] {len(human_actions)} 项需人工补充:")
            for action in human_actions:
                print(f"    [!] {action['field']}: {action['action']}")

    return results


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parents[5] / "data" / "raw" / "commodity")
    commodity_arg = sys.argv[2] if len(sys.argv) > 2 else "纯碱"
    run_commodity_scan(commodity=commodity_arg, output_dir=out)
