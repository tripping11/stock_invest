"""Shared A-share provider routing for scan entrypoints.

This module keeps engine code agnostic to the current primary Tier 1 vendor.
The default provider is Tushare, while AkShare and BaoStock remain fallback
options behind adapter boundaries.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from adapters import akshare_adapter, baostock_adapter, tushare_adapter
from utils.evidence_helpers import now_iso
from utils.value_utils import normalize_text


_DEFAULT_SCAN_PROVIDER = "tushare"
_SCAN_PROVIDER_ENV = "A_SHARE_SCAN_PROVIDER"
_SCAN_PROVIDERS = {
    "tushare": tushare_adapter,
    "akshare": akshare_adapter,
}
CANONICAL_SCAN_CACHE_NAME = "tier1_scan.json"
CANONICAL_EVIDENCE_CACHE_NAME = "tier1_evidence.json"
LEGACY_SCAN_CACHE_NAME = "akshare_scan.json"
LEGACY_EVIDENCE_CACHE_NAME = "akshare_evidence.json"


def get_scan_adapter_name() -> str:
    requested = normalize_text(os.getenv(_SCAN_PROVIDER_ENV, _DEFAULT_SCAN_PROVIDER)).lower()
    return requested if requested in _SCAN_PROVIDERS else _DEFAULT_SCAN_PROVIDER


def get_scan_adapter():
    return _SCAN_PROVIDERS[get_scan_adapter_name()]


SCAN_ADAPTER = get_scan_adapter()
RADAR_PARTIAL_STEPS = SCAN_ADAPTER.RADAR_PARTIAL_STEPS
RADAR_EXPENSIVE_STEPS = getattr(SCAN_ADAPTER, "RADAR_EXPENSIVE_STEPS", {})
RADAR_ALL_STEPS = SCAN_ADAPTER.RADAR_ALL_STEPS
FULL_SCAN_STEPS = list(getattr(SCAN_ADAPTER, "FULL_SCAN_STEPS", []))

# Reuse the generic cache / retry / trade-day helpers from the existing scan layer.
run_named_scan_steps = akshare_adapter.run_named_scan_steps
resolve_radar_trade_date = akshare_adapter.resolve_radar_trade_date


def canonical_scan_cache_path(base_dir: str | Path) -> Path:
    return Path(base_dir) / CANONICAL_SCAN_CACHE_NAME


def legacy_scan_cache_path(base_dir: str | Path) -> Path:
    return Path(base_dir) / LEGACY_SCAN_CACHE_NAME


def canonical_scan_evidence_path(base_dir: str | Path) -> Path:
    return Path(base_dir) / CANONICAL_EVIDENCE_CACHE_NAME


def legacy_scan_evidence_path(base_dir: str | Path) -> Path:
    return Path(base_dir) / LEGACY_EVIDENCE_CACHE_NAME


def _with_provider_source_meta(result: dict[str, Any], provider_name: str) -> dict[str, Any]:
    normalized = dict(result or {})
    evidence = dict(normalized.get("evidence") or {})
    source_meta = dict(normalized.get("_source_meta") or {})
    source_meta.setdefault("source_type", evidence.get("source_type") or provider_name)
    source_meta.setdefault("source_desc", evidence.get("description", ""))
    source_meta.setdefault("source_url", evidence.get("source_url", ""))
    source_meta.setdefault("source_tier", evidence.get("source_tier"))
    source_meta.setdefault("confidence", evidence.get("confidence"))
    source_meta.setdefault("fetch_time", normalized.get("fetch_timestamp") or evidence.get("fetch_time"))
    source_meta.setdefault("status", normalized.get("status"))
    normalized["_source_meta"] = source_meta
    return normalized


def load_scan_cache(base_dir: str | Path) -> dict[str, Any]:
    candidates = (
        canonical_scan_cache_path(base_dir),
        legacy_scan_cache_path(base_dir),
    )
    for path in candidates:
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
    return {}


def write_scan_cache(base_dir: str | Path, results: dict[str, Any], evidence_list: list[dict[str, Any]]) -> None:
    base_path = Path(base_dir)
    base_path.mkdir(parents=True, exist_ok=True)
    cache_payload = json.dumps(results, ensure_ascii=False, indent=2, default=str)
    evidence_payload = json.dumps(evidence_list, ensure_ascii=False, indent=2, default=str)
    for path in (canonical_scan_cache_path(base_path), legacy_scan_cache_path(base_path)):
        path.write_text(cache_payload, encoding="utf-8")
    for path in (canonical_scan_evidence_path(base_path), legacy_scan_evidence_path(base_path)):
        path.write_text(evidence_payload, encoding="utf-8")


def get_all_a_share_stocks(day: str | None = None) -> dict[str, Any]:
    provider_fn = getattr(SCAN_ADAPTER, "get_all_a_share_stocks", None)
    if callable(provider_fn):
        result = provider_fn(day)
        if akshare_adapter._is_ok_status(result.get("status")) and (result.get("data") or []):
            return _with_provider_source_meta(result, get_scan_adapter_name())

    fallback = baostock_adapter.get_all_a_share_stocks(day)
    if akshare_adapter._is_ok_status(fallback.get("status")):
        return _with_provider_source_meta(fallback, "baostock")

    if "result" in locals():
        return _with_provider_source_meta(result, get_scan_adapter_name())
    return _with_provider_source_meta(fallback, "baostock")


def run_full_scan(stock_code: str, output_dir: str | None = None) -> dict[str, Any]:
    results: dict[str, Any] = {}
    evidence_list: list[dict[str, Any]] = []
    cached_results: dict[str, Any] = {}
    if output_dir:
        cached_results = load_scan_cache(output_dir)

    for name, func in FULL_SCAN_STEPS:
        result = akshare_adapter._resolve_scan_step(
            stock_code,
            name,
            func,
            cached_results=cached_results,
        )
        results[name] = result
        if result.get("evidence"):
            evidence_list.append(result["evidence"])

    if output_dir:
        results["_cache_timestamp"] = now_iso()
        results["_scan_provider"] = get_scan_adapter_name()
        write_scan_cache(output_dir, results, evidence_list)

    return results
