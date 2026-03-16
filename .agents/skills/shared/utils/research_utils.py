"""Compatibility helpers for adapters and Tier 0 utilities.

This module is a **compatibility facade**. It re-exports symbols from the
new focused sub-modules so that existing ``from utils.research_utils import X``
statements keep working. New code should import from the specific sub-module.
"""

from __future__ import annotations

from typing import Any

# ── re-exports from new sub-modules ──────────────────────────
from utils.config_loader import load_source_registry, load_yaml_config  # noqa: F401
from utils.evidence_helpers import now_ts  # noqa: F401
from utils.financial_snapshot import (  # noqa: F401
    extract_latest_revenue_snapshot,
    extract_latest_revenue_terms,
    extract_market_cap,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
)
from utils.opportunity_classifier import (  # noqa: F401
    assess_bottom_pattern,
    assess_business_purity,
    classify_state_ownership,
)
from utils.primary_type_router import build_driver_stack, determine_primary_type, resolve_sector_route  # noqa: F401
from utils.value_utils import (  # noqa: F401
    _pick_revenue_col,
    normalize_text,
    safe_float,
    select_latest_record,
)
from utils.vcrf_probes import assess_survival_boundary, detect_big_bath, score_underwrite_axis  # noqa: F401


CACHE_STALE_HOURS = 24

_DEPRECATED_SYMBOL_MESSAGES = {
    "determine_eco_context": "determine_eco_context was removed. Use utils.framework_utils.determine_opportunity_type instead.",
    "load_crocodile_discipline": "load_crocodile_discipline was removed with the legacy crocodile framework.",
    "get_crocodile_mode_config": "get_crocodile_mode_config was removed with the legacy crocodile framework.",
    "load_industry_mapping": "load_industry_mapping was removed. Use utils.framework_utils.load_sector_classification instead.",
    "evaluate_signal_health": "evaluate_signal_health was removed. Use utils.signal_health_utils.evaluate_signal_health_v2 if this compatibility path is still needed.",
}

__all__ = [
    "CACHE_STALE_HOURS",
    "_pick_revenue_col",
    "assess_bottom_pattern",
    "assess_business_purity",
    "classify_state_ownership",
    "detect_big_bath",
    "extract_latest_revenue_snapshot",
    "extract_latest_revenue_terms",
    "extract_market_cap",
    "get_latest_balance_snapshot",
    "get_latest_income_snapshot",
    "build_driver_stack",
    "determine_primary_type",
    "get_manifest_field_entry",
    "is_usable_status",
    "load_source_registry",
    "load_yaml_config",
    "manifest_field_status",
    "normalize_text",
    "now_ts",
    "resolve_sector_route",
    "safe_float",
    "score_underwrite_axis",
    "select_latest_record",
    "assess_survival_boundary",
]


def get_manifest_field_entry(source_manifest: dict[str, Any] | None, field_name: str) -> dict[str, Any]:
    if not source_manifest:
        return {}
    field_map = source_manifest.get("field_map", {})
    if isinstance(field_map, dict) and field_name in field_map:
        entry = field_map.get(field_name)
        return entry if isinstance(entry, dict) else {}

    fields = source_manifest.get("fields", [])
    if isinstance(fields, dict):
        entry = fields.get(field_name)
        return entry if isinstance(entry, dict) else {}

    for entry in fields:
        if isinstance(entry, dict) and entry.get("field_name") == field_name:
            return entry
    return {}


def manifest_field_status(source_manifest: dict[str, Any] | None, field_name: str) -> str:
    return normalize_text(get_manifest_field_entry(source_manifest, field_name).get("status", "missing")).lower()


def is_usable_status(status: str) -> bool:
    normalized = normalize_text(status).lower()
    return normalized.startswith("ok") or normalized in {"collected", "verified_tier0"}


def __getattr__(name: str) -> Any:
    if name in _DEPRECATED_SYMBOL_MESSAGES:
        raise AttributeError(_DEPRECATED_SYMBOL_MESSAGES[name])
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
