"""Shared valuation case configuration helpers."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from utils.value_utils import normalize_text, safe_float


DEFAULT_ROUTE_CASE_OVERRIDES: dict[str, dict[str, dict[str, Any]]] = {
    "default": {
        "floor": {
            "valuation_method": "conservative_book",
            "assumption": "book-value fallback",
            "preferred_anchor": "equity",
            "equity_multiple": 0.75,
            "profit_multiple": 8.0,
        },
        "normalized": {
            "assumption": "conservative normalized earnings",
            "preferred_anchor": "profit",
            "profit_multiple": 8.0,
            "equity_multiple": 0.90,
        },
        "recognition": {
            "valuation_method": "recognition_multiple",
            "assumption": "sentiment-driven upside",
            "preferred_anchor": "normalized",
            "normalized_multiple": 1.25,
            "profit_multiple": None,
        },
    },
    "core_resource": {
        "floor": {
            "valuation_method": "stressed_book",
            "assumption": "commodity stress persists",
            "preferred_anchor": "equity",
            "equity_multiple": 0.85,
        },
        "normalized": {
            "assumption": "mid-cycle resource earnings",
            "preferred_anchor": "profit",
            "profit_multiple": 9.0,
            "equity_multiple": 1.00,
        },
        "recognition": {
            "valuation_method": "peak_cycle_multiple",
            "assumption": "pricing and sentiment overshoot",
            "preferred_anchor": "profit",
            "profit_multiple": 12.0,
            "normalized_multiple": 1.25,
        },
    },
    "rigid_shovel": {
        "floor": {
            "valuation_method": "replacement_cost_proxy",
            "assumption": "capex remains soft",
            "preferred_anchor": "equity",
            "equity_multiple": 0.80,
        },
        "normalized": {
            "assumption": "mid-cycle capex demand",
            "preferred_anchor": "profit",
            "profit_multiple": 10.0,
            "equity_multiple": 1.05,
        },
        "recognition": {
            "valuation_method": "peak_capex_multiple",
            "assumption": "order boom and rerating",
            "preferred_anchor": "profit",
            "profit_multiple": 13.0,
            "normalized_multiple": 1.25,
        },
    },
    "core_military": {
        "normalized": {
            "assumption": "program margins normalize",
            "preferred_anchor": "profit",
            "profit_multiple": 16.0,
            "equity_multiple": 1.20,
        },
        "recognition": {
            "valuation_method": "recognition_multiple",
            "assumption": "program certainty premium",
            "preferred_anchor": "profit",
            "profit_multiple": 20.0,
            "normalized_multiple": 1.35,
        },
    },
    "consumer": {
        "floor": {
            "valuation_method": "no_growth_owner_earnings",
            "assumption": "no-growth downside anchor",
            "preferred_anchor": "profit",
            "profit_multiple": 8.0,
            "equity_multiple": 0.75,
        },
        "normalized": {
            "assumption": "owner earnings normalize",
            "preferred_anchor": "profit",
            "profit_multiple": 15.0,
            "equity_multiple": 1.30,
        },
        "recognition": {
            "valuation_method": "recognition_multiple",
            "assumption": "quality rerating and momentum",
            "preferred_anchor": "normalized",
            "normalized_multiple": 1.35,
            "profit_multiple": 20.0,
        },
    },
    "tech": {
        "floor": {
            "valuation_method": "no_growth_owner_earnings",
            "assumption": "no-growth downside anchor",
            "preferred_anchor": "profit",
            "profit_multiple": 8.0,
            "equity_multiple": 0.75,
        },
        "normalized": {
            "assumption": "demand and mix normalize",
            "preferred_anchor": "profit",
            "profit_multiple": 18.0,
            "equity_multiple": 1.40,
        },
        "recognition": {
            "valuation_method": "recognition_multiple",
            "assumption": "quality rerating and momentum",
            "preferred_anchor": "normalized",
            "normalized_multiple": 1.35,
            "profit_multiple": 20.0,
        },
    },
    "financial_asset": {
        "floor": {
            "valuation_method": "stressed_nav",
            "assumption": "asset discount persists",
            "preferred_anchor": "equity",
            "equity_multiple": 0.90,
        },
        "normalized": {
            "assumption": "mid-cycle ROE on current equity",
            "preferred_anchor": "equity",
            "equity_multiple": 1.10,
        },
        "recognition": {
            "valuation_method": "recognition_multiple",
            "assumption": "discount closes",
            "preferred_anchor": "normalized",
            "normalized_multiple": 1.20,
        },
    },
}

DEFAULT_FLOOR_PROTECTION_SCORE_BANDS: list[tuple[float, float]] = [
    (0.60, 20),
    (0.75, 45),
    (0.85, 65),
    (1.00, 85),
    (1.20, 100),
]

DEFAULT_NORMALIZED_VALUE_RATIO_BANDS: list[tuple[float, float]] = [
    (0.80, 30),
    (1.00, 50),
    (1.20, 65),
    (1.50, 85),
    (2.00, 100),
]


def _deep_merge(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def resolve_route_case_overrides(discipline: dict[str, Any], sector_route: str) -> dict[str, dict[str, Any]]:
    route_key = normalize_text(sector_route).lower() or "unknown"
    config_overrides = discipline.get("route_case_overrides", {}) or {}
    merged = _deep_merge(DEFAULT_ROUTE_CASE_OVERRIDES.get("default", {}), DEFAULT_ROUTE_CASE_OVERRIDES.get(route_key, {}))
    merged = _deep_merge(merged, config_overrides.get("default", {}))
    merged = _deep_merge(merged, config_overrides.get(route_key, {}))
    return merged


def resolve_case_equity_value(
    case_cfg: dict[str, Any] | None,
    *,
    equity: float | None = None,
    profit: float | None = None,
    normalized: float | None = None,
) -> float | None:
    cfg = case_cfg or {}
    preferred_anchor = normalize_text(cfg.get("preferred_anchor")).lower() or "equity"
    anchor_values = {
        "equity": (
            safe_float(cfg.get("equity_multiple")),
            equity,
        ),
        "profit": (
            safe_float(cfg.get("profit_multiple")),
            profit,
        ),
        "normalized": (
            safe_float(cfg.get("normalized_multiple")),
            normalized,
        ),
    }
    anchor_order = [preferred_anchor] + [name for name in ("equity", "profit", "normalized") if name != preferred_anchor]
    for anchor_name in anchor_order:
        multiple, anchor_value = anchor_values.get(anchor_name, (None, None))
        if multiple is not None and anchor_value is not None:
            return anchor_value * multiple
    return None


def resolve_score_bands(
    discipline: dict[str, Any],
    key: str,
    default: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    raw_bands = discipline.get("score_bands", {}).get(key)
    bands: list[tuple[float, float]] = []
    for item in raw_bands or []:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            threshold = safe_float(item[0])
            score = safe_float(item[1])
        elif isinstance(item, dict):
            threshold = safe_float(item.get("threshold"))
            score = safe_float(item.get("score"))
        else:
            threshold = None
            score = None
        if threshold is None or score is None:
            continue
        bands.append((threshold, score))
    return bands or list(default)
