"""Signal-health helpers aligned to the whole-market opportunity framework."""

from __future__ import annotations

from typing import Any

from utils.research_utils import is_usable_status, manifest_field_status, normalize_text


LEGACY_MODE_MAP = {
    "resource_body": "cyclical",
    "shovel_play": "cyclical",
    "military": "special_situation",
}

SIGNAL_LAYOUTS = {
    "compounder": (["price_signal", "pb_signal"], ["inventory_signal", "capex_signal"]),
    "cyclical": (["price_signal", "inventory_signal", "capex_signal"], ["pb_signal"]),
    "turnaround": (["price_signal", "pb_signal"], ["inventory_signal", "capex_signal"]),
    "asset_play": (["pb_signal", "price_signal"], ["inventory_signal", "capex_signal"]),
    "special_situation": (["price_signal", "pb_signal"], ["inventory_signal", "capex_signal"]),
}


def _resolve_signal_mode(opportunity_context: dict[str, Any]) -> str:
    primary_type = normalize_text(
        opportunity_context.get("primary_type") or opportunity_context.get("opportunity_type")
    ).lower()
    if primary_type:
        return primary_type

    legacy_mode = normalize_text(opportunity_context.get("four_signal_mode")).lower()
    if legacy_mode in LEGACY_MODE_MAP:
        return LEGACY_MODE_MAP[legacy_mode]

    return legacy_mode or "unknown"


def _signal_layout(mode: str) -> tuple[list[str], list[str]]:
    return SIGNAL_LAYOUTS.get(mode, (["price_signal", "capex_signal"], ["inventory_signal", "pb_signal"]))


def evaluate_signal_health_v2(
    eco_context: dict[str, Any] | None,
    source_manifest: dict[str, Any] | None,
    commodity_data: dict[str, Any] | None,
    macro_data: dict[str, Any] | None,
) -> dict[str, Any]:
    context = eco_context or {}
    source_manifest = source_manifest or {}
    commodity_data = commodity_data or {}
    macro_data = macro_data or {}
    mode = _resolve_signal_mode(context)

    spot_status = manifest_field_status(source_manifest, "spot_price")
    futures_status = normalize_text(commodity_data.get("futures", {}).get("status", "")).lower()
    inventory_status = manifest_field_status(source_manifest, "industry_inventory")
    inventory_bundle = commodity_data.get("inventory", {}).get("data", {})
    inventory_coverage = normalize_text(inventory_bundle.get("coverage")).lower()
    exchange_inventory_status = normalize_text(commodity_data.get("exchange_inventory", {}).get("status", "")).lower()
    social_inventory_status = normalize_text(commodity_data.get("social_inventory", {}).get("status", "")).lower()
    pb_status = manifest_field_status(source_manifest, "pb_ratio")
    industry_fai_status = normalize_text(macro_data.get("industry_fai", {}).get("status", "")).lower()
    capex_status = manifest_field_status(source_manifest, "capex_investment")

    price_ready = (
        is_usable_status(spot_status)
        or spot_status.startswith("partial")
        or is_usable_status(futures_status)
        or futures_status.startswith("partial")
    )
    inventory_ready = (
        is_usable_status(inventory_status)
        or inventory_coverage in {"exchange_only", "exchange_and_social", "not_applicable"}
        or exchange_inventory_status.startswith("not_applicable")
    )
    pb_ready = is_usable_status(pb_status)
    capex_ready = is_usable_status(industry_fai_status) or is_usable_status(capex_status)

    inventory_detail_parts = [inventory_status or "missing"]
    if inventory_coverage:
        inventory_detail_parts.append(f"coverage={inventory_coverage}")
    if exchange_inventory_status:
        inventory_detail_parts.append(f"exchange={exchange_inventory_status}")
    if social_inventory_status:
        inventory_detail_parts.append(f"social={social_inventory_status}")

    signal_items = {
        "price_signal": {
            "label": "Spot / futures price",
            "status": spot_status or futures_status or "missing",
            "ready": price_ready,
            "detail": f"spot={spot_status or 'missing'}; futures={futures_status or 'missing'}",
        },
        "inventory_signal": {
            "label": "Industry inventory",
            "status": inventory_status or "missing",
            "ready": inventory_ready,
            "detail": "; ".join(inventory_detail_parts),
        },
        "capex_signal": {
            "label": "Industry / project capex",
            "status": industry_fai_status or capex_status or "missing",
            "ready": capex_ready,
            "detail": f"industry_fai={industry_fai_status or 'missing'}; manifest_capex={capex_status or 'missing'}",
        },
        "pb_signal": {
            "label": "Valuation signal",
            "status": pb_status or "missing",
            "ready": pb_ready,
            "detail": pb_status or "missing",
        },
    }

    core_names, auxiliary_names = _signal_layout(mode)
    core_missing = [name for name in core_names if not signal_items[name]["ready"]]
    auxiliary_missing = [name for name in auxiliary_names if not signal_items[name]["ready"]]
    stale_fields = source_manifest.get("summary", {}).get("stale_fields", [])
    coverage_warnings: list[str] = []
    if mode == "cyclical":
        if inventory_coverage == "exchange_only":
            coverage_warnings.append("inventory_exchange_only")
        elif inventory_coverage == "social_only":
            coverage_warnings.append("inventory_social_only")

    return {
        "mode": mode,
        "signals": signal_items,
        "core_names": core_names,
        "auxiliary_names": auxiliary_names,
        "core_missing": core_missing,
        "auxiliary_missing": auxiliary_missing,
        "core_ready": len(core_missing) == 0,
        "auxiliary_ready": len(auxiliary_missing) == 0,
        "coverage_warnings": coverage_warnings,
        "has_stale_signal": any(
            name in " ".join(stale_fields) for name in ("spot", "inventory", "fixed_asset_investment", "industry_fai")
        ),
        "stale_fields": stale_fields,
    }
