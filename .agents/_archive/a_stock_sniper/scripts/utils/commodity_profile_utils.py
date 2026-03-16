"""Commodity/signal profile helpers loaded from YAML config."""

from __future__ import annotations

from typing import Any

from utils.research_utils import load_yaml_config, normalize_text


def load_commodity_profiles() -> dict[str, Any]:
    return load_yaml_config("commodity_profiles.yaml")


def _profiles_dict() -> dict[str, dict[str, Any]]:
    return load_commodity_profiles().get("profiles", {}) or {}


def _normalized_aliases(profile_name: str, profile: dict[str, Any]) -> list[str]:
    aliases = [profile_name]
    aliases.extend(profile.get("aliases", []) or [])
    ordered: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        text = normalize_text(alias)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def resolve_signal_profile(commodity_name: str, extra_texts: list[str] | None = None) -> dict[str, Any]:
    profiles = _profiles_dict()
    requested = normalize_text(commodity_name)
    texts = [requested]
    texts.extend(normalize_text(item) for item in (extra_texts or []))
    texts = [item for item in texts if item]

    for profile_name, profile in profiles.items():
        aliases = _normalized_aliases(profile_name, profile)
        if requested and requested in aliases:
            return {"name": profile_name, **profile}

    haystack = " ".join(texts)
    scored: list[tuple[int, str, dict[str, Any]]] = []
    for profile_name, profile in profiles.items():
        aliases = _normalized_aliases(profile_name, profile)
        best = 0
        for alias in aliases:
            if alias and alias in haystack:
                best = max(best, len(alias))
        if best > 0:
            scored.append((best, profile_name, profile))
    if scored:
        scored.sort(reverse=True)
        _, profile_name, profile = scored[0]
        return {"name": profile_name, **profile}

    return {
        "name": requested,
        "aliases": [requested] if requested else [],
        "spot_symbols": [requested] if requested else [],
        "social_inventory": {
            "enabled": True,
            "primary": "行业协会 / 生意社",
            "url": "https://www.100ppi.com/",
            "frequency": "周度",
            "fields": ["库存量"],
        },
    }


def build_profile_maps() -> dict[str, dict[str, Any]]:
    futures_symbol_map: dict[str, str] = {}
    exchange_inventory_symbol_map: dict[str, str] = {}
    tqsdk_symbol_map: dict[str, str] = {}
    social_inventory_map: dict[str, dict[str, Any]] = {}

    for profile_name, profile in _profiles_dict().items():
        futures_symbol = normalize_text(profile.get("futures_symbol"))
        exchange_symbol = normalize_text(profile.get("exchange_inventory_symbol"))
        tqsdk_symbol = normalize_text(profile.get("tqsdk_symbol"))
        social_inventory = profile.get("social_inventory", {}) or {}
        aliases = _normalized_aliases(profile_name, profile)

        for alias in aliases:
            if futures_symbol:
                futures_symbol_map[alias] = futures_symbol
            if exchange_symbol:
                exchange_inventory_symbol_map[alias] = exchange_symbol
            if tqsdk_symbol:
                tqsdk_symbol_map[alias] = tqsdk_symbol
            if social_inventory:
                social_inventory_map[alias] = social_inventory

    return {
        "futures_symbol_map": futures_symbol_map,
        "exchange_inventory_symbol_map": exchange_inventory_symbol_map,
        "tqsdk_symbol_map": tqsdk_symbol_map,
        "social_inventory_map": social_inventory_map,
    }


def build_industry_fai_map() -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for profile_name, profile in _profiles_dict().items():
        labels = [normalize_text(item) for item in (profile.get("industry_fai_labels", []) or []) if normalize_text(item)]
        if not labels:
            continue
        aliases = _normalized_aliases(profile_name, profile)
        for alias in aliases:
            result[alias] = labels
    return result
