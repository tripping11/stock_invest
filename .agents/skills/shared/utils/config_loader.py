"""YAML configuration loaders for the investment framework."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


SHARED_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = SHARED_DIR / "config"


def load_yaml_config(filename: str) -> dict[str, Any]:
    path = CONFIG_DIR / filename
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_scoring_rules() -> dict[str, Any]:
    return load_yaml_config("scoring_rules.yaml")


def load_valuation_discipline() -> dict[str, Any]:
    return load_yaml_config("valuation_discipline.yaml")


def load_sector_classification() -> dict[str, Any]:
    return load_yaml_config("sector_classification.yaml")


def load_moat_dictionary() -> dict[str, Any]:
    return load_yaml_config("moat_dictionary.yaml")


def load_source_registry() -> dict[str, Any]:
    return load_yaml_config("source_registry.yaml")


def load_vcrf_weights() -> dict[str, Any]:
    return load_yaml_config("vcrf_weights.yaml")


def load_vcrf_state_machine() -> dict[str, Any]:
    return load_yaml_config("vcrf_state_machine.yaml")


def load_vcrf_degradation() -> dict[str, Any]:
    return load_yaml_config("vcrf_degradation.yaml")


def _assert_axis_normalized(weights: dict[str, float], *, tolerance: float) -> None:
    total = sum(float(value) for value in weights.values())
    if abs(total - 1.0) > tolerance:
        raise ValueError(f"VCRF axis weights must sum to 1.0, got {total:.6f}")


def _apply_axis_overrides(
    base_axis: dict[str, Any],
    overlay_axis: dict[str, Any] | None,
) -> dict[str, float]:
    merged = {key: float(value) for key, value in base_axis.items()}
    for key, delta in (overlay_axis or {}).items():
        merged[key] = float(merged.get(key, 0.0)) + float(delta)
    return merged


def resolve_vcrf_weight_template(primary_type: str, sector_route: str) -> dict[str, dict[str, float]]:
    config = load_vcrf_weights()
    meta = config.get("_meta", {})
    tolerance = float(meta.get("tolerance", 0.001))
    templates = config.get("base_templates", {})
    if primary_type not in templates:
        raise KeyError(f"Unknown VCRF primary_type: {primary_type}")

    template = deepcopy(templates[primary_type])
    overrides = config.get("sector_overrides", {}).get(sector_route, {})
    underwrite = _apply_axis_overrides(template.get("underwrite", {}), overrides.get("underwrite"))
    realization = _apply_axis_overrides(template.get("realization", {}), overrides.get("realization"))

    if meta.get("enforce_normalization", True):
        _assert_axis_normalized(underwrite, tolerance=tolerance)
        _assert_axis_normalized(realization, tolerance=tolerance)

    return {
        "underwrite": underwrite,
        "realization": realization,
    }
