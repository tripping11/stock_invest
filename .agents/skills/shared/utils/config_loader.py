"""YAML configuration loaders for the investment framework."""
from __future__ import annotations

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
