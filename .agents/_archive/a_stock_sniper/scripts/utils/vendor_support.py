"""Helpers for loading vendored GitHub components safely."""
from __future__ import annotations

import os
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SCRIPTS_DIR.parents[3]
DEFAULT_VENDOR_DIR = PROJECT_ROOT / ".vendor"

ASCII_VENDOR_MAP = {
    "docling": Path("D:/vendor_docling"),
}


def ensure_vendor_path(package_name: str) -> bool:
    """Add a vendored package directory to sys.path if it exists."""
    candidates = []
    override = ASCII_VENDOR_MAP.get(package_name)
    if override:
        candidates.append(override)
    candidates.append(DEFAULT_VENDOR_DIR / package_name)

    for candidate in candidates:
        if candidate.exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return True
    return False


def get_vendor_env(var_name: str, default: str = "") -> str:
    return os.getenv(var_name, default).strip()
