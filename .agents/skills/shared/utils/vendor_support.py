"""Helpers for loading vendored GitHub components safely."""
from __future__ import annotations

import os
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SCRIPTS_DIR.parents[3]
DEFAULT_VENDOR_DIR = PROJECT_ROOT / ".vendor"

def _env_vendor_override(package_name: str) -> Path | None:
    """Check for a VENDOR_{PACKAGE}_PATH environment variable override."""
    env_key = f"VENDOR_{package_name.upper()}_PATH"
    raw = os.getenv(env_key, "").strip()
    return Path(raw) if raw else None


def ensure_vendor_path(package_name: str) -> bool:
    """Add a vendored package directory to sys.path if it exists."""
    candidates = []
    env_override = _env_vendor_override(package_name)
    if env_override:
        candidates.append(env_override)
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
