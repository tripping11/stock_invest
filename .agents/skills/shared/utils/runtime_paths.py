"""Pure path helpers for runtime entrypoints.

This module only resolves and composes paths. It must not create directories,
perform I/O, or log side effects.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


UTILS_DIR = Path(__file__).resolve().parent
SHARED_DIR = UTILS_DIR.parent
SKILLS_DIR = SHARED_DIR.parent
AGENTS_DIR = SKILLS_DIR.parent
REPO_ROOT = AGENTS_DIR.parent


def _normalize_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return Path.cwd() / path


def resolve_base_dir(
    cli_base_dir: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    env_mapping = env if env is not None else os.environ
    if cli_base_dir:
        return _normalize_path(cli_base_dir)
    env_base_dir = (env_mapping.get("A_STOCK_BASE") or "").strip()
    if env_base_dir:
        return _normalize_path(env_base_dir)
    return REPO_ROOT


def stock_paths(base_dir: str | Path, stock_code: str) -> dict[str, Path]:
    root = _normalize_path(base_dir)
    normalized_code = str(stock_code).split(".", 1)[0].strip()
    return {
        "base_dir": root,
        "raw_dir": root / "data" / "raw" / normalized_code,
        "processed_dir": root / "data" / "processed" / normalized_code,
        "evidence_dir": root / "evidence" / normalized_code,
        "report_dir": root / "reports",
    }


def market_scan_paths(base_dir: str | Path) -> dict[str, Path]:
    root = _normalize_path(base_dir)
    return {
        "base_dir": root,
        "processed_dir": root / "data" / "processed" / "market_scan",
        "report_dir": root / "reports",
        "radar_cache_root": root / "data" / "processed" / "radar_cache",
    }
