#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
ENGINE_PATH = (
    REPO_ROOT
    / ".agents"
    / "skills"
    / "market-opportunity-scanner"
    / "scripts"
    / "engines"
    / "radar_scan_engine.py"
)


def _load_engine():
    spec = importlib.util.spec_from_file_location("quant_scan_engine", ENGINE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load market scan engine: {ENGINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 量化选股 market scan.")
    parser.add_argument("scope", nargs="?", default="A-share")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--base-dir", type=Path, default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    engine = _load_engine()
    result = engine.run_radar_scan(args.scope, args.limit, base_dir=args.base_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
