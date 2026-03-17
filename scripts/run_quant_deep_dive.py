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
    / "single-stock-deep-dive"
    / "scripts"
    / "engines"
    / "deep_sniper_engine.py"
)


def _load_engine():
    spec = importlib.util.spec_from_file_location("quant_deep_dive_engine", ENGINE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load deep dive engine: {ENGINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 量化选股 deep dive.")
    parser.add_argument("stock_code")
    parser.add_argument("company_name")
    parser.add_argument("--skip-tier0", action="store_true")
    parser.add_argument("--base-dir", type=Path, default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    engine = _load_engine()
    result = engine.deep_sniper(
        args.stock_code,
        args.company_name,
        include_tier0=not args.skip_tier0,
        base_dir=args.base_dir,
    )
    print(
        json.dumps(
            {
                "report_path": result["report_path"],
                "verdict": result["gate_result"]["scorecard"]["verdict"],
                "position_state": result.get("position_state"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
