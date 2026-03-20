"""Single-stock deep dive engine for the whole-market framework."""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any

import yaml


SKILLS_DIR = Path(__file__).resolve().parents[3]
SHARED_DIR = SKILLS_DIR / "shared"
sys.path.insert(0, str(SHARED_DIR))

from adapters.provider_router import run_full_scan  # noqa: E402
from adapters.cninfo_adapter import run_tier0_prep  # noqa: E402
from adapters.commodity_adapter import run_commodity_scan  # noqa: E402
from adapters.docling_page_adapter import run_docling_page_parse  # noqa: E402
from adapters.stats_gov_adapter import run_macro_scan  # noqa: E402
from adapters.tier0_pdf_adapter import run_pdf_index  # noqa: E402
from adapters.tier0_report_pack_adapter import download_tier0_report_pack  # noqa: E402
from engines.report_engine import generate_deep_dive_report  # noqa: E402
from engines.synthesis_engine import build_investment_synthesis  # noqa: E402
from engines.valuation_engine import build_three_case_valuation  # noqa: E402
from utils.framework_utils import determine_opportunity_type, normalize_text  # noqa: E402
from utils.market_utils import infer_market_from_stock_code  # noqa: E402
from utils.source_lineage import summarize_scan_data_lineage  # noqa: E402
from utils.runtime_paths import resolve_base_dir, stock_paths  # noqa: E402
from validators.tier0_autofill import run_tier0_autofill  # noqa: E402
from validators.tier0_verifier import run_tier0_verification  # noqa: E402
from validators.universal_gate import evaluate_universal_gates  # noqa: E402


BASE_DIR = resolve_base_dir()
with open(SKILLS_DIR / "single-stock-deep-dive" / "config" / "deep_dive_defaults.yaml", "r", encoding="utf-8") as handle:
    DEFAULTS = (yaml.safe_load(handle) or {}).get("defaults", {})


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _wrap_step_result(result: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    data = result if isinstance(result, dict) else fallback
    raw_status = normalize_text(data.get("status")) if isinstance(data, dict) else ""
    raw_status_lower = raw_status.lower()
    error_text = normalize_text((data or {}).get("error")) if isinstance(data, dict) else ""
    if raw_status_lower.startswith("ok"):
        status = "ok"
    elif raw_status_lower.startswith("partial"):
        status = "partial"
    elif raw_status_lower.startswith("skipped"):
        status = "skipped"
    elif raw_status_lower.startswith("error") or error_text:
        status = "error"
    elif data:
        status = "ok"
    else:
        status = "empty"
    return {
        "status": status,
        "raw_status": raw_status or None,
        "data": data,
        "error": error_text or None,
    }


def _step_ok(step_result: dict[str, Any]) -> bool:
    return str(step_result.get("status") or "") == "ok"


def _step_data(step_result: dict[str, Any]) -> dict[str, Any]:
    data = step_result.get("data")
    return data if isinstance(data, dict) else {}


def deep_sniper(
    stock_code: str,
    company_name: str,
    *,
    include_tier0: bool | None = None,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    include_tier0 = DEFAULTS.get("include_tier0", True) if include_tier0 is None else include_tier0
    resolved_base_dir = (
        resolve_base_dir(base_dir)
        if base_dir is not None or os.getenv("A_STOCK_BASE")
        else BASE_DIR
    )
    paths = stock_paths(resolved_base_dir, stock_code)
    raw_dir = paths["raw_dir"]
    processed_dir = paths["processed_dir"]
    evidence_dir = paths["evidence_dir"]
    report_dir = paths["report_dir"]
    for directory in (raw_dir, processed_dir, evidence_dir, report_dir):
        directory.mkdir(parents=True, exist_ok=True)

    execution_log: dict[str, Any] = {
        "stock_code": stock_code,
        "company_name": company_name,
        "started_at": datetime.datetime.now().isoformat(),
        "steps": [],
    }

    def run_step(step_name: str, runner, fallback: dict[str, Any]) -> dict[str, Any]:
        execution_log["steps"].append({"step": step_name, "status": "running"})
        try:
            wrapped = _wrap_step_result(runner(), fallback)
            execution_log["steps"][-1]["status"] = wrapped["status"]
            if wrapped.get("raw_status"):
                execution_log["steps"][-1]["raw_status"] = wrapped["raw_status"]
            if wrapped.get("error"):
                execution_log["steps"][-1]["error"] = wrapped["error"]
            _save_json(processed_dir / "execution_log.json", execution_log)
            return wrapped
        except Exception as exc:  # noqa: BLE001
            execution_log["steps"][-1]["status"] = "failed"
            execution_log["steps"][-1]["error"] = str(exc)
            execution_log["steps"][-1]["traceback"] = traceback.format_exc()
            _save_json(processed_dir / "execution_log.json", execution_log)
            return {
                "status": "error",
                "raw_status": None,
                "data": fallback,
                "error": str(exc),
            }

    scan_step = run_step("tier1_scan", lambda: run_full_scan(stock_code, str(raw_dir)), {})
    scan_data = _step_data(scan_step)
    opportunity = determine_opportunity_type(
        stock_code,
        scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=scan_data.get("revenue_breakdown", {}).get("data", []),
        extra_texts=[company_name],
    )

    tier0_prep_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    report_pack_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    pdf_index_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    tier0_autofill_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    tier0_verification_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    if include_tier0:
        tier0_prep_step = run_step("tier0_prep", lambda: run_tier0_prep(stock_code, company_name, str(evidence_dir)), {})
        report_pack_step = run_step("tier0_report_pack", lambda: download_tier0_report_pack(stock_code, company_name, str(evidence_dir)), {})
        pdf_index_step = run_step("tier0_pdf_index", lambda: run_pdf_index(stock_code, str(evidence_dir)), {})
        run_step(
            "docling_pages",
            lambda: run_docling_page_parse(stock_code, str(evidence_dir), pdf_index_result=_step_data(pdf_index_step)),
            {},
        )
        if _step_ok(tier0_prep_step) and _step_ok(pdf_index_step):
            tier0_autofill_step = run_step(
                "tier0_autofill",
                lambda: run_tier0_autofill(
                    stock_code,
                    _step_data(tier0_prep_step),
                    scan_data,
                    _step_data(pdf_index_step),
                    str(evidence_dir),
                ),
                {},
            )
        if _step_ok(tier0_prep_step) and _step_ok(tier0_autofill_step) and _step_ok(pdf_index_step) and _step_ok(report_pack_step):
            tier0_verification_step = run_step(
                "tier0_verification",
                lambda: run_tier0_verification(
                    stock_code,
                    _step_data(tier0_prep_step),
                    _step_data(tier0_autofill_step),
                    _step_data(pdf_index_step),
                    _step_data(report_pack_step),
                    str(evidence_dir),
                ),
                {},
            )

    commodity_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    macro_step = {"status": "empty", "raw_status": None, "data": {}, "error": None}
    if opportunity.get("primary_type") == "cyclical":
        commodity_name = opportunity.get("industry_text") or normalize_text(scan_data.get("company_profile", {}).get("data", {}).get("行业")) or "纯碱"
        commodity_step = run_step("commodity_scan", lambda: run_commodity_scan(commodity_name, str(raw_dir)), {})
        macro_step = run_step("macro_scan", lambda: run_macro_scan(str(raw_dir), normalize_text(opportunity.get("industry_text"))), {})

    gate_result = evaluate_universal_gates(
        stock_code,
        scan_data,
        opportunity_context=opportunity,
        extra_texts=[company_name, json.dumps(_step_data(tier0_verification_step), ensure_ascii=False, default=str)],
    )
    valuation_result = build_three_case_valuation(stock_code, scan_data, gate_result.get("driver_stack", opportunity))
    synthesis_result = build_investment_synthesis(stock_code, company_name, gate_result, valuation_result)
    report_result = generate_deep_dive_report(
        stock_code,
        company_name,
        market=infer_market_from_stock_code(stock_code) or DEFAULTS.get("market", "A-share"),
        scan_data=scan_data,
        gate_result=gate_result,
        valuation_result=valuation_result,
        synthesis_result=synthesis_result,
        report_dir=str(report_dir),
    )

    payload = {
        "stock_code": stock_code,
        "company_name": company_name,
        "opportunity": opportunity,
        "data_lineage": summarize_scan_data_lineage(scan_data),
        "driver_stack": gate_result.get("driver_stack"),
        "underwrite_axis": gate_result.get("underwrite_axis"),
        "realization_axis": gate_result.get("realization_axis"),
        "position_state": gate_result.get("position_state"),
        "prev_state": gate_result.get("prev_state"),
        "transition_reason": gate_result.get("transition_reason"),
        "flow_stage": gate_result.get("flow_stage"),
        "gate_result": gate_result,
        "valuation_result": valuation_result,
        "synthesis_result": synthesis_result,
        "step_statuses": {
            "tier1_scan": scan_step.get("status"),
            "tier0_prep": tier0_prep_step.get("status"),
            "tier0_report_pack": report_pack_step.get("status"),
            "tier0_pdf_index": pdf_index_step.get("status"),
            "tier0_autofill": tier0_autofill_step.get("status"),
            "tier0_verification": tier0_verification_step.get("status"),
            "commodity_scan": commodity_step.get("status"),
            "macro_scan": macro_step.get("status"),
        },
        "tier0_prep": _step_data(tier0_prep_step),
        "tier0_autofill": _step_data(tier0_autofill_step),
        "tier0_verification": _step_data(tier0_verification_step),
        "commodity_data": _step_data(commodity_step),
        "macro_data": _step_data(macro_step),
        "report_path": report_result["report_path"],
    }
    _save_json(processed_dir / "deep_dive_result.json", payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single-stock deep dive.")
    parser.add_argument("stock_code")
    parser.add_argument("company_name")
    parser.add_argument("--skip-tier0", action="store_true")
    parser.add_argument("--base-dir", type=Path, default=None)
    args = parser.parse_args()
    result = deep_sniper(
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
