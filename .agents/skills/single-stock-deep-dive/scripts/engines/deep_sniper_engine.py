"""Single-stock deep dive engine for the whole-market framework."""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path
from typing import Any

import yaml


SKILLS_DIR = Path(__file__).resolve().parents[3]
SHARED_DIR = SKILLS_DIR / "shared"
sys.path.insert(0, str(SHARED_DIR))

from adapters.akshare_adapter import run_full_scan  # noqa: E402
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
from validators.tier0_autofill import run_tier0_autofill  # noqa: E402
from validators.tier0_verifier import run_tier0_verification  # noqa: E402
from validators.universal_gate import evaluate_universal_gates  # noqa: E402


BASE_DIR = Path(__file__).resolve().parents[5]
with open(SKILLS_DIR / "single-stock-deep-dive" / "config" / "deep_dive_defaults.yaml", "r", encoding="utf-8") as handle:
    DEFAULTS = (yaml.safe_load(handle) or {}).get("defaults", {})


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def deep_sniper(stock_code: str, company_name: str, *, include_tier0: bool | None = None) -> dict[str, Any]:
    include_tier0 = DEFAULTS.get("include_tier0", True) if include_tier0 is None else include_tier0
    raw_dir = BASE_DIR / "data" / "raw" / stock_code
    processed_dir = BASE_DIR / "data" / "processed" / stock_code
    evidence_dir = BASE_DIR / "evidence" / stock_code
    report_dir = BASE_DIR / "reports"
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
            result = runner()
            execution_log["steps"][-1]["status"] = "done"
            _save_json(processed_dir / "execution_log.json", execution_log)
            return result if isinstance(result, dict) else fallback
        except Exception as exc:  # noqa: BLE001
            execution_log["steps"][-1]["status"] = "failed"
            execution_log["steps"][-1]["error"] = str(exc)
            _save_json(processed_dir / "execution_log.json", execution_log)
            return fallback

    scan_data = run_step("tier1_scan", lambda: run_full_scan(stock_code, str(raw_dir)), {})
    opportunity = determine_opportunity_type(
        stock_code,
        scan_data.get("company_profile", {}).get("data", {}),
        revenue_records=scan_data.get("revenue_breakdown", {}).get("data", []),
        extra_texts=[company_name],
    )

    tier0_prep: dict[str, Any] = {}
    report_pack: dict[str, Any] = {}
    pdf_index: dict[str, Any] = {}
    tier0_autofill: dict[str, Any] = {}
    tier0_verification: dict[str, Any] = {}
    if include_tier0:
        tier0_prep = run_step("tier0_prep", lambda: run_tier0_prep(stock_code, company_name, str(evidence_dir)), {})
        report_pack = run_step("tier0_report_pack", lambda: download_tier0_report_pack(stock_code, company_name, str(evidence_dir)), {})
        pdf_index = run_step("tier0_pdf_index", lambda: run_pdf_index(stock_code, str(evidence_dir)), {})
        run_step("docling_pages", lambda: run_docling_page_parse(stock_code, str(evidence_dir), pdf_index_result=pdf_index), {})
        if tier0_prep and pdf_index:
            tier0_autofill = run_step(
                "tier0_autofill",
                lambda: run_tier0_autofill(stock_code, tier0_prep, scan_data, pdf_index, str(evidence_dir)),
                {},
            )
        if tier0_prep and tier0_autofill and pdf_index and report_pack:
            tier0_verification = run_step(
                "tier0_verification",
                lambda: run_tier0_verification(stock_code, tier0_prep, tier0_autofill, pdf_index, report_pack, str(evidence_dir)),
                {},
            )

    commodity_data: dict[str, Any] = {}
    macro_data: dict[str, Any] = {}
    if opportunity.get("primary_type") == "cyclical":
        commodity_name = opportunity.get("industry_text") or normalize_text(scan_data.get("company_profile", {}).get("data", {}).get("行业")) or "纯碱"
        commodity_data = run_step("commodity_scan", lambda: run_commodity_scan(commodity_name, str(raw_dir)), {})
        macro_data = run_step("macro_scan", lambda: run_macro_scan(str(raw_dir), normalize_text(opportunity.get("industry_text"))), {})

    gate_result = evaluate_universal_gates(
        stock_code,
        scan_data,
        opportunity_context=opportunity,
        extra_texts=[company_name, json.dumps(tier0_verification, ensure_ascii=False, default=str)],
    )
    valuation_result = build_three_case_valuation(stock_code, scan_data, opportunity)
    synthesis_result = build_investment_synthesis(stock_code, company_name, gate_result, valuation_result)
    report_result = generate_deep_dive_report(
        stock_code,
        company_name,
        market=DEFAULTS.get("market", "A-share"),
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
        "gate_result": gate_result,
        "valuation_result": valuation_result,
        "synthesis_result": synthesis_result,
        "tier0_prep": tier0_prep,
        "tier0_autofill": tier0_autofill,
        "tier0_verification": tier0_verification,
        "commodity_data": commodity_data,
        "macro_data": macro_data,
        "report_path": report_result["report_path"],
    }
    _save_json(processed_dir / "deep_dive_result.json", payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single-stock deep dive.")
    parser.add_argument("stock_code")
    parser.add_argument("company_name")
    parser.add_argument("--skip-tier0", action="store_true")
    args = parser.parse_args()
    result = deep_sniper(args.stock_code, args.company_name, include_tier0=not args.skip_tier0)
    print(json.dumps({"report_path": result["report_path"], "verdict": result["gate_result"]["scorecard"]["verdict"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
