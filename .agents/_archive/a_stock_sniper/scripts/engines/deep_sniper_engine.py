"""
deep_sniper_engine.py - 深度狙击引擎
串联 adapters → redcard_gate → validators → report 的完整流程。
"""
from __future__ import annotations

import datetime
import json
import os
import sys
from pathlib import Path
from typing import Any

# 将 scripts 目录加入 path
SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_DIR)

from adapters.akshare_adapter import run_full_scan
from adapters.cninfo_adapter import run_tier0_prep
from adapters.commodity_adapter import run_commodity_scan
from adapters.docling_page_adapter import run_docling_page_parse
from adapters.stats_gov_adapter import run_macro_scan
from adapters.tier0_report_pack_adapter import download_tier0_report_pack
from adapters.tier0_pdf_adapter import run_pdf_index
from engines.report_engine import build_scorecard_v2, generate_report
from engines.valuation_engine import estimate_current_ps
from utils.commodity_profile_utils import resolve_signal_profile
from utils.research_utils import (
    build_source_manifest,
    detect_data_freshness,
    determine_eco_context,
    extract_latest_revenue_terms,
    get_crocodile_mode_config,
    load_industry_mapping,
    load_source_registry,
    normalize_text,
    safe_float,
    status_to_plan_actual,
)
from validators.redcard_gate import run_redcard_gate
from validators.tier0_autofill import run_tier0_autofill
from validators.tier0_verifier import run_tier0_verification

BASE_DIR = os.environ.get("A_STOCK_BASE", str(Path(__file__).resolve().parents[5]))


def _save_log(log: dict[str, Any], output_dir: str) -> None:
    with open(os.path.join(output_dir, "execution_log.json"), "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2, default=str)


def _safe_console_text(value: Any) -> str:
    text = str(value)
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return text.encode(encoding, errors="replace").decode(encoding, errors="replace")


def _load_json_if_exists(path: str) -> dict[str, Any] | None:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _build_eco_hint_texts(scan_data: dict, company_name: str) -> list[str]:
    profile = scan_data.get("company_profile", {}).get("data", {}) if isinstance(scan_data.get("company_profile"), dict) else {}
    hints = [
        company_name,
        str(profile.get("股票简称") or profile.get("证券简称") or ""),
        str(profile.get("主营业务") or ""),
        str(profile.get("经营范围") or ""),
    ]
    hints.extend(extract_latest_revenue_terms(scan_data.get("revenue_breakdown", {}).get("data", []), limit=12))
    return [item for item in dict.fromkeys(text.strip() for text in hints if str(text).strip())]


def _collect_human_actions(
    scan_data: dict,
    commodity_data: dict,
    macro_data: dict,
    source_manifest: dict,
) -> list[dict]:
    """Aggregate all human_action_needed items from every data source."""
    actions: list[dict] = []

    for source_name, dataset in [("commodity", commodity_data), ("macro", macro_data)]:
        for item in dataset.get("_human_actions_summary", []):
            merged = dict(item)
            merged["source"] = source_name
            actions.append(merged)

    for field_name, result in scan_data.items():
        if field_name.startswith("_") or not isinstance(result, dict):
            continue
        status = str(result.get("status", "")).lower()
        if "stale" in status:
            actions.append(
                {
                    "source": "akshare",
                    "field": field_name,
                    "action": f"刷新过期数据: {field_name}",
                    "priority": "yellow",
                    "detail": result.get("freshness_warning", ""),
                }
            )
        if "column_mismatch" in status:
            actions.append(
                {
                    "source": "akshare",
                    "field": field_name,
                    "action": f"列名不匹配需检查 akshare 版本: {field_name}",
                    "priority": "red",
                    "detail": str(result.get("column_mismatch", {})),
                }
            )

    for stale_field in source_manifest.get("summary", {}).get("stale_fields", []):
        if not any(action.get("field") == stale_field for action in actions):
            actions.append(
                {
                    "source": "manifest",
                    "field": stale_field,
                    "action": f"manifest 标记的过期数据: {stale_field}",
                    "priority": "yellow",
                }
            )

    return actions


def deep_sniper(stock_code: str, company_name: str, *, resume: bool = False) -> dict:
    """执行完整深度狙击流程。"""

    raw_dir = f"{BASE_DIR}/data/raw/{stock_code}"
    processed_dir = f"{BASE_DIR}/data/processed/{stock_code}"
    evidence_dir = f"{BASE_DIR}/evidence/{stock_code}"
    report_dir = f"{BASE_DIR}/reports"

    for directory in [raw_dir, processed_dir, evidence_dir, report_dir]:
        os.makedirs(directory, exist_ok=True)

    execution_log: dict[str, Any] = {
        "stock_code": stock_code,
        "company_name": company_name,
        "resume_mode": resume,
        "start_time": datetime.datetime.now().isoformat(),
        "steps": [],
        "step_failures": [],
        "plan_vs_actual": [],
    }

    def log_step(step_name: str, status: str, detail: str = "") -> None:
        entry = {
            "step": step_name,
            "status": status,
            "detail": detail,
            "time": datetime.datetime.now().isoformat(),
        }
        execution_log["steps"].append(entry)
        _save_log(execution_log, processed_dir)
        print(f"\n{'=' * 60}")
        print(f"[Step] {_safe_console_text(step_name)} → {_safe_console_text(status)}")
        if detail:
            print(f"[Detail] {_safe_console_text(detail)}")
        print(f"{'=' * 60}")

    def execute_step(
        step_name: str,
        *,
        runner,
        fallback: dict[str, Any],
        resume_path: str | None = None,
        detail_builder=None,
    ) -> dict[str, Any]:
        if resume and resume_path:
            cached = _load_json_if_exists(resume_path)
            if cached is not None:
                detail = f"resume from {resume_path}"
                if detail_builder:
                    try:
                        built = detail_builder(cached)
                        if built:
                            detail = f"{detail} | {built}"
                    except Exception:
                        pass
                log_step(step_name, "SKIPPED", detail)
                return cached

        log_step(step_name, "RUNNING")
        try:
            result = runner()
            detail = ""
            if detail_builder:
                try:
                    detail = detail_builder(result) or ""
                except Exception:
                    detail = ""
            log_step(step_name, "DONE", detail)
            return result if isinstance(result, dict) else fallback
        except Exception as exc:  # noqa: BLE001
            failure = {"step": step_name, "error": str(exc), "time": datetime.datetime.now().isoformat()}
            execution_log["step_failures"].append(failure)
            log_step(step_name, "FAILED", str(exc))
            return dict(fallback)

    print("\n" + "=" * 60)
    print(f"=  深度狙击引擎启动: {stock_code} {company_name}")
    print("=" * 60)

    scan_path = os.path.join(raw_dir, "akshare_scan.json")
    scan_data = execute_step(
        "0a. Tier 1 数据采集 (akshare)",
        runner=lambda: run_full_scan(stock_code, raw_dir),
        fallback={},
        resume_path=scan_path,
        detail_builder=lambda result: (
            f"成功字段: "
            f"{sum(1 for k in result if not k.startswith('_') and isinstance(result[k], dict) and str(result[k].get('status', '')).lower().startswith('ok'))}/"
            f"{len([k for k in result if not k.startswith('_')])}"
        ),
    )

    industry_mapping = load_industry_mapping()
    source_registry = load_source_registry()
    try:
        eco_context = determine_eco_context(
            stock_code,
            scan_data.get("company_profile", {}).get("data", {}),
            industry_mapping,
            extra_texts=_build_eco_hint_texts(scan_data, company_name),
        )
        execution_log["eco_context"] = eco_context
        log_step(
            "0b. 生态位识别",
            "DONE",
            f"{eco_context.get('eco_circle')} / commodity={eco_context.get('commodity') or 'N/A'} / mode={eco_context.get('four_signal_mode')}",
        )
    except Exception as exc:  # noqa: BLE001
        eco_context = {"eco_circle": "unknown", "commodity": "", "four_signal_mode": "unknown"}
        execution_log["eco_context"] = eco_context
        execution_log["step_failures"].append({"step": "0b. 生态位识别", "error": str(exc), "time": datetime.datetime.now().isoformat()})
        log_step("0b. 生态位识别", "FAILED", str(exc))

    tier0_prep_path = os.path.join(evidence_dir, "tier0_checklist.json")
    tier0_prep = execute_step(
        "1. Tier 0 核验清单生成",
        runner=lambda: run_tier0_prep(stock_code, company_name, evidence_dir),
        fallback={"checklist": {"total_items": 0}},
        resume_path=tier0_prep_path,
        detail_builder=lambda result: f"共 {result.get('checklist', {}).get('total_items', 0)} 个待核验字段",
    )

    annual_report_manifest = os.path.join(evidence_dir, "annual_reports", "annual_reports_manifest.json")
    annual_report_result = execute_step(
        "1a. Tier 0 年报下载",
        runner=lambda: download_tier0_report_pack(stock_code, company_name, evidence_dir),
        fallback={"status": "failed", "downloaded_count": 0},
        resume_path=annual_report_manifest,
        detail_builder=lambda result: f"{result.get('status', 'failed')} | downloaded={result.get('downloaded_count', 0)}",
    )
    execution_log["annual_reports"] = annual_report_result

    pdf_index_manifest = os.path.join(evidence_dir, "pdf_index", "pdf_index_manifest.json")
    pdf_index_result = execute_step(
        "1b. Tier 0 PDF 证据索引",
        runner=lambda: run_pdf_index(stock_code, evidence_dir),
        fallback={"status": "failed", "field_hits": {}},
        resume_path=pdf_index_manifest,
        detail_builder=lambda result: (
            f"{result.get('status', 'failed')} | field_hints="
            f"{sum(1 for item in (result.get('field_hits') or {}).values() if item.get('matched'))}"
        ),
    )
    execution_log["pdf_index"] = pdf_index_result

    docling_manifest = os.path.join(evidence_dir, "docling", "docling_manifest.json")
    docling_result = execute_step(
        "1b.1 Docling 定点页解析",
        runner=lambda: run_docling_page_parse(stock_code, evidence_dir, pdf_index_result=pdf_index_result),
        fallback={"status": "failed", "windows": [], "outputs": []},
        resume_path=docling_manifest,
        detail_builder=lambda result: f"{result.get('status')} | windows={len(result.get('windows', []))}",
    )
    execution_log["docling_page_parse"] = docling_result

    tier0_autofill_path = os.path.join(evidence_dir, "tier0_autofill.json")
    tier0_autofill_result = execute_step(
        "1c. Tier 0 自动回填",
        runner=lambda: run_tier0_autofill(stock_code, tier0_prep, scan_data, pdf_index_result, evidence_dir),
        fallback={"items": [], "auto_filled_count": 0, "review_required_count": 0},
        resume_path=tier0_autofill_path,
        detail_builder=lambda result: (
            f"auto_filled={result.get('auto_filled_count', 0)} | "
            f"review_required={result.get('review_required_count', 0)}"
        ),
    )
    execution_log["tier0_autofill"] = {
        "auto_filled_count": tier0_autofill_result.get("auto_filled_count", 0),
        "review_required_count": tier0_autofill_result.get("review_required_count", 0),
    }

    tier0_verification_path = os.path.join(evidence_dir, "tier0_verification.json")
    tier0_verification_result = execute_step(
        "1d. Tier 0 自动核验",
        runner=lambda: run_tier0_verification(
            stock_code,
            tier0_autofill_result.get("updated_tier0_prep", tier0_prep),
            tier0_autofill_result,
            pdf_index_result,
            annual_report_result,
            evidence_dir,
        ),
        fallback={"verified_count": 0, "failed_items": [], "updated_tier0_prep": tier0_prep},
        resume_path=tier0_verification_path,
        detail_builder=lambda result: f"verified={result.get('verified_count', 0)} | failed={len(result.get('failed_items', []))}",
    )
    execution_log["tier0_verification"] = {
        "verified_count": tier0_verification_result.get("verified_count", 0),
        "failed_items": tier0_verification_result.get("failed_items", []),
    }
    tier0_prep = tier0_verification_result.get("updated_tier0_prep", tier0_prep)

    commodity_name = normalize_text(eco_context.get("commodity"))
    signal_profile = resolve_signal_profile(
        eco_context.get("commodity") or eco_context.get("industry_text") or "",
        extra_texts=_build_eco_hint_texts(scan_data, company_name),
    )
    commodity_name = normalize_text(signal_profile.get("name") or commodity_name)
    execution_log["signal_profile"] = {
        "name": signal_profile.get("name", ""),
        "price_proxy": signal_profile.get("price_proxy", ""),
    }

    commodity_path = os.path.join(raw_dir, "commodity", "commodity_scan.json")
    if commodity_name:
        commodity_data = execute_step(
            "2. 大宗商品四维信号扫描",
            runner=lambda: run_commodity_scan(commodity=commodity_name, output_dir=f"{raw_dir}/commodity"),
            fallback={},
            resume_path=commodity_path,
            detail_builder=lambda result: (
                f"商品={commodity_name} | 成功: "
                f"{sum(1 for k in result if not k.startswith('_') and isinstance(result[k], dict) and str(result[k].get('status', '')).lower().startswith('ok'))}/"
                f"{len([k for k in result if not k.startswith('_') and not str(result.get(k, {}).get('status', '')).lower().startswith('not_applicable')])}"
            ),
        )
    else:
        commodity_data = {"status": "skipped", "reason": "missing_commodity_mapping"}
        os.makedirs(os.path.dirname(commodity_path), exist_ok=True)
        with open(commodity_path, "w", encoding="utf-8") as f:
            json.dump(commodity_data, f, ensure_ascii=False, indent=2, default=str)
        log_step("2. 大宗商品四维信号扫描", "SKIPPED", "未识别到商品映射，跳过商品扫描")

    industry_kw = commodity_name or eco_context.get("commodity") or eco_context.get("industry_text") or ""
    macro_path = os.path.join(raw_dir, "macro", "macro_scan.json")
    macro_data = execute_step(
        "3. 宏观&行业数据扫描",
        runner=lambda: run_macro_scan(output_dir=f"{raw_dir}/macro", industry_keyword=industry_kw),
        fallback={},
        resume_path=macro_path,
        detail_builder=lambda result: f"industry_keyword={industry_kw} | fields={len([k for k in result if not k.startswith('_')])}",
    )

    source_manifest_path = os.path.join(evidence_dir, "source_manifest.json")
    source_manifest = execute_step(
        "4a. Source Manifest 生成",
        runner=lambda: build_source_manifest(
            stock_code,
            scan_data=scan_data,
            tier0_prep=tier0_prep,
            tier0_autofill_result=tier0_autofill_result,
            pdf_index_result=pdf_index_result,
            commodity_data=commodity_data,
            macro_data=macro_data,
            eco_context=eco_context,
            source_registry=source_registry,
        ),
        fallback={"summary": {"tier0_required_missing": [], "partial_fields": [], "tier0_pdf_hints": [], "stale_fields": []}},
        resume_path=source_manifest_path,
        detail_builder=lambda result: (
            f"Tier0 缺口: {len(result.get('summary', {}).get('tier0_required_missing', []))} | "
            f"PDF hints: {len(result.get('summary', {}).get('tier0_pdf_hints', []))} | "
            f"部分字段: {len(result.get('summary', {}).get('partial_fields', []))}"
        ),
    )
    with open(source_manifest_path, "w", encoding="utf-8") as f:
        json.dump(source_manifest, f, ensure_ascii=False, indent=2, default=str)

    gate_path = os.path.join(processed_dir, "redcard_gate.json")
    gate_result = execute_step(
        "4b. 红牌否决闸门",
        runner=lambda: run_redcard_gate(
            scan_data,
            stock_code,
            source_manifest=source_manifest,
            eco_context=eco_context,
            tier0_prep=tier0_prep,
            tier0_autofill=tier0_autofill_result,
            commodity_data=commodity_data,
            macro_data=macro_data,
        ),
        fallback={"gate_verdict": "FAILED", "checks": [], "pass_count": 0, "pending_count": 0, "signal_health": {}},
        resume_path=gate_path,
        detail_builder=lambda result: f"gate={result.get('gate_verdict', 'FAILED')}",
    )
    with open(gate_path, "w", encoding="utf-8") as f:
        json.dump(gate_result, f, ensure_ascii=False, indent=2, default=str)

    gate_verdict = gate_result.get("gate_verdict")
    if gate_verdict == "🔴 KILLED":
        execution_log["final_verdict"] = "KILLED_AT_GATE"
    elif gate_verdict == "🟠 BLOCKED_PENDING_TIER0":
        execution_log["final_verdict"] = "BLOCKED_PENDING_TIER0"
    elif gate_verdict:
        execution_log["final_verdict"] = "READY_FOR_ANALYSIS"
    else:
        execution_log["final_verdict"] = "PARTIAL_FAILURE"

    execution_log["end_time"] = datetime.datetime.now().isoformat()
    execution_log["data_locations"] = {
        "raw": raw_dir,
        "processed": processed_dir,
        "evidence": evidence_dir,
    }

    planned_fields = [
        "company_profile",
        "financial_summary",
        "revenue_breakdown",
        "income_statement",
        "balance_sheet",
        "valuation_history",
        "stock_kline",
        "realtime_quote",
        "spot_price",
        "exchange_inventory",
        "social_inventory",
        "inventory",
        "futures",
        "fixed_asset_investment",
        "ppi",
    ]
    for field in planned_fields:
        if field in scan_data:
            actual = status_to_plan_actual(scan_data[field].get("status"))
        elif field in commodity_data:
            actual = status_to_plan_actual(commodity_data[field].get("status"))
        elif field in macro_data:
            actual = status_to_plan_actual(macro_data[field].get("status"))
        else:
            actual = "partial_or_failed"
        execution_log["plan_vs_actual"].append({"field": field, "planned": "acquire", "actual": actual})

    if execution_log.get("final_verdict") == "READY_FOR_ANALYSIS":
        signal_health = gate_result.get("signal_health", {})
        if not signal_health.get("core_ready", True):
            execution_log["final_verdict"] = "BLOCKED_CORE_SIGNAL_GAPS"
        elif (
            signal_health.get("auxiliary_missing")
            or signal_health.get("coverage_warnings")
            or source_manifest.get("summary", {}).get("stale_fields")
        ):
            execution_log["final_verdict"] = "COMPLETED_WITH_WARNINGS"
        else:
            execution_log["final_verdict"] = "COMPLETED"

    if execution_log.get("final_verdict") in {"COMPLETED", "COMPLETED_WITH_WARNINGS"}:
        pre_scorecard = build_scorecard_v2(
            scan_data,
            source_manifest,
            eco_context,
            tier0_autofill_result,
            tier0_prep=tier0_prep,
            commodity_data=commodity_data,
        )
        pre_score = pre_scorecard.get("total_score", 0)
        pb_val = safe_float(scan_data.get("valuation_history", {}).get("data", {}).get("pb"))
        current_vs_high = safe_float(scan_data.get("stock_kline", {}).get("data", {}).get("current_vs_5yr_high"))
        current_vs_high = current_vs_high or safe_float(scan_data.get("stock_kline", {}).get("data", {}).get("current_vs_high"))
        eco_circle = normalize_text(eco_context.get("eco_circle"))
        if eco_circle == "core_military":
            current_ps = safe_float(
                estimate_current_ps(
                    scan_data.get("realtime_quote", {}).get("data", {}),
                    scan_data,
                    tier0_autofill_result,
                    eco_context,
                ).get("current_ps")
            )
            military_cfg = get_crocodile_mode_config(eco_context.get("four_signal_mode")).get("valuation", {}) or {}
            military_pass_ps = safe_float(military_cfg.get("entry_ps_pass")) or 2.5
            military_caution_ps = safe_float(military_cfg.get("entry_ps_caution")) or 4.0
            military_hard_ps = military_caution_ps * 1.25
            if current_ps is not None and current_ps > military_hard_ps:
                execution_log["final_verdict"] = "COMPLETED_REJECTED"
                execution_log["rejection_reason"] = f"PS={current_ps:.2f}>{military_hard_ps:.2f}"
            elif current_ps is not None and current_ps > military_caution_ps and execution_log["final_verdict"] == "COMPLETED":
                execution_log["final_verdict"] = "COMPLETED_WATCHLIST_ONLY"
                execution_log["rejection_reason"] = f"PS={current_ps:.2f}>{military_caution_ps:.2f}锛岃繘鍏ュ啗宸ヨ瀵熷尯闂?PS 绾緥锛?"
            elif current_ps is not None and current_ps <= military_pass_ps and execution_log["final_verdict"] == "COMPLETED_WATCHLIST_ONLY" and pre_score >= 70:
                execution_log["final_verdict"] = "COMPLETED"
            pb_val = None

        if pre_score < 55:
            execution_log["final_verdict"] = "COMPLETED_REJECTED"
        elif pre_score < 70:
            execution_log["final_verdict"] = "COMPLETED_WATCHLIST_ONLY"

        if (pb_val is not None and pb_val > 1.5) or (current_vs_high is not None and current_vs_high >= 70):
            execution_log["final_verdict"] = "COMPLETED_REJECTED"
            reasons = []
            if pb_val is not None and pb_val > 1.5:
                reasons.append(f"PB={pb_val:.2f}>1.5")
            if current_vs_high is not None and current_vs_high >= 70:
                reasons.append(f"距高点仅回撤{100 - current_vs_high:.1f}%")
            execution_log["rejection_reason"] = " / ".join(reasons)
        elif pb_val is not None and pb_val > 1.0 and execution_log["final_verdict"] == "COMPLETED":
            execution_log["final_verdict"] = "COMPLETED_WATCHLIST_ONLY"
            execution_log["rejection_reason"] = f"PB={pb_val:.2f}>1.0，进入等待区间（PB纪律）"

    human_actions = _collect_human_actions(scan_data, commodity_data, macro_data, source_manifest)
    execution_log["data_freshness"] = detect_data_freshness(scan_data)
    execution_log["human_required_actions"] = human_actions

    log_step("5. 自动报告生成", "RUNNING")
    try:
        report_result = generate_report(
            stock_code,
            company_name,
            scan_data=scan_data,
            gate_result=gate_result,
            source_manifest=source_manifest,
            eco_context=eco_context,
            commodity_data=commodity_data,
            macro_data=macro_data,
            tier0_autofill_result=tier0_autofill_result,
            tier0_verification_result=tier0_verification_result,
            annual_report_result=annual_report_result,
            execution_log=execution_log,
            report_dir=report_dir,
            processed_dir=processed_dir,
            evidence_dir=evidence_dir,
            human_required_actions=human_actions,
        )
        execution_log["report"] = report_result
        log_step("5. 自动报告生成", "DONE", report_result.get("report_path", ""))
    except Exception as exc:  # noqa: BLE001
        execution_log["step_failures"].append({"step": "5. 自动报告生成", "error": str(exc), "time": datetime.datetime.now().isoformat()})
        execution_log["report"] = {"status": "failed", "error": str(exc)}
        log_step("5. 自动报告生成", "FAILED", str(exc))

    if human_actions:
        log_step("6. 人工补充清单", "DONE", f"{len(human_actions)} 项需人工补充")
    else:
        log_step("6. 人工补充清单", "DONE", "无需人工补充")

    execution_log["end_time"] = datetime.datetime.now().isoformat()
    _save_log(execution_log, processed_dir)

    print("\n" + "=" * 60)
    print(f"=  流程执行完成: {stock_code} {company_name}")
    print(f"=  最终状态: {execution_log.get('final_verdict', 'UNKNOWN')}")
    print("=" * 60)

    return execution_log


if __name__ == "__main__":
    argv = [arg for arg in sys.argv[1:] if arg != "--resume"]
    resume_mode = "--resume" in sys.argv[1:]
    code = argv[0] if len(argv) > 0 else "600328"
    name = argv[1] if len(argv) > 1 else "中盐化工"
    deep_sniper(code, name, resume=resume_mode)
