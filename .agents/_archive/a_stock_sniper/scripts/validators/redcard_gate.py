"""
redcard_gate.py - 红牌否决闸门
证伪优先：先判生态位、Tier 0、国资底线，再逐条审查八大原则。
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_DIR)

from engines.valuation_engine import estimate_current_ps
from utils.research_utils import (
    assess_bottom_pattern,
    assess_business_purity,
    assess_price_trigger,
    classify_state_ownership,
    get_tier0_item,
    manifest_field_status,
    normalize_text,
    safe_float,
)
from utils.hard_rule_utils import (
    evaluate_business_simplicity,
    evaluate_shovel_capex_hard_rule,
    resolve_military_group_snapshot,
    scan_moat_dictionary,
)
from utils.signal_health_utils import evaluate_signal_health_v2


VERDICT_TO_STATUS = {
    "PASS": "通过",
    "KILL": "不通过",
    "BLOCK": "待补证",
    "CAUTION": "待补证",
}


def _normalized_status(verdict: str) -> str:
    text = str(verdict or "").upper()
    for key, value in VERDICT_TO_STATUS.items():
        if key in text:
            return value
    return "待补证"


def _append_result(
    results: list[dict],
    *,
    rule: str,
    verdict: str,
    reason: str,
    confidence: str = "medium",
    note: str = "",
) -> None:
    item = {
        "rule": rule,
        "verdict": verdict,
        "normalized_status": _normalized_status(verdict),
        "reason": reason,
        "confidence": confidence,
    }
    if note:
        item["note"] = note
    results.append(item)


def _collect_company_name_hints(profile: dict, quote_data: dict) -> list[str]:
    hints: list[str] = []
    for dataset in (profile, quote_data):
        if not isinstance(dataset, dict):
            continue
        for key, value in dataset.items():
            key_text = normalize_text(key)
            value_text = normalize_text(value)
            if not value_text:
                continue
            if any(token in key_text for token in ("名称", "简称", "公司")):
                hints.append(value_text)
    return [item for item in dict.fromkeys(hints) if item]


def _build_rule2_reason(
    eco_context: dict,
    *,
    price_trigger: dict,
    price_signal: dict,
    inventory_signal: dict,
    capex_signal: dict,
) -> tuple[str, str]:
    mode = eco_context.get("four_signal_mode")
    trigger_verdict = normalize_text(price_trigger.get("verdict")).lower()
    if mode == "shovel_play":
        if trigger_verdict == "pass":
            return "🟢 PASS", f"Capex 与下游景气验证通过: {capex_signal['detail']} | price={price_trigger['reason']}"
        if trigger_verdict == "caution":
            return "🟡 CAUTION", f"Capex 已确认，但下游景气/价格仅出现初步改善: {price_trigger['reason']}"
        return "🟠 BLOCK", f"铲子股缺少下游景气确认，仍不足以证明订单质变: {price_trigger['reason']}"

    if trigger_verdict == "pass":
        return "🟢 PASS", f"价格已确认发生质变，且具备交叉验证: inventory={inventory_signal['detail']} / capex={capex_signal['detail']}"
    if trigger_verdict == "caution":
        return "🟡 CAUTION", f"周期低位信号已出现，但价格尚未形成明确反转确认: {price_trigger['reason']}"
    if not price_signal["ready"]:
        return "🟠 BLOCK", f"缺少现货/期货价格信号: {price_signal['detail']}"
    return "🟠 BLOCK", f"价格尚未形成明确反转确认: {price_trigger['reason']}"


def _assess_military_position(scan_data: dict, tier0_autofill: dict, eco_context: dict) -> dict:
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    current_vs_high_num = safe_float(kline.get("current_vs_5yr_high")) or safe_float(kline.get("current_vs_high"))
    ps_snapshot = estimate_current_ps(quote, scan_data, tier0_autofill, eco_context)
    current_ps = ps_snapshot.get("current_ps")

    if current_ps is None or current_vs_high_num is None:
        return {"verdict": "block", "reason": "军工路径缺少 PS 代理收入或位置数据"}
    if current_ps <= 2.5 and current_vs_high_num <= 60:
        return {"verdict": "pass", "reason": f"军工低 PS 区间：PS={current_ps:.2f}, 距高点{current_vs_high_num:.1f}%"}
    if current_ps <= 4.0 and current_vs_high_num <= 70:
        return {"verdict": "caution", "reason": f"军工估值回落但未到极低区：PS={current_ps:.2f}, 距高点{current_vs_high_num:.1f}%"}
    return {"verdict": "kill", "reason": f"军工估值或位置仍偏热：PS={current_ps:.2f}, 距高点{current_vs_high_num:.1f}%"}


def run_redcard_gate(
    scan_data: dict,
    stock_code: str,
    *,
    source_manifest: dict | None = None,
    eco_context: dict | None = None,
    tier0_prep: dict | None = None,
    tier0_autofill: dict | None = None,
    commodity_data: dict | None = None,
    macro_data: dict | None = None,
) -> dict:
    """
    执行红牌否决闸门检查。
    输出分两层：
    1. 生态归属与审计前置检查
    2. 鳄鱼八大原则逐条审查
    """
    source_manifest = source_manifest or {}
    eco_context = eco_context or {}
    tier0_prep = tier0_prep or {}
    tier0_autofill = tier0_autofill or {}
    commodity_data = commodity_data or {}
    macro_data = macro_data or {}

    results: list[dict] = []
    killed = False
    blocked = False

    autofill_items = {item.get("field_name"): item for item in tier0_autofill.get("items", [])}
    profile = scan_data.get("company_profile", {}).get("data", {})
    quote_data = scan_data.get("realtime_quote", {}).get("data", {})

    controller = normalize_text(profile.get("实际控制人") or profile.get("控股股东"))
    if not controller:
        controller = normalize_text(autofill_items.get("actual_controller", {}).get("candidate_value"))
    actual_controller_item = get_tier0_item(tier0_prep, "actual_controller")

    eco_circle = eco_context.get("eco_circle") or "unknown"
    industry = eco_context.get("industry_text") or ""
    matched_by = eco_context.get("matched_by") or "unmatched"
    company_name_hints = _collect_company_name_hints(profile, quote_data)
    military_snapshot = {"matched": False, "group_name": "", "listed_platforms": [], "securitization_status": "missing"}

    signal_health = evaluate_signal_health_v2(eco_context, source_manifest, commodity_data, macro_data)
    price_trigger = assess_price_trigger(commodity_data, eco_context)
    ownership = classify_state_ownership(
        stock_code,
        controller,
        tier0_item=actual_controller_item,
        company_name_hints=company_name_hints,
    )

    def _finalize_gate_result() -> dict:
        principle_checks = [item for item in results if item.get("rule", "").split(".", 1)[0] in {str(i) for i in range(1, 9)}]
        prechecks = [item for item in results if item not in principle_checks]
        principle_summary = {
            "total_rules": len(principle_checks),
            "pass_count": sum(1 for item in principle_checks if item.get("normalized_status") == "通过"),
            "fail_count": sum(1 for item in principle_checks if item.get("normalized_status") == "不通过"),
            "pending_count": sum(1 for item in principle_checks if item.get("normalized_status") == "待补证"),
        }
        return {
            "stock_code": stock_code,
            "gate_verdict": "🔴 KILLED" if killed else "🟠 BLOCKED_PENDING_TIER0" if blocked else "🟢 PASSED",
            "checks": results,
            "prechecks": prechecks,
            "principle_checks": principle_checks,
            "principle_summary": principle_summary,
            "kill_count": sum(1 for r in results if "KILL" in r["verdict"]),
            "block_count": sum(1 for r in results if "BLOCK" in r["verdict"]),
            "precheck_pass_count": sum(1 for item in prechecks if item.get("normalized_status") == "通过"),
            "precheck_pending_count": sum(1 for item in prechecks if item.get("normalized_status") == "待补证"),
            "pass_count": principle_summary["pass_count"],
            "pending_count": principle_summary["pending_count"],
            "signal_health": signal_health,
            "ownership": ownership,
            "military_snapshot": military_snapshot,
        }

    # 0. 生态位预检
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    pb_val = safe_float(valuation.get("pb"))
    if eco_circle in {"core_resource", "rigid_shovel"} and (pb_val is None or pb_val > 1.0):
        _append_result(
            results,
            rule="0. 极限破净闸门",
            verdict="🔴 KILL",
            reason=f"当前 PB={pb_val if pb_val is not None else 'N/A'} > 1.0。资源/铲子股不破净绝不看，物理否决！",
            confidence="high",
        )
        killed = True

    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    if purity["top_ratio"] > 0 and purity["top_ratio"] < 0.7:
        _append_result(
            results,
            rule="3. 主营纯粹度",
            verdict="🔴 KILL",
            reason=f"最大分项占比仅 {purity['top_ratio']:.1%} < 70%，纯度红线否决。",
            confidence="high",
        )
        killed = True

    if eco_circle == "unknown" and industry:
        _append_result(
            results,
            rule="0. 生态归属",
            verdict="🔴 KILL",
            reason=f"行业 '{industry}' 不在资源/配套/军工圈层内",
        )
        killed = True
    elif eco_circle != "unknown":
        _append_result(
            results,
            rule="0. 生态归属",
            verdict="🟢 PASS",
            reason=f"识别为 {eco_circle}，matched_by={matched_by or 'unknown'}",
            note=f"commodity={eco_context.get('commodity') or 'N/A'} / mode={eco_context.get('four_signal_mode') or 'unknown'}",
        )
    else:
        _append_result(
            results,
            rule="0. 生态归属",
            verdict="🟠 BLOCK",
            reason="无法自动判定行业归属",
            confidence="low",
        )
        blocked = True

    if eco_circle == "core_military":
        military_snapshot = resolve_military_group_snapshot(
            stock_code,
            controller,
            company_name_hints=company_name_hints,
        )
        if military_snapshot.get("matched"):
            _append_result(
                results,
                rule="0. 军工集团穿透",
                verdict="🟢 PASS",
                reason=(
                    f"匹配到军工集团 {military_snapshot['group_name']}，"
                    f"上市平台={','.join(military_snapshot.get('listed_platforms', [])[:4]) or 'N/A'}，"
                    f"资产证券化状态={military_snapshot.get('securitization_status')}"
                ),
            )
        else:
            _append_result(
                results,
                rule="0. 军工集团穿透",
                verdict="🟠 BLOCK",
                reason="军工路径未穿透到十大军工集团控制链，无法启用 PS 军工估值",
                confidence="low",
            )
            blocked = True

    missing_tier0 = source_manifest.get("summary", {}).get("tier0_required_missing", [])
    mandatory_missing = source_manifest.get("summary", {}).get("mandatory_evidence_missing", [])
    if missing_tier0:
        _append_result(
            results,
            rule="0. Tier 0 关键证据",
            verdict="🟠 BLOCK",
            reason=f"缺少 Tier 0 关键字段: {', '.join(missing_tier0)}",
            confidence="low",
        )
        blocked = True
    else:
        _append_result(
            results,
            rule="0. Tier 0 关键证据",
            verdict="🟢 PASS",
            reason="Tier 0 必填字段已自动核验完毕",
            confidence="high",
        )

    # 1. 国资底线
    if ownership["gate_verdict"] == "PASS":
        _append_result(
            results,
            rule="1. 国资底线",
            verdict="🟢 PASS",
            reason=ownership["reason"],
            note=f"ownership={ownership['label']}",
        )
    elif ownership["gate_verdict"] == "KILL":
        _append_result(
            results,
            rule="1. 国资底线",
            verdict="🔴 KILL",
            reason=ownership["reason"],
            note=f"ownership={ownership['label']}",
        )
        killed = True
    else:
        _append_result(
            results,
            rule="1. 国资底线",
            verdict="🟠 BLOCK",
            reason=ownership["reason"],
            confidence="low",
            note=f"ownership={ownership['label']}",
        )
        blocked = True

    # 2. 底层质变 / 强制防伪
    price_signal = signal_health["signals"]["price_signal"]
    inventory_signal = signal_health["signals"]["inventory_signal"]
    capex_signal = signal_health["signals"]["capex_signal"]

    if eco_context.get("four_signal_mode") == "shovel_play" and not price_signal["ready"]:
        mandatory_missing = list(dict.fromkeys([*mandatory_missing, "downstream_price_proxy"]))
        capex_signal["detail"] = f"{capex_signal['detail']} | downstream_price={price_signal['detail']}"

    if eco_context.get("four_signal_mode") == "shovel_play":
        capex_hard_rule = evaluate_shovel_capex_hard_rule(macro_data)
        if mandatory_missing or not capex_signal["ready"] or not capex_hard_rule["pass"]:
            _append_result(
                results,
                rule="2. 底层质变与Capex证伪",
                verdict="🔴 KILL",
                reason=f"{capex_hard_rule['reason']} | raw={capex_signal['detail']}",
                confidence="low",
            )
            killed = True
        else:
            verdict, reason = _build_rule2_reason(
                eco_context,
                price_trigger=price_trigger,
                price_signal=price_signal,
                inventory_signal=inventory_signal,
                capex_signal=capex_signal,
            )
            _append_result(
                results,
                rule="2. 底层质变与Capex证伪",
                verdict=verdict,
                reason=f"{reason} | {capex_hard_rule['reason']}",
            )
            if "BLOCK" in verdict:
                blocked = True
    else:
        if not price_signal["ready"]:
            _append_result(
                results,
                rule="2. 底层质变与Capex证伪",
                verdict="🟠 BLOCK",
                reason=f"缺少现货/期货价格信号: {price_signal['detail']}",
                confidence="low",
            )
            blocked = True
        elif not (inventory_signal["ready"] or capex_signal["ready"]):
            _append_result(
                results,
                rule="2. 底层质变与Capex证伪",
                verdict="🟠 BLOCK",
                reason="仅有价格信号，缺少库存/Capex 交叉验证",
                confidence="low",
            )
            blocked = True
        else:
            verdict, reason = _build_rule2_reason(
                eco_context,
                price_trigger=price_trigger,
                price_signal=price_signal,
                inventory_signal=inventory_signal,
                capex_signal=capex_signal,
            )
            _append_result(results, rule="2. 底层质变与Capex证伪", verdict=verdict, reason=reason)
            if "BLOCK" in verdict:
                blocked = True

    # 3. 主营纯粹度
    purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
    if purity["pass"]:
        _append_result(
            results,
            rule="3. 主营纯粹度",
            verdict="🟢 PASS",
            reason=f"{purity['latest_report_date']} 主营最大分项 '{purity['top_segment']}' 占比 {purity['top_ratio']:.1%}",
        )
    elif purity["top_ratio"] > 0:
        _append_result(
            results,
            rule="3. 主营纯粹度",
            verdict="🔴 KILL",
            reason=f"最新分项 '{purity['top_segment']}' 占比仅 {purity['top_ratio']:.1%}，低于 70%",
        )
        killed = True
    else:
        _append_result(
            results,
            rule="3. 主营纯粹度",
            verdict="🟠 BLOCK",
            reason="缺少有效主营构成数据",
            confidence="low",
        )
        blocked = True

    # 4. 业务极简
    cost_status = manifest_field_status(source_manifest, "cost_structure")
    simplicity = evaluate_business_simplicity(eco_circle, tier0_autofill)
    if simplicity["status"] == "pass" and cost_status in {"verified_tier0", "autofilled_tier0_hint", "partial_autofill"}:
        _append_result(
            results,
            rule="4. 业务极简",
            verdict="🟢 PASS",
            reason=simplicity["reason"],
            note=simplicity.get("formula", ""),
        )
    elif simplicity["status"] == "kill":
        _append_result(
            results,
            rule="4. 业务极简",
            verdict="🔴 KILL",
            reason=simplicity["reason"],
        )
        killed = True
    else:
        _append_result(
            results,
            rule="4. 业务极简",
            verdict="🟠 BLOCK",
            reason=simplicity["reason"],
            confidence="low",
            note=simplicity.get("formula", ""),
        )
        blocked = True

    # 5. 位置形态
    valuation = scan_data.get("valuation_history", {}).get("data", {})
    kline = scan_data.get("stock_kline", {}).get("data", {})
    bottom = _assess_military_position(scan_data, tier0_autofill, eco_context) if eco_circle == "core_military" else assess_bottom_pattern(kline, valuation, eco_context)
    if bottom["verdict"] == "kill":
        _append_result(results, rule="5. 位置形态", verdict="🔴 KILL", reason=bottom["reason"], confidence="high")
        killed = True
    elif bottom["verdict"] == "pass":
        _append_result(results, rule="5. 位置形态", verdict="🟢 PASS", reason=bottom["reason"], confidence="high")
    elif bottom["verdict"] == "caution":
        _append_result(results, rule="5. 位置形态", verdict="🟡 CAUTION", reason=bottom["reason"], confidence="medium")
    else:
        _append_result(results, rule="5. 位置形态", verdict="🟠 BLOCK", reason=bottom["reason"], confidence="medium")
        blocked = True

    # 6. 稀缺护城河
    mineral_status = manifest_field_status(source_manifest, "mineral_rights")
    license_status = manifest_field_status(source_manifest, "license_moat")
    moat_scan = scan_moat_dictionary(eco_circle, tier0_autofill)
    if eco_circle == "core_resource" and (mineral_status == "verified_tier0" or moat_scan["matched"]):
        _append_result(
            results,
            rule="6. 稀缺护城河",
            verdict="🟢 PASS",
            reason="矿权/资源证据已完成 Tier 0 核验" if mineral_status == "verified_tier0" else f"命中护城河字典: {', '.join(hit['label'] for hit in moat_scan['hits'])}",
        )
    elif eco_circle == "core_military" and (moat_scan["matched"] or military_snapshot.get("matched")):
        _append_result(
            results,
            rule="6. 稀缺护城河",
            verdict="🟢 PASS",
            reason=(
                f"军工集团={military_snapshot.get('group_name')}，"
                f"资质命中={','.join(hit['label'] for hit in moat_scan['hits']) or '集团控制链已确认'}"
            ),
            note="PS 估值路径已启用，资质仍建议继续补公告级证据",
        )
    elif eco_circle == "rigid_shovel" and (license_status == "verified_tier0" or moat_scan["matched"] or cost_status == "verified_tier0"):
        _append_result(
            results,
            rule="6. 稀缺护城河",
            verdict="🟢 PASS",
            reason=(
                f"命中牌照/资质: {', '.join(hit['label'] for hit in moat_scan['hits'])}"
                if moat_scan["matched"]
                else "已具备成本/准入壁垒的 Tier 0 证据基础"
            ),
            note="民爆/港口/核电设备等牌照型护城河建议持续补证",
        )
    else:
        _append_result(
            results,
            rule="6. 稀缺护城河",
            verdict="🟠 BLOCK",
            reason="缺少矿权/牌照/准入壁垒的 Tier 0 硬证据",
            confidence="low",
        )
        blocked = True

    # 7. 市值弹性
    quote = scan_data.get("realtime_quote", {}).get("data", {})
    try:
        mktcap_float = float(quote.get("总市值")) if quote.get("总市值") is not None else None
    except (TypeError, ValueError):
        mktcap_float = None

    if mktcap_float is None:
        _append_result(results, rule="7. 市值弹性", verdict="🟠 BLOCK", reason="缺少总市值数据", confidence="low")
        blocked = True
    else:
        mktcap_yi = mktcap_float / 1e8
        if mktcap_yi <= 200:
            _append_result(results, rule="7. 市值弹性", verdict="🟢 PASS", reason=f"总市值 {mktcap_yi:.0f} 亿，符合 <200 亿弹性要求")
        elif mktcap_yi <= 500:
            _append_result(results, rule="7. 市值弹性", verdict="🟡 CAUTION", reason=f"总市值 {mktcap_yi:.0f} 亿，仍可跟踪但弹性已打折")
        else:
            _append_result(results, rule="7. 市值弹性", verdict="🟠 BLOCK", reason=f"总市值 {mktcap_yi:.0f} 亿，显著高于弹性偏好区间")
            blocked = True

    # 8. 绝对生态位
    if eco_circle == "unknown":
        _append_result(results, rule="8. 绝对生态位", verdict="🟠 BLOCK", reason="生态位未识别，无法证明主线资金必买", confidence="low")
        blocked = True
    elif matched_by in {"industry_keyword", "context_text", "extra_context"}:
        _append_result(results, rule="8. 绝对生态位", verdict="🟢 PASS", reason=f"由 {matched_by} 自动识别到能力圈内生态位")
    elif matched_by == "company_override":
        _append_result(
            results,
            rule="8. 绝对生态位",
            verdict="🟡 CAUTION",
            reason="当前依赖公司级映射，建议继续增强通用行业识别",
            confidence="medium",
        )
    else:
        _append_result(results, rule="8. 绝对生态位", verdict="🟠 BLOCK", reason="缺少可复用的生态位识别依据", confidence="low")
        blocked = True

    principle_checks = [item for item in results if item.get("rule", "").split(".", 1)[0] in {str(i) for i in range(1, 9)}]
    prechecks = [item for item in results if item not in principle_checks]
    principle_summary = {
        "total_rules": len(principle_checks),
        "pass_count": sum(1 for item in principle_checks if item.get("normalized_status") == "通过"),
        "fail_count": sum(1 for item in principle_checks if item.get("normalized_status") == "不通过"),
        "pending_count": sum(1 for item in principle_checks if item.get("normalized_status") == "待补证"),
    }

    gate_result = {
        "stock_code": stock_code,
        "gate_verdict": "🔴 KILLED" if killed else "🟠 BLOCKED_PENDING_TIER0" if blocked else "🟢 PASSED",
        "checks": results,
        "prechecks": prechecks,
        "principle_checks": principle_checks,
        "principle_summary": principle_summary,
        "kill_count": sum(1 for r in results if "KILL" in r["verdict"]),
        "block_count": sum(1 for r in results if "BLOCK" in r["verdict"]),
        "precheck_pass_count": sum(1 for item in prechecks if item.get("normalized_status") == "通过"),
        "precheck_pending_count": sum(1 for item in prechecks if item.get("normalized_status") == "待补证"),
        "pass_count": principle_summary["pass_count"],
        "pending_count": principle_summary["pending_count"],
        "signal_health": signal_health,
        "ownership": ownership,
        "military_snapshot": military_snapshot,
    }
    return _finalize_gate_result()


def run_from_file(
    scan_file: str,
    stock_code: str,
    output_dir: str | None = None,
    manifest_file: str | None = None,
) -> dict:
    with open(scan_file, "r", encoding="utf-8") as f:
        scan_data = json.load(f)
    manifest_data = None
    if manifest_file and os.path.exists(manifest_file):
        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)

    result = run_redcard_gate(scan_data, stock_code, source_manifest=manifest_data)
    print(f"\n{'=' * 60}")
    print(f"红牌否决闸门 - {stock_code}")
    print(f"{'=' * 60}")
    print(f"总体判定: {result['gate_verdict']}")
    print(f"通过: {result['pass_count']} | 否决: {result['kill_count']} | 待定: {result['pending_count']}")
    print(f"{'=' * 60}")
    for check in result["checks"]:
        print(f"  {check['verdict']}  {check['rule']}: {check['reason']}")
    print(f"{'=' * 60}\n")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "redcard_gate.json"), "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    return result


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    scan_f = sys.argv[2] if len(sys.argv) > 2 else str(Path(__file__).resolve().parents[5] / "data" / "raw" / code / "akshare_scan.json")
    out = sys.argv[3] if len(sys.argv) > 3 else str(Path(__file__).resolve().parents[5] / "data" / "processed" / code)
    manifest_f = sys.argv[4] if len(sys.argv) > 4 else None
    run_from_file(scan_f, code, out, manifest_f)
