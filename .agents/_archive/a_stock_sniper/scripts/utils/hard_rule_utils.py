"""Hard-rule helpers for the crocodile discipline workflow."""

from __future__ import annotations

import json
from typing import Any

from utils.research_utils import load_yaml_config, normalize_text, safe_float


SUSPICIOUS_DIVERSIFICATION_TERMS = (
    "并购",
    "收购",
    "理财",
    "金融资产",
    "公允价值",
    "投资收益",
    "供应链贸易",
    "贸易业务",
    "地产",
    "互联网",
    "小贷",
    "证券投资",
)


def _autofill_map(tier0_autofill_result: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    items = (tier0_autofill_result or {}).get("items", [])
    return {item.get("field_name"): item for item in items if isinstance(item, dict) and item.get("field_name")}


def _collect_autofill_texts(tier0_autofill_result: dict[str, Any] | None) -> list[str]:
    texts: list[str] = []
    for item in (tier0_autofill_result or {}).get("items", []):
        if not isinstance(item, dict):
            continue
        snippet = normalize_text(item.get("snippet"))
        if snippet:
            texts.append(snippet)
        candidate_value = item.get("candidate_value")
        if isinstance(candidate_value, dict):
            for key in ("summary", "resource_moat_hint", "price_cost_comment", "capacity_label", "primary_product"):
                text = normalize_text(candidate_value.get(key))
                if text:
                    texts.append(text)
            try:
                texts.append(json.dumps(candidate_value, ensure_ascii=False, default=str))
            except TypeError:
                texts.append(str(candidate_value))
        else:
            text = normalize_text(candidate_value)
            if text:
                texts.append(text)
    return [text for text in texts if text]


def resolve_military_group_snapshot(
    stock_code: str,
    controller_text: str,
    *,
    company_name_hints: list[str] | None = None,
) -> dict[str, Any]:
    router_cfg = load_yaml_config("military_router.yaml")
    groups = router_cfg.get("groups", []) or []
    override_map = router_cfg.get("company_overrides", {}) or {}
    override_name = normalize_text(override_map.get(str(stock_code)))
    corpus = " ".join(
        normalize_text(part)
        for part in [controller_text, *(company_name_hints or [])]
        if normalize_text(part)
    )

    for group in groups:
        group_name = normalize_text(group.get("name"))
        aliases = [normalize_text(alias) for alias in group.get("aliases", []) if normalize_text(alias)]
        if override_name and override_name == group_name:
            return {
                "matched": True,
                "group_name": group_name,
                "matched_by": "company_override",
                "listed_platforms": group.get("listed_platforms", []),
                "securitization_status": normalize_text(group.get("securitization_status")) or "registry_tracked",
            }
        if any(alias and alias in corpus for alias in aliases):
            return {
                "matched": True,
                "group_name": group_name,
                "matched_by": "controller_alias",
                "listed_platforms": group.get("listed_platforms", []),
                "securitization_status": normalize_text(group.get("securitization_status")) or "registry_tracked",
            }

    return {
        "matched": False,
        "group_name": "",
        "matched_by": "unmatched",
        "listed_platforms": [],
        "securitization_status": "missing",
    }


def _extract_indicator_from_record(record: dict[str, Any]) -> tuple[str, float] | None:
    preferred_tokens = ("累计同比", "累计增长", "累计增速", "同比增长", "同比", "增速")
    for token in preferred_tokens:
        for key, value in record.items():
            key_text = normalize_text(key)
            if not key_text or token not in key_text:
                continue
            number = safe_float(value)
            if number is not None:
                return (key_text, number)
    return None


def evaluate_shovel_capex_hard_rule(macro_data: dict[str, Any] | None) -> dict[str, Any]:
    industry_fai = (macro_data or {}).get("industry_fai", {}) if isinstance((macro_data or {}).get("industry_fai", {}), dict) else {}
    status = normalize_text(industry_fai.get("status")).lower()
    data = industry_fai.get("data", {}) if isinstance(industry_fai.get("data", {}), dict) else {}
    latest_records = data.get("latest_records") or []

    latest_yoy_pct = safe_float(data.get("latest_yoy_pct"))
    selected_indicator = normalize_text(data.get("selected_indicator"))
    if latest_yoy_pct is None:
        for record in latest_records:
            if not isinstance(record, dict):
                continue
            extracted = _extract_indicator_from_record(record)
            if extracted:
                selected_indicator, latest_yoy_pct = extracted
                break

    if latest_yoy_pct is None:
        return {
            "pass": False,
            "reason": "[FATAL: 强制防伪失败] 无法获取真实下游固定资产投资(Capex)数据。拒绝听信研报小作文脑补，直接阻断！",
            "selected_indicator": selected_indicator,
            "latest_yoy_pct": None,
            "status": "missing",
        }
    if latest_yoy_pct <= 0:
        return {
            "pass": False,
            "reason": f"[FATAL: 强制扩产证伪] 下游Capex累计同比={latest_yoy_pct:.2f}% 未转正。无真实扩产，铲子股逻辑破产！",
            "selected_indicator": selected_indicator,
            "latest_yoy_pct": latest_yoy_pct,
            "status": "ok",
        }
    return {
        "pass": True,
        "reason": f"下游Capex累计同比={latest_yoy_pct:.2f}% 已转正",
        "selected_indicator": selected_indicator,
        "latest_yoy_pct": latest_yoy_pct,
        "status": status or "ok",
    }


def evaluate_business_simplicity(
    eco_circle: str,
    tier0_autofill_result: dict[str, Any] | None,
    llm_business_eval: str | None = None,
) -> dict[str, Any]:
    autofill = _autofill_map(tier0_autofill_result)
    cost_candidate = autofill.get("cost_structure", {}).get("candidate_value") or {}
    cost_summary = normalize_text(cost_candidate.get("summary")) if isinstance(cost_candidate, dict) else ""
    semantic_check = cost_candidate.get("semantic_check", {}) if isinstance(cost_candidate, dict) else {}
    semantic_pass = bool(semantic_check.get("semantic_pass"))
    corpus = " ".join(_collect_autofill_texts(tier0_autofill_result))
    suspicious = [term for term in SUSPICIOUS_DIVERSIFICATION_TERMS if term in corpus]
    llm_eval_text = normalize_text(llm_business_eval)

    if llm_eval_text and any(word in llm_eval_text for word in ("多元化", "跨界", "无法极简计算")):
        return {"status": "kill", "reason": "大模型 RAG 判定业务不纯，无法抽象为极简数学公式！", "formula": ""}

    if suspicious:
        return {
            "status": "kill",
            "formula": "",
            "reason": f"检测到并购/理财/跨界痕迹: {', '.join(suspicious[:4])}",
        }

    if eco_circle == "core_resource":
        formula = "(主营售价 - 刚性成本) * 产能"
    elif eco_circle == "rigid_shovel":
        formula = "(过路费/服务单价 - 固定制造成本) * 产能"
    elif eco_circle == "core_military":
        formula = "(型号配套单价 - 固定研制/制造成本) * 批产规模"
    else:
        formula = ""

    if semantic_pass and formula:
        return {
            "status": "pass",
            "formula": formula,
            "reason": f"已通过成本语义校验，可抽象为 {formula}",
        }

    if cost_summary:
        return {
            "status": "block",
            "formula": formula,
            "reason": "已命中成本段落，但仍无法稳定抽象为极简利润公式",
        }

    return {
        "status": "block",
        "formula": formula,
        "reason": "缺少可核验的成本结构段落，无法证明业务极简",
    }


def scan_moat_dictionary(
    eco_circle: str,
    tier0_autofill_result: dict[str, Any] | None,
) -> dict[str, Any]:
    dictionary = load_yaml_config("moat_dictionary.yaml").get("categories", {}) or {}
    category_items = dictionary.get(eco_circle, []) or []
    corpus = " ".join(_collect_autofill_texts(tier0_autofill_result))
    hits: list[dict[str, Any]] = []
    for item in category_items:
        label = normalize_text(item.get("label"))
        matched = [keyword for keyword in item.get("keywords", []) if normalize_text(keyword) and normalize_text(keyword) in corpus]
        if matched:
            hits.append({"label": label, "matched_keywords": matched[:5]})
    return {
        "hits": hits,
        "score_bonus": min(3, len(hits)),
        "matched": bool(hits),
    }
