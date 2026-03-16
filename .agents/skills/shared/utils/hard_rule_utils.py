"""Compatibility helpers for legacy hard-rule analyses.

The active pipeline uses universal_gate.py. This module remains only as a
lightweight compatibility layer for Tier 0 text bundles and legacy reference
data.
"""

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

SIMPLICITY_FORMULAS = {
    "compounder": "revenue * sustainable margin",
    "cyclical": "(unit price - unit cost) * normalized volume",
    "turnaround": "repair margin * recovered revenue",
    "asset_play": "asset value - holding discount",
    "special_situation": "scenario value * probability",
}


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
        if not isinstance(group, dict):
            continue
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
    preferred_tokens = ("同比", "增长", "增速")
    for token in preferred_tokens:
        for key, value in record.items():
            key_text = normalize_text(key)
            if key_text and token in key_text:
                number = safe_float(value)
                if number is not None:
                    return key_text, number

    for key, value in record.items():
        number = safe_float(value)
        if number is not None:
            return normalize_text(key), number
    return None


def evaluate_shovel_capex_hard_rule(macro_data: dict[str, Any] | None) -> dict[str, Any]:
    industry_fai = (macro_data or {}).get("industry_fai", {})
    if not isinstance(industry_fai, dict):
        industry_fai = {}
    status = normalize_text(industry_fai.get("status")).lower()
    data = industry_fai.get("data", {})
    if not isinstance(data, dict):
        data = {}
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
            "reason": "capex growth evidence is missing",
            "selected_indicator": selected_indicator,
            "latest_yoy_pct": None,
            "status": "missing",
        }
    if latest_yoy_pct <= 0:
        return {
            "pass": False,
            "reason": f"capex growth remains non-positive at {latest_yoy_pct:.2f}%",
            "selected_indicator": selected_indicator,
            "latest_yoy_pct": latest_yoy_pct,
            "status": "ok",
        }
    return {
        "pass": True,
        "reason": f"capex growth has turned positive at {latest_yoy_pct:.2f}%",
        "selected_indicator": selected_indicator,
        "latest_yoy_pct": latest_yoy_pct,
        "status": status or "ok",
    }


def evaluate_business_simplicity(
    opportunity_type: str,
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
    formula = SIMPLICITY_FORMULAS.get(normalize_text(opportunity_type).lower(), "")

    if llm_eval_text and any(word in llm_eval_text for word in ("多元化", "跨界", "无法简化")):
        return {"status": "kill", "reason": "business model remains too diversified to simplify cleanly", "formula": ""}

    if suspicious:
        return {
            "status": "kill",
            "formula": "",
            "reason": f"detected diversification terms: {', '.join(suspicious[:4])}",
        }

    if semantic_pass and formula:
        return {
            "status": "pass",
            "formula": formula,
            "reason": f"cost semantics support a simple {normalize_text(opportunity_type).lower() or 'business'} formula",
        }

    if cost_summary:
        return {
            "status": "block",
            "formula": formula,
            "reason": "cost structure exists, but the operating formula is still not stable enough",
        }

    return {
        "status": "block",
        "formula": formula,
        "reason": "missing verifiable cost structure evidence",
    }


def scan_moat_dictionary(
    opportunity_type: str,
    tier0_autofill_result: dict[str, Any] | None,
) -> dict[str, Any]:
    dictionary = load_yaml_config("moat_dictionary.yaml").get("categories", {}) or {}
    corpus = " ".join(_collect_autofill_texts(tier0_autofill_result))
    hits: list[dict[str, Any]] = []
    for category_name, item in dictionary.items():
        if not isinstance(item, dict):
            continue
        label = normalize_text(item.get("label")) or normalize_text(category_name) or normalize_text(opportunity_type)
        matched = [keyword for keyword in item.get("keywords", []) if normalize_text(keyword) and normalize_text(keyword) in corpus]
        if matched:
            hits.append({"label": label, "matched_keywords": matched[:5]})
    return {
        "hits": hits,
        "score_bonus": min(3, len(hits)),
        "matched": bool(hits),
    }
