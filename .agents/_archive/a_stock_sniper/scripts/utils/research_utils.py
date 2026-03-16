"""Shared config, evidence, and gating helpers for the sniper workflow."""
from __future__ import annotations

import datetime
import math
from pathlib import Path
from typing import Any

import yaml


CACHE_STALE_HOURS = 24


SKILL_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = SKILL_DIR / "config"

FIELD_TO_DATASET = {
    "actual_controller": ("scan", "company_profile"),
    "revenue_breakdown": ("scan", "revenue_breakdown"),
    "net_profit": ("scan", "income_statement"),
    "total_equity": ("scan", "balance_sheet"),
    "pb_ratio": ("scan", "valuation_history"),
    "pb_percentile": ("scan", "valuation_history"),
    "stock_price": ("scan", "stock_kline"),
    "market_cap": ("scan", "realtime_quote"),
    "spot_price": ("commodity", "spot_price"),
    "industry_inventory": ("commodity", "inventory"),
    "capex_investment": ("macro", "fixed_asset_investment"),
    "mineral_rights": (None, None),
    "license_moat": (None, None),
    "cost_structure": (None, None),
    "capacity": (None, None),
}

STATE_OWNERSHIP_SCORES = {
    "central_soe": 25,
    "provincial_soe": 22,
    "local_soe": 15,
    "state_backed_unclear": 5,
    "platform_unknown": 5,
    "private": 0,
    "unknown": 0,
}

CENTRAL_SOE_KEYWORDS = (
    "国务院国资委",
    "中央企业",
    "央企",
    "财政部",
    "国务院",
    "中央汇金",
    "中国盐业集团",
    "中盐集团",
    "中国石油",
    "中国石化",
    "中国铝业",
    "中国五矿",
    "中国兵器",
    "中国航空",
    "中国船舶",
    "中国中车",
    "中国建筑",
    "中国中铁",
    "中国交建",
    "中国能建",
)
PROVINCIAL_SOE_KEYWORDS = (
    "省国资委",
    "省人民政府",
    "省财政厅",
    "省属国资",
)
LOCAL_SOE_KEYWORDS = (
    "市国资委",
    "市人民政府",
    "区国资委",
    "县国资委",
    "地方国资",
    "地方国资管理机构",
    "地方国有控股",
    "地方国资平台",
    "国有法人",
)
STATE_BACKED_KEYWORDS = (
    "国资",
    "国有",
    "国有资本",
    "国有独资",
    "国有控股",
    "国有资产监督管理",
)
PRIVATE_KEYWORDS = (
    "自然人",
    "个人",
    "家族",
    "合伙企业",
    "投资管理",
    "私募",
    "民营",
)
PLATFORM_HINT_KEYWORDS = (
    "集团",
    "控股",
    "投资",
    "资本",
    "发展",
    "运营",
)


def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_yaml_config(filename: str) -> dict[str, Any]:
    path = CONFIG_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_source_registry() -> dict[str, Any]:
    return load_yaml_config("source_registry.yaml")


def load_industry_mapping() -> dict[str, Any]:
    return load_yaml_config("industry_mapping.yaml")


def load_crocodile_discipline() -> dict[str, Any]:
    return load_yaml_config("crocodile_discipline.yaml")


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in set(base) | set(override):
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = _deep_merge_dict(base_value, override_value)
        elif key in override:
            merged[key] = override_value
        else:
            merged[key] = base_value
    return merged


def get_crocodile_mode_config(mode: str | None = None) -> dict[str, Any]:
    discipline = load_crocodile_discipline()
    defaults = discipline.get("defaults", {}) or {}
    overrides = (discipline.get("modes", {}) or {}).get(normalize_text(mode), {}) or {}
    return _deep_merge_dict(defaults, overrides)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    normalized = normalize_text(text)
    return any(keyword in normalized for keyword in keywords)


def classify_state_ownership(
    stock_code: str,
    controller_text: str,
    *,
    tier0_item: dict[str, Any] | None = None,
    company_name_hints: list[str] | None = None,
    industry_mapping: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mapping = industry_mapping or load_industry_mapping()
    override = (mapping.get("company_overrides", {}) or {}).get(str(stock_code), {}) or {}
    evidence = (tier0_item or {}).get("evidence", {}) if isinstance(tier0_item, dict) else {}

    controller_text = normalize_text(controller_text)
    company_name_hints = _dedupe_texts(company_name_hints or [])
    controller_nature = normalize_text(evidence.get("controller_nature") or override.get("controller_nature"))
    controller_type = normalize_text(evidence.get("controller_type") or override.get("controller_type"))
    ultimate_controller = normalize_text(evidence.get("ultimate_controller") or override.get("ultimate_controller"))
    override_category = normalize_text(override.get("ownership_type")).lower()

    basis_parts = [part for part in (controller_text, controller_nature, controller_type, ultimate_controller) if part]
    basis_text = "；".join(dict.fromkeys(basis_parts))
    corpus = " ".join(part for part in basis_parts if part)
    corpus = normalize_text(f"{corpus} {evidence.get('snippet', '')}")

    def build_result(category: str, label: str, strict_pass: bool, gate_verdict: str, reason: str) -> dict[str, Any]:
        return {
            "category": category,
            "label": label,
            "score": STATE_OWNERSHIP_SCORES.get(category, 0),
            "strict_pass": strict_pass,
            "state_owned": category not in {"private", "unknown", "platform_unknown"},
            "gate_verdict": gate_verdict,
            "reason": reason,
            "basis_text": basis_text or controller_text,
            "controller_nature": controller_nature,
            "controller_type": controller_type,
            "ultimate_controller": ultimate_controller,
        }

    if override_category == "central_soe":
        return build_result("central_soe", "央企控股", True, "PASS", f"公司级映射识别为央企控股：{basis_text or controller_text}")
    if override_category == "provincial_soe":
        return build_result("provincial_soe", "省国资控股", True, "PASS", f"公司级映射识别为省国资控股：{basis_text or controller_text}")
    if override_category == "local_soe":
        return build_result(
            "local_soe",
            "地方国资控股",
            False,
            "KILL",
            f"公司级映射识别为地方国资控股，但不属于仅央企/省国资委白名单：{basis_text or controller_text}",
        )
    if override_category == "private":
        return build_result("private", "民企/非国资", False, "KILL", f"公司级映射识别为非国资：{basis_text or controller_text}")

    if _contains_any(corpus, PRIVATE_KEYWORDS):
        return build_result("private", "民企/非国资", False, "KILL", f"控制链条命中民企特征：{basis_text or controller_text}")
    if _contains_any(corpus, CENTRAL_SOE_KEYWORDS):
        return build_result("central_soe", "央企控股", True, "PASS", f"控制链条命中央企特征：{basis_text or controller_text}")
    if _contains_any(corpus, PROVINCIAL_SOE_KEYWORDS):
        return build_result("provincial_soe", "省国资控股", True, "PASS", f"控制链条命中省国资特征：{basis_text or controller_text}")
    if _contains_any(corpus, LOCAL_SOE_KEYWORDS):
        return build_result(
            "local_soe",
            "地方国资控股",
            False,
            "KILL",
            f"已识别为地方国资控股，但不属于仅央企/省国资委白名单：{basis_text or controller_text}",
        )
    if _contains_any(corpus, STATE_BACKED_KEYWORDS):
        return build_result(
            "state_backed_unclear",
            "国资背景待穿透",
            False,
            "BLOCK",
            f"存在国资背景描述，但最终控制层级仍需穿透确认：{basis_text or controller_text}",
        )

    self_referential = bool(controller_text) and any(name and name in controller_text for name in company_name_hints)
    if self_referential or _contains_any(controller_text, PLATFORM_HINT_KEYWORDS):
        return build_result(
            "platform_unknown",
            "平台型控制主体待穿透",
            False,
            "BLOCK",
            f"控制主体更像集团/平台名称，需继续穿透到最终国资主体：{controller_text}",
        )

    return build_result("unknown", "控制关系不明", False, "BLOCK", "无法从当前证据自动确认控制人属性")


def extract_primary_industry(company_profile: dict[str, Any]) -> str:
    for key in ("行业", "所属行业", "申万行业", "申万一级行业", "申万二级行业", "中信行业", "中信一级行业"):
        value = normalize_text(company_profile.get(key))
        if value:
            return value
    return ""


def extract_context_text(company_profile: dict[str, Any]) -> str:
    parts = []
    for key in (
        "行业",
        "所属行业",
        "申万行业",
        "申万一级行业",
        "申万二级行业",
        "中信行业",
        "中信一级行业",
        "主营业务",
        "经营范围",
        "公司名称",
        "证券简称",
        "产品类型",
    ):
        value = normalize_text(company_profile.get(key))
        if value:
            parts.append(value)
    return " ".join(dict.fromkeys(parts))


def _match_eco_circle_from_text(text: str, mapping: dict[str, Any]) -> tuple[str, dict[str, Any], str] | None:
    normalized = normalize_text(text)
    if not normalized:
        return None

    exact_terms = {token for token in normalized.split() if token}
    matches: list[tuple[int, int, int, str, dict[str, Any], str]] = []
    for eco_circle, cfg in mapping.get("eco_circles", {}).items():
        for candidate in cfg.get("industries", []):
            if candidate and candidate in normalized:
                exact_bonus = 100 if candidate in exact_terms else 0
                matches.append((exact_bonus, len(candidate), len(normalized), eco_circle, cfg, candidate))
    if not matches:
        return None
    matches.sort(reverse=True)
    _, _, _, eco_circle, cfg, candidate = matches[0]
    return eco_circle, cfg, candidate


def _dedupe_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = normalize_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def determine_eco_context(
    stock_code: str,
    company_profile: dict[str, Any],
    industry_mapping: dict[str, Any] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    mapping = industry_mapping or load_industry_mapping()
    overrides = mapping.get("company_overrides", {})
    override = overrides.get(str(stock_code), {})
    industry_text = extract_primary_industry(company_profile)
    context_text = extract_context_text(company_profile)
    extra_context = " ".join(_dedupe_texts(extra_texts or []))

    for match_source, text in (
        ("extra_context", extra_context),
        ("context_text", context_text),
        ("industry_keyword", industry_text),
    ):
        matched = _match_eco_circle_from_text(text, mapping)
        if matched:
            eco_circle, cfg, commodity = matched
            return {
                "stock_code": stock_code,
                "industry_text": industry_text or commodity,
                "eco_circle": eco_circle,
                "eco_label": cfg.get("label", eco_circle),
                "four_signal_mode": cfg.get("four_signal_mode", "unknown"),
                "commodity": commodity,
                "mandatory_evidence": cfg.get("mandatory_evidence", []),
                "matched_by": match_source,
                "out_of_bounds": False,
            }

    if override:
        eco_circle = override.get("eco_circle")
        circle_cfg = mapping.get("eco_circles", {}).get(eco_circle, {})
        return {
            "stock_code": stock_code,
            "industry_text": industry_text or override.get("sub_industry", ""),
            "eco_circle": eco_circle,
            "eco_label": circle_cfg.get("label", eco_circle or "unknown"),
            "four_signal_mode": override.get("four_signal_mode", circle_cfg.get("four_signal_mode", "unknown")),
            "commodity": override.get("commodity") or override.get("sub_industry") or industry_text or "",
            "mandatory_evidence": circle_cfg.get("mandatory_evidence", []),
            "matched_by": "company_override",
            "out_of_bounds": False,
        }

    out_of_bounds_examples = mapping.get("out_of_bounds", {}).get("examples", [])
    return {
        "stock_code": stock_code,
        "industry_text": industry_text,
        "eco_circle": "unknown",
        "eco_label": "越界标的" if any(x in industry_text for x in out_of_bounds_examples) else "未知圈层",
        "four_signal_mode": "unknown",
        "commodity": industry_text or "",
        "mandatory_evidence": [],
        "matched_by": "unmatched",
        "out_of_bounds": any(x in industry_text for x in out_of_bounds_examples),
    }


def get_tier0_item(tier0_prep: dict[str, Any] | None, field_name: str) -> dict[str, Any] | None:
    if not tier0_prep:
        return None
    checklist = tier0_prep.get("checklist", {}).get("checklist", [])
    target_field = normalize_text(field_name)
    for item in checklist:
        if item.get("field") == target_field:
            return item
    return None


def is_real_success(status: str) -> bool:
    normalized = normalize_text(status).lower()
    return normalized.startswith("ok")


def is_stale_status(status: str) -> bool:
    normalized = normalize_text(status).lower()
    return "stale" in normalized


def is_partial_success(status: str) -> bool:
    normalized = normalize_text(status).lower()
    return normalized.startswith("partial") or normalized.startswith("manual_required")


def detect_data_freshness(scan_data: dict[str, Any]) -> dict[str, Any]:
    """Scan all fields in scan_data for freshness, return summary."""
    stale_fields: list[str] = []
    fresh_fields: list[str] = []
    unknown_fields: list[str] = []

    for field_name, result in scan_data.items():
        if field_name.startswith("_") or not isinstance(result, dict):
            continue
        status = normalize_text(result.get("status", "")).lower()
        fetch_ts = result.get("fetch_timestamp", "")

        if "stale" in status:
            stale_fields.append(field_name)
            continue

        if fetch_ts:
            try:
                ts = datetime.datetime.fromisoformat(str(fetch_ts))
                age_hours = (datetime.datetime.now() - ts).total_seconds() / 3600
                if age_hours > CACHE_STALE_HOURS:
                    stale_fields.append(field_name)
                else:
                    fresh_fields.append(field_name)
            except (ValueError, TypeError):
                unknown_fields.append(field_name)
        else:
            unknown_fields.append(field_name)

    return {
        "stale_fields": sorted(stale_fields),
        "fresh_fields": sorted(fresh_fields),
        "unknown_freshness": sorted(unknown_fields),
        "stale_count": len(stale_fields),
        "overall_fresh": len(stale_fields) == 0,
    }


def summarize_dataset_status(result: dict[str, Any] | None) -> tuple[str, dict[str, Any]]:
    if not result:
        return ("missing", {})
    status = normalize_text(result.get("status", "missing"))
    evidence = result.get("evidence") or {}
    if is_real_success(status):
        return ("collected", evidence)
    if normalize_text(status).lower().startswith("not_applicable"):
        return ("not_applicable", evidence)
    if is_partial_success(status):
        return ("partial", evidence)
    return ("missing", evidence)


def get_manifest_field_entry(source_manifest: dict[str, Any] | None, field_name: str) -> dict[str, Any]:
    if not source_manifest:
        return {}
    field_map = source_manifest.get("field_map", {})
    if isinstance(field_map, dict) and field_name in field_map:
        return field_map.get(field_name) or {}
    fields = source_manifest.get("fields", [])
    if isinstance(fields, dict):
        item = fields.get(field_name)
        return item if isinstance(item, dict) else {}
    for item in fields:
        if item.get("field_name") == field_name:
            return item
    return {}


def manifest_field_status(source_manifest: dict[str, Any] | None, field_name: str) -> str:
    return normalize_text(get_manifest_field_entry(source_manifest, field_name).get("status", "missing")).lower()


def is_usable_status(status: str) -> bool:
    normalized = normalize_text(status).lower()
    return normalized.startswith("ok") or normalized in {"collected", "verified_tier0"}


def evaluate_signal_health(
    eco_context: dict[str, Any] | None,
    source_manifest: dict[str, Any] | None,
    commodity_data: dict[str, Any] | None,
    macro_data: dict[str, Any] | None,
) -> dict[str, Any]:
    eco = eco_context or {}
    source_manifest = source_manifest or {}
    commodity_data = commodity_data or {}
    macro_data = macro_data or {}
    mode = eco.get("four_signal_mode") or "unknown"

    spot_status = manifest_field_status(source_manifest, "spot_price")
    futures_status = normalize_text(commodity_data.get("futures", {}).get("status", "")).lower()
    inventory_status = manifest_field_status(source_manifest, "industry_inventory")
    pb_status = manifest_field_status(source_manifest, "pb_ratio")
    industry_fai_status = normalize_text(macro_data.get("industry_fai", {}).get("status", "")).lower()
    capex_status = manifest_field_status(source_manifest, "capex_investment")

    price_ready = (
        is_usable_status(spot_status)
        or spot_status.startswith("partial")
        or is_usable_status(futures_status)
        or futures_status.startswith("partial")
    )
    inventory_ready = is_usable_status(inventory_status)
    pb_ready = is_usable_status(pb_status)
    capex_ready = is_usable_status(industry_fai_status) or is_usable_status(capex_status)

    signal_items = {
        "price_signal": {
            "label": "现货/期货价格",
            "status": spot_status or futures_status or "missing",
            "ready": price_ready,
            "detail": f"spot={spot_status or 'missing'}; futures={futures_status or 'missing'}",
        },
        "inventory_signal": {
            "label": "行业库存",
            "status": inventory_status or "missing",
            "ready": inventory_ready,
            "detail": inventory_status or "missing",
        },
        "capex_signal": {
            "label": "行业/项目Capex",
            "status": industry_fai_status or capex_status or "missing",
            "ready": capex_ready,
            "detail": f"industry_fai={industry_fai_status or 'missing'}; manifest_capex={capex_status or 'missing'}",
        },
        "pb_signal": {
            "label": "PB估值水位",
            "status": pb_status or "missing",
            "ready": pb_ready,
            "detail": pb_status or "missing",
        },
    }

    if mode == "shovel_play":
        core_names = ["price_signal", "capex_signal"]
        auxiliary_names = ["inventory_signal", "pb_signal"]
    elif mode == "military":
        core_names = ["capex_signal", "pb_signal"]
        auxiliary_names = ["price_signal", "inventory_signal"]
    else:
        core_names = ["price_signal", "capex_signal"]
        auxiliary_names = ["inventory_signal", "pb_signal"]

    core_missing = [name for name in core_names if not signal_items[name]["ready"]]
    auxiliary_missing = [name for name in auxiliary_names if not signal_items[name]["ready"]]
    stale_fields = source_manifest.get("summary", {}).get("stale_fields", [])

    return {
        "mode": mode,
        "signals": signal_items,
        "core_names": core_names,
        "auxiliary_names": auxiliary_names,
        "core_missing": core_missing,
        "auxiliary_missing": auxiliary_missing,
        "core_ready": len(core_missing) == 0,
        "auxiliary_ready": len(auxiliary_missing) == 0,
        "has_stale_signal": any(name in " ".join(stale_fields) for name in ("spot", "inventory", "fixed_asset_investment", "industry_fai")),
        "stale_fields": stale_fields,
    }


def build_source_manifest(
    stock_code: str,
    *,
    scan_data: dict[str, Any] | None = None,
    tier0_prep: dict[str, Any] | None = None,
    tier0_autofill_result: dict[str, Any] | None = None,
    pdf_index_result: dict[str, Any] | None = None,
    commodity_data: dict[str, Any] | None = None,
    macro_data: dict[str, Any] | None = None,
    eco_context: dict[str, Any] | None = None,
    source_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = source_registry or load_source_registry()
    datasets = {
        "scan": scan_data or {},
        "commodity": commodity_data or {},
        "macro": macro_data or {},
    }
    manifest_fields = []
    field_map: dict[str, dict[str, Any]] = {}
    missing_tier0 = []
    partial_fields = []
    pdf_hint_fields = []
    pdf_field_hits = (pdf_index_result or {}).get("field_hits", {})
    autofill_map = {
        item.get("field_name"): item
        for item in (tier0_autofill_result or {}).get("items", [])
        if isinstance(item, dict) and item.get("field_name")
    }
    dataset_paths = {
        ("scan", "company_profile"): "akshare_scan.json -> company_profile",
        ("scan", "revenue_breakdown"): "akshare_scan.json -> revenue_breakdown",
        ("scan", "income_statement"): "akshare_scan.json -> income_statement",
        ("scan", "balance_sheet"): "akshare_scan.json -> balance_sheet",
        ("scan", "valuation_history"): "akshare_scan.json -> valuation_history",
        ("scan", "stock_kline"): "akshare_scan.json -> stock_kline",
        ("scan", "realtime_quote"): "akshare_scan.json -> realtime_quote",
        ("commodity", "spot_price"): "commodity_scan.json -> spot_price",
        ("commodity", "inventory"): "commodity_scan.json -> inventory",
        ("macro", "fixed_asset_investment"): "macro_scan.json -> fixed_asset_investment",
    }

    def _manifest_candidate(
        *,
        status: str,
        actual_tier: int | None,
        source_type: str,
        source_path: str,
        value: Any = None,
        confidence: str = "low",
        description: str = "",
        fetch_time: str = "",
        snippet: str = "",
        notes: list[str] | None = None,
        source_url: str = "",
        pages: Any = None,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "actual_tier": actual_tier,
            "source_type": source_type,
            "source_path": source_path,
            "source_url": source_url,
            "value": value,
            "confidence": confidence or "low",
            "description": description,
            "fetch_time": fetch_time,
            "snippet": snippet,
            "pages": pages,
            "notes": list(notes or []),
        }

    source_priority = {
        "verified_tier0": 0,
        "collected": 1,
        "autofilled_tier0_hint": 2,
        "tier0_pdf_hint": 3,
        "partial_autofill": 4,
        "partial": 5,
        "pending_tier0": 6,
        "not_applicable": 7,
        "missing": 8,
    }

    for field_name, config in registry.get("fields", {}).items():
        tier0_item = get_tier0_item(tier0_prep, field_name)
        required_tier = int(config.get("required_tier", 9))
        notes = []
        candidates: list[dict[str, Any]] = []

        if tier0_item and tier0_item.get("verified") and tier0_item.get("evidence"):
            evidence = tier0_item["evidence"]
            candidates.append(
                _manifest_candidate(
                    status="verified_tier0",
                    actual_tier=0,
                    source_type=normalize_text(evidence.get("source_type")) or "tier0_verified",
                    source_path=normalize_text(evidence.get("pdf_name"))
                    or f"tier0_checklist.json -> {field_name}",
                    source_url=normalize_text(evidence.get("source_url")),
                    value=evidence.get("value"),
                    confidence=normalize_text(evidence.get("confidence")) or "high",
                    description=normalize_text(evidence.get("description")),
                    fetch_time=normalize_text(evidence.get("fetch_time")),
                    snippet=normalize_text(evidence.get("snippet")),
                    pages=evidence.get("page_no"),
                )
            )

        autofill_item = autofill_map.get(field_name)
        if autofill_item:
            autofill_status = normalize_text(autofill_item.get("review_status")) or "autofilled_tier0_hint"
            manifest_status = "partial_autofill" if autofill_status == "partial_autofill" else "autofilled_tier0_hint"
            candidates.append(
                _manifest_candidate(
                    status=manifest_status,
                    actual_tier=0,
                    source_type=normalize_text(autofill_item.get("value_source")) or "tier0_autofill",
                    source_path=f"tier0_autofill.json -> {field_name}",
                    value=autofill_item.get("candidate_value"),
                    confidence=normalize_text(autofill_item.get("confidence")) or "medium",
                    description="Tier 0 autofill candidate",
                    fetch_time=normalize_text((tier0_autofill_result or {}).get("generated_at")),
                    snippet=normalize_text(autofill_item.get("snippet")),
                    pages=autofill_item.get("pages"),
                    notes=[autofill_status] if autofill_status else [],
                )
            )

        dataset_type, dataset_key = FIELD_TO_DATASET.get(field_name, (None, None))
        if dataset_type and dataset_key:
            result = datasets.get(dataset_type, {}).get(dataset_key)
            dataset_status, evidence = summarize_dataset_status(result)
            if dataset_status != "missing":
                data_value = result.get("data") if isinstance(result, dict) else None
                candidates.append(
                    _manifest_candidate(
                        status=dataset_status,
                        actual_tier=evidence.get("source_tier"),
                        source_type=normalize_text(evidence.get("source_type")) or dataset_key,
                        source_path=dataset_paths.get((dataset_type, dataset_key), f"{dataset_type}:{dataset_key}"),
                        source_url=normalize_text(evidence.get("source_url")),
                        value=data_value,
                        confidence=normalize_text(evidence.get("confidence")) or ("medium" if dataset_status == "collected" else "low"),
                        description=normalize_text(evidence.get("description")),
                        fetch_time=normalize_text(evidence.get("fetch_time")),
                        snippet=normalize_text(evidence.get("snippet") or evidence.get("description")),
                    )
                )

        pdf_hint = pdf_field_hits.get(field_name, {})
        if required_tier == 0 and not tier0_item:
            notes.append("Tier 0 checklist item missing")
        if required_tier == 0 and pdf_hint.get("matched"):
            candidates.append(
                _manifest_candidate(
                    status="tier0_pdf_hint",
                    actual_tier=0,
                    source_type="cninfo_pdf_keyword_hit",
                    source_path=f"pdf_index/tier0_field_hits.json -> {field_name}",
                    value=None,
                    confidence="medium",
                    description=f"PDF keyword hits: {', '.join(pdf_hint.get('matched_keywords', []))}",
                    fetch_time=now_ts(),
                    snippet=normalize_text(((pdf_hint.get("hits") or [{}])[0]).get("snippet")),
                    pages=sorted({hit.get("page") for hit in pdf_hint.get("hits", []) if hit.get("page")}),
                    notes=["requires_human_review"],
                )
            )

        if not candidates:
            candidates.append(
                _manifest_candidate(
                    status="missing",
                    actual_tier=None,
                    source_type="missing",
                    source_path="",
                    notes=["no_source_collected"],
                )
            )

        candidates.sort(key=lambda item: source_priority.get(item.get("status", "missing"), 99))
        selected = dict(candidates[0])

        if tier0_item and not tier0_item.get("verified") and required_tier == 0:
            if selected["status"] in {"collected", "missing", "autofilled_tier0_hint", "partial_autofill"}:
                selected["status"] = "pending_tier0"
            notes.append("Tier 0 required but not yet verified")

        if required_tier == 0 and selected["status"] != "verified_tier0":
            missing_tier0.append(field_name)

        if selected["status"] in {"partial", "partial_autofill", "pending_tier0", "tier0_pdf_hint", "autofilled_tier0_hint"}:
            partial_fields.append(field_name)
        if selected["status"] == "tier0_pdf_hint":
            pdf_hint_fields.append(field_name)

        entry = {
            "field_name": field_name,
            "required_tier": required_tier,
            "actual_tier": selected.get("actual_tier"),
            "status": selected.get("status", "missing"),
            "confidence": selected.get("confidence", "low"),
            "source_type": selected.get("source_type", ""),
            "source_path": selected.get("source_path", ""),
            "source_url": selected.get("source_url", ""),
            "value": selected.get("value"),
            "description": selected.get("description", ""),
            "fetch_time": selected.get("fetch_time", ""),
            "snippet": selected.get("snippet", ""),
            "pages": selected.get("pages"),
            "notes": list(dict.fromkeys([*selected.get("notes", []), *notes])),
            "candidates": candidates,
        }
        manifest_fields.append(entry)
        field_map[field_name] = entry

    mandatory_evidence = list((eco_context or {}).get("mandatory_evidence", []))
    manual_blocks = []
    if "downstream_capex" in mandatory_evidence:
        manual_blocks.append("downstream_capex")

    # Detect stale fields across all data sources
    stale_fields: list[str] = []
    for dataset_name, dataset in datasets.items():
        if not isinstance(dataset, dict):
            continue
        for field_name, result in dataset.items():
            if field_name.startswith("_") or not isinstance(result, dict):
                continue
            status_str = normalize_text(result.get("status", "")).lower()
            if "stale" in status_str:
                stale_fields.append(f"{dataset_name}.{field_name}")

    return {
        "stock_code": stock_code,
        "generated_at": now_ts(),
        "eco_context": eco_context or {},
        "fields": manifest_fields,
        "field_map": field_map,
        "summary": {
            "tier0_required_missing": sorted(set(missing_tier0)),
            "partial_fields": sorted(set(partial_fields)),
            "tier0_pdf_hints": sorted(set(pdf_hint_fields)),
            "mandatory_evidence_missing": manual_blocks,
            "stale_fields": sorted(set(stale_fields)),
        },
    }


def extract_latest_revenue_snapshot(revenue_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not revenue_records:
        return []
    valid_records = [r for r in revenue_records if normalize_text(r.get("报告日期"))]
    if not valid_records:
        return []
    latest_date = max(normalize_text(r.get("报告日期")) for r in valid_records)
    latest_records = [r for r in valid_records if normalize_text(r.get("报告日期")) == latest_date]

    for category in ("按产品分类", "按行业分类"):
        selected = [r for r in latest_records if normalize_text(r.get("分类类型")) == category]
        if selected:
            return selected
    return latest_records


def extract_latest_revenue_terms(revenue_records: list[dict[str, Any]], limit: int = 10) -> list[str]:
    snapshot = extract_latest_revenue_snapshot(revenue_records)
    if not snapshot:
        return []

    name_col = _pick_revenue_col(snapshot, ("主营构成", "产品名称", "分类名称", "名称"), contains=("构成", "产品", "名称"))
    ratio_col = _pick_revenue_col(snapshot, ("收入比例", "营业收入占比", "占比"), contains=("比例", "占比"))
    revenue_col = _pick_revenue_col(snapshot, ("主营收入", "营业收入"), contains=("收入",))
    type_col = _pick_revenue_col(snapshot, ("分类类型", "分类方向", "类型"), contains=("分类", "类型"))

    ranked: list[tuple[float, str]] = []
    for record in snapshot:
        name = normalize_text(record.get(name_col or ""))
        category = normalize_text(record.get(type_col or ""))
        if not name or any(token in name for token in ("其他", "合计", "国内", "国外")):
            continue
        ratio = safe_float(record.get(ratio_col or ""))
        if ratio is None:
            revenue = safe_float(record.get(revenue_col or "")) or 0.0
            ratio = revenue
        if ratio > 1:
            ratio = ratio / 100.0 if ratio <= 100 else ratio
        category_bonus = 0.05 if "按产品" in category else 0.0
        ranked.append((ratio + category_bonus, name))

    ranked.sort(reverse=True)
    return _dedupe_texts([name for _, name in ranked[:limit]])


def _pick_revenue_col(records: list[dict[str, Any]], candidates: tuple[str, ...], contains: tuple[str, ...] = ()) -> str | None:
    """Find the first matching column key across revenue records."""
    if not records:
        return None
    keys = set()
    for r in records:
        keys.update(r.keys())
    for c in candidates:
        if c in keys:
            return c
    for k in keys:
        if any(token in str(k) for token in contains):
            return str(k)
    return None


def assess_business_purity(revenue_records: list[dict[str, Any]]) -> dict[str, Any]:
    if not revenue_records:
        return {
            "latest_report_date": "",
            "top_segment": "",
            "top_ratio": 0.0,
            "pass": False,
            "data_quality": "no_revenue_data",
        }

    # Robust column name probing
    date_col = _pick_revenue_col(revenue_records, ("报告日期", "报告期", "日期"), contains=("日期", "报告"))
    type_col = _pick_revenue_col(revenue_records, ("分类类型", "分类方向", "类型"), contains=("分类", "类型"))
    name_col = _pick_revenue_col(revenue_records, ("主营构成", "产品名称", "分类名称", "名称"), contains=("构成", "产品", "名称"))
    ratio_col = _pick_revenue_col(revenue_records, ("收入比例", "营业收入占比", "占比"), contains=("比例", "占比"))

    if not date_col:
        return {
            "latest_report_date": "",
            "top_segment": "",
            "top_ratio": 0.0,
            "pass": False,
            "data_quality": "column_mismatch_date",
            "available_columns": list(set().union(*(r.keys() for r in revenue_records[:3]))),
        }

    valid_records = [r for r in revenue_records if normalize_text(r.get(date_col))]
    if not valid_records:
        return {
            "latest_report_date": "",
            "top_segment": "",
            "top_ratio": 0.0,
            "pass": False,
            "data_quality": "no_valid_dated_records",
        }
    latest_date = max(normalize_text(r.get(date_col)) for r in valid_records)
    latest_records = [r for r in valid_records if normalize_text(r.get(date_col)) == latest_date]

    categories = ("按行业分类", "按产品分类") if type_col else (None,)
    for category in categories:
        if category is not None and type_col:
            snapshot = [r for r in latest_records if normalize_text(r.get(type_col)) == category]
        else:
            snapshot = latest_records
        ranked = []
        for record in snapshot:
            name = normalize_text(record.get(name_col or "主营构成"))
            if not name or "其他" in name or "合计" in name:
                continue
            try:
                ratio = float(record.get(ratio_col or "收入比例"))
            except (TypeError, ValueError):
                continue
            # Auto-detect 0-100 vs 0-1 scale
            if ratio > 1.0:
                ratio = ratio / 100.0
            ranked.append({"segment": name, "ratio": ratio})

        ranked.sort(key=lambda item: item["ratio"], reverse=True)
        if ranked:
            top = ranked[0]
            return {
                "latest_report_date": latest_date,
                "top_segment": top["segment"],
                "top_ratio": round(top["ratio"], 4),
                "pass": top["ratio"] >= 0.7,
                "data_quality": "ok",
            }

    return {
        "latest_report_date": latest_date,
        "top_segment": "",
        "top_ratio": 0.0,
        "pass": False,
        "data_quality": "no_ranked_segments",
    }


def assess_bottom_pattern(
    kline_summary: dict[str, Any],
    valuation_summary: dict[str, Any],
    eco_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latest_close = safe_float(kline_summary.get("latest_close"))
    high_5y = safe_float(kline_summary.get("high_5y"))
    current_vs_high = safe_float(kline_summary.get("current_vs_high"))
    pb = safe_float(valuation_summary.get("pb"))
    pb_percentile = safe_float(valuation_summary.get("pb_percentile"))

    if latest_close is None or high_5y is None or current_vs_high is None or pb is None or pb_percentile is None:
        return {"pass": False, "verdict": "pending", "reason": "缺少K线或PB数据"}

    mode_cfg = get_crocodile_mode_config((eco_context or {}).get("four_signal_mode"))
    bottom_cfg = mode_cfg.get("bottom", {}) or {}
    pass_cfg = bottom_cfg.get("pass", {}) or {}
    caution_cfg = bottom_cfg.get("caution", {}) or {}
    reject_cfg = bottom_cfg.get("reject", {}) or {}

    reject_pb = safe_float(reject_cfg.get("min_pb")) or 1.5
    reject_percentile = safe_float(reject_cfg.get("min_pb_percentile")) or 50.0
    reject_vs_high = safe_float(reject_cfg.get("min_current_vs_high")) or 70.0
    pass_pb = safe_float(pass_cfg.get("max_pb")) or 0.85
    pass_percentile = safe_float(pass_cfg.get("max_pb_percentile")) or 20.0
    pass_vs_high = safe_float(pass_cfg.get("max_current_vs_high")) or 55.0
    caution_pb = safe_float(caution_cfg.get("max_pb")) or 1.0
    caution_percentile = safe_float(caution_cfg.get("max_pb_percentile")) or 35.0
    caution_vs_high = safe_float(caution_cfg.get("max_current_vs_high")) or 65.0

    if current_vs_high >= max(80.0, reject_vs_high + 10.0) and pb_percentile > 75:
        return {
            "pass": False,
            "verdict": "kill",
            "reason": f"股价接近5年高位({current_vs_high:.1f}% of high), PB分位{pb_percentile:.1f}%",
        }
    if current_vs_high <= pass_vs_high and pb <= pass_pb and pb_percentile <= pass_percentile:
        return {
            "pass": True,
            "verdict": "pass",
            "reason": f"股价仅为5年高点的{current_vs_high:.1f}%, PB={pb:.3f}, 分位{pb_percentile:.1f}%",
        }
    if current_vs_high <= caution_vs_high and pb <= reject_pb and pb_percentile <= max(caution_percentile, 65.0):
        return {
            "pass": False,
            "verdict": "caution",
            "reason": f"已较高点明显回撤，但仍未进入破净深折区：距高点{current_vs_high:.1f}%, PB={pb:.3f}, 分位{pb_percentile:.1f}%",
        }
    if (pb > reject_pb and pb_percentile >= reject_percentile) or (
        current_vs_high >= reject_vs_high and pb_percentile >= reject_percentile
    ):
        return {
            "pass": False,
            "verdict": "kill",
            "reason": f"估值或位置仍显著偏高，PB={pb:.3f}, PB分位{pb_percentile:.1f}%, 距高点{current_vs_high:.1f}%",
        }
    if current_vs_high <= caution_vs_high and pb <= caution_pb and pb_percentile <= caution_percentile:
        return {
            "pass": False,
            "verdict": "caution",
            "reason": f"已接近低位但仍未进入破净深折区：距高点{current_vs_high:.1f}%, PB={pb:.3f}, 分位{pb_percentile:.1f}%",
        }
    return {
        "pass": False,
        "verdict": "kill",
        "reason": f"位置与估值仍不具备左侧防守性：距高点{current_vs_high:.1f}%, PB={pb:.3f}, 分位{pb_percentile:.1f}%",
    }


def _extract_numeric_from_record(record: dict[str, Any], exact_keys: tuple[str, ...], contains: tuple[str, ...]) -> float | None:
    for key in exact_keys:
        if key in record:
            num = safe_float(record.get(key))
            if num is not None:
                return num
    for key, value in record.items():
        key_text = normalize_text(key).lower()
        if any(token in key_text for token in contains):
            num = safe_float(value)
            if num is not None:
                return num
    return None


def extract_price_series(commodity_data: dict[str, Any]) -> list[float]:
    spot_entry = commodity_data.get("spot_price", {}) if isinstance(commodity_data.get("spot_price", {}), dict) else {}
    records = spot_entry.get("data", [])
    if not isinstance(records, list):
        return []

    prices: list[float] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        close_value = _extract_numeric_from_record(
            record,
            exact_keys=("收盘价", "close", "最新价", "收盘", "settle"),
            contains=("收盘", "close", "最新", "结算"),
        )
        if close_value is not None:
            prices.append(close_value)
    return prices


def assess_price_trigger(commodity_data: dict[str, Any], eco_context: dict[str, Any] | None = None) -> dict[str, Any]:
    mode_cfg = get_crocodile_mode_config((eco_context or {}).get("four_signal_mode"))
    trigger_cfg = mode_cfg.get("price_trigger", {}) or {}
    min_points = int(trigger_cfg.get("min_points", 20) or 20)
    spot_status = normalize_text(commodity_data.get("spot_price", {}).get("status")).lower()

    prices = extract_price_series(commodity_data)
    futures_data = commodity_data.get("futures", {}).get("data", {}) if isinstance(commodity_data.get("futures", {}), dict) else {}
    latest_close = prices[-1] if prices else safe_float(futures_data.get("latest_close"))
    low_60d = min(prices[-60:]) if prices else safe_float(futures_data.get("low_60d"))
    avg_20d = (sum(prices[-20:]) / 20.0) if len(prices) >= 20 else None
    ret_20d = ((prices[-1] / prices[-20]) - 1.0) if len(prices) >= 20 and prices[-20] not in (None, 0) else None
    rebound_from_low = ((latest_close / low_60d) - 1.0) if latest_close not in (None, 0) and low_60d not in (None, 0) else None
    above_avg20 = ((latest_close / avg_20d) - 1.0) if latest_close not in (None, 0) and avg_20d not in (None, 0) else None

    if latest_close is None or low_60d is None:
        return {
            "verdict": "pending",
            "ready": False,
            "reason": "缺少价格序列，无法判断是否已出现真实价格拐点",
            "metrics": {"points": len(prices)},
        }

    pass_hits = 0
    caution_hits = 0
    checks = [
        (rebound_from_low, safe_float(trigger_cfg.get("rebound_from_60d_low_pct")) or 0.08, safe_float(trigger_cfg.get("caution_rebound_from_60d_low_pct")) or 0.03),
        (above_avg20, safe_float(trigger_cfg.get("above_avg20_pct")) or 0.02, safe_float(trigger_cfg.get("caution_above_avg20_pct")) or 0.00),
        (ret_20d, safe_float(trigger_cfg.get("return_20d_pct")) or 0.05, safe_float(trigger_cfg.get("caution_return_20d_pct")) or 0.00),
    ]
    for value, pass_threshold, caution_threshold in checks:
        if value is None:
            continue
        if value >= pass_threshold:
            pass_hits += 1
            caution_hits += 1
        elif value >= caution_threshold:
            caution_hits += 1

    detail = (
        f"points={len(prices)}, rebound_60d={((rebound_from_low or 0) * 100):.1f}%, "
        f"above_20d_avg={((above_avg20 or 0) * 100):.1f}%, return_20d={((ret_20d or 0) * 100):.1f}%"
    )
    metrics = {
        "points": len(prices),
        "latest_close": latest_close,
        "low_60d": low_60d,
        "avg_20d": avg_20d,
        "rebound_from_low_pct": (rebound_from_low or 0) * 100,
        "above_avg20_pct": (above_avg20 or 0) * 100,
        "return_20d_pct": (ret_20d or 0) * 100,
    }
    if len(prices) >= min_points and pass_hits >= 3:
        if spot_status.startswith("partial"):
            return {
                "verdict": "caution",
                "ready": False,
                "reason": f"仅有期货/代理价格反弹，尚不足以当作现货质变：{detail}",
                "metrics": metrics,
            }
        return {
            "verdict": "pass",
            "ready": True,
            "reason": f"价格已确认脱离冰点并形成上行：{detail}",
            "metrics": metrics,
        }
    if caution_hits >= 2 or (rebound_from_low is not None and rebound_from_low > 0):
        return {
            "verdict": "caution",
            "ready": False,
            "reason": f"价格有所修复，但尚未形成“已反转”触发：{detail}",
            "metrics": metrics,
        }
    return {
        "verdict": "pending",
        "ready": False,
        "reason": f"价格仍在冰点附近或继续走弱：{detail}",
        "metrics": metrics,
    }


def _date_to_sortable(value: Any) -> str:
    text = normalize_text(value)
    return text.replace("-", "").replace("/", "")


def select_latest_record(records: list[dict[str, Any]], date_keys: tuple[str, ...] = ("报告日", "报告日期")) -> dict[str, Any]:
    if not records:
        return {}
    valid_rows = []
    for row in records:
        for key in date_keys:
            if _date_to_sortable(row.get(key)):
                valid_rows.append(row)
                break
    if not valid_rows:
        return records[0]

    def sort_key(row: dict[str, Any]) -> str:
        for key in date_keys:
            candidate = _date_to_sortable(row.get(key))
            if candidate:
                return candidate
        return ""

    return max(valid_rows, key=sort_key)


def extract_market_cap(quote: dict[str, Any]) -> float | None:
    for key, value in quote.items():
        key_text = normalize_text(key)
        if "市值" in key_text and "总" in key_text:
            num = safe_float(value)
            if num is not None:
                return num
    return None


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def extract_first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def get_latest_income_snapshot(records: list[dict[str, Any]]) -> dict[str, Any]:
    row = select_latest_record(records)
    if not row:
        return {"report_date": "", "net_profit": None, "raw": {}}
    report_date = normalize_text(extract_first_value(row, ("报告日", "报告日期")))
    net_profit = extract_first_value(
        row,
        ("归属于母公司所有者的净利润", "归属于母公司股东的净利润", "净利润"),
    )
    return {
        "report_date": report_date,
        "net_profit": safe_float(net_profit),
        "raw": row,
    }


def get_latest_balance_snapshot(records: list[dict[str, Any]]) -> dict[str, Any]:
    row = select_latest_record(records)
    if not row:
        return {"report_date": "", "total_equity": None, "raw": {}}
    report_date = normalize_text(extract_first_value(row, ("报告日", "报告日期")))
    total_equity = extract_first_value(
        row,
        ("归属于母公司股东权益合计", "归属于母公司所有者权益合计", "所有者权益(或股东权益)合计"),
    )
    return {
        "report_date": report_date,
        "total_equity": safe_float(total_equity),
        "raw": row,
    }


def status_to_plan_actual(status: str) -> str:
    normalized = normalize_text(status).lower()
    if normalized.startswith("ok"):
        return "acquired"
    if normalized.startswith("not_applicable"):
        return "not_applicable"
    if normalized.startswith("partial") or normalized.startswith("manual_required"):
        return "partial_or_failed"
    return "partial_or_failed"
