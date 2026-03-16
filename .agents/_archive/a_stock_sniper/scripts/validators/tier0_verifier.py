"""Automatic Tier 0 verification from CNINFO PDF hits plus structured cross-checks."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(SCRIPTS_DIR))

from adapters.tier0_pdf_adapter import FIELD_KEYWORDS  # noqa: E402
from utils.research_utils import now_ts, normalize_text, safe_float  # noqa: E402


FIELD_REPORT_PREFERENCE = {
    "actual_controller": ("年报",),
    "revenue_breakdown": ("半年报", "年报", "三季报"),
    "net_profit": ("三季报", "半年报", "年报", "一季报"),
    "total_equity": ("三季报", "半年报", "年报", "一季报"),
    "capex_investment": ("年报", "半年报", "三季报"),
    "mineral_rights": ("年报", "半年报"),
    "license_moat": ("年报", "半年报"),
    "cost_structure": ("年报", "半年报"),
    "capacity": ("年报", "半年报", "三季报"),
}

OWNERSHIP_META_PATTERNS = {
    "controller_nature": (
        "实际控制人性质",
        "控股股东性质",
        "最终控制人性质",
    ),
    "controller_type": (
        "实际控制人类型",
        "控股股东类型",
        "最终控制人类型",
    ),
}


def _report_kind(title: str) -> str:
    if "第三季度报告" in title:
        return "三季报"
    if "半年度报告" in title:
        return "半年报"
    if "年度报告" in title:
        return "年报"
    if "第一季度报告" in title:
        return "一季报"
    return "其他"


def _report_kind_from_date(report_date: str) -> str:
    text = normalize_text(report_date).replace("-", "").replace("/", "")
    if text.endswith("0331"):
        return "一季报"
    if text.endswith("0630"):
        return "半年报"
    if text.endswith("0930"):
        return "三季报"
    if text.endswith("1231"):
        return "年报"
    return ""


def _build_pdf_catalog(report_pack_result: dict[str, Any], pdf_index_result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    indexed = {item.get("pdf_name"): item for item in (pdf_index_result or {}).get("indexed_files", [])}
    catalog: dict[str, dict[str, Any]] = {}
    for report in (report_pack_result or {}).get("downloaded_files", []):
        pdf_name = Path(report.get("local_path", "")).name or ""
        pdf_summary = indexed.get(pdf_name, {})
        catalog[pdf_name] = {
            **report,
            "pdf_name": pdf_name,
            "report_kind": report.get("report_kind") or _report_kind(report.get("title", "")),
            "hits_file": pdf_summary.get("hits_file", ""),
            "pages_file": pdf_summary.get("pages_file", ""),
        }
    return catalog


def _load_json(path: str) -> Any:
    if not path or not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _collect_field_candidates(field_name: str, pdf_catalog: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    field_keywords = FIELD_KEYWORDS.get(field_name, [])
    candidates: list[dict[str, Any]] = []
    for pdf_name, meta in pdf_catalog.items():
        keyword_hits = _load_json(meta.get("hits_file", ""))
        hits: list[dict[str, Any]] = []
        matched_keywords: list[str] = []
        for keyword in field_keywords:
            keyword_entries = keyword_hits.get(keyword, [])
            if keyword_entries:
                matched_keywords.append(keyword)
                for hit in keyword_entries:
                    hits.append(
                        {
                            "page": hit.get("page"),
                            "keyword": keyword,
                            "snippet": hit.get("snippet", ""),
                        }
                    )
        if not hits:
            continue
        candidates.append(
            {
                **meta,
                "matched_keywords": matched_keywords,
                "hits": hits,
            }
        )
    return candidates


def _pick_best_candidate(field_name: str, autofill_item: dict[str, Any], pdf_catalog: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    candidates = _collect_field_candidates(field_name, pdf_catalog)
    if not candidates:
        return None

    report_preference = list(FIELD_REPORT_PREFERENCE.get(field_name, ("年报",)))
    report_date = ""
    if isinstance(autofill_item.get("candidate_value"), dict):
        report_date = normalize_text(autofill_item["candidate_value"].get("report_date") or autofill_item["candidate_value"].get("latest_report_date"))
    inferred_kind = _report_kind_from_date(report_date)
    if inferred_kind and inferred_kind in report_preference:
        report_preference = [inferred_kind] + [kind for kind in report_preference if kind != inferred_kind]

    rank_map = {kind: idx for idx, kind in enumerate(report_preference)}

    def sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
        date_key = int(str(item.get("announcement_time", "")).replace("-", "") or 0)
        return (
            rank_map.get(item.get("report_kind", ""), len(rank_map) + 10),
            -date_key,
            -len(item.get("hits", [])),
        )

    candidates.sort(key=sort_key)
    return candidates[0]


def _pages_text(best_candidate: dict[str, Any]) -> str:
    pages = _load_json(best_candidate.get("pages_file", ""))
    page_map = {int(page.get("page")): page.get("text", "") for page in pages if page.get("page")}
    chosen_pages = sorted({int(hit.get("page")) for hit in best_candidate.get("hits", []) if hit.get("page")})
    return " ".join(page_map.get(page_no, "") for page_no in chosen_pages)


def _number_variants(value: float | int | None) -> list[str]:
    if value is None:
        return []
    try:
        value = float(value)
    except (TypeError, ValueError):
        return []
    base = f"{value:.2f}"
    compact = base.replace(",", "")
    integer = f"{int(round(value))}"
    return [
        base,
        f"{value:,.2f}",
        compact,
        integer,
        f"{int(round(value)):,}",
    ]


def _build_evidence(
    field_name: str,
    value: Any,
    best_candidate: dict[str, Any],
    confidence: str,
    verification_method: str,
) -> dict[str, Any]:
    pages = sorted({int(hit.get("page")) for hit in best_candidate.get("hits", []) if hit.get("page")})
    title = best_candidate.get("title", best_candidate.get("pdf_name", ""))
    return {
        "field_name": field_name,
        "value": value,
        "source_tier": 0,
        "source_type": "cninfo_pdf_crosscheck",
        "source_url": best_candidate.get("detail_url", ""),
        "description": f"CNINFO PDF cross-check: {title}",
        "announcement_title": title,
        "page_no": ",".join(str(page) for page in pages),
        "fetch_time": now_ts(),
        "confidence": confidence,
        "verification_method": verification_method,
        "matched_keywords": best_candidate.get("matched_keywords", []),
        "pdf_name": best_candidate.get("pdf_name", ""),
        "snippet": (best_candidate.get("hits") or [{}])[0].get("snippet", "")[:240],
    }


def _extract_inline_meta(text: str, labels: tuple[str, ...]) -> str:
    normalized = re.sub(r"\s+", " ", normalize_text(text))
    if not normalized:
        return ""
    for label in labels:
        pos = normalized.find(label)
        if pos < 0:
            continue
        tail = normalized[pos + len(label): pos + len(label) + 48]
        tail = tail.lstrip(" :：")
        value_chars: list[str] = []
        for ch in tail:
            if ch in " \t\r\n|，。；、()（）[]【】":
                break
            value_chars.append(ch)
        value = normalize_text("".join(value_chars).lstrip(":："))
        if value:
            return value
    return ""


def _extract_ownership_meta(text: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for field_name, labels in OWNERSHIP_META_PATTERNS.items():
        value = _extract_inline_meta(text, labels)
        if value:
            metadata[field_name] = value
    return metadata


def _verify_field(field_name: str, autofill_item: dict[str, Any], best_candidate: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
    candidate_value = autofill_item.get("candidate_value")
    full_text = _pages_text(best_candidate)
    snippets_text = " ".join(str(hit.get("snippet", "")) for hit in best_candidate.get("hits", []))
    combined_text = f"{snippets_text} {full_text}"

    if field_name == "actual_controller":
        value = normalize_text(candidate_value)
        ownership_meta = _extract_ownership_meta(combined_text)
        if value and value in combined_text:
            evidence = _build_evidence(field_name, value, best_candidate, "high", "pdf_string_match")
            evidence.update(ownership_meta)
            return True, evidence
        if value:
            evidence = _build_evidence(field_name, value, best_candidate, "medium", "pdf_keyword_hit_plus_manual_string_candidate")
            evidence.update(ownership_meta)
            return True, evidence
        return False, None

    if field_name in {"net_profit", "total_equity"} and isinstance(candidate_value, dict):
        number_key = "net_profit" if field_name == "net_profit" else "total_equity"
        amount = safe_float(candidate_value.get(number_key))
        report_date = normalize_text(candidate_value.get("report_date"))
        variants = _number_variants(amount)
        has_number_match = any(token and token in combined_text for token in variants)
        expected_kind = _report_kind_from_date(report_date)
        kind_match = not expected_kind or best_candidate.get("report_kind") == expected_kind
        if amount is not None and kind_match and has_number_match:
            return True, _build_evidence(field_name, candidate_value, best_candidate, "high", "pdf_numeric_match")
        if amount is not None and kind_match:
            return True, _build_evidence(field_name, candidate_value, best_candidate, "medium", "pdf_period_match_plus_tier1_crosscheck")
        return False, None

    if field_name == "revenue_breakdown" and isinstance(candidate_value, dict):
        segment = normalize_text(candidate_value.get("top_segment"))
        ratio = safe_float(candidate_value.get("top_ratio"))
        if segment and (segment in combined_text or best_candidate.get("report_kind") in {"半年报", "年报"}):
            confidence = "high" if segment in combined_text and ratio is not None else "medium"
            method = "pdf_segment_match" if segment in combined_text else "pdf_segment_section_plus_tier1_ratio"
            return True, _build_evidence(field_name, candidate_value, best_candidate, confidence, method)
        return False, None

    if field_name == "capex_investment":
        summary = candidate_value if isinstance(candidate_value, dict) else {}
        summary_text = normalize_text(summary.get("summary") if isinstance(summary, dict) else candidate_value)
        if summary_text or best_candidate.get("hits"):
            return True, _build_evidence(field_name, candidate_value, best_candidate, "medium", "pdf_capex_section_hit")
        return False, None

    if field_name == "mineral_rights":
        summary = candidate_value if isinstance(candidate_value, dict) else {}
        summary_text = normalize_text(summary.get("summary") if isinstance(summary, dict) else candidate_value)
        if any(keyword in combined_text for keyword in ("采矿权", "矿权", "采矿许可证")) or summary_text:
            return True, _build_evidence(field_name, candidate_value, best_candidate, "medium", "pdf_mining_asset_hit")
        return False, None

    if field_name == "license_moat":
        summary = candidate_value if isinstance(candidate_value, dict) else {}
        summary_text = normalize_text(summary.get("summary") if isinstance(summary, dict) else candidate_value)
        if any(
            keyword in combined_text
            for keyword in (
                "民用爆炸物品生产许可证",
                "武器装备科研生产许可证",
                "武器装备承制资格",
                "军工保密资格",
                "型号配套",
            )
        ) or summary_text:
            return True, _build_evidence(field_name, candidate_value, best_candidate, "medium", "pdf_license_moat_hit")
        return False, None

    if field_name == "cost_structure":
        summary = candidate_value if isinstance(candidate_value, dict) else {}
        semantic_check = summary.get("semantic_check", {}) if isinstance(summary, dict) else {}
        if semantic_check and not semantic_check.get("semantic_pass"):
            return False, None
        summary_text = normalize_text(summary.get("summary") if isinstance(summary, dict) else candidate_value)
        if any(keyword in combined_text for keyword in ("原材料", "成本", "生产成本")) or summary_text:
            return True, _build_evidence(field_name, candidate_value, best_candidate, "medium", "pdf_cost_section_hit")
        return False, None

    if field_name == "capacity" and isinstance(candidate_value, dict):
        capacity_ton = safe_float(candidate_value.get("capacity_ton"))
        capacity_label = normalize_text(candidate_value.get("capacity_label"))
        if capacity_ton and (capacity_label in combined_text or "万吨/年" in combined_text):
            return True, _build_evidence(field_name, candidate_value, best_candidate, "medium", "pdf_capacity_hit")
        return False, None

    return False, None


def run_tier0_verification(
    stock_code: str,
    tier0_prep: dict[str, Any],
    tier0_autofill_result: dict[str, Any],
    pdf_index_result: dict[str, Any],
    report_pack_result: dict[str, Any],
    output_dir: str | None = None,
) -> dict[str, Any]:
    verified_prep = copy.deepcopy(tier0_prep)
    checklist = verified_prep.get("checklist", {}).get("checklist", [])
    autofill_map = {item.get("field_name"): item for item in (tier0_autofill_result or {}).get("items", [])}
    pdf_catalog = _build_pdf_catalog(report_pack_result or {}, pdf_index_result or {})

    verified_items: list[dict[str, Any]] = []
    failed_items: list[dict[str, Any]] = []
    verified_count = 0

    for item in checklist:
        field_name = item.get("field")
        autofill_item = autofill_map.get(field_name)
        if not autofill_item:
            continue

        best_candidate = _pick_best_candidate(field_name, autofill_item, pdf_catalog)
        if not best_candidate:
            failed_items.append({"field_name": field_name, "reason": "no_matching_pdf_hits"})
            continue

        verified, evidence = _verify_field(field_name, autofill_item, best_candidate)
        if not verified or not evidence:
            failed_items.append({"field_name": field_name, "reason": "verification_conditions_not_met"})
            continue

        item["verified"] = True
        item["evidence"] = evidence
        item["verified_by"] = "tier0_auto_verifier"
        verified_items.append(
            {
                "field_name": field_name,
                "announcement_title": evidence.get("announcement_title", ""),
                "page_no": evidence.get("page_no", ""),
                "confidence": evidence.get("confidence", "medium"),
                "verification_method": evidence.get("verification_method", ""),
            }
        )
        verified_count += 1

    verified_prep["checklist"]["verified_count"] = verified_count

    result = {
        "stock_code": stock_code,
        "generated_at": now_ts(),
        "verified_count": verified_count,
        "verified_items": verified_items,
        "failed_items": failed_items,
        "updated_tier0_prep": verified_prep,
    }

    if output_dir:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        with open(out_path / "tier0_verification.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        with open(out_path / "tier0_checklist_verified.json", "w", encoding="utf-8") as f:
            json.dump(verified_prep, f, ensure_ascii=False, indent=2, default=str)
        with open(out_path / "tier0_checklist.json", "w", encoding="utf-8") as f:
            json.dump(verified_prep, f, ensure_ascii=False, indent=2, default=str)

    return result
