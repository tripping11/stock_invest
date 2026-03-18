"""Tier 0 checklist autofill from PDF hits plus Tier 1 cross-check data."""
from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path

from adapters.provider_router import load_scan_cache
from utils.evidence_helpers import now_ts
from utils.financial_snapshot import (
    extract_latest_revenue_snapshot,
    extract_latest_revenue_terms,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
)
from utils.opportunity_classifier import assess_business_purity
from utils.value_utils import _pick_revenue_col, normalize_text, safe_float


ORG_PATTERN = re.compile(r"[\u4e00-\u9fa5A-Za-z0-9（）()]{2,40}?(?:集团)?(?:有限责任公司|股份有限公司|有限公司)")
AMOUNT_UNIT_PATTERN = re.compile(r"(?P<value>\d+(?:,\d{3})*(?:\.\d+)?)\s*(?P<unit>亿元|万元|元)")
PCT_PATTERN = re.compile(r"(?P<label>成本降幅|价格同比.*?下降|销量同比增幅)\D{0,10}(?P<value>\d+(?:\.\d+)?)%")
# ── 产能提取：通用模式，匹配 "数字 + 万吨/年 + 产品名" 的各种排列 ──
CAPACITY_PATTERNS = [
    # 数字在前：100 万吨/年纯碱
    re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*万吨/?年[，,]?\s*(?P<label>[\u4e00-\u9fa5A-Za-z]{2,16})"),
    # 产品在前：纯碱产能 100 万吨/年
    re.compile(r"(?P<label>[\u4e00-\u9fa5A-Za-z]{2,16})\S{0,12}?(?P<value>\d+(?:\.\d+)?)\s*万吨/?年"),
    # 万千瓦 (电力行业)
    re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*万千瓦(?P<label>[\u4e00-\u9fa5A-Za-z]{0,12})"),
]

# ── 成本结构关键原材料：全市场通用 ──
KEY_INPUT_KEYWORDS = [
    # 能源 / 动力
    "原煤", "煤炭", "动力煤", "焦煤", "焦炭", "兰炭", "天然气",
    "电力", "蒸汽", "柴油", "燃料",
    # 化工
    "天然碱", "原盐", "石灰石", "硫酸", "烧碱", "纯碱", "乙烯",
    # 金属 / 矿产
    "铁矿石", "铁精粉", "铜精矿", "铝土矿", "锌精矿", "废钢",
    "矿石", "精矿",
    # 农业 / 其他
    "饲料", "生猪", "原料", "辅料", "包装物", "水",
]
PROJECT_KEYWORDS = ["项目", "在建工程", "采矿权", "矿权", "扩建", "优化", "迁建", "改造"]
GENERIC_NAME_TOKENS = ("其他", "合计", "国内", "国外", "补充")

COST_STRUCTURE_CORE_KEYWORDS = (
    "成本",
    "原材料",
    "营业成本",
    "生产成本",
    "单位成本",
    "毛利",
    "毛利率",
)
# 动态扩展：核心关键词 + 原材料关键词构成完整集合
COST_STRUCTURE_RELATED_KEYWORDS = COST_STRUCTURE_CORE_KEYWORDS + tuple(KEY_INPUT_KEYWORDS)


def _pages_from_hits(field_hits: dict) -> list[int]:
    return sorted({int(hit.get("page")) for hit in field_hits.get("hits", []) if hit.get("page")})


def _short_snippet(field_hits: dict, max_len: int = 180) -> str:
    hits = field_hits.get("hits") or []
    if not hits:
        return ""
    snippet = str(hits[0].get("snippet", "")).replace("\n", " ").strip()
    return snippet[:max_len]


def _combined_hit_text(field_hits: dict, docling_text: str) -> str:
    snippets = " ".join(str(hit.get("snippet", "")) for hit in field_hits.get("hits", []))
    return f"{snippets} {docling_text}".strip()


def _load_docling_text(output_dir: str | None) -> str:
    if not output_dir:
        return ""
    docling_dir = Path(output_dir) / "docling_pages"
    if not docling_dir.exists():
        return ""
    parts: list[str] = []
    for md_file in sorted(docling_dir.glob("*.md")):
        try:
            parts.append(md_file.read_text(encoding="utf-8"))
        except OSError:
            continue
    return "\n".join(parts)


def _extract_actual_controller(field_hits: dict) -> str:
    snippets = " ".join(str(hit.get("snippet", "")) for hit in field_hits.get("hits", []))
    matches = ORG_PATTERN.findall(snippets)
    if not matches:
        return ""

    def rank(name: str) -> tuple[int, int]:
        score = 0
        # 通用国资/集团控制人信号（不偏向任何特定公司）
        for keyword in ("中国", "国资", "集团", "国务院", "财政部", "省政府"):
            if keyword in name:
                score += 3
        if "股份有限公司" in name:
            score -= 1
        return (score, len(name))

    return sorted(set(matches), key=rank, reverse=True)[0]


def _extract_primary_product_row(revenue_records: list[dict]) -> dict:
    if not revenue_records:
        return {}

    type_col = _pick_revenue_col(revenue_records, ("分类类型", "分类方向", "类型"), contains=("分类", "类型"))
    name_col = _pick_revenue_col(revenue_records, ("主营构成", "产品名称", "分类名称", "名称"), contains=("构成", "产品", "名称"))
    revenue_col = _pick_revenue_col(revenue_records, ("主营收入", "营业收入"), contains=("收入",))
    cost_col = _pick_revenue_col(revenue_records, ("主营成本", "营业成本"), contains=("成本",))
    margin_col = _pick_revenue_col(revenue_records, ("毛利率",), contains=("毛利",))
    date_col = _pick_revenue_col(revenue_records, ("报告日期", "报告期", "日期"), contains=("日期", "报告"))

    product_rows = revenue_records
    if type_col:
        preferred = [row for row in revenue_records if "按产品" in normalize_text(row.get(type_col))]
        if preferred:
            product_rows = preferred

    ranked: list[tuple[int, str, float, dict]] = []
    for row in product_rows:
        name = normalize_text(row.get(name_col or ""))
        if not name or any(token in name for token in GENERIC_NAME_TOKENS):
            continue
        revenue = safe_float(row.get(revenue_col or "")) or 0.0
        has_cost = safe_float(row.get(cost_col or "")) is not None
        date_text = normalize_text(row.get(date_col or "")).replace("-", "").replace("/", "")
        ranked.append((1 if has_cost else 0, date_text, revenue, row))

    if not ranked:
        return {}

    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    row = ranked[0][3]
    revenue = safe_float(row.get(revenue_col or "")) or 0.0
    cost = safe_float(row.get(cost_col or ""))
    margin = safe_float(row.get(margin_col or ""))
    if margin is not None and margin > 1:
        margin = margin / 100.0
    cost_ratio = (cost / revenue) if cost is not None and revenue > 0 else None
    return {
        "report_date": normalize_text(row.get(date_col or "")),
        "product_name": normalize_text(row.get(name_col or "")),
        "product_revenue": revenue,
        "product_cost": cost,
        "gross_margin": margin,
        "cost_ratio": cost_ratio,
    }


def _amount_to_yuan(value_text: str, unit_text: str) -> float | None:
    value = safe_float(str(value_text).replace(",", ""))
    if value is None:
        return None
    if unit_text == "亿元":
        return value * 1e8
    if unit_text == "万元":
        return value * 1e4
    return value


def _extract_amount_near_keyword(text: str, keyword: str) -> float | None:
    for match in re.finditer(keyword, text):
        window = text[match.start(): match.start() + 120]
        amount_match = AMOUNT_UNIT_PATTERN.search(window)
        if amount_match:
            return _amount_to_yuan(amount_match.group("value"), amount_match.group("unit"))
    return None


def _extract_cost_structure(scan_data: dict, field_hits: dict, docling_text: str) -> dict:
    combined_text = _combined_hit_text(field_hits, docling_text)
    product_row = _extract_primary_product_row(scan_data.get("revenue_breakdown", {}).get("data", []))
    key_inputs = [item for item in KEY_INPUT_KEYWORDS if item in combined_text]
    pct_map: dict[str, float] = {}
    for match in PCT_PATTERN.finditer(combined_text):
        pct_map[normalize_text(match.group("label"))] = safe_float(match.group("value")) or 0.0

    return {
        "summary": _short_snippet(field_hits),
        "pages": _pages_from_hits(field_hits),
        "primary_product": product_row.get("product_name", ""),
        "report_date": product_row.get("report_date", ""),
        "product_revenue_yuan": product_row.get("product_revenue"),
        "product_cost_yuan": product_row.get("product_cost"),
        "gross_margin": product_row.get("gross_margin"),
        "cost_ratio": product_row.get("cost_ratio"),
        "key_inputs": key_inputs,
        "cost_drop_pct": pct_map.get("成本降幅"),
        "price_drop_pct": next((value for label, value in pct_map.items() if "价格同比" in label), None),
        "volume_growth_pct": next((value for label, value in pct_map.items() if "销量同比" in label), None),
        "price_cost_comment": "成本降幅远低于售价降幅" if "成本降幅远低于售价降幅" in combined_text else "",
    }

def _evaluate_cost_structure_semantics(candidate_value: dict, field_hits: dict, docling_text: str) -> dict:
    combined_text = _combined_hit_text(field_hits, docling_text)
    summary_text = normalize_text(candidate_value.get("summary"))
    corpus = normalize_text(f"{summary_text} {combined_text}")
    core_hits = [keyword for keyword in COST_STRUCTURE_CORE_KEYWORDS if keyword in corpus]
    related_hits = [keyword for keyword in COST_STRUCTURE_RELATED_KEYWORDS if keyword in corpus]
    summary_core_hits = [keyword for keyword in COST_STRUCTURE_CORE_KEYWORDS if keyword in summary_text]
    summary_cost_phrases = [
        phrase
        for phrase in ("成本分析", "收入和成本分析", "主营业务成本", "营业成本", "生产成本", "毛利率")
        if phrase in summary_text
    ]
    suspicious_financial_phrases = [
        phrase
        for phrase in ("应收账款", "应收票据", "预付款项", "使用权资产", "无形资产", "年初至报告期末")
        if phrase in summary_text
    ]
    key_inputs = candidate_value.get("key_inputs") or []
    has_numeric_support = any(
        candidate_value.get(key) not in (None, "", [], {})
        for key in ("product_cost_yuan", "gross_margin", "cost_ratio", "cost_drop_pct", "price_drop_pct")
    )
    snippet_has_cost_context = bool(summary_cost_phrases) or len(summary_core_hits) >= 2
    semantic_pass = (
        bool(core_hits)
        and (bool(key_inputs) or has_numeric_support or len(related_hits) >= 3)
        and snippet_has_cost_context
        and not (bool(suspicious_financial_phrases) and not summary_cost_phrases)
    )
    return {
        "semantic_pass": semantic_pass,
        "core_hits": core_hits,
        "related_hits": related_hits,
        "summary_core_hits": summary_core_hits,
        "summary_cost_phrases": summary_cost_phrases,
        "suspicious_financial_phrases": suspicious_financial_phrases,
        "key_inputs": key_inputs,
        "has_numeric_support": has_numeric_support,
    }


def _extract_capex_investment(field_hits: dict, docling_text: str) -> dict:
    combined_text = _combined_hit_text(field_hits, docling_text)
    project_mentions = [keyword for keyword in PROJECT_KEYWORDS if keyword in combined_text]
    cip_balance = _extract_amount_near_keyword(combined_text, "在建工程")
    mine_right_payment = _extract_amount_near_keyword(combined_text, "采矿权价款")
    return {
        "summary": _short_snippet(field_hits),
        "pages": _pages_from_hits(field_hits),
        "cip_balance_yuan": cip_balance,
        "mine_right_payment_yuan": mine_right_payment,
        "project_mentions": project_mentions,
    }


def _extract_mineral_rights(field_hits: dict, docling_text: str) -> dict:
    combined_text = _combined_hit_text(field_hits, docling_text)
    return {
        "summary": _short_snippet(field_hits),
        "pages": _pages_from_hits(field_hits),
        "mine_right_payment_yuan": _extract_amount_near_keyword(combined_text, "采矿权价款"),
        "resource_moat_hint": "国内发现储量最大的天然碱资源" if "国内发现储量最大的天然碱资源" in combined_text else "",
        "has_mining_right": any(keyword in combined_text for keyword in ("采矿权", "矿权", "采矿许可证")),
    }


def _extract_capacity(scan_data: dict, field_hits: dict, docling_text: str) -> dict:
    combined_text = _combined_hit_text(field_hits, docling_text)
    product_row = _extract_primary_product_row(scan_data.get("revenue_breakdown", {}).get("data", []))
    explicit_items: list[dict] = []
    for pattern in CAPACITY_PATTERNS:
        for match in pattern.finditer(combined_text):
            value = safe_float(match.group("value"))
            if value is None:
                continue
            explicit_items.append(
                {
                    "label": normalize_text(match.groupdict().get("label", "")),
                    "capacity_wan_ton": value,
                    "capacity_ton": value * 10000,
                }
            )

    chosen = None
    primary_product = normalize_text(product_row.get("product_name"))
    if explicit_items and primary_product:
        chosen = next((item for item in explicit_items if primary_product in item.get("label", "")), None)
    if chosen is None and explicit_items:
        chosen = explicit_items[0]

    return {
        "summary": _short_snippet(field_hits),
        "pages": _pages_from_hits(field_hits),
        "primary_product": primary_product,
        "explicit_capacity_hits": explicit_items[:5],
        "capacity_ton": chosen.get("capacity_ton") if chosen else None,
        "capacity_wan_ton": chosen.get("capacity_wan_ton") if chosen else None,
        "capacity_label": chosen.get("label", "") if chosen else "",
        "proxy_product_revenue_yuan": product_row.get("product_revenue"),
        "proxy_product_cost_yuan": product_row.get("product_cost"),
        "proxy_cost_ratio": product_row.get("cost_ratio"),
    }


def _build_candidate(field_name: str, scan_data: dict, field_hits: dict, docling_text: str) -> dict | None:
    if not field_hits.get("matched") and field_name != "capacity":
        return None

    pages = _pages_from_hits(field_hits)
    candidate = {
        "field_name": field_name,
        "review_status": "needs_human_review",
        "matched_keywords": field_hits.get("matched_keywords", []),
        "pages": pages,
        "snippet": _short_snippet(field_hits),
        "confidence": "medium",
    }

    if field_name == "actual_controller":
        value = _extract_actual_controller(field_hits)
        candidate["candidate_value"] = value or candidate["snippet"]
        candidate["value_source"] = "pdf_extract"
        return candidate

    if field_name == "revenue_breakdown":
        purity = assess_business_purity(scan_data.get("revenue_breakdown", {}).get("data", []))
        candidate["candidate_value"] = {
            "latest_report_date": purity.get("latest_report_date", ""),
            "top_segment": purity.get("top_segment", ""),
            "top_ratio": purity.get("top_ratio", 0.0),
            "latest_terms": extract_latest_revenue_terms(scan_data.get("revenue_breakdown", {}).get("data", []), limit=8),
        }
        candidate["value_source"] = "tier1_crosscheck+pdf"
        return candidate

    if field_name == "net_profit":
        latest_income = get_latest_income_snapshot(scan_data.get("income_statement", {}).get("data", []))
        candidate["candidate_value"] = {
            "report_date": latest_income.get("report_date", ""),
            "net_profit": latest_income.get("net_profit"),
        }
        candidate["value_source"] = "tier1_crosscheck+pdf"
        return candidate

    if field_name == "total_equity":
        latest_balance = get_latest_balance_snapshot(scan_data.get("balance_sheet", {}).get("data", []))
        candidate["candidate_value"] = {
            "report_date": latest_balance.get("report_date", ""),
            "total_equity": latest_balance.get("total_equity"),
        }
        candidate["value_source"] = "tier1_crosscheck+pdf"
        return candidate

    if field_name == "cost_structure":
        candidate_value = _extract_cost_structure(scan_data, field_hits, docling_text)
        semantic_check = _evaluate_cost_structure_semantics(candidate_value, field_hits, docling_text)
        candidate_value["semantic_check"] = semantic_check
        candidate["candidate_value"] = candidate_value
        if not semantic_check.get("semantic_pass"):
            candidate["review_status"] = "partial_autofill"
            candidate["confidence"] = "low"
        candidate["value_source"] = "pdf_extract+revenue_crosscheck"
        return candidate

    if field_name == "capex_investment":
        candidate["candidate_value"] = _extract_capex_investment(field_hits, docling_text)
        candidate["value_source"] = "pdf_extract"
        return candidate

    if field_name == "mineral_rights":
        candidate["candidate_value"] = _extract_mineral_rights(field_hits, docling_text)
        candidate["value_source"] = "pdf_extract"
        return candidate

    if field_name == "capacity":
        capacity_value = _extract_capacity(scan_data, field_hits, docling_text)
        if not capacity_value.get("capacity_ton") and not capacity_value.get("proxy_product_revenue_yuan"):
            return None
        candidate["candidate_value"] = capacity_value
        candidate["value_source"] = "pdf_extract+revenue_proxy"
        return candidate

    candidate["candidate_value"] = {
        "summary": candidate["snippet"],
        "pages": pages,
    }
    candidate["value_source"] = "pdf_extract"
    candidate["confidence"] = "low"
    return candidate


def run_tier0_autofill(
    stock_code: str,
    tier0_prep: dict,
    scan_data: dict,
    pdf_index_result: dict,
    output_dir: str | None = None,
) -> dict:
    updated = copy.deepcopy(tier0_prep)
    checklist = updated.get("checklist", {}).get("checklist", [])
    pdf_field_hits = (pdf_index_result or {}).get("field_hits", {})
    docling_text = _load_docling_text(output_dir)
    items = []

    for item in checklist:
        field_name = item.get("field")
        field_hits = pdf_field_hits.get(field_name, {})
        candidate = _build_candidate(field_name, scan_data, field_hits, docling_text)
        if candidate:
            item["auto_filled"] = True
            item["review_status"] = candidate["review_status"]
            item["auto_fill_candidate"] = candidate
            items.append(candidate)
        else:
            item["auto_filled"] = False
            item["review_status"] = "no_candidate"

    result = {
        "stock_code": stock_code,
        "generated_at": now_ts(),
        "auto_filled_count": len(items),
        "review_required_count": len(items),
        "items": items,
        "updated_tier0_prep": updated,
    }

    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        with open(output_path / "tier0_autofill.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        with open(output_path / "tier0_checklist_autofilled.json", "w", encoding="utf-8") as f:
            json.dump(updated, f, ensure_ascii=False, indent=2, default=str)

    return result


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    base = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parents[5] / "evidence" / code
    raw_dir = Path(__file__).resolve().parents[5] / "data" / "raw" / code
    tier0_path = base / "tier0_checklist.json"
    pdf_path = base / "pdf_index" / "pdf_index_manifest.json"
    scan_data = load_scan_cache(raw_dir)
    if not scan_data:
        raise FileNotFoundError(f"no Tier1 scan cache found under {raw_dir}")
    with open(tier0_path, "r", encoding="utf-8") as f:
        tier0_prep = json.load(f)
    with open(pdf_path, "r", encoding="utf-8") as f:
        pdf_index_result = json.load(f)
    print(json.dumps(run_tier0_autofill(code, tier0_prep, scan_data, pdf_index_result, str(base)), ensure_ascii=False, indent=2, default=str))
