"""
Tier 0 PDF indexing for downloaded CNINFO periodic reports.

The output is intentionally auditable:
- per-file extracted pages
- per-file keyword hits
- field-level merged hit summary

Uses fuzzy matching to handle PDF line-break/space artifacts.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from pypdf import PdfReader


DEFAULT_KEYWORDS = [
    "实际控制人",
    "控股股东",
    "主营业务",
    "分产品",
    "分行业",
    "矿权",
    "采矿许可证",
    "资源储量",
    "在建工程",
    "固定资产投资",
    "资本开支",
    "净利润",
    "归属于上市公司股东的净利润",
    "净资产",
    "归属于上市公司股东的净资产",
    "成本分析",
    "原材料",
    "产能",
    "生产能力",
    "万吨/年",
    "年产",
    "民用爆炸物品生产许可证",
    "武器装备科研生产许可证",
    "武器装备承制资格",
    "军工保密资格",
    "型号配套",
]

FIELD_KEYWORDS = {
    "actual_controller": ["实际控制人", "控股股东"],
    "revenue_breakdown": ["主营业务", "分产品", "分行业"],
    "net_profit": ["净利润", "归属于上市公司股东的净利润"],
    "total_equity": ["净资产", "归属于上市公司股东的净资产"],
    "capex_investment": ["固定资产投资", "资本开支", "在建工程"],
    "mineral_rights": ["矿权", "采矿许可证", "资源储量", "采矿权"],
    "license_moat": ["民用爆炸物品生产许可证", "武器装备科研生产许可证", "武器装备承制资格", "军工保密资格", "型号配套"],
    "cost_structure": ["成本分析", "原材料", "生产成本"],
    "capacity": ["产能", "生产能力", "万吨/年", "年产"],
}

# 关键词变体映射：处理 PDF 中常见的等价表达
KEYWORD_VARIANTS = {
    "矿权": ["采矿权", "矿业权", "探矿权", "矿产资源"],
    "采矿许可证": ["采矿权证", "矿业许可"],
    "资源储量": ["保有资源量", "探明储量", "资源量"],
    "实际控制人": ["实控人"],
    "资本开支": ["资本性支出", "资本支出"],
    "固定资产投资": ["固定资产投入", "固投"],
    "在建工程": ["建设工程", "工程建设"],
    "成本分析": ["成本构成", "成本结构"],
    "原材料": ["原料", "材料成本"],
    "生产成本": ["制造成本", "营业成本"],
    "主营业务": ["主要业务", "核心业务"],
    "产能": ["产品产能", "设计产能", "年产能"],
    "生产能力": ["产能规模", "产能产量", "产品产量"],
    "万吨/年": ["万 吨/年", "万吨 年"],
    "民用爆炸物品生产许可证": ["民爆生产许可证", "工业炸药生产许可"],
    "武器装备科研生产许可证": ["武器装备科研生产许可", "军工生产许可证"],
    "武器装备承制资格": ["装备承制单位资格", "承制资格"],
    "军工保密资格": ["保密资格", "涉密资格"],
}


def _normalize_text(text: str) -> str:
    return " ".join((text or "").split())


def _compress_text(text: str) -> str:
    """移除所有空白字符，用于模糊匹配"""
    return re.sub(r'\s+', '', text or "")


def _fuzzy_find(text: str, keyword: str) -> list[int]:
    """在文本中模糊查找关键词，返回所有匹配位置。

    策略：
    1. 先在原始文本中精确匹配
    2. 再在压缩文本（去空白）中匹配，然后映射回原始位置
    3. 最后尝试关键词变体
    """
    positions = []

    # 1. 精确匹配
    pos = 0
    while True:
        idx = text.find(keyword, pos)
        if idx < 0:
            break
        positions.append(idx)
        pos = idx + 1

    if positions:
        return positions

    # 2. 压缩文本匹配（处理 PDF 换行/空格干扰）
    compressed = _compress_text(text)
    compressed_keyword = _compress_text(keyword)
    if compressed_keyword and compressed_keyword in compressed:
        comp_idx = compressed.find(compressed_keyword)
        orig_char_count = 0
        orig_pos = 0
        for i, ch in enumerate(text):
            if not ch.isspace():
                if orig_char_count == comp_idx:
                    orig_pos = i
                    break
                orig_char_count += 1
        positions.append(orig_pos)
        return positions

    # 3. 关键词变体匹配
    variants = KEYWORD_VARIANTS.get(keyword, [])
    for variant in variants:
        pos = text.find(variant)
        if pos >= 0:
            positions.append(pos)
            return positions
        compressed_variant = _compress_text(variant)
        if compressed_variant and compressed_variant in compressed:
            comp_idx = compressed.find(compressed_variant)
            orig_char_count = 0
            orig_pos = 0
            for i, ch in enumerate(text):
                if not ch.isspace():
                    if orig_char_count == comp_idx:
                        orig_pos = i
                        break
                    orig_char_count += 1
            positions.append(orig_pos)
            return positions

    return positions


def _announcement_date_key(pdf_name: str) -> int:
    prefix = pdf_name[:10]
    digits = "".join(ch for ch in prefix if ch.isdigit())
    if len(digits) == 8:
        return int(digits)
    return 0


def _report_priority(pdf_name: str) -> tuple[int, int]:
    if "第三季度报告" in pdf_name:
        kind_rank = 0
    elif "半年度报告" in pdf_name:
        kind_rank = 1
    elif "年度报告" in pdf_name:
        kind_rank = 2
    elif "第一季度报告" in pdf_name:
        kind_rank = 3
    else:
        kind_rank = 4
    return kind_rank, -_announcement_date_key(pdf_name)


def extract_pdf_pages(pdf_path: str) -> list[dict[str, Any]]:
    reader = PdfReader(pdf_path)
    pages: list[dict[str, Any]] = []
    for index, page in enumerate(reader.pages, start=1):
        raw_text = page.extract_text() or ""
        normalized = _normalize_text(raw_text)
        pages.append(
            {
                "page": index,
                "char_count": len(normalized),
                "text": normalized,
            }
        )
    return pages


def build_keyword_hits(pages: list[dict[str, Any]], keywords: list[str]) -> dict[str, list[dict[str, Any]]]:
    """使用模糊匹配在 PDF 页面中查找关键词"""
    hits: dict[str, list[dict[str, Any]]] = {}
    for keyword in keywords:
        keyword_hits: list[dict[str, Any]] = []
        for page in pages:
            text = page["text"]
            positions = _fuzzy_find(text, keyword)
            for pos in positions:
                start = max(pos - 80, 0)
                end = min(pos + len(keyword) + 180, len(text))
                keyword_hits.append(
                    {
                        "page": page["page"],
                        "snippet": text[start:end],
                    }
                )
        # 去重：同一页面只保留第一个命中
        seen_pages = set()
        deduped = []
        for hit in keyword_hits:
            if hit["page"] not in seen_pages:
                seen_pages.add(hit["page"])
                deduped.append(hit)
        if deduped:
            hits[keyword] = deduped

        # 如果主关键词没命中，尝试变体
        if keyword not in hits:
            for variant in KEYWORD_VARIANTS.get(keyword, []):
                variant_hits: list[dict[str, Any]] = []
                for page in pages:
                    text = page["text"]
                    variant_positions = _fuzzy_find(text, variant)
                    for pos in variant_positions:
                        start = max(pos - 80, 0)
                        end = min(pos + len(variant) + 180, len(text))
                        if page["page"] not in seen_pages:
                            seen_pages.add(page["page"])
                            variant_hits.append(
                                {
                                    "page": page["page"],
                                    "snippet": text[start:end],
                                    "matched_variant": variant,
                                }
                            )
                if variant_hits:
                    hits[keyword] = variant_hits
                    break

    return hits


def build_field_hits(
    indexed_files: list[dict[str, Any]],
    keywords: dict[str, list[str]] | None = None,
    max_snippets_per_field: int = 8,
) -> dict[str, dict[str, Any]]:
    field_map = keywords or FIELD_KEYWORDS
    summaries: dict[str, dict[str, Any]] = {}

    indexed_files = sorted(indexed_files, key=lambda item: _report_priority(item.get("pdf_name", "")))

    for field_name, field_keywords in field_map.items():
        field_hits: list[dict[str, Any]] = []
        matched_keywords: set[str] = set()

        for pdf_summary in indexed_files:
            pdf_name = pdf_summary.get("pdf_name", "")
            hits_file = pdf_summary.get("hits_file")
            if not hits_file or not Path(hits_file).exists():
                continue
            with open(hits_file, "r", encoding="utf-8") as f:
                keyword_hits = json.load(f)

            for keyword in field_keywords:
                for hit in keyword_hits.get(keyword, []):
                    matched_keywords.add(keyword)
                    field_hits.append(
                        {
                            "pdf_name": pdf_name,
                            "page": hit.get("page"),
                            "keyword": keyword,
                            "snippet": hit.get("snippet", ""),
                        }
                    )

        field_hits.sort(key=lambda item: (_report_priority(item.get("pdf_name", "")), item.get("page") or 0))
        summaries[field_name] = {
            "field_name": field_name,
            "matched": bool(field_hits),
            "matched_keywords": sorted(matched_keywords),
            "hit_count": len(field_hits),
            "hits": field_hits[:max_snippets_per_field],
        }

    return summaries


def index_pdf_file(pdf_path: str, output_dir: str, keywords: list[str] | None = None) -> dict[str, Any]:
    pdf = Path(pdf_path)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pages = extract_pdf_pages(str(pdf))
    hits = build_keyword_hits(pages, keywords or DEFAULT_KEYWORDS)

    pages_path = out_dir / f"{pdf.stem}_pages.json"
    hits_path = out_dir / f"{pdf.stem}_keyword_hits.json"
    summary_path = out_dir / f"{pdf.stem}_summary.json"

    with open(pages_path, "w", encoding="utf-8") as f:
        json.dump(pages, f, ensure_ascii=False, indent=2)
    with open(hits_path, "w", encoding="utf-8") as f:
        json.dump(hits, f, ensure_ascii=False, indent=2)

    summary = {
        "pdf_name": pdf.name,
        "page_count": len(pages),
        "non_empty_pages": sum(1 for page in pages if page["char_count"] > 0),
        "keyword_hit_count": sum(len(v) for v in hits.values()),
        "pages_file": str(pages_path),
        "hits_file": str(hits_path),
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def run_pdf_index(stock_code: str, evidence_dir: str, keywords: list[str] | None = None) -> dict[str, Any]:
    evidence_path = Path(evidence_dir)
    reports_dir = evidence_path / "annual_reports"
    output_dir = evidence_path / "pdf_index"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "pdf_index_manifest.json"
    field_hits_path = output_dir / "tier0_field_hits.json"

    if not reports_dir.exists():
        field_hits = build_field_hits([])
        with open(field_hits_path, "w", encoding="utf-8") as f:
            json.dump(field_hits, f, ensure_ascii=False, indent=2)
        result = {
            "stock_code": stock_code,
            "status": "skipped: annual_reports directory missing",
            "indexed_files": [],
            "field_hits": field_hits,
            "field_hits_file": str(field_hits_path),
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return result

    pdf_files = sorted(reports_dir.glob("*.pdf"), key=lambda path: _report_priority(path.name))
    if not pdf_files:
        field_hits = build_field_hits([])
        with open(field_hits_path, "w", encoding="utf-8") as f:
            json.dump(field_hits, f, ensure_ascii=False, indent=2)
        result = {
            "stock_code": stock_code,
            "status": "skipped: no pdf files found",
            "indexed_files": [],
            "field_hits": field_hits,
            "field_hits_file": str(field_hits_path),
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return result

    summaries: list[dict[str, Any]] = []
    for pdf_file in pdf_files:
        print(f"[tier0_pdf_adapter] 索引 PDF: {pdf_file.name}")
        summaries.append(index_pdf_file(str(pdf_file), str(output_dir), keywords))

    field_hits = build_field_hits(summaries)
    with open(field_hits_path, "w", encoding="utf-8") as f:
        json.dump(field_hits, f, ensure_ascii=False, indent=2)

    result = {
        "stock_code": stock_code,
        "status": "ok",
        "indexed_files": summaries,
        "output_dir": str(output_dir),
        "field_hits": field_hits,
        "field_hits_file": str(field_hits_path),
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    evidence = sys.argv[2] if len(sys.argv) > 2 else str(Path(__file__).resolve().parents[5] / "evidence" / code)
    print(json.dumps(run_pdf_index(code, evidence), ensure_ascii=False, indent=2))
