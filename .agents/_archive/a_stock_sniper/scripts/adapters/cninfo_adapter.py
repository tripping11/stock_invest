"""
cninfo_adapter.py - Tier 0 official disclosure helpers.

This adapter does two things:
1. prepare the Tier 0 verification checklist;
2. query CNINFO annual reports and download recent PDFs into evidence/.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests


CNINFO_STATIC_BASE_URL = "http://static.cninfo.com.cn/"
CNINFO_DISCLOSURE_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STOCK_DATA_URLS = {
    "沪深京": "http://www.cninfo.com.cn/new/data/szse_stock.json",
    "港股": "http://www.cninfo.com.cn/new/data/hke_stock.json",
    "三板": "http://www.cninfo.com.cn/new/data/gfzr_stock.json",
    "基金": "http://www.cninfo.com.cn/new/data/fund_stock.json",
    "债券": "http://www.cninfo.com.cn/new/data/bond_stock.json",
}
CNINFO_MARKET_COLUMN_MAP = {
    "沪深京": "szse",
    "港股": "hke",
    "三板": "third",
    "基金": "fund",
    "债券": "bond",
    "监管": "regulator",
    "预披露": "pre_disclosure",
}
CNINFO_CATEGORY_MAP = {
    "年报": "category_ndbg_szsh",
    "半年报": "category_bndbg_szsh",
    "一季报": "category_yjdbg_szsh",
    "三季报": "category_sjdbg_szsh",
}


def _now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_ymd() -> str:
    return datetime.datetime.now().strftime("%Y%m%d")


def _date_range_str(start_date: str, end_date: str) -> str:
    return f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}~{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\\\|?*]+', "_", str(name or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned[:120] or "annual_report"


def _make_evidence(
    field,
    value,
    source_desc,
    url="",
    confidence="high",
    announcement_title="",
    page_no="",
):
    return {
        "field_name": field,
        "value": value,
        "source_tier": 0,
        "source_type": "cninfo_official",
        "source_url": url,
        "description": source_desc,
        "announcement_title": announcement_title,
        "page_no": page_no,
        "fetch_time": _now(),
        "confidence": confidence,
    }


def _get_stock_org_id_map(market: str = "沪深京") -> dict[str, str]:
    url = CNINFO_STOCK_DATA_URLS.get(market, CNINFO_STOCK_DATA_URLS["沪深京"])
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()
    return {
        str(item.get("code", "")).zfill(6): item.get("orgId", "")
        for item in data.get("stockList", [])
        if item.get("code") and item.get("orgId")
    }


def _request_cninfo_announcements(payload: dict) -> dict:
    response = requests.post(CNINFO_DISCLOSURE_QUERY_URL, data=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def _normalize_announcement(item: dict) -> dict:
    announcement_time = item.get("announcementTime")
    announcement_dt = ""
    if announcement_time:
        announcement_dt = datetime.datetime.fromtimestamp(int(announcement_time) / 1000).strftime("%Y-%m-%d")
    title = str(item.get("announcementTitle", ""))
    adjunct_url = str(item.get("adjunctUrl", "") or "")
    stock_code = str(item.get("secCode", "")).zfill(6)
    announcement_id = str(item.get("announcementId", ""))
    org_id = item.get("orgId", "")
    return {
        "stock_code": stock_code,
        "company_name": item.get("secName", ""),
        "announcement_id": announcement_id,
        "org_id": org_id,
        "title": title,
        "announcement_time": announcement_dt,
        "detail_url": (
            f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode={stock_code}"
            f"&announcementId={announcement_id}&orgId={org_id}&announcementTime={announcement_dt}"
        ),
        "pdf_url": urljoin(CNINFO_STATIC_BASE_URL, adjunct_url) if adjunct_url else "",
        "adjunct_url": adjunct_url,
        "adjunct_type": item.get("adjunctType", ""),
        "is_summary": "摘要" in title,
    }


def get_cninfo_company_url(stock_code: str) -> dict:
    """Generate CNINFO stock pages for manual backtracking."""
    urls = {
        "company_profile": f"http://www.cninfo.com.cn/new/disclosure/stock?orgId=&stockCode={stock_code}",
        "annual_reports": f"http://www.cninfo.com.cn/new/disclosure/stock?orgId=&stockCode={stock_code}#702702",
        "announcements": f"http://www.cninfo.com.cn/new/disclosure/stock?orgId=&stockCode={stock_code}#702704",
    }
    return {"urls": urls, "status": "ok"}


def fetch_disclosure_announcements(
    stock_code: str,
    *,
    market: str = "沪深京",
    category: str = "",
    start_date: str = "20200101",
    end_date: str | None = None,
    keyword: str = "",
) -> dict:
    """Query CNINFO disclosure records and return normalized announcements."""
    resolved_end_date = end_date or _today_ymd()
    org_id_map = _get_stock_org_id_map(market)
    normalized_code = str(stock_code).zfill(6)
    org_id = org_id_map.get(normalized_code)
    if not org_id:
        return {
            "status": "error: org_id_not_found",
            "stock_code": normalized_code,
            "announcements": [],
        }

    payload = {
        "pageNum": "1",
        "pageSize": "30",
        "column": CNINFO_MARKET_COLUMN_MAP.get(market, "szse"),
        "tabName": "fulltext",
        "plate": "",
        "stock": f"{normalized_code},{org_id}",
        "searchkey": keyword,
        "secid": "",
        "category": CNINFO_CATEGORY_MAP.get(category, category),
        "trade": "",
        "seDate": _date_range_str(start_date, resolved_end_date),
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true",
    }

    first_page = _request_cninfo_announcements(payload)
    total_count = int(first_page.get("totalAnnouncement") or 0)
    announcements = list(first_page.get("announcements") or [])
    total_pages = max(1, (total_count + 29) // 30) if total_count else 1

    for page_num in range(2, total_pages + 1):
        payload["pageNum"] = str(page_num)
        page_json = _request_cninfo_announcements(payload)
        announcements.extend(page_json.get("announcements") or [])

    normalized = [_normalize_announcement(item) for item in announcements]
    normalized.sort(key=lambda item: (item["announcement_time"], item["announcement_id"]), reverse=True)
    return {
        "status": "ok",
        "stock_code": normalized_code,
        "market": market,
        "category": category,
        "start_date": start_date,
        "end_date": resolved_end_date,
        "total_count": len(normalized),
        "announcements": normalized,
    }


def generate_tier0_checklist(stock_code: str, company_name: str) -> dict:
    """Generate the list of fields that must be checked against Tier 0 evidence."""
    checklist = [
        {
            "field": "actual_controller",
            "description": "实际控制人/控股股东",
            "where_to_find": "年报 -> 公司治理 -> 股权控制关系图",
            "cninfo_section": "定期报告 -> 年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "revenue_breakdown",
            "description": "主营业务构成及各业务占比",
            "where_to_find": "年报 -> 经营情况讨论与分析 -> 主营业务分产品/分行业",
            "cninfo_section": "定期报告 -> 年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "net_profit",
            "description": "归母净利润",
            "where_to_find": "年报/半年报 -> 财务报表 -> 利润表",
            "cninfo_section": "定期报告 -> 年报/半年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "total_equity",
            "description": "归母净资产",
            "where_to_find": "年报/半年报 -> 财务报表 -> 资产负债表",
            "cninfo_section": "定期报告 -> 年报/半年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "mineral_rights",
            "description": "矿权/采矿许可证/资源储量",
            "where_to_find": "年报 -> 主要资产情况；临时公告搜索“矿权”“采矿许可”",
            "cninfo_section": "定期报告 + 临时公告",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "license_moat",
            "description": "行政牌照 / 军工资质 / 民爆许可",
            "where_to_find": "年报 -> 资质证照；临时公告搜索“许可证”“资质”“承制资格”",
            "cninfo_section": "定期报告 + 临时公告",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "cost_structure",
            "description": "主要成本构成",
            "where_to_find": "年报 -> 经营情况 -> 成本分析",
            "cninfo_section": "定期报告 -> 年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "capex_investment",
            "description": "资本开支计划/在建工程/行业 Capex 支撑",
            "where_to_find": "年报 -> 在建工程明细/重大投资；国家统计局 -> 固定资产投资",
            "cninfo_section": "定期报告 -> 年报 + 国家统计局",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "capacity",
            "description": "产能规模",
            "where_to_find": "年报 -> 主要业务 -> 产能产量",
            "cninfo_section": "定期报告 -> 年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "related_party_transactions",
            "description": "关联交易情况",
            "where_to_find": "年报 -> 关联方关系及交易",
            "cninfo_section": "定期报告 -> 年报",
            "verified": False,
            "evidence": None,
        },
        {
            "field": "impairment",
            "description": "资产减值情况",
            "where_to_find": "年报 -> 财务报表附注 -> 资产减值损失",
            "cninfo_section": "定期报告 -> 年报",
            "verified": False,
            "evidence": None,
        },
    ]

    return {
        "stock_code": stock_code,
        "company_name": company_name,
        "checklist": checklist,
        "total_items": len(checklist),
        "verified_count": 0,
        "status": "ok",
    }


def record_verification(
    checklist: dict,
    field: str,
    value,
    announcement_title: str,
    page_no: str,
    url: str = "",
) -> dict:
    """Record one verified Tier 0 field."""
    for item in checklist["checklist"]:
        if item["field"] == field:
            item["verified"] = True
            item["evidence"] = _make_evidence(
                field,
                value,
                f"巨潮资讯 - {announcement_title}",
                url=url,
                announcement_title=announcement_title,
                page_no=page_no,
            )
            checklist["verified_count"] += 1
            return {"status": "ok", "field": field, "verified": True}
    return {"status": "error: field not found"}


def download_annual_reports(
    stock_code: str,
    company_name: str,
    evidence_dir: str,
    *,
    start_date: str = "20200101",
    end_date: str | None = None,
    max_reports: int = 3,
    include_summary: bool = False,
) -> dict:
    """Download recent CNINFO annual report PDFs into evidence/{code}/annual_reports."""
    annual_report_dir = Path(evidence_dir) / "annual_reports"
    annual_report_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = annual_report_dir / "annual_reports_manifest.json"

    disclosure_result = fetch_disclosure_announcements(
        stock_code,
        category="年报",
        start_date=start_date,
        end_date=end_date,
    )
    if disclosure_result.get("status") != "ok":
        result = {
            "stock_code": str(stock_code).zfill(6),
            "company_name": company_name,
            "status": disclosure_result.get("status", "error"),
            "downloaded_files": [],
            "query": disclosure_result,
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return result

    selected = []
    for item in disclosure_result.get("announcements", []):
        if "年度报告" not in item["title"]:
            continue
        if not include_summary and item.get("is_summary"):
            continue
        selected.append(item)
        if len(selected) >= max_reports:
            break

    downloaded_files = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for item in selected:
        file_name = f"{item['announcement_time']}_{_safe_filename(item['title'])}_{item['announcement_id']}.pdf"
        file_path = annual_report_dir / file_name
        status = "cached" if file_path.exists() else "downloaded"

        if item.get("pdf_url") and not file_path.exists():
            response = session.get(item["pdf_url"], timeout=60)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                f.write(response.content)

        downloaded_files.append(
            {
                **item,
                "local_path": str(file_path),
                "download_status": status,
                "file_exists": file_path.exists(),
            }
        )

    result = {
        "stock_code": str(stock_code).zfill(6),
        "company_name": company_name,
        "status": "ok" if downloaded_files else "partial: no_full_annual_reports_selected",
        "annual_reports_dir": str(annual_report_dir),
        "query_total_count": disclosure_result.get("total_count", 0),
        "downloaded_count": len(downloaded_files),
        "downloaded_files": downloaded_files,
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def run_tier0_prep(stock_code: str, company_name: str, output_dir: str | None = None) -> dict:
    """Generate Tier 0 checklist and URLs."""
    print(f"[cninfo_adapter] 生成 {stock_code} {company_name} 的 Tier 0 核验清单 ...")
    urls = get_cninfo_company_url(stock_code)
    checklist = generate_tier0_checklist(stock_code, company_name)
    result = {
        "urls": urls["urls"],
        "checklist": checklist,
    }
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "tier0_checklist.json"), "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        print(f"[cninfo_adapter] Tier 0 核验清单已保存到 {output_dir}")
    return result


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    name = sys.argv[2] if len(sys.argv) > 2 else "中盐化工"
    out = sys.argv[3] if len(sys.argv) > 3 else str(Path(__file__).resolve().parents[5] / "evidence" / code)
    prep_result = run_tier0_prep(code, name, out)
    annual_result = download_annual_reports(code, name, out)
    print(json.dumps({"tier0_prep": prep_result, "annual_reports": annual_result}, ensure_ascii=False, indent=2))
