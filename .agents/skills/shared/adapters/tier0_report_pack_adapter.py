"""
Download a usable Tier 0 periodic-report pack from CNINFO.

The pack is stored under evidence/{code}/annual_reports for compatibility
with the existing pipeline, but it contains the latest full regular reports:
annual, half-year, Q3, and Q1.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from adapters.cninfo_adapter import fetch_disclosure_announcements
from utils.report_helpers import report_kind


DEFAULT_CATEGORY_LIMITS = {
    "年报": 2,
    "半年报": 1,
    "三季报": 1,
    "一季报": 1,
}


def _safe_filename(name: str) -> str:
    invalid = '<>:"/\\|?*'
    cleaned = "".join("_" if ch in invalid else ch for ch in (name or "").strip())
    cleaned = "_".join(cleaned.split())
    return cleaned[:120] or "periodic_report"


def _report_kind_legacy(title: str) -> str:
    if "第三季度报告" in title:
        return "三季报"
    if "半年度报告" in title:
        return "半年报"
    if "年度报告" in title:
        return "年报"
    if "第一季度报告" in title:
        return "一季报"
    return "其他"


_report_kind = report_kind


def download_tier0_report_pack(
    stock_code: str,
    company_name: str,
    evidence_dir: str,
    *,
    start_date: str = "20200101",
    end_date: str | None = None,
    category_limits: dict[str, int] | None = None,
    include_summary: bool = False,
) -> dict[str, Any]:
    reports_dir = Path(evidence_dir) / "annual_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = reports_dir / "annual_reports_manifest.json"

    limits = dict(DEFAULT_CATEGORY_LIMITS)
    if category_limits:
        limits.update(category_limits)

    selected: list[dict[str, Any]] = []
    query_log: dict[str, Any] = {}
    seen_ids: set[str] = set()
    for category, limit in limits.items():
        disclosure_result = fetch_disclosure_announcements(
            stock_code,
            category=category,
            start_date=start_date,
            end_date=end_date,
        )
        query_log[category] = {
            "status": disclosure_result.get("status"),
            "total_count": disclosure_result.get("total_count", 0),
        }
        if disclosure_result.get("status") != "ok":
            continue

        picked = 0
        for item in disclosure_result.get("announcements", []):
            title = str(item.get("title", ""))
            if item.get("announcement_id") in seen_ids:
                continue
            if not include_summary and item.get("is_summary"):
                continue
            if _report_kind(title) != category:
                continue
            seen_ids.add(item.get("announcement_id"))
            selected.append({**item, "report_kind": category})
            picked += 1
            if picked >= limit:
                break

    selected.sort(key=lambda item: (item.get("announcement_time", ""), item.get("announcement_id", "")), reverse=True)

    downloaded_files: list[dict[str, Any]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for item in selected:
        file_name = f"{item['announcement_time']}_{_safe_filename(item['title'])}_{item['announcement_id']}.pdf"
        file_path = reports_dir / file_name
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
        "status": "ok" if downloaded_files else "partial: no_periodic_reports_selected",
        "annual_reports_dir": str(reports_dir),
        "query_log": query_log,
        "downloaded_count": len(downloaded_files),
        "downloaded_files": downloaded_files,
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result
