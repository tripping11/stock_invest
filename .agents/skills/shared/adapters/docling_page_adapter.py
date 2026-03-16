"""Targeted Docling page-range parser for CNInfo annual reports."""
from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    from utils.vendor_support import ensure_vendor_path
except ModuleNotFoundError:
    SHARED_DIR = Path(__file__).resolve().parents[1]
    shared_dir_str = str(SHARED_DIR)
    if shared_dir_str not in sys.path:
        sys.path.insert(0, shared_dir_str)
    from utils.vendor_support import ensure_vendor_path


def _load_docling():
    if not ensure_vendor_path("docling"):
        return None
    try:
        from docling.document_converter import DocumentConverter  # type: ignore

        return DocumentConverter
    except Exception:
        return None


def _collect_target_pages(pdf_index_result: dict | None, max_pages: int = 18) -> list[int]:
    pages: set[int] = set()
    field_hits = (pdf_index_result or {}).get("field_hits", {})
    for hit in field_hits.values():
        for page_no in hit.get("pages", []):
            if isinstance(page_no, int) and page_no > 0:
                pages.add(page_no)
    return sorted(pages)[:max_pages]


def _group_pages(pages: list[int], max_window: int = 12) -> list[tuple[int, int]]:
    if not pages:
        return []
    groups: list[list[int]] = [[pages[0]]]
    for page in pages[1:]:
        current = groups[-1]
        if page - current[-1] <= 2 and len(current) < max_window:
            current.append(page)
        else:
            groups.append([page])
    return [(group[0], group[-1]) for group in groups]


def run_docling_page_parse(
    stock_code: str,
    evidence_dir: str,
    *,
    pdf_index_result: dict | None = None,
    max_pages: int = 18,
) -> dict:
    converter_cls = _load_docling()
    if converter_cls is None:
        return {"status": "skipped: docling_unavailable", "windows": [], "outputs": []}

    reports_dir = Path(evidence_dir) / "annual_reports"
    pdf_files = sorted(reports_dir.glob("*.pdf"), reverse=True)
    if not pdf_files:
        return {"status": "skipped: no_annual_reports", "windows": [], "outputs": []}

    target_pages = _collect_target_pages(pdf_index_result, max_pages=max_pages)
    windows = _group_pages(target_pages or [1, 2, 3, 4, 5, 6], max_window=12)
    if not windows:
        return {"status": "skipped: no_target_pages", "windows": [], "outputs": []}

    out_dir = Path(evidence_dir) / "docling_pages"
    out_dir.mkdir(parents=True, exist_ok=True)
    converter = converter_cls()
    outputs = []
    latest_pdf = pdf_files[0]

    for idx, window in enumerate(windows, start=1):
        try:
            result = converter.convert(str(latest_pdf), page_range=window)
            markdown = result.document.export_to_markdown()
            out_file = out_dir / f"window_{idx}_{window[0]}_{window[1]}.md"
            out_file.write_text(markdown, encoding="utf-8")
            outputs.append(
                {
                    "window": {"start": window[0], "end": window[1]},
                    "report_file": str(latest_pdf),
                    "markdown_path": str(out_file),
                    "char_count": len(markdown),
                }
            )
        except Exception as exc:
            outputs.append(
                {
                    "window": {"start": window[0], "end": window[1]},
                    "report_file": str(latest_pdf),
                    "error": str(exc),
                }
            )

    manifest = {
        "stock_code": stock_code,
        "status": "ok" if any("markdown_path" in item for item in outputs) else "partial_or_failed",
        "target_pages": target_pages,
        "windows": [{"start": s, "end": e} for s, e in windows],
        "outputs": outputs,
    }
    with open(out_dir / "docling_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return manifest


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "600328"
    base_dir = Path(__file__).resolve().parents[5] / "evidence" / code
    manifest_path = base_dir / "pdf_index" / "pdf_index_manifest.json"
    pdf_index_result = {}
    if manifest_path.exists():
        pdf_index_result = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(json.dumps(run_docling_page_parse(code, str(base_dir), pdf_index_result=pdf_index_result), ensure_ascii=False, indent=2))
