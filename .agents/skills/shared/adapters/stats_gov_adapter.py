"""
stats_gov_adapter.py - macro and industry data adapter.

Primary path:
- GitHub/PyPI wrapper `cn-stats` over data.stats.gov.cn

Fallback path:
- akshare macro endpoints when the stats.gov wrapper is unavailable
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time
from typing import Any

import akshare as ak
import pandas as pd

from utils.commodity_profile_utils import build_industry_fai_map, resolve_signal_profile
from utils.evidence_helpers import make_evidence as _shared_make_evidence, now_ts


CNSTATS_REPO_URL = "https://github.com/songjian/cnstats"
CNSTATS_PACKAGE_NAME = "cn-stats"
STATS_GOV_URL = "https://data.stats.gov.cn"

INDUSTRY_FAI_LABEL_MAP: dict[str, list[str]] = {
    "纯碱": ["化学原料和化学制品制造业"],
    "氯碱": ["化学原料和化学制品制造业"],
    "煤化工": ["石油、煤炭及其他燃料加工业", "化学原料和化学制品制造业"],
    "磷化工": ["化学原料和化学制品制造业"],
    "氟化工": ["化学原料和化学制品制造业"],
    "钛白粉": ["化学原料和化学制品制造业"],
    "化学原料": ["化学原料和化学制品制造业"],
    "基础化工": ["化学原料和化学制品制造业"],
    "煤炭开采": ["煤炭开采和洗选业"],
    "动力煤": ["煤炭开采和洗选业"],
    "焦煤": ["煤炭开采和洗选业"],
    "焦炭": ["石油、煤炭及其他燃料加工业"],
    "钢铁": ["黑色金属冶炼和压延加工业", "黑色金属矿采选业"],
    "螺纹钢": ["黑色金属冶炼和压延加工业"],
    "热卷": ["黑色金属冶炼和压延加工业"],
    "铁矿石": ["黑色金属矿采选业"],
    "有色金属": ["有色金属冶炼和压延加工业", "有色金属矿采选业"],
    "铜": ["有色金属冶炼和压延加工业", "有色金属矿采选业"],
    "铝": ["有色金属冶炼和压延加工业", "有色金属矿采选业"],
    "锌": ["有色金属冶炼和压延加工业", "有色金属矿采选业"],
    "稀土": ["有色金属冶炼和压延加工业", "有色金属矿采选业"],
    "黄金": ["有色金属矿采选业"],
    "工程机械": ["专用设备制造业", "通用设备制造业"],
    "矿山设备": ["专用设备制造业"],
    "专用设备": ["专用设备制造业"],
    "通用设备": ["通用设备制造业"],
    "港口": ["装卸搬运和仓储业", "水上运输业", "交通运输、仓储和邮政业"],
    "造船": ["铁路、船舶、航空航天和其他运输设备制造业"],
    "船舶": ["铁路、船舶、航空航天和其他运输设备制造业"],
    "油服": ["石油和天然气开采业", "专用设备制造业"],
    "民爆": ["化学原料和化学制品制造业"],
}
INDUSTRY_FAI_LABEL_MAP.update(build_industry_fai_map())



def _make_evidence(field: str, value: Any, source_desc: str, tier: int = 0, url: str = "", confidence: str = "medium") -> dict[str, Any]:
    return _shared_make_evidence(field, value, source_desc, source_type="stats_gov", tier=tier, url=url or STATS_GOV_URL, confidence=confidence)


def _recent_records(df: pd.DataFrame, n: int = 24) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.tail(n).to_dict("records")


def _detect_data_freshness(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {"latest_year": None, "oldest_year": None, "age_years": None, "is_stale": True, "staleness_note": "无数据"}

    years: list[int] = []
    for record in records:
        for key in ("月份", "日期", "报告期", "统计期", "查询日期"):
            raw = str(record.get(key, ""))
            digits = "".join(ch for ch in raw if ch.isdigit())
            if len(digits) >= 4:
                years.append(int(digits[:4]))
                break

    if not years:
        return {"latest_year": None, "oldest_year": None, "age_years": None, "is_stale": True, "staleness_note": "无法解析日期"}

    current_year = datetime.datetime.now().year
    latest_year = max(years)
    oldest_year = min(years)
    age_years = current_year - latest_year
    return {
        "latest_year": latest_year,
        "oldest_year": oldest_year,
        "age_years": age_years,
        "is_stale": age_years > 1,
        "staleness_note": f"数据最新年份={latest_year}, 距今 {age_years} 年" if age_years > 1 else "",
    }


def _load_cnstats_query():
    try:
        from cnstats.stats import stats as cnstats_query  # type: ignore

        return cnstats_query
    except Exception:
        return None


def _query_cnstats(zbcode: str, datestr: str = "LAST3", retries: int = 2) -> tuple[pd.DataFrame, str]:
    query = _load_cnstats_query()
    if query is None:
        return pd.DataFrame(), "cnstats_unavailable"

    last_error = ""
    for candidate_datestr in [datestr] + ([] if datestr == "LAST1" else ["LAST1"]):
        for attempt in range(retries + 1):
            try:
                df = query(zbcode, candidate_datestr, as_df=True)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return df, ""
                last_error = f"empty_result:{zbcode}:{candidate_datestr}"
            except Exception as exc:  # pragma: no cover - network variability
                last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(1.0 + attempt)
    return pd.DataFrame(), last_error or "cnstats_query_failed"


def _col(df: pd.DataFrame, index: int) -> str:
    return str(df.columns[index])


def _latest_non_null_period(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return ""
    date_col = _col(df, 2)
    value_col = _col(df, 3)
    valid = df[df[value_col].notna()].copy()
    if valid.empty:
        return ""
    valid[date_col] = valid[date_col].astype(str)
    return str(valid[date_col].max())


def _records_for_period(df: pd.DataFrame, period: str, limit: int | None = None) -> list[dict[str, Any]]:
    if df is None or df.empty or not period:
        return []
    date_col = _col(df, 2)
    value_col = _col(df, 3)
    rows = df[(df[date_col].astype(str) == str(period)) & df[value_col].notna()].copy()
    if limit is not None:
        rows = rows.head(limit)
    return rows.to_dict("records")


def _contains_any(text: str, patterns: list[str]) -> bool:
    return any(pattern in text for pattern in patterns if pattern)


def _resolve_industry_targets(industry_keyword: str) -> list[str]:
    keyword = str(industry_keyword or "").strip()
    if not keyword:
        return []

    targets: list[str] = []
    profile = resolve_signal_profile(keyword)
    targets.extend(str(item).strip() for item in (profile.get("industry_fai_labels", []) or []) if str(item).strip())
    for token, labels in INDUSTRY_FAI_LABEL_MAP.items():
        if token in keyword or keyword in token:
            targets.extend(labels)

    if not targets:
        if any(token in keyword for token in ("化工", "氯碱", "纯碱", "磷", "氟", "钛白")):
            targets.extend(["化学原料和化学制品制造业"])
        elif any(token in keyword for token in ("煤", "焦")):
            targets.extend(["煤炭开采和洗选业", "石油、煤炭及其他燃料加工业"])
        elif any(token in keyword for token in ("钢", "铁矿")):
            targets.extend(["黑色金属冶炼和压延加工业", "黑色金属矿采选业"])
        elif any(token in keyword for token in ("铜", "铝", "锌", "稀土", "金")):
            targets.extend(["有色金属冶炼和压延加工业", "有色金属矿采选业"])
        elif any(token in keyword for token in ("工程机械", "矿山设备", "设备")):
            targets.extend(["专用设备制造业", "通用设备制造业"])
        elif any(token in keyword for token in ("港口", "航运")):
            targets.extend(["装卸搬运和仓储业", "水上运输业", "交通运输、仓储和邮政业"])
        elif any(token in keyword for token in ("造船", "船舶")):
            targets.extend(["铁路、船舶、航空航天和其他运输设备制造业"])

    return list(dict.fromkeys(targets))


def _filter_by_industry(df: pd.DataFrame, industry_keyword: str) -> tuple[pd.DataFrame, list[str]]:
    if df is None or df.empty:
        return pd.DataFrame(), []

    name_col = _col(df, 0)
    targets = _resolve_industry_targets(industry_keyword)
    if targets:
        matched = df[df[name_col].astype(str).apply(lambda v: _contains_any(v, targets))].copy()
        if not matched.empty:
            return matched, targets

    keyword = str(industry_keyword or "").strip()
    if keyword:
        fallback = df[df[name_col].astype(str).str.contains(keyword, na=False, regex=False)].copy()
        if not fallback.empty:
            return fallback, [keyword]

    return pd.DataFrame(), targets


def _build_human_action(field: str, action: str, *, priority: str = "red", detail: str = "") -> dict[str, Any]:
    payload = {
        "field": field,
        "action": action,
        "data_source": "国家统计局",
        "url": STATS_GOV_URL,
        "priority": priority,
    }
    if detail:
        payload["detail"] = detail
    return payload


def _extract_growth_indicator(records: list[dict[str, Any]]) -> tuple[str, float | None]:
    preferred_tokens = ("累计同比", "累计增长", "累计增速", "同比增长", "同比", "增速")
    for record in records:
        if not isinstance(record, dict):
            continue
        for token in preferred_tokens:
            for key, value in record.items():
                key_text = str(key or "")
                if token not in key_text:
                    continue
                number = pd.to_numeric(value, errors="coerce")
                if pd.notna(number):
                    return key_text, float(number)
    return "", None


def get_fixed_asset_investment() -> dict[str, Any]:
    df, error = _query_cnstats("A0402", "LAST3")
    if not df.empty:
        latest_period = _latest_non_null_period(df)
        latest_rows = _records_for_period(df, latest_period, limit=12)
        freshness = _detect_data_freshness(latest_rows)
        evidence = _make_evidence(
            "fixed_asset_investment",
            f"{len(latest_rows)} indicators @ {latest_period}",
            f"{CNSTATS_PACKAGE_NAME} -> {CNSTATS_REPO_URL} -> stats.gov A0402",
            tier=0,
            url=STATS_GOV_URL,
            confidence="high",
        )
        return {
            "data": {
                "scope": "national",
                "latest_period": latest_period,
                "records": latest_rows,
                "data_freshness": freshness,
                "source_repo": CNSTATS_REPO_URL,
            },
            "evidence": evidence,
            "status": "ok_cnstats",
        }

    try:
        df = ak.macro_china_gdzctz()
        if df is not None and not df.empty:
            records = _recent_records(df, 24)
            freshness = _detect_data_freshness(records)
            status = "partial: national_fai_proxy_only"
            if freshness["is_stale"]:
                status = f"stale: national_fai_data_from_{freshness.get('latest_year', 'unknown')}"
            return {
                "data": {
                    "scope": "national",
                    "records": records,
                    "data_freshness": freshness,
                    "limitation": "仅全国口径代理，非分行业 Capex",
                    "manual_url": STATS_GOV_URL,
                },
                "evidence": _make_evidence(
                    "fixed_asset_investment",
                    f"{len(records)} periods",
                    "akshare macro_china_gdzctz fallback",
                    tier=1,
                    url=STATS_GOV_URL,
                    confidence="low",
                ),
                "status": status,
                "human_action_needed": _build_human_action(
                    "fixed_asset_investment",
                    "补充分行业固定资产投资增速数据",
                    detail=f"cnstats failed: {error}",
                ),
            }
    except Exception:
        pass

    return {
        "data": {"note": "固定资产投资代理数据不可用", "manual_url": STATS_GOV_URL},
        "evidence": _make_evidence(
            "fixed_asset_investment",
            "manual_required",
            f"无法自动获取固定资产投资数据: {error}",
            tier=0,
            url=STATS_GOV_URL,
            confidence="low",
        ),
        "status": "manual_required: fixed_asset_investment_unavailable",
        "human_action_needed": _build_human_action("fixed_asset_investment", "补充固定资产投资数据", detail=error),
    }


def get_industry_fai(industry_keyword: str = "") -> dict[str, Any]:
    df, error = _query_cnstats("A0403", "LAST3")
    if df.empty:
        return {
            "data": {"scope": "industry", "industry": industry_keyword, "note": "cnstats query failed"},
            "evidence": _make_evidence(
                "industry_fai",
                "manual_required",
                f"无法自动获取分行业固定资产投资增速: {error}",
                tier=0,
                url=STATS_GOV_URL,
                confidence="low",
            ),
            "status": "manual_required: industry_fai_auto_failed",
            "human_action_needed": _build_human_action("industry_fai", f"补充 {industry_keyword or '目标'} 行业固定资产投资增速", detail=error),
        }

    matched_df, matched_labels = _filter_by_industry(df, industry_keyword)
    if matched_df.empty:
        return {
            "data": {
                "scope": "industry",
                "industry": industry_keyword,
                "matched_labels": matched_labels,
                "note": "industry keyword not matched in A0403",
            },
            "evidence": _make_evidence(
                "industry_fai",
                "manual_required",
                f"A0403 未匹配到行业口径: {industry_keyword}",
                tier=0,
                url=STATS_GOV_URL,
                confidence="low",
            ),
            "status": "manual_required: industry_keyword_not_matched",
            "human_action_needed": _build_human_action("industry_fai", f"补充 {industry_keyword or '目标'} 行业固定资产投资增速", detail="A0403 未匹配到对应行业"),
        }

    latest_period = _latest_non_null_period(matched_df)
    latest_rows = _records_for_period(matched_df, latest_period)
    if not latest_rows:
        return {
            "data": {
                "scope": "industry",
                "industry": industry_keyword,
                "matched_labels": matched_labels,
                "note": "matched rows exist but latest values are null",
            },
            "evidence": _make_evidence(
                "industry_fai",
                "manual_required",
                f"A0403 匹配到行业，但最新可用值为空: {industry_keyword}",
                tier=0,
                url=STATS_GOV_URL,
                confidence="low",
            ),
            "status": "manual_required: industry_latest_null",
            "human_action_needed": _build_human_action("industry_fai", f"补充 {industry_keyword or '目标'} 行业固定资产投资增速", detail="最新统计期为空"),
        }

    code_col = _col(matched_df, 1)
    date_col = _col(matched_df, 2)
    matched_codes = {row[code_col] for row in latest_rows if row.get(code_col)}
    history_df = matched_df[matched_df[code_col].isin(matched_codes)].copy()
    history_df[date_col] = history_df[date_col].astype(str)
    history_df = history_df.sort_values(by=[code_col, date_col], ascending=[True, False])
    history_records = history_df.head(max(6, len(matched_codes) * 3)).to_dict("records")
    freshness = _detect_data_freshness(latest_rows)
    selected_indicator, latest_yoy_pct = _extract_growth_indicator(latest_rows)

    return {
        "data": {
            "scope": "industry",
            "industry": industry_keyword,
            "matched_labels": matched_labels,
            "latest_period": latest_period,
            "latest_records": latest_rows,
            "recent_history": history_records,
            "data_freshness": freshness,
            "source_repo": CNSTATS_REPO_URL,
            "selected_indicator": selected_indicator,
            "latest_yoy_pct": latest_yoy_pct,
            "positive_growth": latest_yoy_pct is not None and latest_yoy_pct > 0,
        },
        "evidence": _make_evidence(
            "industry_fai",
            f"{industry_keyword} -> {', '.join(matched_labels) or 'matched'} @ {latest_period}",
            f"{CNSTATS_PACKAGE_NAME} -> {CNSTATS_REPO_URL} -> stats.gov A0403",
            tier=0,
            url=STATS_GOV_URL,
            confidence="high",
        ),
        "status": "ok_cnstats",
    }


def get_industrial_value_added(industry_keyword: str = "") -> dict[str, Any]:
    df, error = _query_cnstats("A0205", "LAST3")
    if not df.empty and industry_keyword:
        matched_df, matched_labels = _filter_by_industry(df, industry_keyword)
        latest_period = _latest_non_null_period(matched_df)
        latest_rows = _records_for_period(matched_df, latest_period)
        if latest_rows:
            freshness = _detect_data_freshness(latest_rows)
            return {
                "data": {
                    "scope": "industry",
                    "industry": industry_keyword,
                    "matched_labels": matched_labels,
                    "latest_period": latest_period,
                    "records": latest_rows,
                    "data_freshness": freshness,
                    "source_repo": CNSTATS_REPO_URL,
                },
                "evidence": _make_evidence(
                    "industrial_value_added",
                    f"{industry_keyword} @ {latest_period}",
                    f"{CNSTATS_PACKAGE_NAME} -> {CNSTATS_REPO_URL} -> stats.gov A0205",
                    tier=0,
                    url=STATS_GOV_URL,
                    confidence="high",
                ),
                "status": "ok_cnstats",
            }

    try:
        df = ak.macro_china_industrial_production_yoy()
        if df is not None and len(df) > 0:
            records = _recent_records(df, 24)
            freshness = _detect_data_freshness(records)
            status = "ok" if not freshness["is_stale"] else f"stale: {freshness.get('staleness_note', '')}"
            result = {
                "data": records,
                "evidence": _make_evidence(
                    "industrial_value_added",
                    f"{len(records)} periods",
                    "akshare macro_china_industrial_production_yoy fallback",
                    tier=1,
                    url=STATS_GOV_URL,
                    confidence="medium" if not freshness["is_stale"] else "low",
                ),
                "status": status,
            }
            if freshness["is_stale"]:
                result["human_action_needed"] = _build_human_action("industrial_value_added", "核验工业增加值数据是否为最新", priority="yellow", detail=freshness["staleness_note"])
            return result
    except Exception as exc:
        return {"data": {}, "evidence": {}, "status": f"error: {exc}"}

    return {
        "data": {},
        "evidence": _make_evidence(
            "industrial_value_added",
            "manual_required",
            f"工业增加值自动获取失败: {error}",
            tier=0,
            url=STATS_GOV_URL,
            confidence="low",
        ),
        "status": "manual_required: industrial_value_added_failed",
    }


def get_ppi() -> dict[str, Any]:
    candidate_codes = ["A010H", "A010B"]
    for zbcode in candidate_codes:
        df, error = _query_cnstats(zbcode, "LAST3")
        if df.empty:
            continue
        latest_period = _latest_non_null_period(df)
        latest_rows = _records_for_period(df, latest_period, limit=12)
        freshness = _detect_data_freshness(latest_rows)
        return {
            "data": {
                "latest_period": latest_period,
                "records": latest_rows,
                "data_freshness": freshness,
                "source_repo": CNSTATS_REPO_URL,
                "zbcode": zbcode,
            },
            "evidence": _make_evidence(
                "ppi",
                f"{len(latest_rows)} indicators @ {latest_period}",
                f"{CNSTATS_PACKAGE_NAME} -> {CNSTATS_REPO_URL} -> stats.gov {zbcode}",
                tier=0,
                url=STATS_GOV_URL,
                confidence="high",
            ),
            "status": "ok_cnstats",
        }

    try:
        df = ak.macro_china_ppi_yearly()
        if df is not None and len(df) > 0:
            records = _recent_records(df, 24)
            freshness = _detect_data_freshness(records)
            status = "ok" if not freshness["is_stale"] else f"stale: {freshness.get('staleness_note', '')}"
            return {
                "data": records,
                "evidence": _make_evidence(
                    "ppi",
                    f"{len(records)} months",
                    "akshare macro_china_ppi_yearly fallback",
                    tier=1,
                    url=STATS_GOV_URL,
                    confidence="medium" if not freshness["is_stale"] else "low",
                ),
                "status": status,
            }
    except Exception as exc:
        return {"data": [], "evidence": {}, "status": f"error: {exc}"}

    return {
        "data": [],
        "evidence": _make_evidence("ppi", "manual_required", "PPI 自动获取失败", tier=0, url=STATS_GOV_URL, confidence="low"),
        "status": "manual_required: ppi_unavailable",
    }


def run_macro_scan(output_dir: str | None = None, industry_keyword: str = "") -> dict[str, Any]:
    print("[stats_gov_adapter] 开始扫描宏观&行业数据 ...")
    results: dict[str, Any] = {}

    steps: list[tuple[str, Any]] = [
        ("fixed_asset_investment", get_fixed_asset_investment),
        ("industrial_value_added", lambda: get_industrial_value_added(industry_keyword)),
        ("ppi", get_ppi),
    ]

    for name, func in steps:
        print(f"  [{name}] ...", end=" ")
        result = func()
        results[name] = result
        print(result["status"])

    if industry_keyword:
        print(f"  [industry_fai: {industry_keyword}] ...", end=" ")
        industry_result = get_industry_fai(industry_keyword)
        results["industry_fai"] = industry_result
        print(industry_result["status"])

    human_actions: list[dict[str, Any]] = []
    for name, result in results.items():
        action = result.get("human_action_needed")
        if action:
            action["field"] = name
            human_actions.append(action)
    results["_human_actions_summary"] = human_actions

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "macro_scan.json"), "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        print(f"[stats_gov_adapter] 结果已保存到 {output_dir}")
        if human_actions:
            print(f"[stats_gov_adapter] [WARNING] {len(human_actions)} 项需人工补充:")
            for action in human_actions:
                print(f"    [!] {action['field']}: {action['action']}")

    return results


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parents[5] / "data" / "raw" / "macro")
    industry = sys.argv[2] if len(sys.argv) > 2 else ""
    run_macro_scan(output_dir=out, industry_keyword=industry)
