"""
radar_scan_engine.py - market-wide radar scan.

Primary enhancement layer:
- pywencai semantic pre-screen for broader market coverage

Fallback / enrichment layer:
- akshare board list, constituents, realtime quotes, kline
"""
from __future__ import annotations

import datetime
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import akshare as ak


SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_DIR)

from adapters.akshare_adapter import get_company_profile, get_revenue_breakdown, get_stock_kline
from engines.valuation_engine import estimate_current_ps
from utils.research_utils import (
    classify_state_ownership,
    determine_eco_context,
    extract_latest_revenue_terms,
    get_crocodile_mode_config,
    load_industry_mapping,
    normalize_text,
    safe_float as shared_safe_float,
)


def _get_base_dir() -> str:
    return os.environ.get("A_STOCK_BASE", str(Path(__file__).resolve().parents[5]))


HONEST_IDLE_MESSAGE = "🔴 扫描完毕：全市场发热，未达破净极寒冰点。触发诚实空窗纪律，系统强制休眠！"


BOARD_ALIAS_MAP = {
    "煤炭开采": ["煤炭行业", "煤化工"],
    "焦煤焦炭": ["煤化工"],
    "钢铁": ["钢铁行业"],
    "铁矿石": ["钢铁行业"],
    "铜铝锌": ["有色金属", "小金属"],
    "稀土": ["小金属"],
    "锂": ["能源金属"],
    "纯碱": ["化学原料", "煤化工", "磷肥及磷化工", "氮肥", "钾肥"],
    "氯碱": ["化学原料", "化学制品"],
    "钛白粉": ["化学制品"],
    "磷化工": ["磷肥及磷化工"],
    "氟化工": ["化学制品"],
    "石油天然气": ["石油行业", "采掘行业"],
    "黄金": ["贵金属"],
    "民爆": ["化学制品"],
    "工程机械": ["工程机械"],
    "港口": ["航运港口"],
    "造船": ["船舶制造"],
    "矿山设备": ["专用设备"],
    "油服": ["石油行业"],
    "核电设备": ["电源设备", "核电"],
    "特种钢": ["钢铁行业"],
    "航空发动机": ["航天航空"],
    "军工电子": ["航天航空"],
    "导弹武器": ["航天航空"],
    "核工业": ["核电"],
}


def _safe_float(value: Any, default: float | None = None) -> float | None:
    num = shared_safe_float(value)
    return default if num is None else num


def _normalize_code(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    return text.zfill(6)


def _resolve_ownership_snapshot(stock_code: str, controller: str, stock_name: str = "") -> dict[str, Any]:
    hints = [stock_name] if stock_name else []
    return classify_state_ownership(stock_code, controller, company_name_hints=hints)


def _ownership_status(stock_code: str, controller: str, stock_name: str = "") -> str:
    return normalize_text(_resolve_ownership_snapshot(stock_code, controller, stock_name).get("category")) or "unknown"


def _load_pywencai():
    try:
        import pywencai  # type: ignore

        return pywencai
    except Exception:
        return None


def _build_scope_keywords(scope: str, industry_mapping: dict[str, Any]) -> list[str]:
    scope = (scope or "all").strip()
    if scope.lower() == "all":
        keywords: list[str] = []
        for cfg in industry_mapping.get("eco_circles", {}).values():
            keywords.extend(cfg.get("industries", []))
    else:
        keywords = [item.strip() for item in scope.split(",") if item.strip()]

    expanded: list[str] = []
    for keyword in keywords:
        expanded.append(keyword)
        expanded.extend(BOARD_ALIAS_MAP.get(keyword, []))
    return list(dict.fromkeys(expanded))


def _board_eco_context(board_name: str, industry_mapping: dict[str, Any]) -> dict[str, Any]:
    return determine_eco_context("board", {"行业": board_name}, industry_mapping, extra_texts=[board_name])


def _load_radar_thresholds(mode: str) -> dict[str, float]:
    mode_cfg = get_crocodile_mode_config(mode)
    radar_cfg = mode_cfg.get("radar", {}) or {}
    red_cfg = radar_cfg.get("red", {}) or {}
    yellow_cfg = radar_cfg.get("yellow", {}) or {}
    return {
        "red_max_pb": _safe_float(red_cfg.get("max_pb"), 0.8) or 0.8,
        "red_max_current_vs_high": _safe_float(red_cfg.get("max_current_vs_high"), 60) or 60,
        "red_max_market_cap_yi": _safe_float(red_cfg.get("max_market_cap_yi"), 300) or 300,
        "yellow_max_pb": _safe_float(yellow_cfg.get("max_pb"), 1.2) or 1.2,
        "yellow_max_current_vs_high": _safe_float(yellow_cfg.get("max_current_vs_high"), 70) or 70,
    }


def _load_military_ps_thresholds(mode: str) -> dict[str, float]:
    valuation_cfg = get_crocodile_mode_config(mode).get("valuation", {}) or {}
    return {
        "entry_ps_pass": _safe_float(valuation_cfg.get("entry_ps_pass"), 2.5) or 2.5,
        "entry_ps_caution": _safe_float(valuation_cfg.get("entry_ps_caution"), 4.0) or 4.0,
    }


def _candidate_hits_ice_point(candidate: dict[str, Any]) -> bool:
    radar_score = _safe_float(candidate.get("radar_score"), 0.0) or 0.0
    if radar_score < 60:
        return False

    eco_circle = normalize_text(candidate.get("eco_circle"))
    if eco_circle == "core_military":
        current_ps = _safe_float(candidate.get("current_ps"))
        ps_thresholds = _load_military_ps_thresholds(candidate.get("four_signal_mode") or "military")
        return current_ps is not None and current_ps <= ps_thresholds["entry_ps_pass"]

    pb = _safe_float(candidate.get("pb"))
    return pb is not None and pb <= 1.0


def _should_trigger_honest_idle_breaker(candidates: list[dict[str, Any]]) -> bool:
    scoped_candidates = [item for item in candidates if normalize_text(item.get("eco_circle")) != "unknown"]
    if not scoped_candidates:
        return True
    for candidate in scoped_candidates:
        eco_circle = normalize_text(candidate.get("eco_circle"))
        radar_score = _safe_float(candidate.get("radar_score"), 0.0) or 0.0
        if radar_score < 85:
            continue
        if eco_circle == "core_military" and (_safe_float(candidate.get("current_ps"), 99.0) or 99.0) <= 2.5:
            return False
        if eco_circle != "core_military" and (_safe_float(candidate.get("pb"), 99.0) or 99.0) <= 1.0:
            return False
    return True


def _display_entry_metric(candidate: dict[str, Any]) -> str:
    if normalize_text(candidate.get("eco_circle")) == "core_military":
        current_ps = _safe_float(candidate.get("current_ps"))
        return "N/A" if current_ps is None else f"PS {current_ps:.2f}"
    pb = _safe_float(candidate.get("pb"))
    return "N/A" if pb is None else f"PB {pb:.2f}"


def _load_cached_radar_payload() -> dict[str, Any]:
    base_dir = Path(_get_base_dir()) / "data" / "raw" / "radar"
    if not base_dir.exists():
        return {}
    candidates = sorted(base_dir.glob("*/radar_candidates.json"), reverse=True)
    for path in candidates:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
    return {}


def _load_cached_board_matches() -> list[dict[str, Any]]:
    return _load_cached_radar_payload().get("board_matches", [])


def _load_cached_spot_map() -> dict[str, dict[str, Any]]:
    payload = _load_cached_radar_payload()
    spot_map: dict[str, dict[str, Any]] = {}
    for item in payload.get("candidates", []):
        code = _normalize_code(item.get("code"))
        if not code:
            continue
        market_cap_yi = _safe_float(item.get("market_cap_yi"))
        spot_map[code] = {
            "代码": code,
            "市净率": item.get("pb"),
            "总市值": market_cap_yi * 1e8 if market_cap_yi is not None else None,
        }
    return spot_map


def _load_cached_board_candidates(board_name: str) -> list[dict[str, str]]:
    payload = _load_cached_radar_payload()
    candidates: list[dict[str, str]] = []
    for item in payload.get("candidates", []):
        if item.get("board_name") == board_name:
            candidates.append({"code": _normalize_code(item.get("code")), "name": normalize_text(item.get("name"))})
    return candidates


def _match_boards(scope: str, industry_mapping: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        board_df = ak.stock_board_industry_name_em()
    except Exception:
        return _load_cached_board_matches()

    keywords = _build_scope_keywords(scope, industry_mapping)
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _, row in board_df.iterrows():
        board_name = normalize_text(row.get("板块名称"))
        if not board_name or board_name in seen:
            continue
        eco_context = _board_eco_context(board_name, industry_mapping)
        keyword_match = any(keyword in board_name or board_name in keyword for keyword in keywords) if keywords else False
        direct_match = eco_context.get("eco_circle") != "unknown"
        if not (keyword_match or direct_match):
            continue
        seen.add(board_name)
        matches.append(
            {
                "board_name": board_name,
                "board_code": normalize_text(row.get("板块代码")),
                "board_change_pct": _safe_float(row.get("涨跌幅"), 0.0),
                "board_total_mktcap": _safe_float(row.get("总市值"), 0.0),
                "eco_circle": eco_context.get("eco_circle", "unknown"),
                "matched_by": eco_context.get("matched_by", "unmatched"),
            }
        )
    return matches


def _extract_stock_row(row: Any) -> dict[str, str]:
    data = row.to_dict()
    return {
        "code": _normalize_code(data.get("代码") or data.get("code") or data.get("股票代码")),
        "name": normalize_text(data.get("名称") or data.get("name") or data.get("股票名称")),
    }


def _classify_temperature(candidate: dict[str, Any]) -> tuple[str, str]:
    pb = candidate.get("pb")
    current_ps = candidate.get("current_ps")
    current_vs_high = candidate.get("current_vs_high")
    eco_circle = candidate.get("eco_circle")
    soe_status = candidate.get("soe_status")
    market_cap_yi = candidate.get("market_cap_yi")
    thresholds = _load_radar_thresholds(candidate.get("four_signal_mode") or "unknown")

    if soe_status == "private":
        return ("green", "民企/非国资，不纳入雷达白名单")
    if soe_status == "local_soe":
        return ("green", "地方国资，不属于央企/省国资白名单")
    if eco_circle == "unknown":
        return ("green", "能力圈外，仅供参考")

    if eco_circle == "rigid_shovel":
        if (
            pb is not None
            and pb <= thresholds["yellow_max_pb"]
            and current_vs_high is not None
            and current_vs_high <= thresholds["red_max_current_vs_high"]
        ):
            return ("yellow", "铲子股处于低位，但仍需下游Capex强验证")
        return ("green", "铲子股未见明确信号")

    if eco_circle == "core_military":
        ps_thresholds = _load_military_ps_thresholds(candidate.get("four_signal_mode") or "military")
        if (
            current_ps is not None
            and current_ps <= ps_thresholds["entry_ps_pass"]
            and current_vs_high is not None
            and current_vs_high <= thresholds["red_max_current_vs_high"]
            and (market_cap_yi is None or market_cap_yi <= thresholds["red_max_market_cap_yi"])
        ):
            return ("red", "低PS + 低位回落，军工进入优先观察名单")
        if (
            current_ps is not None
            and current_ps <= ps_thresholds["entry_ps_caution"]
            and (current_vs_high is None or current_vs_high <= thresholds["yellow_max_current_vs_high"])
        ):
            return ("yellow", "军工PS回落到观察区，但还未到极低区")
        if current_ps is None:
            return ("green", "军工路径缺少PS代理收入，暂不进红黄区")
        return ("green", "军工PS仍偏高，继续等待")

    if pb is not None and current_vs_high is not None:
        if (
            pb <= thresholds["red_max_pb"]
            and current_vs_high <= thresholds["red_max_current_vs_high"]
            and (market_cap_yi is None or market_cap_yi <= thresholds["red_max_market_cap_yi"])
        ):
            return ("red", "低PB + 低位盘整，进入优先观察名单")
        if pb <= thresholds["yellow_max_pb"] or current_vs_high <= thresholds["yellow_max_current_vs_high"]:
            return ("yellow", "估值或位置接近观察区")

    if soe_status in {"unknown", "state_backed_unclear", "platform_unknown"}:
        return ("yellow", "估值尚可，但国资归属仍待 Tier 0 穿透")
    return ("green", "当前温度一般")


def _compute_radar_score(candidate: dict[str, Any]) -> float:
    score = 0.0
    pb = candidate.get("pb")
    current_ps = candidate.get("current_ps")
    current_vs_high = candidate.get("current_vs_high")
    market_cap_yi = candidate.get("market_cap_yi")
    eco_circle = candidate.get("eco_circle")
    soe_status = candidate.get("soe_status")
    zone = candidate.get("zone")

    if zone == "red":
        score += 40
    elif zone == "yellow":
        score += 25

    if soe_status in {"central_soe", "provincial_soe"}:
        score += 20
    elif soe_status in {"unknown", "state_backed_unclear", "platform_unknown"}:
        score += 8

    if eco_circle == "core_resource":
        score += 15
    elif eco_circle == "rigid_shovel":
        score += 12
    elif eco_circle == "core_military":
        score += 10

    if eco_circle == "core_military":
        ps_thresholds = _load_military_ps_thresholds(candidate.get("four_signal_mode") or "military")
        if current_ps is not None and current_ps <= ps_thresholds["entry_ps_pass"]:
            score += 20
        elif current_ps is not None and current_ps <= ps_thresholds["entry_ps_caution"]:
            score += 12
        elif current_ps is not None:
            score += max(0, min(8, 12 - current_ps))
    elif pb is not None:
        score += max(0, min(20, 20 - max(pb, 0) * 4))
    if current_vs_high is not None:
        score += max(0, 20 - max(current_vs_high - 40, 0) / 3)
    if market_cap_yi is not None and market_cap_yi <= 200:
        score += 5
    if candidate.get("data_quality") == "incomplete_data":
        score -= 10
    return round(score, 2)


def _pick_wencai_col(columns: list[str], keywords: tuple[str, ...], *, exact: bool = False) -> str | None:
    normalized = [normalize_text(col) for col in columns]
    for keyword in keywords:
        for col in normalized:
            if exact and col == keyword:
                return col
            if not exact and keyword in col:
                return col
    return None


def _build_wencai_queries(scope: str, industry_mapping: dict[str, Any]) -> list[str]:
    if (scope or "").strip().lower() == "all":
        return [
            "A股 国企 市净率小于1.5 总市值小于300亿",
            "A股 煤炭 钢铁 有色 化工 港口 造船 工程机械 民爆 军工 国企 总市值小于300亿",
        ]

    keywords = _build_scope_keywords(scope, industry_mapping)
    keyword_text = " ".join(keywords[:8]) if keywords else scope
    return [
        f"A股 {keyword_text} 国企 总市值小于300亿",
        f"A股 {keyword_text} 市净率小于1.8 总市值小于300亿",
    ]


def _fetch_wencai_seed_candidates(scope: str, industry_mapping: dict[str, Any], limit: int = 40) -> list[dict[str, Any]]:
    pywencai = _load_pywencai()
    if pywencai is None:
        return []

    seeds: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    queries = _build_wencai_queries(scope, industry_mapping)
    for query in queries:
        try:
            df = pywencai.get(query=query, query_type="stock", loop=False)
        except Exception:
            continue
        if df is None or not hasattr(df, "iterrows"):
            continue

        columns = [normalize_text(col) for col in list(df.columns)]
        code_col = _pick_wencai_col(columns, ("股票代码", "code"), exact=False)
        name_col = _pick_wencai_col(columns, ("股票简称", "股票名称", "简称"), exact=False)
        pb_col = _pick_wencai_col(columns, ("市净率", "pb"), exact=False)
        market_cap_col = _pick_wencai_col(columns, ("总市值",), exact=False)
        industry_col = _pick_wencai_col(columns, ("所属同花顺行业", "所属行业"), exact=False)
        controller_col = _pick_wencai_col(columns, ("最终控制人[", "最终控制人"), exact=False)
        nature_col = _pick_wencai_col(columns, ("企业性质", "最终控制人类型"), exact=False)

        if not code_col or not name_col:
            continue

        for _, row in df.iterrows():
            code = _normalize_code(row.get(code_col))
            name = normalize_text(row.get(name_col))
            if not code or not name or code in seen_codes:
                continue
            if "ST" in name.upper():
                continue

            industry_text = normalize_text(row.get(industry_col)) if industry_col else ""
            board_name = industry_text.split("-", 1)[0] if "-" in industry_text else (industry_text or "pywencai")
            eco_context = determine_eco_context(code, {"行业": industry_text or board_name}, industry_mapping, extra_texts=[industry_text, board_name, name])
            if eco_context.get("eco_circle") == "unknown":
                continue

            pb = _safe_float(row.get(pb_col)) if pb_col else None
            market_cap_yuan = _safe_float(row.get(market_cap_col))
            controller = ""
            if controller_col:
                controller = normalize_text(row.get(controller_col))
            if not controller and nature_col:
                controller = normalize_text(row.get(nature_col))

            seeds.append(
                {
                    "code": code,
                    "name": name,
                    "board_name": board_name,
                    "pb": pb,
                    "market_cap_yuan": market_cap_yuan,
                    "controller": controller,
                    "seed_source": "pywencai",
                    "matched_by": eco_context.get("matched_by", "unmatched"),
                    "eco_circle": eco_context.get("eco_circle", "unknown"),
                }
            )
            seen_codes.add(code)

    seeds.sort(key=lambda item: (item.get("pb") is None, item.get("pb") if item.get("pb") is not None else 99, item.get("market_cap_yuan") if item.get("market_cap_yuan") is not None else 9e18))
    return seeds[:limit]


def _scan_stock_candidate(
    stock_code: str,
    stock_name: str,
    board_name: str,
    industry_mapping: dict[str, Any],
    *,
    pb: float | None = None,
    market_cap_yuan: float | None = None,
    controller_hint: str = "",
    prefer_profile: bool = False,
) -> dict[str, Any]:
    if "ST" in stock_name.upper():
        return {
            "code": stock_code,
            "name": stock_name,
            "industry": board_name,
            "eco_circle": "unknown",
            "commodity": "",
            "matched_by": "st_filtered",
            "controller": controller_hint,
            "soe_status": _ownership_status(stock_code, controller_hint, stock_name),
            "pb": pb,
            "pb_percentile": None,
            "market_cap_yi": round(market_cap_yuan / 1e8, 2) if market_cap_yuan is not None else None,
            "current_ps": None,
            "current_vs_high": None,
            "zone": "green",
            "note": "ST 风险票，雷达过滤",
            "radar_score": 0.0,
            "data_quality": "incomplete_data",
        }

    eco_context = determine_eco_context(
        stock_code,
        {"行业": board_name},
        industry_mapping,
        extra_texts=[board_name, stock_name],
    )
    kline = get_stock_kline(stock_code, period="daily", years=5)
    kline_data = kline.get("data") or {}
    current_vs_high = _safe_float(kline_data.get("current_vs_5yr_high"))
    if current_vs_high is None:
        current_vs_high = _safe_float(kline_data.get("current_vs_high"))

    controller = normalize_text(controller_hint)
    ownership = _resolve_ownership_snapshot(stock_code, controller, stock_name)
    soe_status = normalize_text(ownership.get("category")) or "unknown"
    market_cap_yi = round(market_cap_yuan / 1e8, 2) if market_cap_yuan is not None else None
    candidate = {
        "code": stock_code,
        "name": stock_name,
        "industry": eco_context.get("industry_text") or board_name,
        "eco_circle": eco_context.get("eco_circle", "unknown"),
        "commodity": eco_context.get("commodity", ""),
        "four_signal_mode": eco_context.get("four_signal_mode", "unknown"),
        "matched_by": eco_context.get("matched_by", "unmatched"),
        "controller": controller,
        "soe_status": soe_status,
        "ownership_label": ownership.get("label", ""),
        "ownership_gate_verdict": ownership.get("gate_verdict", ""),
        "pb": pb,
        "pb_percentile": None,
        "market_cap_yi": market_cap_yi,
        "current_ps": None,
        "current_vs_high": current_vs_high,
        "consolidation_months": _safe_float(kline_data.get("consolidation_months")),
        "volume_ratio_20_vs_120": _safe_float(kline_data.get("volume_ratio_20_vs_120")),
    }
    zone, note = _classify_temperature(candidate)

    need_profile = (
        prefer_profile
        or zone in {"red", "yellow"}
        or eco_context.get("matched_by") in {"company_override", "unmatched"}
        or eco_context.get("eco_circle") == "core_military"
    )
    if need_profile:
        profile = get_company_profile(stock_code)
        profile_data = profile.get("data", {})
        revenue_breakdown = get_revenue_breakdown(stock_code)
        extra_texts = [board_name, stock_name]
        extra_texts.extend(extract_latest_revenue_terms(revenue_breakdown.get("data", []), limit=8))
        eco_context = determine_eco_context(stock_code, profile_data, industry_mapping, extra_texts=extra_texts)
        controller = normalize_text(profile_data.get("实际控制人") or profile_data.get("控股股东") or controller_hint)
        ownership = _resolve_ownership_snapshot(stock_code, controller, stock_name)
        soe_status = normalize_text(ownership.get("category")) or "unknown"
        candidate["industry"] = eco_context.get("industry_text") or board_name
        candidate["eco_circle"] = eco_context.get("eco_circle", "unknown")
        candidate["commodity"] = eco_context.get("commodity", "")
        candidate["four_signal_mode"] = eco_context.get("four_signal_mode", "unknown")
        candidate["matched_by"] = eco_context.get("matched_by", "unmatched")
        candidate["controller"] = controller
        candidate["soe_status"] = soe_status
        candidate["ownership_label"] = ownership.get("label", "")
        candidate["ownership_gate_verdict"] = ownership.get("gate_verdict", "")
        if candidate["eco_circle"] == "core_military":
            ps_snapshot = estimate_current_ps(
                {"总市值": market_cap_yuan},
                {"revenue_breakdown": revenue_breakdown, "financial_summary": {"data": []}},
                None,
                eco_context,
            )
            candidate["current_ps"] = _safe_float(ps_snapshot.get("current_ps"))
            candidate["sales_proxy_basis"] = normalize_text(ps_snapshot.get("basis"))
            candidate["proxy_report_date"] = normalize_text(ps_snapshot.get("report_date"))
        zone, note = _classify_temperature(candidate)

    candidate["zone"] = zone
    candidate["note"] = note
    candidate["radar_score"] = _compute_radar_score(candidate)
    if candidate.get("eco_circle") == "core_military":
        candidate["data_quality"] = "ok" if candidate.get("current_ps") is not None and candidate.get("current_vs_high") is not None else "incomplete_data"
    else:
        candidate["data_quality"] = "ok" if candidate["pb"] is not None and candidate.get("current_vs_high") is not None else "incomplete_data"
    return candidate


def _build_board_summaries(board_matches: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    board_index = {item["board_name"]: item for item in board_matches}
    ordered_names = list(dict.fromkeys([item["board_name"] for item in board_matches] + [normalize_text(item.get("board_name")) for item in candidates if item.get("board_name")]))
    summaries: list[dict[str, Any]] = []
    for board_name in ordered_names:
        if not board_name:
            continue
        board_candidates = [item for item in candidates if item.get("board_name") == board_name]
        board_meta = board_index.get(board_name, {})
        summaries.append(
            {
                "board_name": board_name,
                "board_change_pct": board_meta.get("board_change_pct"),
                "candidate_count": len(board_candidates),
                "red_count": sum(1 for item in board_candidates if item.get("zone") == "red"),
                "yellow_count": sum(1 for item in board_candidates if item.get("zone") == "yellow"),
                "green_count": sum(1 for item in board_candidates if item.get("zone") == "green"),
                "top_candidate": next(
                    (
                        {
                            "code": item.get("code"),
                            "name": item.get("name"),
                            "zone": item.get("zone"),
                            "radar_score": item.get("radar_score"),
                        }
                        for item in sorted(board_candidates, key=lambda row: row.get("radar_score", 0), reverse=True)
                    ),
                    None,
                ),
            }
        )
    return summaries


def _render_zone_table(items: list[dict[str, Any]]) -> str:
    if not items:
        return "| - | - | - | - | - | - | - |\n|---|---|---|---|---|---|---|\n| 空 |  |  |  |  |  |  |"
    lines = [
        "| 代码 | 名称 | 圈层 | PB | 距高点 | 分数 | 备注 |",
        "|---|---|---|---|---|---|---|",
    ]
    for item in sorted(items, key=lambda row: row.get("radar_score", 0), reverse=True):
        pb = "N/A" if item.get("pb") is None else f"{item['pb']:.2f}"
        current_vs_high = "N/A" if item.get("current_vs_high") is None else f"{item['current_vs_high']:.1f}%"
        score = f"{item.get('radar_score', 0):.1f}"
        lines.append(f"| {item['code']} | {item['name']} | {item['eco_circle']} | {pb} | {current_vs_high} | {score} | {item['note']} |")
    return "\n".join(lines)


def _render_board_table(items: list[dict[str, Any]]) -> str:
    if not items:
        return "| - | - | - | - | - | - |\n|---|---|---|---|---|---|\n| 空 |  |  |  |  |  |"
    lines = [
        "| 板块 | 涨跌幅 | 红区 | 黄区 | 绿区 | 最强候选 |",
        "|---|---|---|---|---|---|",
    ]
    for item in items:
        top_candidate = item.get("top_candidate") or {}
        top_text = "-"
        if top_candidate:
            top_text = f"{top_candidate.get('code')} {top_candidate.get('name')} ({top_candidate.get('zone')}, {top_candidate.get('radar_score')})"
        change_pct = item.get("board_change_pct")
        lines.append(f"| {item['board_name']} | {change_pct if change_pct is not None else 'N/A'} | {item['red_count']} | {item['yellow_count']} | {item['green_count']} | {top_text} |")
    return "\n".join(lines)


def _write_radar_report(
    report_path: str,
    scope: str,
    board_matches: list[dict[str, Any]],
    grouped: dict[str, list[dict[str, Any]]],
    board_summaries: list[dict[str, Any]],
    top_candidates: list[dict[str, Any]],
    *,
    all_candidates: list[dict[str, Any]] | None = None,
    coverage_warning: bool = False,
    wencai_seed_count: int = 0,
    out_of_scope_candidates: list[dict[str, Any]] | None = None,
) -> None:
    headline = "全域过热，空仓等待"
    if grouped["red"]:
        headline = "红色狩猎区已出现，优先进入深度狙击"
    elif grouped["yellow"]:
        headline = "黄色观察区为主，等待 Tier 0 与宏观信号继续收敛"
    elif coverage_warning:
        headline = "覆盖不足，暂不下全域过热结论"

    lines = [
        "# A股资源-铲子行动温度图",
        "",
        f"> 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> 扫描范围: {scope}",
        f"> 匹配板块数: {len(board_matches)}",
        f"> 语义预筛候选数: {wencai_seed_count}",
        f"> 雷达结论: {headline}",
        "",
        "## 板块热度摘要",
        "",
        _render_board_table(board_summaries),
        "",
        "## 优先跟踪候选",
        "",
        _render_zone_table(top_candidates),
        "",
        "## 红色狩猎区",
        "",
        _render_zone_table(grouped["red"]),
        "",
        "## 黄色观察区",
        "",
        _render_zone_table(grouped["yellow"]),
        "",
        "## 绿色休息区",
        "",
        _render_zone_table(grouped["green"]),
        "",
    ]
    if out_of_scope_candidates:
        lines.extend(
            [
                "## 能力圈外（仅供参考）",
                "",
                _render_zone_table(out_of_scope_candidates[:15]),
                "",
            ]
        )
    if not grouped["red"] and not grouped["yellow"] and not coverage_warning:
        lines.extend(["## 结论", "", "全域过热，空仓等待。"])
    if coverage_warning:
        lines.extend(["## 结论", "", "当前样本覆盖仍不足，先扩样本池，不直接下“全域过热”结论。"])

    incomplete_count = sum(1 for item in (all_candidates or []) if item.get("data_quality") == "incomplete_data")
    lines.extend(
        [
            "",
            "---",
            "",
            "## 数据质量说明与局限性",
            "",
            "> **⚠️ 重要免责声明**",
            ">",
            "> 1. **国资属性**: 语义预筛和 akshare 只做初筛，最终仍需 Tier 0 年报控制关系图核验。",
            "> 2. **PB / 位置数据**: 来自实时快照 + K线回溯，口径可能与财报静态口径有差异。",
            "> 3. **未纳入库存/Capex 完整验证**: 雷达只负责初筛，完整四维验证要进入深度狙击流程。",
            "> 4. **pywencai 语义筛选**: 作为候选扩样层使用，若同花顺查询异常，会自动回退到板块扫描。",
            "",
            f"数据不完整的标的数量: {incomplete_count}",
            "",
        ]
    )

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def run_radar_scan(scope: str = "all", board_limit: int = 18, stock_limit_per_board: int = 12) -> dict[str, Any]:
    industry_mapping = load_industry_mapping()
    effective_board_limit = max(board_limit, 18) if (scope or "").strip().lower() == "all" else board_limit
    board_matches = _match_boards(scope, industry_mapping)[:effective_board_limit]
    try:
        spot_df = ak.stock_zh_a_spot_em()
        spot_map = {_normalize_code(row.get("代码")): row.to_dict() for _, row in spot_df.iterrows()}
    except Exception:
        spot_map = _load_cached_spot_map()

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    _base = _get_base_dir()
    raw_dir = os.path.join(_base, "data", "raw", "radar", timestamp)
    processed_dir = os.path.join(_base, "data", "processed", "radar", timestamp)
    report_path = os.path.join(_base, "reports", f"行动温度图_{timestamp}.md")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    all_candidates: list[dict[str, Any]] = []
    out_of_scope_candidates: list[dict[str, Any]] = []
    seen_codes: set[str] = set()

    wencai_seeds = _fetch_wencai_seed_candidates(scope, industry_mapping)
    for seed in wencai_seeds:
        try:
            candidate = _scan_stock_candidate(
                seed["code"],
                seed["name"],
                seed["board_name"],
                industry_mapping,
                pb=_safe_float(seed.get("pb")),
                market_cap_yuan=_safe_float(seed.get("market_cap_yuan")),
                controller_hint=normalize_text(seed.get("controller")),
            )
            candidate["board_name"] = seed["board_name"]
            candidate["seed_source"] = seed.get("seed_source", "pywencai")
            all_candidates.append(candidate)
            if candidate.get("eco_circle") == "unknown":
                out_of_scope_candidates.append(candidate)
            else:
                grouped[candidate["zone"]].append(candidate)
            seen_codes.add(candidate["code"])
        except Exception as exc:
            all_candidates.append(
                {
                    "code": seed["code"],
                    "name": seed["name"],
                    "board_name": seed["board_name"],
                    "zone": "green",
                    "note": f"pywencai_seed_error: {exc}",
                    "data_quality": "incomplete_data",
                }
            )
            seen_codes.add(seed["code"])

    for board in board_matches:
        print(f"[radar] 扫描板块: {board['board_name']}")
        board_candidates: list[dict[str, str]] = []
        local_seen: set[str] = set()
        try:
            cons_df = ak.stock_board_industry_cons_em(symbol=board["board_name"])
            for _, row in cons_df.iterrows():
                stock = _extract_stock_row(row)
                if not stock["code"] or stock["code"] in local_seen:
                    continue
                local_seen.add(stock["code"])
                board_candidates.append(stock)
                if len(board_candidates) >= stock_limit_per_board:
                    break
        except Exception:
            for stock in _load_cached_board_candidates(board["board_name"]):
                if not stock["code"] or stock["code"] in local_seen:
                    continue
                local_seen.add(stock["code"])
                board_candidates.append(stock)
                if len(board_candidates) >= stock_limit_per_board:
                    break

        for stock in board_candidates:
            if stock["code"] in seen_codes:
                continue
            try:
                spot_row = spot_map.get(stock["code"], {})
                candidate = _scan_stock_candidate(
                    stock["code"],
                    stock["name"],
                    board["board_name"],
                    industry_mapping,
                    pb=_safe_float(spot_row.get("市净率")),
                    market_cap_yuan=_safe_float(spot_row.get("总市值")),
                )
                candidate["board_name"] = board["board_name"]
                candidate["seed_source"] = "ak_board"
                all_candidates.append(candidate)
                if candidate.get("eco_circle") == "unknown":
                    out_of_scope_candidates.append(candidate)
                else:
                    grouped[candidate["zone"]].append(candidate)
                seen_codes.add(candidate["code"])
            except Exception as exc:
                all_candidates.append(
                    {
                        "code": stock["code"],
                        "name": stock["name"],
                        "board_name": board["board_name"],
                        "zone": "green",
                        "note": f"scan_error: {exc}",
                        "data_quality": "incomplete_data",
                    }
                )
                seen_codes.add(stock["code"])

    in_scope_candidates = [item for item in all_candidates if item.get("eco_circle") != "unknown"]
    board_summaries = _build_board_summaries(board_matches, in_scope_candidates)
    top_candidates = sorted([item for item in all_candidates if item.get("zone") in {"red", "yellow"}], key=lambda row: row.get("radar_score", 0), reverse=True)[:10]
    coverage_warning = (scope or "").strip().lower() == "all" and len(board_summaries) < 8 and len(all_candidates) < 20
    headline = (
        "覆盖不足，暂不下全域过热结论"
        if coverage_warning and not grouped["red"] and not grouped["yellow"]
        else "全域过热，空仓等待"
        if not grouped["red"] and not grouped["yellow"]
        else "红色狩猎区已出现，优先进入深度狙击"
        if grouped["red"]
        else "黄色观察区为主，等待 Tier 0 与宏观信号继续收敛"
    )

    result = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scope": scope,
        "board_matches": board_matches,
        "wencai_seeds": wencai_seeds,
        "candidates": all_candidates,
        "board_summaries": board_summaries,
        "top_candidates": top_candidates,
        "summary": {
            "red_count": len(grouped["red"]),
            "yellow_count": len(grouped["yellow"]),
            "green_count": len(grouped["green"]),
            "headline": headline,
            "coverage_warning": coverage_warning,
            "board_match_count": len(board_matches),
            "wencai_seed_count": len(wencai_seeds),
            "candidate_count": len(all_candidates),
            "out_of_scope_count": len(out_of_scope_candidates),
        },
        "report_path": report_path,
    }

    honest_idle_triggered = _should_trigger_honest_idle_breaker(in_scope_candidates)
    result["summary"]["honest_idle_triggered"] = honest_idle_triggered
    if honest_idle_triggered:
        result["status"] = "honest_idle_sleep"
        result["message"] = HONEST_IDLE_MESSAGE
        result["summary"]["headline"] = HONEST_IDLE_MESSAGE
        result["report_path"] = None
        print(f"[radar] {HONEST_IDLE_MESSAGE}")
        with open(os.path.join(raw_dir, "radar_candidates.json"), "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        return result

    with open(os.path.join(raw_dir, "radar_candidates.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    with open(os.path.join(processed_dir, "radar_summary.json"), "w", encoding="utf-8") as f:
        json.dump({**result["summary"], "board_summaries": board_summaries, "top_candidates": top_candidates}, f, ensure_ascii=False, indent=2, default=str)

    _write_radar_report(
        report_path,
        scope,
        board_matches,
        grouped,
        board_summaries,
        top_candidates,
        all_candidates=all_candidates,
        coverage_warning=coverage_warning,
        wencai_seed_count=len(wencai_seeds),
        out_of_scope_candidates=out_of_scope_candidates,
    )
    print(f"[radar] 报告已生成: {report_path}")
    return result


if __name__ == "__main__":
    scan_scope = sys.argv[1] if len(sys.argv) > 1 else "all"
    board_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 18
    stock_limit = int(sys.argv[3]) if len(sys.argv) > 3 else 12
    run_radar_scan(scan_scope, board_limit, stock_limit)
