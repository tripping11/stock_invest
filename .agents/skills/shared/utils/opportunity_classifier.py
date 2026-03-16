"""Opportunity typing, moat, management, catalyst, and bottom-pattern assessors."""
from __future__ import annotations

from typing import Any

from utils.config_loader import load_moat_dictionary, load_sector_classification
from utils.financial_snapshot import extract_latest_revenue_snapshot, extract_latest_revenue_terms
from utils.value_utils import clamp, extract_first_value, normalize_text, safe_float


OPPORTUNITY_TYPE_LABELS = {
    "compounder": "Compounder",
    "cyclical": "Cyclical",
    "turnaround": "Turnaround",
    "asset_play": "Asset play",
    "special_situation": "Special situation",
    "unknown": "Unknown",
}

GOOD_MANAGEMENT_KEYWORDS = ("分红", "回购", "激励", "提质增效", "现金流", "股东回报", "降本增效")
BAD_MANAGEMENT_KEYWORDS = ("占用", "违规", "处罚", "造假", "内控缺陷", "减持", "商誉爆雷")
CATALYST_KEYWORDS = {
    "pricing recovery": ("提价", "价差修复", "涨价", "价格反转"),
    "order growth": ("订单增长", "中标", "新签订单", "产销两旺"),
    "balance sheet repair": ("降杠杆", "再融资", "债务重组", "现金流转正"),
    "asset unlock": ("资产注入", "重估", "分拆", "出售资产", "REIT"),
    "capital return": ("回购", "分红提升", "特别分红"),
    "policy tailwind": ("政策支持", "补贴", "批复", "牌照", "军品列装"),
    "cycle turn": ("去库存", "补库", "产能出清", "景气回升"),
}


def assess_business_purity(revenue_records: list[dict[str, Any]]) -> dict[str, Any]:
    snapshot = extract_latest_revenue_snapshot(revenue_records)
    if not snapshot:
        return {
            "latest_report_date": "",
            "top_segment": "",
            "top_ratio": 0.0,
            "pass": False,
            "data_quality": "missing_revenue_breakdown",
        }

    from utils.value_utils import _pick_revenue_col

    name_col = _pick_revenue_col(snapshot, ("主营构成", "产品名称", "分类名称", "名称"), contains=("名称",))
    ratio_col = _pick_revenue_col(snapshot, ("收入比例", "营业收入占比", "占比"), contains=("占比",))
    revenue_col = _pick_revenue_col(snapshot, ("主营收入", "营业收入"), contains=("收入",))
    ranked: list[tuple[float, str]] = []
    total_revenue = 0.0
    for row in snapshot:
        name = normalize_text(row.get(name_col or ""))
        if not name or any(token in name for token in ("其他", "合计", "国内", "国外")):
            continue
        ratio = safe_float(row.get(ratio_col or ""))
        revenue = safe_float(row.get(revenue_col or ""))
        if revenue is not None:
            total_revenue += revenue
        score = ratio if ratio is not None else (revenue or 0.0)
        ranked.append((score, name))
    ranked.sort(reverse=True)
    top_value, top_name = ranked[0] if ranked else (0.0, "")
    top_ratio = top_value
    if ratio_col is None and total_revenue > 0:
        top_ratio = top_value / total_revenue * 100
    return {
        "latest_report_date": normalize_text(extract_first_value(snapshot[0], ("报告日期", "报告期", "日期", "报告日"))),
        "top_segment": top_name,
        "top_ratio": top_ratio,
        "pass": top_ratio >= 50,
        "data_quality": "ok",
    }


def assess_bottom_pattern(kline_summary: dict[str, Any], valuation_summary: dict[str, Any]) -> dict[str, Any]:
    current_vs_high = safe_float(kline_summary.get("current_vs_5yr_high")) or safe_float(kline_summary.get("current_vs_high"))
    pb = safe_float(valuation_summary.get("pb"))
    pb_percentile = safe_float(valuation_summary.get("pb_percentile"))
    score = 0.0
    reasons: list[str] = []
    if current_vs_high is not None:
        if current_vs_high <= 55:
            score += 2.0
            reasons.append(f"price is still far below prior highs ({current_vs_high:.1f}% of 5y high)")
        elif current_vs_high >= 85:
            score -= 1.5
            reasons.append(f"price is already near highs ({current_vs_high:.1f}% of 5y high)")
    if pb is not None:
        if pb <= 0.9:
            score += 2.0
            reasons.append(f"PB is deep value at {pb:.2f}")
        elif pb >= 2.5:
            score -= 1.5
            reasons.append(f"PB is already elevated at {pb:.2f}")
    if pb_percentile is not None:
        if pb_percentile <= 20:
            score += 1.0
            reasons.append(f"PB percentile is low at {pb_percentile:.1f}%")
        elif pb_percentile >= 80:
            score -= 1.0
            reasons.append(f"PB percentile is high at {pb_percentile:.1f}%")
    return {
        "score": clamp(score, -3.0, 5.0),
        "signal": "favorable" if score >= 3 else "mixed" if score >= 1 else "unfavorable",
        "reason": "; ".join(reasons) if reasons else "insufficient price-position evidence",
    }


def classify_state_ownership(stock_code: str, controller_text: str, *, company_name_hints: list[str] | None = None) -> dict[str, Any]:
    text = normalize_text(controller_text)
    if not text and company_name_hints:
        text = " ".join(company_name_hints)
    if any(token in text for token in ("国务院国资委", "中央企业", "央企", "中央汇金")):
        return {"category": "central_soe", "label": "central SOE", "score_impact": 2, "reason": text or "controller shows central state ownership"}
    if any(token in text for token in ("省国资委", "省人民政府", "省属国资")):
        return {"category": "provincial_soe", "label": "provincial SOE", "score_impact": 1, "reason": text or "controller shows provincial state ownership"}
    if any(token in text for token in ("市国资委", "地方国资", "地方国有")):
        return {"category": "local_soe", "label": "local SOE", "score_impact": 0, "reason": text or "controller shows local state ownership"}
    if any(token in text for token in ("国有", "国资")):
        return {"category": "state_backed", "label": "state-backed", "score_impact": 0, "reason": text or "controller suggests state backing"}
    if any(token in text for token in ("自然人", "家族", "民营", "私募", "投资管理")):
        return {"category": "private", "label": "private / non-state", "score_impact": 0, "reason": text or "controller looks private"}
    return {"category": "unknown", "label": "ownership unclear", "score_impact": 0, "reason": text or "controller not clearly classified"}


def determine_opportunity_type(
    stock_code: str,
    company_profile: dict[str, Any],
    *,
    revenue_records: list[dict[str, Any]] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    mapping = load_sector_classification()
    override = (mapping.get("company_overrides", {}) or {}).get(str(stock_code), {}) or {}
    industry_text = normalize_text(
        company_profile.get("行业")
        or company_profile.get("所属行业")
        or company_profile.get("申万行业")
        or company_profile.get("申万一级行业")
        or ""
    )
    base_texts = [
        industry_text,
        normalize_text(company_profile.get("主营业务")),
        normalize_text(company_profile.get("经营范围")),
        normalize_text(company_profile.get("公司名称")),
    ]
    base_texts.extend(extract_latest_revenue_terms(revenue_records or [], limit=12))
    base_texts.extend(extra_texts or [])
    combined = " ".join(text for text in base_texts if text)

    if override.get("primary_type"):
        primary_type = normalize_text(override.get("primary_type")).lower()
        secondary_types = override.get("secondary_types", []) or []
        return {
            "primary_type": primary_type,
            "primary_label": OPPORTUNITY_TYPE_LABELS.get(primary_type, primary_type),
            "secondary_types": secondary_types,
            "matched_terms": [normalize_text(override.get("name"))] if override.get("name") else [],
            "confidence": "high",
            "industry_text": industry_text,
            "reason": f"company override maps the stock to {OPPORTUNITY_TYPE_LABELS.get(primary_type, primary_type)}",
            "sentence": f"This is primarily a {OPPORTUNITY_TYPE_LABELS.get(primary_type, primary_type)} opportunity because the company override already maps it there.",
        }

    scores: dict[str, int] = {}
    hits_by_type: dict[str, list[str]] = {}
    for type_name, cfg in (mapping.get("opportunity_types", {}) or {}).items():
        score = 0
        hits: list[str] = []
        for keyword in cfg.get("keywords", []) or []:
            if keyword and keyword in combined:
                score += 2
                hits.append(keyword)
        for signal in cfg.get("signals", []) or []:
            if signal and signal in combined:
                score += 1
                hits.append(signal)
        scores[type_name] = score
        hits_by_type[type_name] = hits

    sorted_types = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    primary_type, primary_score = sorted_types[0] if sorted_types else ("unknown", 0)
    secondary_types = [name for name, score in sorted_types[1:3] if score > 0]
    confidence = "high" if primary_score >= 6 else "medium" if primary_score >= 3 else "low"
    matched_terms = hits_by_type.get(primary_type, [])
    if primary_score <= 0:
        primary_type = "unknown"
        matched_terms = []
    reason = (
        f"matched sector and context terms: {', '.join(dict.fromkeys(matched_terms[:5]))}"
        if matched_terms
        else "available business text does not cleanly fit a single opportunity bucket"
    )
    label = OPPORTUNITY_TYPE_LABELS.get(primary_type, primary_type)
    return {
        "primary_type": primary_type,
        "primary_label": label,
        "secondary_types": secondary_types,
        "matched_terms": matched_terms,
        "confidence": confidence,
        "industry_text": industry_text,
        "reason": reason,
        "sentence": f"This is primarily a {label} opportunity because {reason}.",
    }


def assess_moat_quality(
    company_profile: dict[str, Any],
    *,
    revenue_records: list[dict[str, Any]] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    dictionary = load_moat_dictionary()
    combined = " ".join(
        text
        for text in [
            normalize_text(company_profile.get("主营业务")),
            normalize_text(company_profile.get("经营范围")),
            normalize_text(company_profile.get("公司名称")),
            *extract_latest_revenue_terms(revenue_records or [], limit=8),
            *(extra_texts or []),
        ]
        if text
    )
    matches: list[str] = []
    for cfg in (dictionary.get("categories", {}) or {}).values():
        for keyword in cfg.get("keywords", []) or []:
            if keyword and keyword in combined:
                matches.append(cfg.get("label", keyword))
                break
    unique_matches = list(dict.fromkeys(matches))
    score = min(10, len(unique_matches) * 3 + (1 if unique_matches else 0))
    verdict = "strong" if score >= 8 else "moderate" if score >= 5 else "weak"
    return {
        "score": score,
        "verdict": verdict,
        "matched_categories": unique_matches,
        "reason": ", ".join(unique_matches) if unique_matches else "no durable moat evidence surfaced from available text",
    }


def assess_management_quality(
    company_profile: dict[str, Any],
    ownership: dict[str, Any],
    *,
    extra_texts: list[str] | None = None,
) -> dict[str, Any]:
    combined = " ".join(
        text
        for text in [
            normalize_text(company_profile.get("主营业务")),
            normalize_text(company_profile.get("经营范围")),
            normalize_text(company_profile.get("实际控制人")),
            *(extra_texts or []),
        ]
        if text
    )
    score = 5 + int(ownership.get("score_impact", 0))
    reasons: list[str] = [normalize_text(ownership.get("label"))] if ownership.get("label") else []
    red_flags: list[str] = []
    for keyword in GOOD_MANAGEMENT_KEYWORDS:
        if keyword in combined:
            score += 1
            reasons.append(keyword)
    for keyword in BAD_MANAGEMENT_KEYWORDS:
        if keyword in combined:
            score -= 2
            red_flags.append(keyword)
    score = int(clamp(score, 0, 10))
    verdict = "strong" if score >= 8 else "adequate" if score >= 5 else "weak"
    return {
        "score": score,
        "verdict": verdict,
        "reasons": list(dict.fromkeys(reasons)),
        "red_flags": list(dict.fromkeys(red_flags)),
    }


def assess_catalyst_strength(*texts: str) -> dict[str, Any]:
    combined = " ".join(normalize_text(text) for text in texts if normalize_text(text))
    matched: list[str] = []
    for label, keywords in CATALYST_KEYWORDS.items():
        if any(keyword in combined for keyword in keywords):
            matched.append(label)
    score = min(10, len(matched) * 2)
    verdict = "strong" if score >= 6 else "moderate" if score >= 3 else "weak"
    return {
        "score": score,
        "verdict": verdict,
        "catalysts": matched,
        "reason": ", ".join(matched) if matched else "no concrete catalyst surfaced from current text",
    }
