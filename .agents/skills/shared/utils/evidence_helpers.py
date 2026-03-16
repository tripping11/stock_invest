"""Shared evidence and timestamp helpers.

All adapters should import from here instead of defining their own
``_now()`` / ``_make_evidence()`` variants.
"""
from __future__ import annotations

import datetime
from typing import Any


def now_iso() -> str:
    """ISO 8601 timestamp with *T* separator — used for cache freshness."""
    return datetime.datetime.now().isoformat()


def now_ts() -> str:
    """Human-readable timestamp for evidence records and logs."""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_evidence(
    field: str,
    value: Any,
    source_desc: str,
    *,
    source_type: str = "unknown",
    tier: int = 1,
    url: str = "",
    confidence: str = "medium",
) -> dict[str, Any]:
    """Build a standard evidence dict.

    Parameters
    ----------
    field : str
        The data field this evidence covers (e.g. ``"spot_price"``).
    value : Any
        A short representation of the collected value.
    source_desc : str
        Free-text description of the data source or method.
    source_type : str
        Tag such as ``"akshare"``, ``"cninfo_official"``, ``"commodity_data"``,
        ``"stats_gov"``.
    tier : int
        Evidence tier (0 = official filings, 1 = aggregator, 2 = commodity).
    url : str
        Optional URL pointing to the upstream source.
    confidence : str
        One of ``"high"``, ``"medium"``, ``"low"``.
    """
    return {
        "field_name": field,
        "value": value,
        "source_tier": tier,
        "source_type": source_type,
        "source_url": url,
        "description": source_desc,
        "fetch_time": now_ts(),
        "confidence": confidence,
    }
