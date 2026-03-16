"""Framework helpers for the whole-market investment workflow.

This module is now a **compatibility facade**. All implementations have been
moved to focused sub-modules; this file re-exports every public symbol so
that existing ``from utils.framework_utils import X`` statements keep working.
"""
from __future__ import annotations

# ── re-exports from config_loader ────────────────────────────
from utils.config_loader import (  # noqa: F401
    CONFIG_DIR,
    SHARED_DIR,
    load_moat_dictionary,
    load_scoring_rules,
    load_sector_classification,
    load_source_registry,
    load_valuation_discipline,
    load_yaml_config,
)

# ── re-exports from value_utils ──────────────────────────────
from utils.value_utils import (  # noqa: F401
    _pick_revenue_col,
    _sortable_date,
    clamp,
    extract_first_value,
    normalize_text,
    safe_float,
    select_latest_record,
)

# ── re-exports from financial_snapshot ───────────────────────
from utils.financial_snapshot import (  # noqa: F401
    extract_latest_price,
    extract_latest_revenue_snapshot,
    extract_latest_revenue_terms,
    extract_market_cap,
    get_latest_balance_snapshot,
    get_latest_income_snapshot,
)

from utils.primary_type_router import (  # noqa: F401
    build_driver_stack,
    determine_primary_type,
    infer_preliminary_cycle_state,
    resolve_sector_route,
)
from utils.vcrf_probes import (  # noqa: F401
    assess_business_or_asset_quality,
    assess_governance_anti_fraud,
    assess_intrinsic_value_floor,
    assess_normalized_earnings_power,
    assess_survival_boundary,
    detect_big_bath,
    score_underwrite_axis,
)

# ── re-exports from opportunity_classifier ───────────────────
from utils.opportunity_classifier import (  # noqa: F401
    BAD_MANAGEMENT_KEYWORDS,
    CATALYST_KEYWORDS,
    GOOD_MANAGEMENT_KEYWORDS,
    OPPORTUNITY_TYPE_LABELS,
    assess_bottom_pattern,
    assess_business_purity,
    assess_catalyst_strength,
    assess_management_quality,
    assess_moat_quality,
    classify_state_ownership,
    determine_opportunity_type,
)

# ── re-exports from score_verdict ────────────────────────────
from utils.score_verdict import pick_score_verdict  # noqa: F401
