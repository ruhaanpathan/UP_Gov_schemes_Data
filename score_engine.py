"""
═══════════════════════════════════════════════════════════════
  IMPACT SCORE ENGINE — Data-Driven Computation
  
  Computes impact_score (0-10) from actual metrics:
    1. Scale Score      (25%) — beneficiaries relative to UP population
    2. Budget Efficiency (20%) — beneficiaries per crore spent
    3. Disbursement Rate (20%) — actual spend vs allocated budget
    4. Coverage/Reach    (20%) — % of target group covered
    5. Longevity         (15%) — years since launch
═══════════════════════════════════════════════════════════════
"""
import math
from datetime import datetime

UP_POPULATION_LAKH = 2412  # ~24.12 Crore (2024 estimate)
CURRENT_YEAR = datetime.now().year
MAX_SCHEME_AGE = 10  # normalizer for longevity (2016-2026)


def _clamp(val, lo=0.0, hi=10.0):
    return max(lo, min(hi, val))


def _safe_div(a, b, default=0.0):
    return a / b if b and b > 0 else default


def scale_score(beneficiaries_lakh):
    """Score 0-10 based on how many people reached relative to UP population."""
    if not beneficiaries_lakh or beneficiaries_lakh <= 0:
        return 0.0
    # Log-scaled: 0.1L→~2, 1L→~4, 10L→~6, 50L→~8, 160L→~10
    ratio = beneficiaries_lakh / UP_POPULATION_LAKH
    score = 2.0 * math.log10(max(ratio * 10000, 1))
    return _clamp(score)


def efficiency_score(beneficiaries_lakh, budget_crore):
    """Score 0-10 based on beneficiaries per crore spent."""
    if not budget_crore or budget_crore <= 0:
        return 5.0  # neutral if no budget data
    bpc = (beneficiaries_lakh * 100000) / budget_crore  # people per crore
    # Log-scaled: 1→~2, 10→~4, 100→~6, 1000→~8, 10000→~10
    score = 2.0 * math.log10(max(bpc, 1))
    return _clamp(score)


def disbursement_score(disbursed_crore, allocated_crore):
    """Score 0-10 based on actual disbursement vs allocation."""
    if not allocated_crore or allocated_crore <= 0:
        return 5.0  # neutral if no data
    if not disbursed_crore or disbursed_crore <= 0:
        return 3.0  # penalty for no disbursement data
    rate = disbursed_crore / allocated_crore
    # 0%→0, 50%→5, 80%→8, 100%→10
    return _clamp(rate * 10.0)


def coverage_score(reach_percent):
    """Score 0-10 based on coverage of target group."""
    if reach_percent is None or reach_percent <= 0:
        return 3.0
    return _clamp(reach_percent / 10.0)


def longevity_score(launch_year):
    """Score 0-10 based on how long the scheme has been running."""
    if not launch_year:
        return 5.0
    years = CURRENT_YEAR - launch_year
    return _clamp((years / MAX_SCHEME_AGE) * 10.0)


def compute_impact_score(scheme_data, scraped_override=None):
    """
    Compute impact score from data metrics.
    
    Args:
        scheme_data: dict with keys like beneficiaries_lakh, budget_crore, 
                     launch_year, reach_percent, etc.
        scraped_override: dict with live-scraped values that override curated data.
                         Keys: beneficiaries_lakh, disbursed_crore, reach_percent, etc.
    
    Returns:
        dict with impact_score (0-10), component scores, and data sources.
    """
    # Merge scraped overrides
    d = dict(scheme_data)
    sources = {}
    
    if scraped_override:
        for key, val in scraped_override.items():
            if val is not None and val > 0:
                d[key] = val
                sources[key] = "scraped"
            else:
                sources[key] = "curated"
    
    # Compute component scores
    s1 = scale_score(d.get("beneficiaries_lakh", 0))
    s2 = efficiency_score(d.get("beneficiaries_lakh", 0), d.get("budget_crore", 0))
    s3 = disbursement_score(d.get("disbursed_crore", 0), d.get("budget_crore", 0))
    s4 = coverage_score(d.get("reach_percent", 0))
    s5 = longevity_score(d.get("launch_year"))
    
    # Weighted average
    impact = (s1 * 0.25) + (s2 * 0.20) + (s3 * 0.20) + (s4 * 0.20) + (s5 * 0.15)
    impact = round(_clamp(impact), 1)
    
    return {
        "impact_score": impact,
        "components": {
            "scale": round(s1, 1),
            "efficiency": round(s2, 1),
            "disbursement": round(s3, 1),
            "coverage": round(s4, 1),
            "longevity": round(s5, 1),
        },
        "data_sources": sources,
        "formula": "0.25×Scale + 0.20×Efficiency + 0.20×Disbursement + 0.20×Coverage + 0.15×Longevity",
    }


def compute_verdict(impact_score, reach_percent=0, years_active=0):
    """Compute verdict from impact score — no hardcoding."""
    if years_active <= 2:
        return "Too Early to Judge"
    if impact_score >= 7.5:
        return "Major Success"
    elif impact_score >= 6.0:
        return "Success"
    elif impact_score >= 5.0:
        return "Moderate Success"
    elif impact_score >= 4.0:
        return "Mixed"
    else:
        return "Underperformed"
