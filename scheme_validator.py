"""
UP SCHEMES — DEEP POLICY VALIDATION ENGINE
Evaluates 7 dimensions per scheme:
  1. Policy Impact, 2. Beneficiary Authenticity, 3. Inter-Year Consistency,
  4. District Distribution, 5. Budget Efficiency, 6. Political Usefulness, 7. Data Trust Score
"""
import json, os, math
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

UP_POP_LAKH = 2412
UP_DISTRICTS = 75
UP_BPL_PCT = 29.4

# Known flags per scheme for deeper analysis
SCHEME_INTEL = {
    "MKSY": {
        "verified_by": "MKSY Portal + RTI data",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 50, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2020: 9.5, 2021: 14.2, 2022: 18.0, 2023: 22.5, 2024: 26.81},
        "news_sentiment": "positive", "audit_status": "CAG audited",
        "real_world_impact": "Reduced child marriage, improved girl enrollment",
    },
    "MSVY": {
        "verified_by": "District reports",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 10, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2018: 0.5, 2019: 1.2, 2020: 1.8, 2021: 2.3, 2022: 2.8, 2023: 3.2, 2024: 3.5},
        "news_sentiment": "positive", "audit_status": "state audit",
        "real_world_impact": "Reduced dowry burden on poor families",
    },
    "MS": {
        "verified_by": "Mission Shakti dashboard",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 500, "geographic_spread": "all_75",
        "duplicate_risk": "medium", "year_data": {2021: 10, 2022: 25, 2023: 38, 2024: 50},
        "news_sentiment": "positive", "audit_status": "internal review",
        "real_world_impact": "BC Sakhis providing doorstep banking, women safety awareness",
    },
    "OAP": {
        "verified_by": "SSPY Portal (live scraped)",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 85, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2017: 42, 2018: 46, 2019: 49, 2020: 50.5, 2021: 52, 2022: 54, 2023: 55.99, 2024: 67.5},
        "news_sentiment": "mixed", "audit_status": "SSPY live verification + 1.77L fakes removed",
        "real_world_impact": "Basic survival support for elderly, but ₹1000/month insufficient",
    },
    "WPS": {
        "verified_by": "SSPY Portal (live scraped)",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 45, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2017: 18, 2018: 20, 2019: 22, 2020: 23.5, 2021: 25, 2022: 26.12, 2023: 29.03, 2024: 26.13},
        "news_sentiment": "mixed", "audit_status": "SSPY verified",
        "real_world_impact": "Financial lifeline for destitute widows",
    },
    "DPS": {
        "verified_by": "SSPY Portal (live scraped)",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 24, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2017: 7, 2018: 8.5, 2019: 9.5, 2020: 10, 2021: 10.5, 2022: 11, 2023: 11.5, 2024: 11.05},
        "news_sentiment": "neutral", "audit_status": "SSPY verified",
        "real_world_impact": "Disability support but low awareness limits reach",
    },
    "RPLY": {
        "verified_by": "District office records",
        "dbt_linked": True, "aadhaar_linked": False,
        "target_pop_lakh": 15, "geographic_spread": "partial_60",
        "duplicate_risk": "medium", "year_data": {2018: 1, 2019: 2, 2020: 2.5, 2021: 3.2, 2022: 4, 2023: 4.5, 2024: 5},
        "news_sentiment": "negative", "audit_status": "pending",
        "real_world_impact": "₹30K one-time aid inadequate for family that lost breadwinner",
    },
    "KRM": {
        "verified_by": "Bank records (partial)",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 230, "geographic_spread": "all_75",
        "duplicate_risk": "high", "year_data": {2017: 86},
        "news_sentiment": "negative", "audit_status": "multiple complaints",
        "real_world_impact": "Short-term relief but no long-term farm distress fix",
    },
    "MKDKY": {
        "verified_by": "District agriculture office",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 5, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2020: 0.5, 2021: 0.8, 2022: 1.2, 2023: 1.6, 2024: 2.0},
        "news_sentiment": "neutral", "audit_status": "state audit",
        "real_world_impact": "Meaningful accident compensation but slow claims process",
    },
    "KUY": {
        "verified_by": "DISCOM records",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 10, "geographic_spread": "partial_40",
        "duplicate_risk": "medium", "year_data": {2019: 0.3, 2020: 0.5, 2021: 0.8, 2022: 1.0, 2023: 1.2, 2024: 1.5},
        "news_sentiment": "negative", "audit_status": "CAG flagged delays",
        "real_world_impact": "Minimal — only 15% of 10L pump target achieved",
    },
    "KAK": {
        "verified_by": "Electricity board records",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 30, "geographic_spread": "all_75",
        "duplicate_risk": "medium", "year_data": {2019: 2, 2020: 4, 2021: 5.5, 2022: 6.5, 2023: 7.5, 2024: 8},
        "news_sentiment": "neutral", "audit_status": "internal",
        "real_world_impact": "Band-aid — lets farmers pay in installments but doesn't reduce cost",
    },
    "ODOP": {
        "verified_by": "ODOP Portal + export data",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 50, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2019: 5, 2020: 8, 2021: 12, 2022: 16, 2023: 20, 2024: 25},
        "news_sentiment": "very_positive", "audit_status": "export data verified",
        "real_world_impact": "₹93,000 Cr exports, international recognition, artisan livelihoods transformed",
    },
    "SEV": {
        "verified_by": "Portal registrations",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 100, "geographic_spread": "all_75",
        "duplicate_risk": "high", "year_data": {2019: 3, 2020: 5, 2021: 7, 2022: 10, 2023: 12, 2024: 15},
        "news_sentiment": "negative", "audit_status": "no independent audit",
        "real_world_impact": "Registrations ≠ placements — actual job creation questionable",
    },
    "CMAPS": {
        "verified_by": "Industry records",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 10, "geographic_spread": "partial_50",
        "duplicate_risk": "low", "year_data": {2020: 0.5, 2021: 1.0, 2022: 1.5, 2023: 2.2, 2024: 3.0},
        "news_sentiment": "neutral", "audit_status": "industry audited",
        "real_world_impact": "Training provided but low conversion to permanent employment",
    },
    "KS": {
        "verified_by": "Skill centre records",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 25, "geographic_spread": "partial_45",
        "duplicate_risk": "medium", "year_data": {2019: 1, 2020: 1.5, 2021: 2.5, 2022: 3.5, 2023: 4.2, 2024: 5},
        "news_sentiment": "negative", "audit_status": "multiple centres underutilized",
        "real_world_impact": "Training quality inconsistent, job outcomes not tracked",
    },
    "ARS": {
        "verified_by": "CBSE results + school records",
        "dbt_linked": False, "aadhaar_linked": True,
        "target_pop_lakh": 0.5, "geographic_spread": "18_divisions",
        "duplicate_risk": "none", "year_data": {2022: 0.02, 2023: 0.05, 2024: 0.08, 2025: 0.10},
        "news_sentiment": "very_positive", "audit_status": "CBSE verified results",
        "real_world_impact": "93% CBSE pass rate, transforming lives of construction workers' children",
    },
    "SCHOL": {
        "verified_by": "Scholarship portal (down)",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 120, "geographic_spread": "all_75",
        "duplicate_risk": "medium", "year_data": {2017: 50, 2018: 55, 2019: 60, 2020: 62, 2021: 65, 2022: 70, 2023: 75, 2024: 80},
        "news_sentiment": "mixed", "audit_status": "fake applications detected",
        "real_world_impact": "Critical education support but disbursement delays hurt students",
    },
    "FSBU": {
        "verified_by": "School distribution records",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 180, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2018: 100, 2019: 120, 2020: 130, 2021: 140, 2022: 145, 2023: 155, 2024: 160},
        "news_sentiment": "positive", "audit_status": "school verified",
        "real_world_impact": "Reduced dropout rates, lessened financial burden on poor families",
    },
    "AB": {
        "verified_by": "Ayushman Portal (live scraped)",
        "dbt_linked": False, "aadhaar_linked": True,
        "target_pop_lakh": 956, "geographic_spread": "all_75",
        "duplicate_risk": "medium", "year_data": {2019: 8, 2020: 15, 2021: 22, 2022: 30, 2023: 38, 2024: 45},
        "news_sentiment": "positive", "audit_status": "NHA monitored",
        "real_world_impact": "Life-saving for poor families, but hospital refusals and fraud exist",
    },
    "MJAY": {
        "verified_by": "State health department",
        "dbt_linked": False, "aadhaar_linked": True,
        "target_pop_lakh": 30, "geographic_spread": "partial_60",
        "duplicate_risk": "medium", "year_data": {2020: 2, 2021: 4, 2022: 6, 2023: 8, 2024: 10},
        "news_sentiment": "neutral", "audit_status": "internal",
        "real_world_impact": "Fills PMJAY gap but awareness is very low",
    },
    "PMAYG": {
        "verified_by": "PMAY-G Portal (partial scrape)",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 36.85, "geographic_spread": "all_75",
        "duplicate_risk": "low", "year_data": {2017: 5, 2018: 12, 2019: 20, 2020: 25, 2021: 28, 2022: 32, 2023: 35, 2024: 36.38},
        "news_sentiment": "very_positive", "audit_status": "geo-tagged verification",
        "real_world_impact": "36.38L pucca houses built — 98.7% completion rate, transformative",
    },
    "PMAYU": {
        "verified_by": "PMAY-U Portal (partial scrape)",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 21, "geographic_spread": "urban_centres",
        "duplicate_risk": "low", "year_data": {2017: 2, 2018: 5, 2019: 8, 2020: 10, 2021: 12, 2022: 14, 2023: 16, 2024: 17},
        "news_sentiment": "positive", "audit_status": "MoHUA monitored",
        "real_world_impact": "17L urban houses delivered, significant Housing For All contribution",
    },
    "BCS": {
        "verified_by": "SHG/NRLM records",
        "dbt_linked": True, "aadhaar_linked": True,
        "target_pop_lakh": 1, "geographic_spread": "all_75",
        "duplicate_risk": "none", "year_data": {2021: 0.1, 2022: 0.2, 2023: 0.35, 2024: 0.5},
        "news_sentiment": "positive", "audit_status": "bank verified",
        "real_world_impact": "50K women empowered as banking agents, financial inclusion improved",
    },
    "MSY": {
        "verified_by": "SHG records",
        "dbt_linked": False, "aadhaar_linked": False,
        "target_pop_lakh": 10, "geographic_spread": "partial_40",
        "duplicate_risk": "medium", "year_data": {2022: 0.5, 2023: 1.2, 2024: 2.0},
        "news_sentiment": "neutral", "audit_status": "too new",
        "real_world_impact": "Early stage — limited data on outcomes",
    },
}

SENTIMENT_MAP = {"very_positive": 10, "positive": 8, "mixed": 5, "neutral": 5, "negative": 3, "very_negative": 1}


def _clamp(v, lo=0, hi=10):
    return max(lo, min(hi, round(v, 1)))


def calc_policy_impact(scheme, intel):
    """How effective is this scheme at solving its stated problem?"""
    ben = scheme.get("beneficiaries_lakh", 0)
    target = intel.get("target_pop_lakh", 1)
    reach_pct = min(ben / max(target, 0.01) * 100, 100)
    sentiment = SENTIMENT_MAP.get(intel.get("news_sentiment", "neutral"), 5)
    impact_text = intel.get("real_world_impact", "")
    has_measurable = any(w in impact_text.lower() for w in ["reduced", "transformed", "life-saving", "93%", "98.7%"])
    score = (reach_pct / 100 * 4) + (sentiment / 10 * 3) + (3 if has_measurable else 1)
    reasoning = f"Reached {reach_pct:.0f}% of target ({ben}L/{target}L). {impact_text}"
    return _clamp(score), reasoning


def calc_beneficiary_authenticity(scheme, intel):
    """Are the beneficiary numbers real and verified?"""
    score = 5  # base
    flags = []
    if intel.get("dbt_linked"):
        score += 1.5; flags.append("DBT-linked ✓")
    if intel.get("aadhaar_linked"):
        score += 1.5; flags.append("Aadhaar-verified ✓")
    dup = intel.get("duplicate_risk", "medium")
    if dup == "none": score += 1; flags.append("No duplicate risk")
    elif dup == "low": score += 0.5; flags.append("Low duplicate risk")
    elif dup == "high": score -= 2; flags.append("⚠️ HIGH duplicate risk")
    audit = intel.get("audit_status", "")
    if "CAG" in audit or "CBSE" in audit or "live" in audit.lower():
        score += 1; flags.append(f"Verified: {audit}")
    elif "fake" in audit.lower() or "complaint" in audit.lower():
        score -= 1; flags.append(f"⚠️ {audit}")
    return _clamp(score), "; ".join(flags)


def calc_inter_year_consistency(scheme, intel):
    """Do year-over-year numbers show natural growth or suspicious jumps?"""
    yd = intel.get("year_data", {})
    if len(yd) < 2:
        return 5.0, "Insufficient yearly data for trend analysis"
    years = sorted(yd.keys())
    vals = [yd[y] for y in years]
    growth_rates = []
    for i in range(1, len(vals)):
        if vals[i-1] > 0:
            gr = (vals[i] - vals[i-1]) / vals[i-1] * 100
            growth_rates.append(gr)
    if not growth_rates:
        return 5.0, "Cannot compute growth rates"
    avg_growth = sum(growth_rates) / len(growth_rates)
    max_jump = max(abs(g) for g in growth_rates)
    has_decline = any(g < -10 for g in growth_rates)
    score = 7
    flags = []
    if max_jump > 100:
        score -= 2; flags.append(f"Suspicious spike: {max_jump:.0f}% in one year")
    if has_decline:
        score -= 1; flags.append("Has year-over-year decline")
    if 5 < avg_growth < 40:
        score += 1; flags.append(f"Healthy avg growth: {avg_growth:.0f}%/yr")
    elif avg_growth > 80:
        score -= 1; flags.append(f"Unusually rapid growth: {avg_growth:.0f}%/yr")
    trend = f"{years[0]}: {vals[0]}L → {years[-1]}: {vals[-1]}L"
    flags.insert(0, trend)
    return _clamp(score), "; ".join(flags)


def calc_district_distribution(scheme, intel):
    """Is the scheme reaching all 75 districts or concentrated?"""
    spread = intel.get("geographic_spread", "unknown")
    if spread == "all_75":
        return 9.0, f"All {UP_DISTRICTS} districts covered"
    elif spread == "urban_centres":
        return 6.0, "Urban centres only — rural areas excluded"
    elif spread == "18_divisions":
        return 7.0, "All 18 divisions covered (not district-level)"
    elif spread.startswith("partial_"):
        n = int(spread.split("_")[1])
        score = (n / UP_DISTRICTS) * 10
        return _clamp(score), f"~{n}/{UP_DISTRICTS} districts — {100*n//UP_DISTRICTS}% coverage"
    return 5.0, "Distribution data unavailable"


def calc_budget_efficiency(scheme, intel):
    """Cost per beneficiary and utilization analysis."""
    ben = scheme.get("beneficiaries_lakh", 0)
    budget = scheme.get("budget_crore", 0)
    disbursed = scheme.get("disbursed_crore", 0)
    if ben <= 0 or budget <= 0:
        return 5.0, "Insufficient data for efficiency calculation"
    cost_per_person = (budget * 1e7) / (ben * 1e5)
    ppb = scheme.get("per_person_benefit", "")
    util_rate = (disbursed / budget * 100) if disbursed > 0 else 0
    score = 5
    flags = []
    if cost_per_person < 5000:
        score += 2; flags.append(f"Very efficient: ₹{cost_per_person:,.0f}/person")
    elif cost_per_person < 20000:
        score += 1; flags.append(f"Efficient: ₹{cost_per_person:,.0f}/person")
    elif cost_per_person < 100000:
        flags.append(f"Moderate: ₹{cost_per_person:,.0f}/person")
    else:
        score -= 1; flags.append(f"Expensive: ₹{cost_per_person:,.0f}/person")
    if util_rate > 80:
        score += 2; flags.append(f"Utilization: {util_rate:.0f}% ✓")
    elif util_rate > 50:
        score += 1; flags.append(f"Utilization: {util_rate:.0f}%")
    elif util_rate > 0:
        flags.append(f"Low utilization: {util_rate:.0f}%")
    else:
        flags.append("Utilization data unavailable")
    if ppb:
        flags.append(f"Benefit: {ppb}")
    return _clamp(score), "; ".join(flags)


def calc_political_usefulness(scheme, intel):
    """Is this genuine welfare or mainly a political tool?"""
    score = 5
    flags = []
    ben = scheme.get("beneficiaries_lakh", 0)
    if ben > 50:
        score += 1; flags.append(f"Mass reach ({ben}L) — genuine welfare scale")
    yd = intel.get("year_data", {})
    if len(yd) >= 3:
        vals = [yd[y] for y in sorted(yd.keys())]
        if all(vals[i] <= vals[i+1] for i in range(len(vals)-1)):
            score += 1; flags.append("Consistent growth — not election-driven")
        election_years = [2017, 2022]
        for ey in election_years:
            if ey in yd and ey-1 in yd:
                jump = (yd[ey] - yd[ey-1]) / max(yd[ey-1], 0.01) * 100
                if jump > 60:
                    score -= 2; flags.append(f"⚠️ {jump:.0f}% spike in election year {ey}")
    if intel.get("dbt_linked"):
        score += 1; flags.append("DBT reduces misuse potential")
    sentiment = intel.get("news_sentiment", "neutral")
    if sentiment in ("very_positive", "positive"):
        score += 1; flags.append("Positive independent assessment")
    elif sentiment == "negative":
        score -= 1; flags.append("Negative media coverage")
    return _clamp(score), "; ".join(flags)


def calc_data_trust(scheme, intel, scrape_status):
    """How reliable is the data we have for this scheme?"""
    score = 5
    flags = []
    if scrape_status == "LIVE":
        score += 2; flags.append("Live scraped from govt portal ✓")
    elif scrape_status == "PARTIAL":
        score += 0.5; flags.append("Partial scrape (page fetched, no metrics)")
    else:
        flags.append("Research baseline only — no live verification")
    if intel.get("aadhaar_linked"):
        score += 1; flags.append("Aadhaar-authenticated")
    if intel.get("dbt_linked"):
        score += 0.5; flags.append("DBT-tracked")
    yd = intel.get("year_data", {})
    if len(yd) >= 5:
        score += 1; flags.append(f"{len(yd)} years of data")
    verified = intel.get("verified_by", "")
    if "live" in verified.lower() or "CAG" in verified or "CBSE" in verified:
        score += 1; flags.append(f"Source: {verified}")
    dup = intel.get("duplicate_risk", "medium")
    if dup == "high":
        score -= 1.5; flags.append("⚠️ High duplicate risk lowers trust")
    return _clamp(score), "; ".join(flags)


def validate_scheme(scheme, scrape_status="RESEARCH_ONLY"):
    """Run all 7 validation dimensions on a scheme."""
    short = scheme.get("short", "?")
    intel = SCHEME_INTEL.get(short, {})
    if not intel:
        return None

    d1_score, d1_reason = calc_policy_impact(scheme, intel)
    d2_score, d2_reason = calc_beneficiary_authenticity(scheme, intel)
    d3_score, d3_reason = calc_inter_year_consistency(scheme, intel)
    d4_score, d4_reason = calc_district_distribution(scheme, intel)
    d5_score, d5_reason = calc_budget_efficiency(scheme, intel)
    d6_score, d6_reason = calc_political_usefulness(scheme, intel)
    d7_score, d7_reason = calc_data_trust(scheme, intel, scrape_status)

    overall = round((d1_score*0.20 + d2_score*0.15 + d3_score*0.10 +
                      d4_score*0.10 + d5_score*0.15 + d6_score*0.10 + d7_score*0.20), 1)

    if overall >= 7.5: verdict = "Highly Credible & Impactful"
    elif overall >= 6.0: verdict = "Credible & Effective"
    elif overall >= 4.5: verdict = "Moderately Reliable"
    elif overall >= 3.0: verdict = "Questionable"
    else: verdict = "Low Confidence"

    return {
        "name": scheme.get("name"), "short": short,
        "sector": scheme.get("sector"),
        "overall_validation_score": overall,
        "validation_verdict": verdict,
        "beneficiaries_lakh": scheme.get("beneficiaries_lakh", 0),
        "budget_crore": scheme.get("budget_crore", 0),
        "target_group": scheme.get("target_group", ""),
        "scrape_status": scrape_status,
        "dimensions": {
            "policy_impact": {"score": d1_score, "reasoning": d1_reason},
            "beneficiary_authenticity": {"score": d2_score, "reasoning": d2_reason},
            "inter_year_consistency": {"score": d3_score, "reasoning": d3_reason},
            "district_distribution": {"score": d4_score, "reasoning": d4_reason},
            "budget_efficiency": {"score": d5_score, "reasoning": d5_reason},
            "political_usefulness": {"score": d6_score, "reasoning": d6_reason},
            "data_trust_score": {"score": d7_score, "reasoning": d7_reason},
        },
    }


def validate_all_schemes(schemes):
    """Validate all schemes and return results list."""
    results = []
    for s in schemes:
        r = validate_scheme(s, s.get("scrape_status", "RESEARCH_ONLY"))
        if r:
            results.append(r)
    results.sort(key=lambda x: x["overall_validation_score"], reverse=True)
    return results
