"""
═══════════════════════════════════════════════════════════════
  UP GOVERNMENT SCHEMES — DATA-DRIVEN PIPELINE v3.1
  
  HYBRID APPROACH:
    - Uses scheme_data.py as RESEARCH BASELINE (24 schemes)
    - OVERRIDES with live scraped data where available
    - COMPUTES all impact scores via formula (nothing hardcoded)
  
  Stages:
    1. SCRAPE  → Fetch live data from government portals
    2. MERGE   → Combine research baseline + live scraped overrides
    3. SCORE   → Compute impact scores from merged metrics
    4. ANALYZE → Generate charts, reports, CSVs
    5. DASHBOARD → Generate web dashboard data
═══════════════════════════════════════════════════════════════
"""
import sys, os, json, re
from datetime import datetime

def ensure_deps():
    missing = []
    for pkg, imp in [("requests","requests"),("beautifulsoup4","bs4"),
                     ("lxml","lxml"),("pandas","pandas"),("matplotlib","matplotlib"),
                     ("seaborn","seaborn"),("tabulate","tabulate")]:
        try: __import__(imp)
        except ImportError: missing.append(pkg)
    if missing:
        print(f"📦 Installing: {', '.join(missing)}")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing + ["-q"])

ensure_deps()

import pandas as pd

SECTOR_COLORS = {
    "Women & Girl Child":"#E91E63","Social Security":"#FF9800","Agriculture":"#4CAF50",
    "Employment & Industry":"#2196F3","Education":"#9C27B0","Health":"#00BCD4",
    "Housing":"#FF5722","Food & Civil Supplies":"#8BC34A",
}
VERDICT_COLORS = {
    "Major Success":"#00E676","Success":"#69F0AE","Moderate Success":"#FFD740",
    "Mixed":"#FFAB40","Underperformed":"#FF5252","Too Early to Judge":"#B0BEC5",
}
CURRENT_YEAR = datetime.now().year


def stage1_scrape():
    print("\n" + "█" * 60)
    print("  STAGE 1: SCRAPING GOVERNMENT PORTALS")
    print("█" * 60)
    from scrapers import run_all_scrapers
    return run_all_scrapers()


def stage1b_validate(scraped):
    """Validate scraped data and return auto-fix overrides."""
    print("\n" + "█" * 60)
    print("  STAGE 1.5: VALIDATING SCRAPED DATA")
    print("█" * 60)
    from validate_data import run_validation
    validator, overrides = run_validation(scraped)
    return overrides


def stage2_merge(scraped, validation_overrides=None):
    """Merge research baseline with live scraped overrides."""
    v_overrides = validation_overrides or {}
    print("\n" + "█" * 60)
    print("  STAGE 2: MERGING RESEARCH DATA + LIVE SCRAPED OVERRIDES")
    print("█" * 60)
    from scheme_data import SCHEMES as BASELINE

    # Parse SSPY pension data
    sspy_pensions = {}
    sspy = scraped.get("sspy_pensions", {})
    if isinstance(sspy, dict) and sspy.get("status") == "SCRAPED":
        for pension in sspy.get("parsed_data", {}).get("pensions", []):
            name = pension.get("name", "")
            sspy_pensions[name.lower()] = pension

    # Parse Ayushman data
    ayushman_data = {}
    ab = scraped.get("ayushman", {})
    if isinstance(ab, dict) and ab.get("status") == "SCRAPED":
        ayushman_data = ab.get("parsed_data", {})

    # Parse FCS data
    fcs_data = {}
    up_gov = scraped.get("up_gov", {})
    if isinstance(up_gov, dict):
        fcs_data = up_gov.get("parsed_data", {}).get("wheat_procurement", {})

    merged = []
    live_count = 0

    for scheme in BASELINE:
        entry = {
            "name": scheme["name"],
            "short": scheme["short"],
            "sector": scheme["sector"],
            "launch_year": scheme["launch_year"],
            "target_group": scheme.get("target_group", ""),
            "description": scheme.get("description", ""),
            "per_person_benefit": scheme.get("per_person_benefit", ""),
            "achievements": scheme.get("achievements", []),
            "challenges": scheme.get("challenges", []),
            # Research baseline values
            "beneficiaries_lakh": scheme["beneficiaries_lakh"],
            "budget_crore": scheme["budget_crore"],
            "disbursed_crore": 0,
            "reach_percent": scheme.get("reach_percent", 0),
            "data_source": "research",
            "scrape_status": "RESEARCH_ONLY",
        }
        
        name_lower = scheme["name"].lower()

        # ── OVERRIDE with SSPY live data ──
        if "old age" in name_lower or "vridha" in name_lower:
            p = sspy_pensions.get("old age pension")
            if p:
                entry["beneficiaries_lakh"] = p["beneficiaries_lakh"]
                entry["disbursed_crore"] = p["total_disbursed_crore"]
                entry["budget_crore"] = max(scheme["budget_crore"], p["total_disbursed_crore"])
                entry["data_source"] = "sspy-up.gov.in"
                entry["scrape_status"] = "LIVE"
                entry["quarters_active"] = p["quarters_with_data"]
                live_count += 1
        
        elif "widow" in name_lower or "nirashrit" in name_lower:
            p = sspy_pensions.get("widow pension")
            if p:
                entry["beneficiaries_lakh"] = p["beneficiaries_lakh"]
                entry["disbursed_crore"] = p["total_disbursed_crore"]
                entry["budget_crore"] = max(scheme["budget_crore"], p["total_disbursed_crore"])
                entry["data_source"] = "sspy-up.gov.in"
                entry["scrape_status"] = "LIVE"
                entry["quarters_active"] = p["quarters_with_data"]
                live_count += 1
        
        elif "divyang" in name_lower:
            p = sspy_pensions.get("divyang pension")
            if p:
                entry["beneficiaries_lakh"] = p["beneficiaries_lakh"]
                entry["disbursed_crore"] = p["total_disbursed_crore"]
                entry["budget_crore"] = max(scheme["budget_crore"], p["total_disbursed_crore"])
                entry["data_source"] = "sspy-up.gov.in"
                entry["scrape_status"] = "LIVE"
                entry["quarters_active"] = p["quarters_with_data"]
                live_count += 1

        # ── OVERRIDE with Ayushman live data ──
        elif "ayushman" in name_lower:
            if ayushman_data.get("total_beneficiaries") or ayushman_data.get("golden_cards_issued"):
                ben = ayushman_data.get("beneficiaries_lakh", 0)
                # FIX: Portal shows ENROLLED population (~956L), not treated patients (~45L)
                # Validation auto-caps this to the research baseline
                if "ayushman_ben_cap" in v_overrides:
                    ben = min(ben, v_overrides["ayushman_ben_cap"])
                    print(f"  🔧 Ayushman: capped beneficiaries {ayushman_data.get('beneficiaries_lakh',0)}L → {ben}L (enrolled≠treated)")
                if ben > 0:
                    entry["beneficiaries_lakh"] = ben
                entry["enrolled_population_lakh"] = ayushman_data.get("beneficiaries_lakh", 0)
                entry["golden_cards_issued"] = ayushman_data.get("golden_cards_issued", 0)
                entry["empanelled_hospitals"] = ayushman_data.get("empanelled_hospitals", 0)
                entry["claims_submitted"] = ayushman_data.get("claims_submitted", 0)
                entry["claims_settled_pct"] = ayushman_data.get("claims_settled_pct", 0)
                entry["preauth_requests"] = ayushman_data.get("preauth_requests", 0)
                entry["card_coverage_pct"] = ayushman_data.get("card_coverage_pct", 0)
                if entry["claims_settled_pct"] > 0:
                    entry["reach_percent"] = entry["claims_settled_pct"]
                entry["data_source"] = "ayushmanup.in"
                entry["scrape_status"] = "LIVE"
                live_count += 1

        # Mark portal status for other schemes
        elif "mksy" in name_lower or "kanya sumangala" in name_lower:
            if scraped.get("mksy", {}).get("status") == "SCRAPED":
                entry["data_source"] = "mksy.up.gov.in (page only)"
                entry["scrape_status"] = "PARTIAL"
        elif "pmay" in name_lower or "awas" in name_lower:
            if "rural" in name_lower or "gramin" in name_lower:
                if scraped.get("pmay_rural", {}).get("status") == "SCRAPED":
                    entry["data_source"] = "rhreporting.nic.in (page only)"
                    entry["scrape_status"] = "PARTIAL"
            elif "urban" in name_lower:
                if scraped.get("pmay_urban", {}).get("status") == "SCRAPED":
                    entry["data_source"] = "pmaymis.gov.in (page only)"
                    entry["scrape_status"] = "PARTIAL"
        elif "odop" in name_lower or "one district" in name_lower:
            if scraped.get("odop", {}).get("status") == "SCRAPED":
                entry["data_source"] = "odopup.in (page only)"
                entry["scrape_status"] = "PARTIAL"

        merged.append(entry)

    # Add FCS wheat procurement as extra scheme if data available
    if fcs_data and fcs_data.get("farmers", 0) > 0:
        merged.append({
            "name": "Wheat Procurement (FCS)",
            "short": "WP",
            "sector": "Food & Civil Supplies",
            "launch_year": 2016,
            "target_group": "Farmers selling wheat to government",
            "description": f"Government wheat procurement: {fcs_data.get('farmers',0):,.0f} farmers, {fcs_data.get('quantity_lakh_mt',0)} lakh MT in {fcs_data.get('year','')}",
            "per_person_benefit": "MSP price for wheat",
            "achievements": [f"{fcs_data.get('farmers',0):,.0f} farmers enrolled", f"{fcs_data.get('quantity_lakh_mt',0)} lakh MT procured"],
            "challenges": ["Limited to wheat crop only"],
            "beneficiaries_lakh": round(fcs_data.get("farmers", 0) / 100000, 2),
            "budget_crore": 0,
            "disbursed_crore": 0,
            "reach_percent": 0,
            "data_source": "fcs.up.gov.in",
            "scrape_status": "LIVE",
        })
        live_count += 1

    print(f"\n  📊 Total schemes: {len(merged)}")
    print(f"  🌐 Live scraped data for: {live_count} schemes")
    print(f"  📚 Research baseline for: {len(merged) - live_count} schemes")
    print(f"  ⚡ Impact scores will be COMPUTED for all {len(merged)} schemes")

    return merged


def stage3_score(schemes):
    """Compute impact scores from data — NO hardcoded scores."""
    print("\n" + "█" * 60)
    print("  STAGE 3: COMPUTING IMPACT SCORES (DATA-DRIVEN)")
    print("█" * 60)
    
    from score_engine import compute_impact_score, compute_verdict

    for s in schemes:
        metrics = {
            "beneficiaries_lakh": s.get("beneficiaries_lakh", 0),
            "budget_crore": s.get("budget_crore", 0),
            "disbursed_crore": s.get("disbursed_crore", 0),
            "reach_percent": s.get("reach_percent", 0),
            "launch_year": s.get("launch_year"),
        }
        
        # Use operational metrics for reach where available
        if s.get("claims_settled_pct"):
            metrics["reach_percent"] = s["claims_settled_pct"]
        elif s.get("quarters_active"):
            metrics["reach_percent"] = min(s["quarters_active"] * 25, 100)

        result = compute_impact_score(metrics)
        s["impact_score"] = result["impact_score"]
        s["impact_components"] = result["components"]
        
        years = CURRENT_YEAR - s.get("launch_year", CURRENT_YEAR)
        s["verdict"] = compute_verdict(result["impact_score"], metrics["reach_percent"], years)
        
        src = "🌐 LIVE" if s["scrape_status"] == "LIVE" else "📚 Research"
        print(f"  {src} {s['short']:8s} → {s['impact_score']}/10 ({s['verdict']})")
    
    return schemes


def stage4_analyze(schemes):
    print("\n" + "█" * 60)
    print("  STAGE 4: ANALYSIS & VISUALIZATION")
    print("█" * 60)
    if not schemes:
        print("  ❌ No data!")
        return
    from analysis import run_full_analysis
    run_full_analysis(schemes)


def stage5_dashboard(schemes, scraped):
    print("\n" + "█" * 60)
    print("  STAGE 5: GENERATING DASHBOARD DATA")
    print("█" * 60)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    js_schemes = []
    for s in schemes:
        js_schemes.append({
            "name": s["name"], "short": s["short"], "sector": s["sector"],
            "launch_year": s["launch_year"],
            "beneficiaries_lakh": s["beneficiaries_lakh"],
            "budget_crore": s.get("budget_crore", 0),
            "disbursed_crore": s.get("disbursed_crore", 0),
            "per_person_benefit": s.get("per_person_benefit", ""),
            "target_group": s.get("target_group", ""),
            "description": s.get("description", ""),
            "achievements": s.get("achievements", []),
            "challenges": s.get("challenges", []),
            "verdict": s["verdict"],
            "impact_score": s["impact_score"],
            "impact_components": s.get("impact_components", {}),
            "reach_percent": s.get("reach_percent", 0),
            "data_source": s.get("data_source", ""),
            "scrape_status": s.get("scrape_status", ""),
            "golden_cards_issued": s.get("golden_cards_issued", 0),
            "empanelled_hospitals": s.get("empanelled_hospitals", 0),
            "claims_submitted": s.get("claims_submitted", 0),
            "claims_settled_pct": s.get("claims_settled_pct", 0),
        })
    
    live_count = sum(1 for s in js_schemes if s["scrape_status"] == "LIVE")
    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_version": "3.1 — Hybrid (Research + Live Scrape)",
        "total_schemes": len(js_schemes),
        "total_beneficiaries_lakh": round(sum(s["beneficiaries_lakh"] for s in js_schemes), 1),
        "total_budget_crore": sum(s["budget_crore"] for s in js_schemes),
        "sectors": len(set(s["sector"] for s in js_schemes)),
        "avg_impact": round(sum(s["impact_score"] for s in js_schemes) / max(len(js_schemes), 1), 1),
        "live_matched": live_count,
        "data_note": f"Impact scores COMPUTED via formula. {live_count} schemes with live portal data, rest from research.",
    }
    
    js_content = (
        "// ═══ AUTO-GENERATED BY PIPELINE v3.1 — HYBRID DATA-DRIVEN ═══\n"
        f"// Generated: {meta['generated_at']}\n"
        "// Impact scores COMPUTED from data (not hardcoded)\n"
        f"// {live_count} schemes with LIVE scraped data\n\n"
        f"const PIPELINE_META = {json.dumps(meta, indent=2, ensure_ascii=False)};\n\n"
        f"const SCHEMES = {json.dumps(js_schemes, indent=2, ensure_ascii=False)};\n"
    )
    
    with open(os.path.join(base_dir, "dashboard_data.js"), "w", encoding="utf-8") as f:
        f.write(js_content)
    
    print(f"  ✅ dashboard_data.js ({len(js_content):,} bytes)")
    print(f"  📊 {len(js_schemes)} schemes | {live_count} LIVE | All scores computed")


def stage6_deep_validate(schemes):
    """Deep policy validation: 7 dimensions per scheme."""
    print("\n" + "█" * 60)
    print("  STAGE 6: DEEP POLICY VALIDATION (7 DIMENSIONS)")
    print("█" * 60)
    from scheme_validator import validate_all_schemes
    from validation_report_gen import generate_reports

    results = validate_all_schemes(schemes)
    json_path, html_path = generate_reports(results)

    # Console summary
    print(f"\n  📊 Validated {len(results)} schemes across 7 dimensions")
    print(f"  ── Top 3 ──")
    for r in results[:3]:
        print(f"     🏆 {r['short']:6s} {r['overall_validation_score']}/10 — {r['validation_verdict']}")
    print(f"  ── Bottom 3 ──")
    for r in results[-3:]:
        print(f"     ⚠️  {r['short']:6s} {r['overall_validation_score']}/10 — {r['validation_verdict']}")

    avg = round(sum(r['overall_validation_score'] for r in results) / max(len(results),1), 1)
    print(f"\n  📄 JSON: {json_path}")
    print(f"  🌐 HTML: {html_path}")
    print(f"  📊 Average validation score: {avg}/10")
    return results


if __name__ == "__main__":
    print("\n🚀 UP GOVERNMENT SCHEMES — PIPELINE v3.2 (HYBRID + VALIDATED)")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   ⚡ Scrape → Validate → Merge → Score → Analyze → Dashboard → Deep Validate\n")
    
    scraped = stage1_scrape()
    v_overrides = stage1b_validate(scraped)
    schemes = stage2_merge(scraped, v_overrides)
    schemes = stage3_score(schemes)
    stage4_analyze(schemes)
    stage5_dashboard(schemes, scraped)
    stage6_deep_validate(schemes)
    
    print("\n" + "█" * 60)
    print("  ✅ PIPELINE COMPLETE!")
    print(f"  📁 Outputs: {os.path.abspath('output')}")
    print(f"  🌐 Dashboard: {os.path.abspath('index.html')}")
    print(f"  📊 Policy Validation: {os.path.abspath('output/scheme_validation_report.html')}")
    print("  ⚡ All data VALIDATED + SCORED + POLICY ANALYZED")
    print("█" * 60 + "\n")
