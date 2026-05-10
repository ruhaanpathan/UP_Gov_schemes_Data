"""
═══════════════════════════════════════════════════════════════
  UP GOVERNMENT SCHEMES — MASTER PIPELINE
  
  Stages:
    1. SCRAPE  → Fetch live data from 7 government portals
    2. MERGE   → Combine scraped data with curated research data
    3. ANALYZE → Generate charts, reports, CSVs
═══════════════════════════════════════════════════════════════
  Run: python pipeline.py
  Dependencies: pip install -r requirements.txt
═══════════════════════════════════════════════════════════════
"""
import sys, os, json

# ── Auto-install dependencies ──
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
        print("✅ Dependencies installed!\n")

ensure_deps()

import pandas as pd
from datetime import datetime


def stage1_scrape():
    """Stage 1: Scrape live data from government portals."""
    print("\n" + "█" * 60)
    print("  STAGE 1: SCRAPING GOVERNMENT PORTALS")
    print("█" * 60)
    from scrapers import run_all_scrapers
    return run_all_scrapers()


def stage2_merge(scraped):
    """Stage 2: Merge scraped data with curated research data."""
    print("\n" + "█" * 60)
    print("  STAGE 2: MERGING DATA SOURCES")
    print("█" * 60)
    from scheme_data import SCHEMES

    merged = []
    for scheme in SCHEMES:
        entry = scheme.copy()
        entry["data_source"] = "curated_research"
        entry["live_data_available"] = False

        # Try to match with scraped data
        name_lower = scheme["name"].lower()

        # Match pension schemes with SSPY data
        if scraped.get("sspy_pensions") and isinstance(scraped["sspy_pensions"], dict):
            sspy = scraped["sspy_pensions"]
            for s in sspy.get("schemes", []):
                if s.get("status") == "SCRAPED":
                    if ("old age" in name_lower and "Old Age" in s.get("name","")):
                        entry["live_data_available"] = True
                        entry["live_source"] = "sspy-up.gov.in"
                        entry["live_raw"] = s
                    elif ("widow" in name_lower and "Widow" in s.get("name","")):
                        entry["live_data_available"] = True
                        entry["live_source"] = "sspy-up.gov.in"
                        entry["live_raw"] = s
                    elif ("divyang" in name_lower and "Handicap" in s.get("name","")):
                        entry["live_data_available"] = True
                        entry["live_source"] = "sspy-up.gov.in"
                        entry["live_raw"] = s

        # Match MKSY
        if "kanya sumangala" in name_lower and scraped.get("mksy"):
            if scraped["mksy"].get("status") in ("SCRAPED",):
                entry["live_data_available"] = True
                entry["live_source"] = "mksy.up.gov.in"

        # Match PMAY
        if "pmay" in name_lower or "awas" in name_lower:
            if "rural" in name_lower or "gramin" in name_lower:
                if scraped.get("pmay_rural", {}).get("status") not in ("FAILED",None):
                    entry["live_data_available"] = True
                    entry["live_source"] = "pmayg.nic.in"
            elif "urban" in name_lower:
                if scraped.get("pmay_urban", {}).get("status") not in ("FAILED",None):
                    entry["live_data_available"] = True
                    entry["live_source"] = "pmay-urban.gov.in"

        # Match ODOP
        if "odop" in name_lower or "one district" in name_lower:
            if scraped.get("odop", {}).get("status") not in ("FAILED",None):
                entry["live_data_available"] = True
                entry["live_source"] = "odopup.in"

        # Match Ayushman
        if "ayushman" in name_lower:
            if scraped.get("ayushman", {}).get("status") not in ("FAILED",None):
                entry["live_data_available"] = True
                entry["live_source"] = "ayushmanup.in"

        # Match Scholarship
        if "scholarship" in name_lower or "matric" in name_lower:
            if scraped.get("scholarship", {}).get("status") not in ("FAILED",None):
                entry["live_data_available"] = True
                entry["live_source"] = "scholarship.up.gov.in"

        merged.append(entry)

    # Save merged data (use same timestamped folder as analysis)
    from analysis import OUT as out_dir
    os.makedirs(out_dir, exist_ok=True)

    # Clean merged for JSON (remove non-serializable)
    clean = []
    for m in merged:
        c = {k: v for k, v in m.items() if k != "live_raw"}
        clean.append(c)

    with open(os.path.join(out_dir, "merged_data.json"), "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)

    live_count = sum(1 for m in merged if m["live_data_available"])
    print(f"\n  ✅ Merged {len(merged)} schemes")
    print(f"  🌐 Live data matched for {live_count}/{len(merged)} schemes")
    print(f"  📚 Curated research data used as baseline for all {len(merged)} schemes")

    return merged


def stage3_analyze(merged):
    """Stage 3: Run full analysis and generate outputs."""
    print("\n" + "█" * 60)
    print("  STAGE 3: RUNNING FULL ANALYSIS")
    print("█" * 60)

    # Import and run the analysis module
    from analysis import console_report, print_header
    from analysis import (chart1_sector_beneficiaries, chart2_budget_comparison,
                          chart3_verdict_pie, chart4_impact_heatmap,
                          chart5_top_beneficiaries, chart6_sector_comparison,
                          chart7_success_failure, chart8_scatter_impact_vs_budget,
                          save_csv, save_report_txt)

    # Console report
    console_report()

    # Generate all charts
    print_header("GENERATING VISUALIZATIONS")
    chart1_sector_beneficiaries()
    chart2_budget_comparison()
    chart3_verdict_pie()
    chart4_impact_heatmap()
    chart5_top_beneficiaries()
    chart6_sector_comparison()
    chart7_success_failure()
    chart8_scatter_impact_vs_budget()

    # Export data
    print_header("EXPORTING DATA")
    save_csv()
    save_report_txt()

    # Generate pipeline summary
    generate_pipeline_summary(merged)


def generate_pipeline_summary(merged):
    """Generate final pipeline summary report."""
    from analysis import OUT as out_dir
    live = [m for m in merged if m["live_data_available"]]
    curated = [m for m in merged if not m["live_data_available"]]

    summary = []
    summary.append("=" * 70)
    summary.append("  UP GOVERNMENT SCHEMES — PIPELINE EXECUTION SUMMARY")
    summary.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary.append("=" * 70)
    summary.append(f"\n  📊 Total Schemes Analyzed: {len(merged)}")
    summary.append(f"  🌐 Live Data Matched:      {len(live)} schemes")
    summary.append(f"  📚 Curated Data Only:       {len(curated)} schemes")
    summary.append(f"\n  Live Data Sources Used:")
    sources = set(m.get("live_source","") for m in live if m.get("live_source"))
    for s in sources:
        count = sum(1 for m in live if m.get("live_source") == s)
        summary.append(f"    • {s} → {count} schemes")
    summary.append(f"\n  📁 Output Files:")
    for f in sorted(os.listdir(out_dir)):
        size = os.path.getsize(os.path.join(out_dir, f))
        summary.append(f"    • {f} ({size:,} bytes)")

    text = "\n".join(summary)
    print(text)

    with open(os.path.join(out_dir, "pipeline_summary.txt"), "w", encoding="utf-8") as f:
        f.write(text)


def stage4_dashboard(merged, scraped):
    """Stage 4: Generate live dashboard_data.js for the web dashboard."""
    print("\n" + "█" * 60)
    print("  STAGE 4: GENERATING LIVE DASHBOARD DATA")
    print("█" * 60)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Build clean scheme list for JS
    js_schemes = []
    for m in merged:
        js_schemes.append({
            "name": m["name"],
            "short": m.get("short_code", ""),
            "sector": m["sector"],
            "launch_year": m["launch_year"],
            "beneficiaries_lakh": m["beneficiaries_lakh"],
            "budget_crore": m["budget_crore"],
            "per_person_benefit": m.get("per_person_benefit", ""),
            "target_group": m.get("target_group", ""),
            "description": m.get("description", ""),
            "achievements": m.get("achievements", []),
            "challenges": m.get("challenges", []),
            "verdict": m["verdict"],
            "impact_score": m["impact_score"],
            "reach_percent": m["reach_percent"],
            "live_data": m.get("live_data_available", False),
            "live_source": m.get("live_source", ""),
        })

    # Build scrape summary
    scrape_summary = {}
    for name, res in scraped.items():
        if isinstance(res, dict):
            scrape_summary[name] = {
                "status": res.get("status", "UNKNOWN"),
                "source": res.get("source", ""),
            }

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_schemes": len(js_schemes),
        "total_beneficiaries_lakh": round(sum(s["beneficiaries_lakh"] for s in js_schemes), 1),
        "total_budget_crore": sum(s["budget_crore"] for s in js_schemes),
        "sectors": len(set(s["sector"] for s in js_schemes)),
        "avg_impact": round(sum(s["impact_score"] for s in js_schemes) / len(js_schemes), 1),
        "avg_reach": round(sum(s["reach_percent"] for s in js_schemes) / len(js_schemes)),
        "live_matched": sum(1 for s in js_schemes if s["live_data"]),
        "scrape_summary": scrape_summary,
    }

    js_content = (
        "// ═══ AUTO-GENERATED BY PIPELINE — DO NOT EDIT ═══\n"
        f"// Generated: {meta['generated_at']}\n"
        "// Re-run pipeline.py to update this data with latest scrapes\n\n"
        f"const PIPELINE_META = {json.dumps(meta, indent=2, ensure_ascii=False)};\n\n"
        f"const SCHEMES = {json.dumps(js_schemes, indent=2, ensure_ascii=False)};\n"
    )

    out_path = os.path.join(base_dir, "dashboard_data.js")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"  ✅ dashboard_data.js generated ({len(js_content):,} bytes)")
    print(f"  📊 {meta['total_schemes']} schemes | {meta['live_matched']} with live data")
    print(f"  🌐 Open index.html to see the live dashboard!")


# ══════════════════════════════════════════════════════════════
#  MAIN — RUN FULL PIPELINE
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🚀 UP GOVERNMENT SCHEMES — FULL ANALYSIS PIPELINE")
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Stage 1: Scrape
    scraped = stage1_scrape()

    # Stage 2: Merge
    merged = stage2_merge(scraped)

    # Stage 3: Analyze
    stage3_analyze(merged)

    # Stage 4: Dashboard
    stage4_dashboard(merged, scraped)

    print("\n" + "█" * 60)
    print("  ✅ PIPELINE COMPLETE!")
    print(f"  📁 All outputs in: {os.path.abspath('output')}")
    print(f"  📁 Scraped data in: {os.path.abspath('scraped_data')}")
    print(f"  🌐 Dashboard: {os.path.abspath('index.html')}")
    print("█" * 60 + "\n")
