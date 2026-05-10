"""
=============================================================
  UP GOVERNMENT SCHEMES ANALYSIS (2016-2026)
  Comprehensive Analysis: Beneficiaries, Sectors, Impact
  Author: Auto-generated from research data
=============================================================
"""
import os, sys

# --- Auto-install dependencies ---
def install(pkg):
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

for lib in ["pandas", "matplotlib", "seaborn"]:
    try: __import__(lib)
    except ImportError: install(lib)

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from textwrap import wrap
from scheme_data import SCHEMES, SECTOR_COLORS, VERDICT_COLORS

# ── Output directory (timestamped per run) ──
from datetime import datetime as _dt
_run_stamp = _dt.now().strftime("%Y-%m-%d_%H-%M")
OUT = os.path.join(os.path.dirname(__file__), "output", f"run_{_run_stamp}")
os.makedirs(OUT, exist_ok=True)
print(f"  📁 Output folder: {os.path.abspath(OUT)}")

# ── Build DataFrame ──
df = pd.DataFrame(SCHEMES)
df["beneficiaries_cr"] = df["beneficiaries_lakh"] / 100
df["budget_thousand_cr"] = df["budget_crore"] / 1000

# ══════════════════════════════════════════════════════════════
#  SECTION 1: CONSOLE REPORT
# ══════════════════════════════════════════════════════════════
def print_header(title):
    w = 70
    print("\n" + "═"*w)
    print(f"  {title}")
    print("═"*w)

def console_report():
    print_header("UP GOVERNMENT SCHEMES — MASTER ANALYSIS REPORT (2016-2026)")

    # --- Overview ---
    total_ben = df["beneficiaries_lakh"].sum()
    total_bud = df["budget_crore"].sum()
    print(f"\n📊 OVERVIEW")
    print(f"   Total Schemes Analyzed     : {len(df)}")
    print(f"   Total Beneficiaries        : {total_ben:.1f} Lakh ({total_ben/100:.2f} Crore)")
    print(f"   Total Budget Deployed      : ₹{total_bud:,.0f} Crore (₹{total_bud/100000:.2f} Lakh Crore)")
    print(f"   Sectors Covered            : {df['sector'].nunique()}")
    print(f"   Average Impact Score       : {df['impact_score'].mean():.1f}/10")
    print(f"   Average Reach              : {df['reach_percent'].mean():.0f}%")

    # --- Sector-wise ---
    print_header("SECTOR-WISE BREAKDOWN")
    sec = df.groupby("sector").agg(
        schemes=("name","count"),
        beneficiaries=("beneficiaries_lakh","sum"),
        budget=("budget_crore","sum"),
        avg_impact=("impact_score","mean"),
        avg_reach=("reach_percent","mean"),
    ).sort_values("beneficiaries", ascending=False)

    for s, r in sec.iterrows():
        print(f"\n  🏷️  {s}")
        print(f"      Schemes: {r.schemes} | Beneficiaries: {r.beneficiaries:.1f}L | Budget: ₹{r.budget:,.0f}Cr")
        print(f"      Avg Impact: {r.avg_impact:.1f}/10 | Avg Reach: {r.avg_reach:.0f}%")

    # --- Top 10 by beneficiaries ---
    print_header("TOP 10 SCHEMES BY BENEFICIARIES")
    top = df.nlargest(10, "beneficiaries_lakh")
    for i, (_, r) in enumerate(top.iterrows(), 1):
        v = "✅" if "Success" in r.verdict else "⚠️" if r.verdict == "Mixed" else "❌"
        print(f"  {i:2}. {v} {r['name']}")
        print(f"      {r.beneficiaries_lakh:.1f} Lakh | ₹{r.budget_crore:,} Cr | {r.sector}")

    # --- Verdict breakdown ---
    print_header("VERDICT ANALYSIS — WHAT WORKED vs WHAT FLOPPED")
    for verdict in ["Major Success","Success","Moderate Success","Mixed","Underperformed","Too Early to Judge"]:
        vdf = df[df.verdict == verdict]
        if len(vdf) == 0: continue
        icon = {"Major Success":"🏆","Success":"✅","Moderate Success":"👍","Mixed":"⚠️","Underperformed":"❌","Too Early to Judge":"🕐"}[verdict]
        print(f"\n  {icon} {verdict.upper()} ({len(vdf)} schemes)")
        for _, r in vdf.iterrows():
            print(f"      • {r['name']} — {r.beneficiaries_lakh:.1f}L beneficiaries, Score {r.impact_score}/10")

    # --- Detailed scheme cards ---
    print_header("DETAILED SCHEME ANALYSIS")
    for _, r in df.sort_values("impact_score", ascending=False).iterrows():
        icon = "🏆" if r.verdict=="Major Success" else "✅" if "Success" in r.verdict else "⚠️" if r.verdict=="Mixed" else "❌" if r.verdict=="Underperformed" else "🕐"
        print(f"\n  {'─'*60}")
        print(f"  {icon} {r['name']} ({r.short})")
        print(f"  {'─'*60}")
        print(f"  Sector: {r.sector} | Launched: {r.launch_year} | Verdict: {r.verdict}")
        print(f"  Beneficiaries: {r.beneficiaries_lakh:.1f} Lakh | Budget: ₹{r.budget_crore:,} Cr")
        print(f"  Per-Person Benefit: {r.per_person_benefit}")
        print(f"  Impact Score: {r.impact_score}/10 | Reach: {r.reach_percent}%")
        print(f"  Target Group: {r.target_group}")
        print(f"  Description: {r.description}")
        print(f"  ✓ Achievements:")
        for a in r.achievements: print(f"      • {a}")
        print(f"  ✗ Challenges:")
        for c in r.challenges: print(f"      • {c}")

    # --- Local people impact ---
    print_header("LOCAL PEOPLE IMPACT ANALYSIS")
    impacts = [
        ("🏠 Housing", "53+ lakh pucca houses built for rural & urban poor. Families got permanent shelter with toilet, electricity & gas connections. Biggest life-changing scheme for homeless families."),
        ("💊 Healthcare", "45 lakh+ poor patients received FREE hospital treatment worth ₹7,040 Cr. 5 Cr+ Ayushman cards issued. Families no longer go bankrupt due to medical emergencies."),
        ("👵 Social Security", "97+ lakh elderly, widows & disabled receive ₹1,000/month pension. Provides basic survival support to most vulnerable sections."),
        ("👧 Girl Child", "26.81 lakh girls got financial support from birth to graduation. Reduced female foeticide incentive, increased girl education rates."),
        ("🎓 Education", "1.6 Cr+ students get free bags & uniforms yearly. 80 lakh+ SC/OBC/Minority students get scholarships. Atal schools gave construction workers' children world-class education."),
        ("🌾 Agriculture", "86 lakh farmers targeted for loan waiver but actual impact was mixed. Solar pump scheme largely failed. Crop insurance helped but didn't solve root problems."),
        ("💼 Employment", "ODOP was a game-changer — ₹93,000 Cr exports, 1.25L toolkits given. But employment portals & skill programs underdelivered on job creation."),
        ("👩 Women Empowerment", "50,000 BC Sakhis earning through banking. Mission Shakti improved safety infrastructure. Mass marriage scheme reduced dowry burden on 3.5L families."),
    ]
    for title, desc in impacts:
        print(f"\n  {title}")
        for line in wrap(desc, 65):
            print(f"      {line}")

    # --- Key findings ---
    print_header("KEY FINDINGS & CONCLUSIONS")
    findings = [
        "🏆 BEST PERFORMERS: PMAY Rural Housing (98.7% completion), Atal Schools (93% pass rate), ODOP (₹93K Cr exports), Ayushman Bharat (45L treated)",
        "❌ UNDERPERFORMERS: Kisan Uday Solar Pumps (15% of target), Kaushal Satrang (poor job outcomes), Sevayojan (low placement rates)",
        "⚠️ MIXED RESULTS: Kisan Rin Mochan loan waiver was politically popular but many farmers reported non-receipt. Only institutional loans covered.",
        "📈 BIGGEST REACH: Free Bags & Uniform (1.6 Cr students), Scholarships (80L students), Old Age Pension (56L elderly)",
        "💰 BIGGEST BUDGET: PMAY Rural (₹45,000 Cr), Loan Waiver (₹36,000 Cr), PMAY Urban (₹25,000 Cr)",
        "🎯 HIGHEST IMPACT SCORE: Atal Schools (9.5/10), PMAY Rural (9.5/10), ODOP (9.0/10), Ayushman Bharat (9.0/10)",
        "📊 OVERALL: 60% of schemes performed well (Success+), 28% had mixed results, 12% underperformed or too early to judge",
    ]
    for f in findings:
        print(f"\n  {f}")

    print("\n" + "═"*70)
    print("  END OF REPORT")
    print("═"*70 + "\n")

# ══════════════════════════════════════════════════════════════
#  SECTION 2: CHARTS & VISUALIZATIONS
# ══════════════════════════════════════════════════════════════
plt.rcParams.update({
    "figure.facecolor": "#0d1117", "axes.facecolor": "#161b22",
    "axes.edgecolor": "#30363d", "axes.labelcolor": "white",
    "text.color": "white", "xtick.color": "#8b949e",
    "ytick.color": "#8b949e", "font.size": 11,
    "axes.grid": True, "grid.color": "#21262d", "grid.alpha": 0.5,
})

def chart1_sector_beneficiaries():
    """Doughnut: Sector-wise beneficiary distribution"""
    fig, ax = plt.subplots(figsize=(10, 8))
    sec = df.groupby("sector")["beneficiaries_lakh"].sum().sort_values(ascending=False)
    colors = [SECTOR_COLORS.get(s, "#888") for s in sec.index]
    wedges, texts, autotexts = ax.pie(
        sec.values, labels=None, autopct="%1.1f%%", startangle=90,
        colors=colors, pctdistance=0.8,
        wedgeprops=dict(width=0.45, edgecolor="#0d1117", linewidth=2),
    )
    for t in autotexts: t.set_fontsize(10); t.set_color("white"); t.set_fontweight("bold")
    ax.legend(
        [f"{s} ({v:.0f}L)" for s, v in zip(sec.index, sec.values)],
        loc="center left", bbox_to_anchor=(0.95, 0.5), fontsize=9,
        framealpha=0.1, edgecolor="#30363d"
    )
    ax.set_title("Sector-Wise Beneficiary Distribution (in Lakhs)", fontsize=15, fontweight="bold", pad=20)
    total = sec.sum()
    ax.text(0, 0, f"{total:.0f}L\nTotal", ha="center", va="center", fontsize=16, fontweight="bold", color="#58a6ff")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "01_sector_beneficiaries.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 1: Sector Beneficiaries saved")

def chart2_budget_comparison():
    """Horizontal bar: Top schemes by budget"""
    fig, ax = plt.subplots(figsize=(12, 9))
    top = df.nlargest(15, "budget_crore")
    colors = [SECTOR_COLORS.get(r.sector, "#888") for _, r in top.iterrows()]
    bars = ax.barh(range(len(top)), top["budget_crore"].values, color=colors, edgecolor="#0d1117", height=0.7)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels([n[:35] for n in top["name"].values], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Budget (₹ Crore)", fontsize=12)
    ax.set_title("Top 15 Schemes by Budget Allocation (₹ Crore)", fontsize=14, fontweight="bold", pad=15)
    for bar, val in zip(bars, top["budget_crore"].values):
        ax.text(bar.get_width() + 200, bar.get_y() + bar.get_height()/2, f"₹{val:,.0f} Cr", va="center", fontsize=9, color="#58a6ff")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "02_budget_comparison.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 2: Budget Comparison saved")

def chart3_verdict_pie():
    """Pie: Success vs Failure distribution"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    vc = df["verdict"].value_counts()
    colors = [VERDICT_COLORS.get(v, "#888") for v in vc.index]
    ax1.pie(vc.values, labels=vc.index, autopct="%1.0f%%", colors=colors,
            startangle=140, wedgeprops=dict(edgecolor="#0d1117", linewidth=2))
    ax1.set_title("Scheme Verdict Distribution", fontsize=13, fontweight="bold")

    # Impact score by verdict
    vg = df.groupby("verdict")["impact_score"].mean().sort_values(ascending=False)
    bars = ax2.bar(range(len(vg)), vg.values, color=[VERDICT_COLORS.get(v,"#888") for v in vg.index], edgecolor="#0d1117")
    ax2.set_xticks(range(len(vg)))
    ax2.set_xticklabels(["\n".join(wrap(v, 12)) for v in vg.index], fontsize=8)
    ax2.set_ylabel("Avg Impact Score (/10)")
    ax2.set_title("Average Impact Score by Verdict", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, vg.values):
        ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.15, f"{val:.1f}", ha="center", fontsize=10, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "03_verdict_analysis.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 3: Verdict Analysis saved")

def chart4_impact_heatmap():
    """Heatmap: Scheme impact scores & reach by sector"""
    fig, ax = plt.subplots(figsize=(14, 10))
    sorted_df = df.sort_values(["sector", "impact_score"], ascending=[True, False])
    data = sorted_df[["impact_score", "reach_percent"]].values
    names = [f"{r['name'][:30]} [{r.sector[:8]}]" for _, r in sorted_df.iterrows()]
    sns.heatmap(
        data, annot=True, fmt=".1f", cmap="RdYlGn",
        xticklabels=["Impact Score\n(/10)", "Reach\n(%)"],
        yticklabels=names, ax=ax, linewidths=1, linecolor="#0d1117",
        cbar_kws={"shrink": 0.6}
    )
    ax.set_title("Scheme Performance Heatmap — Impact Score & Reach %", fontsize=14, fontweight="bold", pad=15)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "04_impact_heatmap.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 4: Impact Heatmap saved")

def chart5_top_beneficiaries():
    """Bar chart: Top 10 schemes by beneficiaries"""
    fig, ax = plt.subplots(figsize=(13, 8))
    top = df.nlargest(10, "beneficiaries_lakh")
    colors = [SECTOR_COLORS.get(r.sector, "#888") for _, r in top.iterrows()]
    bars = ax.bar(range(len(top)), top["beneficiaries_lakh"].values, color=colors, edgecolor="#0d1117", width=0.7)
    ax.set_xticks(range(len(top)))
    ax.set_xticklabels(["\n".join(wrap(n, 18)) for n in top["name"].values], fontsize=8, rotation=0)
    ax.set_ylabel("Beneficiaries (Lakhs)", fontsize=12)
    ax.set_title("Top 10 Schemes by Number of Beneficiaries (in Lakhs)", fontsize=14, fontweight="bold", pad=15)
    for bar, val in zip(bars, top["beneficiaries_lakh"].values):
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1, f"{val:.0f}L", ha="center", fontsize=10, fontweight="bold", color="#58a6ff")
    # Legend for sectors
    from matplotlib.patches import Patch
    used = list(dict.fromkeys([r.sector for _, r in top.iterrows()]))
    ax.legend([Patch(facecolor=SECTOR_COLORS[s]) for s in used], used, loc="upper right", fontsize=8, framealpha=0.2)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "05_top_beneficiaries.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 5: Top Beneficiaries saved")

def chart6_sector_comparison():
    """Grouped bar: Sector comparison on multiple metrics"""
    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    sec = df.groupby("sector").agg(
        beneficiaries=("beneficiaries_lakh","sum"),
        budget=("budget_crore","sum"),
        avg_impact=("impact_score","mean"),
    ).sort_values("beneficiaries", ascending=False)
    colors = [SECTOR_COLORS.get(s,"#888") for s in sec.index]
    short_names = [s.replace(" & ", "\n& ").replace(" ", "\n") if len(s)>12 else s for s in sec.index]

    for ax, col, title, fmt in zip(axes,
        ["beneficiaries","budget","avg_impact"],
        ["Total Beneficiaries (Lakhs)","Total Budget (₹ Crore)","Avg Impact Score (/10)"],
        [".0f",",.0f",".1f"]):
        bars = ax.bar(range(len(sec)), sec[col].values, color=colors, edgecolor="#0d1117")
        ax.set_xticks(range(len(sec)))
        ax.set_xticklabels(short_names, fontsize=7)
        ax.set_title(title, fontsize=11, fontweight="bold")
        for bar, val in zip(bars, sec[col].values):
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()*1.02, f"{val:{fmt}}", ha="center", fontsize=8, fontweight="bold")
    fig.suptitle("Sector-Wise Comparison Dashboard", fontsize=15, fontweight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "06_sector_comparison.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 6: Sector Comparison saved")

def chart7_success_failure():
    """Stacked: Success vs failure by sector"""
    fig, ax = plt.subplots(figsize=(12, 7))
    categories = {"Succeeded": ["Major Success","Success","Moderate Success"], "Mixed/Failed": ["Mixed","Underperformed","Too Early to Judge"]}
    sectors = df["sector"].unique()
    x = range(len(sectors))
    bottoms = [0]*len(sectors)
    cmap = {"Succeeded": "#00E676", "Mixed/Failed": "#FF5252"}
    for cat, verdicts in categories.items():
        vals = [len(df[(df.sector==s) & (df.verdict.isin(verdicts))]) for s in sectors]
        ax.bar(x, vals, bottom=bottoms, label=cat, color=cmap[cat], edgecolor="#0d1117", width=0.6)
        for i, (v, b) in enumerate(zip(vals, bottoms)):
            if v > 0: ax.text(i, b+v/2, str(v), ha="center", va="center", fontsize=11, fontweight="bold")
        bottoms = [b+v for b,v in zip(bottoms, vals)]
    ax.set_xticks(x)
    ax.set_xticklabels(["\n".join(wrap(s, 14)) for s in sectors], fontsize=8)
    ax.set_ylabel("Number of Schemes")
    ax.set_title("Success vs Mixed/Failed Schemes by Sector", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=11, framealpha=0.2)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "07_success_failure.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 7: Success vs Failure saved")

def chart8_scatter_impact_vs_budget():
    """Scatter: Impact score vs budget with bubble size = beneficiaries"""
    fig, ax = plt.subplots(figsize=(13, 9))
    colors = [SECTOR_COLORS.get(r.sector, "#888") for _, r in df.iterrows()]
    sizes = (df["beneficiaries_lakh"] / df["beneficiaries_lakh"].max()) * 800 + 50
    ax.scatter(df["budget_crore"], df["impact_score"], s=sizes, c=colors, alpha=0.75, edgecolors="white", linewidths=0.5)
    for _, r in df.iterrows():
        ax.annotate(r.short, (r.budget_crore, r.impact_score), fontsize=7, ha="center", va="bottom", color="white")
    ax.set_xlabel("Budget (₹ Crore)", fontsize=12)
    ax.set_ylabel("Impact Score (/10)", fontsize=12)
    ax.set_title("Budget vs Impact Score (bubble size = beneficiaries)", fontsize=14, fontweight="bold", pad=15)
    from matplotlib.patches import Patch
    ax.legend([Patch(facecolor=c) for c in SECTOR_COLORS.values()], SECTOR_COLORS.keys(), loc="lower right", fontsize=8, framealpha=0.2)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "08_impact_vs_budget.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 8: Impact vs Budget Scatter saved")

def save_csv():
    """Export analysis data to CSV"""
    export = df[["name","short","sector","launch_year","beneficiaries_lakh","budget_crore",
                 "per_person_benefit","target_group","verdict","impact_score","reach_percent","description"]].copy()
    export.to_csv(os.path.join(OUT, "UP_Schemes_Analysis.csv"), index=False, encoding="utf-8-sig")

    sec = df.groupby("sector").agg(
        total_schemes=("name","count"),
        total_beneficiaries_lakh=("beneficiaries_lakh","sum"),
        total_budget_crore=("budget_crore","sum"),
        avg_impact_score=("impact_score","mean"),
        avg_reach_pct=("reach_percent","mean"),
    ).round(2)
    sec.to_csv(os.path.join(OUT, "Sector_Summary.csv"), encoding="utf-8-sig")
    print("  ✅ CSV exports saved")

def save_report_txt():
    """Save full report to text file"""
    import io
    old_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()
    console_report()
    sys.stdout = old_stdout
    report = buffer.getvalue()
    with open(os.path.join(OUT, "Full_Analysis_Report.txt"), "w", encoding="utf-8") as f:
        f.write(report)
    print("  ✅ Full report saved to Full_Analysis_Report.txt")

# ══════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🔄 Generating UP Government Schemes Analysis...\n")

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

    print_header("ALL DONE!")
    print(f"  📁 All outputs saved to: {os.path.abspath(OUT)}")
    print(f"  📊 8 Charts (PNG), 2 CSVs, 1 Full Report (TXT)")
    print(f"  🔍 Open the 'output' folder to view everything\n")
