"""
UP GOVERNMENT SCHEMES — DATA-DRIVEN ANALYSIS v3.1
Impact scores COMPUTED from data. Live scraped data highlighted.
"""
import os, sys

for lib in ["pandas","matplotlib","seaborn"]:
    try: __import__(lib)
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable,"-m","pip","install",lib,"-q"])

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from textwrap import wrap
from datetime import datetime as _dt

_run_stamp = _dt.now().strftime("%Y-%m-%d_%H-%M")
OUT = os.path.join(os.path.dirname(__file__), "output", f"run_{_run_stamp}")
os.makedirs(OUT, exist_ok=True)

SECTOR_COLORS = {
    "Social Security":"#FF9800","Health":"#00BCD4","Housing":"#FF5722",
    "Women & Girl Child":"#E91E63","Agriculture":"#4CAF50",
    "Employment & Industry":"#2196F3","Education":"#9C27B0",
    "Food & Civil Supplies":"#8BC34A",
}
VERDICT_COLORS = {
    "Major Success":"#00E676","Success":"#69F0AE","Moderate Success":"#FFD740",
    "Mixed":"#FFAB40","Underperformed":"#FF5252","Too Early to Judge":"#B0BEC5",
}

plt.rcParams.update({
    "figure.facecolor":"#0d1117","axes.facecolor":"#161b22",
    "axes.edgecolor":"#30363d","axes.labelcolor":"white",
    "text.color":"white","xtick.color":"#8b949e",
    "ytick.color":"#8b949e","font.size":11,
    "axes.grid":True,"grid.color":"#21262d","grid.alpha":0.5,
})

def _h(title):
    print("\n" + "═"*70 + f"\n  {title}\n" + "═"*70)


def console_report(df):
    _h("DATA-DRIVEN ANALYSIS REPORT (Computed Scores)")
    live = df[df.scrape_status=="LIVE"]
    print(f"\n  ⚡ Impact scores COMPUTED via formula — not hardcoded")
    print(f"  🌐 {len(live)} schemes with LIVE scraped data")
    print(f"  📚 {len(df)-len(live)} schemes from research baseline\n")

    total_ben = df["beneficiaries_lakh"].sum()
    total_bud = df["budget_crore"].sum()
    print(f"📊 OVERVIEW")
    print(f"   Total Schemes              : {len(df)}")
    print(f"   Total Beneficiaries        : {total_ben:.1f} Lakh ({total_ben/100:.2f} Crore)")
    print(f"   Total Budget               : ₹{total_bud:,.0f} Crore (₹{total_bud/100000:.2f} Lakh Crore)")
    print(f"   Sectors Covered            : {df['sector'].nunique()}")
    print(f"   Avg Impact Score (computed) : {df['impact_score'].mean():.1f}/10")
    print(f"   Avg Reach                  : {df['reach_percent'].mean():.0f}%")

    _h("SECTOR-WISE BREAKDOWN")
    sec = df.groupby("sector").agg(
        schemes=("name","count"), beneficiaries=("beneficiaries_lakh","sum"),
        budget=("budget_crore","sum"), avg_impact=("impact_score","mean"),
        avg_reach=("reach_percent","mean"),
    ).sort_values("beneficiaries", ascending=False)
    for s, r in sec.iterrows():
        print(f"\n  🏷️  {s}")
        print(f"      Schemes: {r.schemes} | Beneficiaries: {r.beneficiaries:.1f}L | Budget: ₹{r.budget:,.0f}Cr")
        print(f"      Avg Impact: {r.avg_impact:.1f}/10 | Avg Reach: {r.avg_reach:.0f}%")

    _h("TOP 10 BY BENEFICIARIES")
    for i, (_, r) in enumerate(df.nlargest(10, "beneficiaries_lakh").iterrows(), 1):
        src = "🌐" if r.scrape_status=="LIVE" else "📚"
        print(f"  {i:2}. {src} {r['name']}")
        print(f"      {r.beneficiaries_lakh:.1f}L | ₹{r.budget_crore:,}Cr | Impact {r.impact_score}/10 | {r.verdict}")

    _h("ALL SCHEMES (Ranked by Computed Impact Score)")
    for i, (_, r) in enumerate(df.sort_values("impact_score", ascending=False).iterrows(), 1):
        src = "🌐" if r.scrape_status=="LIVE" else "📚"
        v = {"Major Success":"🏆","Success":"✅","Moderate Success":"👍","Mixed":"⚠️","Underperformed":"❌","Too Early to Judge":"🕐"}.get(r.verdict,"❓")
        print(f"\n  {i:2}. {v} {r['name']} ({r.short}) {src}")
        print(f"      Impact: {r.impact_score}/10 | Verdict: {r.verdict} | Source: {r.data_source}")
        print(f"      Beneficiaries: {r.beneficiaries_lakh:.1f}L | Budget: ₹{r.budget_crore:,}Cr")
        if isinstance(r.get('impact_components'), dict):
            c = r.impact_components
            print(f"      Components: Scale={c.get('scale',0)} Eff={c.get('efficiency',0)} "
                  f"Disb={c.get('disbursement',0)} Cov={c.get('coverage',0)} Long={c.get('longevity',0)}")

    _h("VERDICT SUMMARY")
    for v in ["Major Success","Success","Moderate Success","Mixed","Underperformed","Too Early to Judge"]:
        vdf = df[df.verdict==v]
        if vdf.empty: continue
        icon = {"Major Success":"🏆","Success":"✅","Moderate Success":"👍","Mixed":"⚠️","Underperformed":"❌","Too Early to Judge":"🕐"}[v]
        print(f"\n  {icon} {v.upper()} ({len(vdf)} schemes)")
        for _, r in vdf.iterrows():
            src = "🌐" if r.scrape_status=="LIVE" else "📚"
            print(f"      {src} {r['name']} — {r.beneficiaries_lakh:.1f}L, Score {r.impact_score}/10")

    _h("IMPACT SCORE FORMULA")
    print("  Score = 0.25×Scale + 0.20×Efficiency + 0.20×Disbursement + 0.20×Coverage + 0.15×Longevity")
    print("  ⚡ All scores computed from data. 🌐 = live scraped, 📚 = research baseline")


def chart_beneficiaries(df):
    fig, ax = plt.subplots(figsize=(14, 10))
    top = df.nlargest(15, "beneficiaries_lakh")
    colors = [SECTOR_COLORS.get(r.sector,"#888") for _, r in top.iterrows()]
    bars = ax.barh(range(len(top)), top["beneficiaries_lakh"].values, color=colors, height=0.65)
    labels = []
    for _, r in top.iterrows():
        src = " 🌐" if r.scrape_status=="LIVE" else ""
        labels.append(f"{r['name'][:32]}{src}")
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Beneficiaries (Lakhs)")
    ax.set_title("Top 15 Schemes by Beneficiaries", fontsize=14, fontweight="bold", pad=15)
    for bar, val in zip(bars, top["beneficiaries_lakh"].values):
        ax.text(bar.get_width()+1, bar.get_y()+bar.get_height()/2, f"{val:.0f}L", va="center", fontsize=9, color="#58a6ff")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "01_beneficiaries.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 1: Beneficiaries")


def chart_impact_scores(df):
    fig, ax = plt.subplots(figsize=(14, 10))
    sorted_df = df.sort_values("impact_score", ascending=True)
    colors = ["#10b981" if s>=7.0 else "#fbbf24" if s>=5.0 else "#ef4444" for s in sorted_df["impact_score"]]
    bars = ax.barh(range(len(sorted_df)), sorted_df["impact_score"].values, color=colors, height=0.6)
    labels = []
    for _, r in sorted_df.iterrows():
        src = " 🌐" if r.scrape_status=="LIVE" else ""
        labels.append(f"{r.short}{src}")
    ax.set_yticks(range(len(sorted_df)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Impact Score (/10)")
    ax.set_xlim(0, 10)
    ax.set_title("Impact Scores — COMPUTED from Data", fontsize=14, fontweight="bold", pad=15)
    for bar, val in zip(bars, sorted_df["impact_score"].values):
        ax.text(bar.get_width()+0.1, bar.get_y()+bar.get_height()/2, f"{val:.1f}", va="center", fontsize=9, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "02_impact_scores.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 2: Impact Scores")


def chart_budget(df):
    fig, ax = plt.subplots(figsize=(13, 9))
    top = df.nlargest(15, "budget_crore")
    colors = [SECTOR_COLORS.get(r.sector,"#888") for _, r in top.iterrows()]
    bars = ax.barh(range(len(top)), top["budget_crore"].values, color=colors, height=0.65)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels([n[:35] for n in top["name"].values], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Budget (₹ Crore)")
    ax.set_title("Top 15 Schemes by Budget", fontsize=14, fontweight="bold", pad=15)
    for bar, val in zip(bars, top["budget_crore"].values):
        ax.text(bar.get_width()+200, bar.get_y()+bar.get_height()/2, f"₹{val:,.0f}Cr", va="center", fontsize=9, color="#58a6ff")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "03_budget.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 3: Budget")


def chart_verdict(df):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    vc = df["verdict"].value_counts()
    colors = [VERDICT_COLORS.get(v,"#888") for v in vc.index]
    ax1.pie(vc.values, labels=vc.index, autopct="%1.0f%%", colors=colors,
            startangle=140, wedgeprops=dict(edgecolor="#0d1117", linewidth=2))
    ax1.set_title("Verdict Distribution", fontsize=13, fontweight="bold")

    vg = df.groupby("verdict")["impact_score"].mean().sort_values(ascending=False)
    bars = ax2.bar(range(len(vg)), vg.values, color=[VERDICT_COLORS.get(v,"#888") for v in vg.index])
    ax2.set_xticks(range(len(vg)))
    ax2.set_xticklabels(["\n".join(wrap(v,12)) for v in vg.index], fontsize=8)
    ax2.set_ylabel("Avg Impact Score")
    ax2.set_title("Avg Computed Impact by Verdict", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, vg.values):
        ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.15, f"{val:.1f}", ha="center", fontsize=10, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "04_verdict.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 4: Verdict Analysis")


def chart_heatmap(df):
    fig, ax = plt.subplots(figsize=(14, 11))
    s = df.sort_values(["sector","impact_score"], ascending=[True, False])
    data = s[["impact_score","reach_percent"]].values
    names = []
    for _, r in s.iterrows():
        src = "🌐" if r.scrape_status=="LIVE" else "📚"
        names.append(f"{src} {r['name'][:28]} [{r.sector[:8]}]")
    sns.heatmap(data, annot=True, fmt=".1f", cmap="RdYlGn",
        xticklabels=["Impact\n(/10)","Reach\n(%)"], yticklabels=names,
        ax=ax, linewidths=1, linecolor="#0d1117", cbar_kws={"shrink":0.6})
    ax.set_title("Performance Heatmap (🌐=Live, 📚=Research)", fontsize=14, fontweight="bold", pad=15)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "05_heatmap.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 5: Heatmap")


def chart_sector(df):
    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    sec = df.groupby("sector").agg(
        ben=("beneficiaries_lakh","sum"), bud=("budget_crore","sum"),
        imp=("impact_score","mean"),
    ).sort_values("ben", ascending=False)
    colors = [SECTOR_COLORS.get(s,"#888") for s in sec.index]
    short = [s.replace(" & ","\n& ") if len(s)>12 else s for s in sec.index]
    for ax, col, title in zip(axes, ["ben","bud","imp"],
        ["Beneficiaries (Lakhs)","Budget (₹Cr)","Avg Impact (/10)"]):
        bars = ax.bar(range(len(sec)), sec[col].values, color=colors)
        ax.set_xticks(range(len(sec)))
        ax.set_xticklabels(short, fontsize=7)
        ax.set_title(title, fontsize=11, fontweight="bold")
        for bar, val in zip(bars, sec[col].values):
            fmt = f"{val:.1f}" if col=="imp" else f"{val:,.0f}"
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()*1.02, fmt, ha="center", fontsize=8, fontweight="bold")
    fig.suptitle("Sector Comparison", fontsize=15, fontweight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "06_sectors.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 6: Sectors")


def chart_scatter(df):
    fig, ax = plt.subplots(figsize=(13, 9))
    colors = [SECTOR_COLORS.get(r.sector,"#888") for _, r in df.iterrows()]
    sizes = (df["beneficiaries_lakh"]/df["beneficiaries_lakh"].max())*800 + 50
    edge = ["white" if r.scrape_status=="LIVE" else "#555" for _, r in df.iterrows()]
    lw = [2 if r.scrape_status=="LIVE" else 0.5 for _, r in df.iterrows()]
    ax.scatter(df["budget_crore"], df["impact_score"], s=sizes, c=colors, alpha=0.75, edgecolors=edge, linewidths=lw)
    for _, r in df.iterrows():
        ax.annotate(r.short, (r.budget_crore, r.impact_score), fontsize=7, ha="center", va="bottom", color="white")
    ax.set_xlabel("Budget (₹ Crore)")
    ax.set_ylabel("Impact Score (COMPUTED /10)")
    ax.set_title("Budget vs Computed Impact (white border = live data)", fontsize=14, fontweight="bold", pad=15)
    from matplotlib.patches import Patch
    ax.legend([Patch(facecolor=c) for c in SECTOR_COLORS.values()], SECTOR_COLORS.keys(), loc="lower right", fontsize=8, framealpha=0.2)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "07_scatter.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 7: Scatter")


def chart_data_sources(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    status_counts = df["scrape_status"].value_counts()
    colors = {"LIVE":"#10b981","PARTIAL":"#fbbf24","RESEARCH_ONLY":"#64748b"}
    ax.pie(status_counts.values, labels=status_counts.index, autopct="%1.0f%%",
        colors=[colors.get(s,"#888") for s in status_counts.index],
        startangle=90, wedgeprops=dict(edgecolor="#0d1117", linewidth=2))
    ax.set_title("Data Source Distribution", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "08_data_sources.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✅ Chart 8: Data Sources")


def save_outputs(df):
    cols = [c for c in ["name","short","sector","launch_year","beneficiaries_lakh",
            "budget_crore","disbursed_crore","impact_score","reach_percent",
            "verdict","data_source","scrape_status"] if c in df.columns]
    df[cols].to_csv(os.path.join(OUT, "UP_Schemes_Analysis.csv"), index=False, encoding="utf-8-sig")
    
    sec = df.groupby("sector").agg(
        total_schemes=("name","count"), total_beneficiaries=("beneficiaries_lakh","sum"),
        total_budget=("budget_crore","sum"), avg_impact=("impact_score","mean"),
        avg_reach=("reach_percent","mean"),
    ).round(2)
    sec.to_csv(os.path.join(OUT, "Sector_Summary.csv"), encoding="utf-8-sig")
    
    import io
    old = sys.stdout; sys.stdout = buf = io.StringIO()
    console_report(df)
    sys.stdout = old
    with open(os.path.join(OUT, "Full_Analysis_Report.txt"), "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    print("  ✅ CSV + Full Report saved")


def run_full_analysis(schemes_list):
    print(f"  📁 Output: {os.path.abspath(OUT)}")
    df = pd.DataFrame(schemes_list)
    if df.empty:
        print("  ❌ No data!"); return
    for col in ["disbursed_crore","budget_crore","reach_percent"]:
        if col not in df.columns: df[col] = 0

    console_report(df)
    _h("GENERATING CHARTS")
    chart_beneficiaries(df)
    chart_impact_scores(df)
    chart_budget(df)
    chart_verdict(df)
    chart_heatmap(df)
    chart_sector(df)
    chart_scatter(df)
    chart_data_sources(df)
    _h("EXPORTING")
    save_outputs(df)
    print(f"\n  📁 All outputs: {os.path.abspath(OUT)}")
