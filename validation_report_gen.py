"""
Generates HTML + JSON validation reports from scheme_validator results.
"""
import json, os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

DIM_LABELS = {
    "policy_impact": ("Policy Impact", "🎯"),
    "beneficiary_authenticity": ("Beneficiary Authenticity", "👤"),
    "inter_year_consistency": ("Inter-Year Consistency", "📈"),
    "district_distribution": ("District Distribution", "🗺️"),
    "budget_efficiency": ("Budget Efficiency", "💰"),
    "political_usefulness": ("Political Usefulness", "⚖️"),
    "data_trust_score": ("Data Trust Score", "🔒"),
}


def _color(score):
    if score >= 7.5: return "#00E676"
    if score >= 6.0: return "#69F0AE"
    if score >= 4.5: return "#FFD740"
    if score >= 3.0: return "#FFAB40"
    return "#FF5252"


def _bar(score):
    w = score * 10
    c = _color(score)
    return f'<div style="background:#1a1a2e;border-radius:4px;height:10px;width:100px;display:inline-block;vertical-align:middle"><div style="background:{c};height:10px;border-radius:4px;width:{w}px"></div></div> <b style="color:{c}">{score}</b>'


def generate_reports(results):
    """Generate JSON + HTML reports. Returns (json_path, html_path)."""
    # JSON
    json_path = os.path.join(OUTPUT_DIR, "scheme_validation.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"generated": datetime.now().isoformat(), "schemes": results}, f, indent=2, ensure_ascii=False)

    # HTML
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    avg = round(sum(r["overall_validation_score"] for r in results) / max(len(results), 1), 1)
    top3 = results[:3]
    bottom3 = results[-3:]

    # Summary table
    summary_rows = ""
    for r in results:
        sc = r["overall_validation_score"]
        c = _color(sc)
        src = "🌐" if r["scrape_status"] == "LIVE" else "📄" if r["scrape_status"] == "PARTIAL" else "📚"
        dims = r["dimensions"]
        summary_rows += f"""<tr>
<td><b>{r['short']}</b></td><td>{r['name']}</td><td>{r['sector']}</td>
<td style="color:{c};font-weight:bold">{sc}/10</td>
<td style="color:{c}">{r['validation_verdict']}</td>
<td>{src}</td>
<td>{dims['policy_impact']['score']}</td>
<td>{dims['beneficiary_authenticity']['score']}</td>
<td>{dims['data_trust_score']['score']}</td>
</tr>"""

    # Detail cards
    detail_cards = ""
    for r in results:
        sc = r["overall_validation_score"]
        c = _color(sc)
        dims_html = ""
        for key, (label, icon) in DIM_LABELS.items():
            d = r["dimensions"][key]
            dims_html += f"""<div style="margin:0.4rem 0;padding:0.5rem;background:#161b22;border-radius:6px">
<div>{icon} <b>{label}</b> {_bar(d['score'])}</div>
<div style="color:#8b949e;font-size:0.82rem;margin-top:3px">{d['reasoning']}</div>
</div>"""

        src_badge = {"LIVE":"🟢 Live Scraped","PARTIAL":"🟡 Partial","RESEARCH_ONLY":"⚪ Research"}.get(r["scrape_status"],"⚪")
        detail_cards += f"""<div style="background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:1.2rem;margin-bottom:1rem">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
<div><h3 style="margin:0;color:#f0f6fc">{r['name']} ({r['short']})</h3>
<span style="color:#8b949e">{r['sector']} • {r['target_group']}</span></div>
<div style="text-align:right"><div style="font-size:2rem;font-weight:bold;color:{c}">{sc}/10</div>
<div style="color:{c};font-size:0.85rem">{r['validation_verdict']}</div>
<div style="font-size:0.8rem;margin-top:2px">{src_badge}</div></div>
</div>
<div style="display:flex;gap:0.5rem;margin-bottom:0.5rem;flex-wrap:wrap">
<span style="background:#21262d;padding:2px 8px;border-radius:4px;font-size:0.8rem">👥 {r['beneficiaries_lakh']}L beneficiaries</span>
<span style="background:#21262d;padding:2px 8px;border-radius:4px;font-size:0.8rem">💰 ₹{r['budget_crore']:,} Cr budget</span>
</div>
{dims_html}
</div>"""

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>UP Schemes — Deep Policy Validation</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#0a0a12;color:#c9d1d9;padding:1.5rem;max-width:1200px;margin:auto}}
h1{{color:#f0f6fc;font-size:1.6rem}} h2{{color:#f0f6fc;margin:1.2rem 0 0.5rem;font-size:1.2rem}}
.meta{{color:#8b949e;font-size:0.85rem;margin-bottom:1rem}}
.cards{{display:flex;gap:1rem;flex-wrap:wrap;margin:0.5rem 0 1rem}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:1rem;min-width:150px;flex:1}}
.card .n{{font-size:1.8rem;font-weight:bold}} .card .l{{color:#8b949e;font-size:0.8rem}}
table{{width:100%;border-collapse:collapse;font-size:0.85rem;margin-bottom:1.5rem}}
th{{background:#161b22;color:#f0f6fc;text-align:left;padding:0.5rem;position:sticky;top:0}}
td{{padding:0.5rem;border-bottom:1px solid #1a1a2e}}
tr:hover{{background:#161b2288}}
</style></head><body>
<h1>📊 UP Government Schemes — Deep Policy Validation Report</h1>
<div class="meta">Generated: {now} IST | {len(results)} schemes analyzed | Pipeline v3.2</div>

<div class="cards">
<div class="card"><div class="n" style="color:{_color(avg)}">{avg}/10</div><div class="l">Avg Validation Score</div></div>
<div class="card"><div class="n" style="color:#00E676">{sum(1 for r in results if r['overall_validation_score']>=7)}</div><div class="l">Highly Credible</div></div>
<div class="card"><div class="n" style="color:#FFD740">{sum(1 for r in results if 4.5<=r['overall_validation_score']<7)}</div><div class="l">Moderate</div></div>
<div class="card"><div class="n" style="color:#FF5252">{sum(1 for r in results if r['overall_validation_score']<4.5)}</div><div class="l">Questionable</div></div>
<div class="card"><div class="n">🏆</div><div class="l">Top: {top3[0]['short']} ({top3[0]['overall_validation_score']})</div></div>
</div>

<h2>📋 Summary Table (Ranked by Validation Score)</h2>
<div style="overflow-x:auto"><table>
<tr><th>Code</th><th>Scheme</th><th>Sector</th><th>Score</th><th>Verdict</th><th>Src</th><th>Impact</th><th>Auth</th><th>Trust</th></tr>
{summary_rows}
</table></div>

<h2>🔍 Detailed Scheme-by-Scheme Validation</h2>
{detail_cards}

<div style="margin-top:1.5rem;padding:1rem;background:#161b22;border-radius:8px;font-size:0.8rem;color:#8b949e">
<b>Methodology:</b> Each scheme scored on 7 dimensions (0-10). Overall = weighted average:
Policy Impact (20%) + Data Trust (20%) + Budget Efficiency (15%) + Beneficiary Authenticity (15%) +
Inter-Year Consistency (10%) + District Distribution (10%) + Political Usefulness (10%).
Data sources: live govt portal scrapes, research baselines, RTI data, CAG reports, and news analysis.
</div>
</body></html>"""

    html_path = os.path.join(OUTPUT_DIR, "scheme_validation_report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    return json_path, html_path
