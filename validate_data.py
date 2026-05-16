"""
═══════════════════════════════════════════════════════════════
  SCRAPED DATA VALIDATOR v1.0
  Auto-runs inside pipeline after scraping.
  Outputs: JSON report + HTML report + console summary
═══════════════════════════════════════════════════════════════
"""
import json, os, re, math
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPED_DIR = os.path.join(BASE_DIR, "scraped_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Known thresholds ──
UP_POPULATION_LAKH = 2412
MAX_BEN_LAKH = 1000
PENSION_PER_QUARTER = 3000  # ₹1000/month × 3

RESEARCH_BASELINES = {
    "Old Age Pension":   {"ben_lakh": 55.99, "tol": 35},
    "Widow Pension":     {"ben_lakh": 29.03, "tol": 30},
    "Divyang Pension":   {"ben_lakh": 12.00, "tol": 30},
}


class Validator:
    def __init__(self):
        self.checks = []  # list of {scraper, level, msg}

    def _add(self, scraper, level, msg):
        self.checks.append({"scraper": scraper, "level": level, "message": msg})

    def ok(self, s, m):    self._add(s, "PASS", m)
    def warn(self, s, m):  self._add(s, "WARN", m)
    def error(self, s, m): self._add(s, "ERROR", m)
    def info(self, s, m):  self._add(s, "INFO", m)

    @property
    def errors(self):   return [c for c in self.checks if c["level"] == "ERROR"]
    @property
    def warnings(self): return [c for c in self.checks if c["level"] == "WARN"]
    @property
    def passes(self):   return [c for c in self.checks if c["level"] == "PASS"]

    def score(self):
        graded = [c for c in self.checks if c["level"] in ("PASS","WARN","ERROR")]
        return round(len(self.passes) / max(len(graded), 1) * 100, 1)


def load_scraped(filename):
    path = os.path.join(SCRAPED_DIR, filename)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_scraped_data(scraped_results=None):
    """
    Validate all scraped data. Can accept master results dict directly
    (from pipeline) or load from files.
    Returns: (Validator, cleaned_overrides dict)
    """
    v = Validator()
    overrides = {}  # cleaned data the pipeline should use

    # ── 1. SSPY PENSIONS ──
    sspy = (scraped_results or {}).get("sspy_pensions") or load_scraped("01_sspy_pensions.json")
    if not sspy:
        v.error("SSPY", "No data file found")
    elif sspy.get("status") != "SCRAPED":
        v.error("SSPY", f"Status: {sspy.get('status','UNKNOWN')}")
    else:
        v.ok("SSPY", "Scraper completed successfully")
        pensions = sspy.get("parsed_data", {}).get("pensions", [])
        if len(pensions) < 3:
            v.warn("SSPY", f"Only {len(pensions)} pensions (expected ≥3)")
        else:
            v.ok("SSPY", f"{len(pensions)} pension schemes parsed")

        for p in pensions:
            pname = p.get("name", "?")
            ben_l = p.get("beneficiaries_lakh", 0)
            disb = p.get("total_disbursed_crore", 0)
            quarters = p.get("quarters_with_data", 0)

            # Range check
            if 0 < ben_l < MAX_BEN_LAKH:
                v.ok("SSPY", f"{pname}: {ben_l}L beneficiaries ✓")
            elif ben_l <= 0:
                v.error("SSPY", f"{pname}: zero beneficiaries")
            else:
                v.error("SSPY", f"{pname}: {ben_l}L exceeds plausible max")

            # Per-person sanity
            if quarters > 0 and ben_l > 0 and disb > 0:
                per_q = (disb * 1e7) / (ben_l * 1e5) / quarters
                if 1500 < per_q < 6000:
                    v.ok("SSPY", f"{pname}: ₹{per_q:,.0f}/person/quarter ✓")
                else:
                    v.warn("SSPY", f"{pname}: ₹{per_q:,.0f}/person/quarter (expected ~₹3,000)")

            # Cross-validate with research
            bl = RESEARCH_BASELINES.get(pname)
            if bl:
                dev = abs(ben_l - bl["ben_lakh"]) / bl["ben_lakh"] * 100
                if dev <= bl["tol"]:
                    v.ok("SSPY", f"{pname}: {dev:.0f}% deviation from research ✓")
                else:
                    v.warn("SSPY", f"{pname}: {dev:.0f}% deviation from research ({ben_l}L vs {bl['ben_lakh']}L)")

        # Grand total check
        gt = sspy.get("parsed_data", {}).get("grand_total", {})
        if gt:
            total = gt.get("total_beneficiaries", 0)
            summed = sum(p.get("beneficiaries", 0) for p in pensions)
            if total > 0 and summed > 0:
                diff = abs(total - summed) / total * 100
                if diff < 5:
                    v.ok("SSPY", f"Grand total matches sum ({diff:.1f}% diff)")
                else:
                    v.warn("SSPY", f"Grand total mismatch: {total:,.0f} vs sum {summed:,.0f}")

    # ── 2. MKSY ──
    mksy = (scraped_results or {}).get("mksy") or load_scraped("02_mksy.json")
    if mksy and mksy.get("status") == "SCRAPED":
        v.ok("MKSY", "Page fetched successfully")
        counters = mksy.get("parsed_data", {}).get("counters", [])
        has_real = False
        for c in counters:
            if len(c.get("text", "")) > 5000:
                v.warn("MKSY", "Counter text is full page dump (no real metrics)")
            nums = c.get("numbers", [])
            real_nums = [n for n in nums if n not in (",", ".", "०", "0")]
            if len(real_nums) > 5:
                has_real = True
        if not has_real:
            v.warn("MKSY", "No usable beneficiary stats extracted — using research baseline")
    else:
        v.error("MKSY", f"Status: {(mksy or {}).get('status', 'MISSING')}")

    # ── 3 & 4. PMAY Rural + Urban ──
    for label, key, fname in [("PMAY-Rural", "pmay_rural", "03_pmay_rural.json"),
                               ("PMAY-Urban", "pmay_urban", "04_pmay_urban.json")]:
        data = (scraped_results or {}).get(key) or load_scraped(fname)
        if not data:
            v.error(label, "No data file")
        elif data.get("status") != "SCRAPED":
            v.error(label, f"Status: {data.get('status','?')}")
        else:
            pd = data.get("parsed_data", {})
            if not pd or not any(v2 for v2 in pd.values() if v2):
                v.error(label, "Page fetched but parsed_data empty (JS portal needs Selenium)")
            else:
                v.ok(label, f"Parsed {len(pd)} data fields")

    # ── 5. ODOP ──
    odop = (scraped_results or {}).get("odop") or load_scraped("05_odop.json")
    if odop and odop.get("status") == "SCRAPED":
        size = odop.get("page_size", 0)
        products = odop.get("parsed_data", {}).get("products", [])
        if size < 1000:
            v.error("ODOP", f"Only {size} bytes received (redirect/error page)")
        elif not products:
            v.warn("ODOP", "Page fetched but no products extracted")
        else:
            v.ok("ODOP", f"{len(products)} products found")
    else:
        v.error("ODOP", f"Status: {(odop or {}).get('status', 'MISSING')}")

    # ── 6. AYUSHMAN (critical validation) ──
    ab = (scraped_results or {}).get("ayushman") or load_scraped("06_ayushman.json")
    if ab and ab.get("status") == "SCRAPED":
        v.ok("Ayushman", "Scraper completed")
        pd = ab.get("parsed_data", {})
        total_ben = pd.get("total_beneficiaries", 0)
        golden = pd.get("golden_cards_issued", 0)
        hospitals = pd.get("empanelled_hospitals", 0)
        claims = pd.get("claims_submitted", 0)
        preauth = pd.get("preauth_requests", 0)
        ben_lakh = pd.get("beneficiaries_lakh", 0)

        # CRITICAL: detect enrolled-vs-treated confusion
        if ben_lakh > 500:
            v.warn("Ayushman",
                f"beneficiaries_lakh={ben_lakh} is ENROLLED population, not treated patients. "
                f"Research baseline=45L treated. Capping to research value.")
            overrides["ayushman_ben_cap"] = 45.0  # use research value

        # Internal consistency
        if golden > 0 and total_ben > 0:
            if golden > total_ben:
                v.error("Ayushman", f"Cards ({golden:,.0f}) > enrolled ({total_ben:,.0f})")
            else:
                v.ok("Ayushman", f"Card coverage: {golden/total_ben*100:.1f}%")

        if claims > 0 and preauth > 0:
            if claims <= preauth:
                v.ok("Ayushman", f"Claims/preauth ratio: {claims/preauth*100:.1f}%")
            else:
                v.warn("Ayushman", f"Claims ({claims:,.0f}) > preauth ({preauth:,.0f})")

        if 100 < hospitals < 20000:
            v.ok("Ayushman", f"{hospitals:,} hospitals — plausible")
        elif hospitals > 0:
            v.warn("Ayushman", f"{hospitals} hospitals — unusual count")
    else:
        v.error("Ayushman", f"Status: {(ab or {}).get('status', 'MISSING')}")

    # ── 7. UP Gov / FCS ──
    upgov = (scraped_results or {}).get("up_gov") or load_scraped("07_up_gov.json")
    if upgov and upgov.get("status") == "SCRAPED":
        v.ok("UP-Gov", "Portal reached")
        wheat = upgov.get("parsed_data", {}).get("wheat_procurement", {})
        if wheat and wheat.get("farmers", 0) > 0:
            farmers = wheat["farmers"]
            qty = wheat.get("quantity_lakh_mt", 0)
            v.ok("FCS", f"Wheat: {farmers:,.0f} farmers, {qty} lakh MT")
            # Check field overlap
            centres = upgov.get("parsed_data", {}).get("procurement_centres", 0)
            if centres == farmers and centres > 0:
                v.warn("FCS", f"procurement_centres == farmers ({centres:,.0f}) — likely regex overlap")
        else:
            v.warn("FCS", "No wheat procurement data")
    else:
        v.error("UP-Gov", "Portal unreachable")

    # ── 8. Scholarship ──
    schol = (scraped_results or {}).get("scholarship") or load_scraped("08_scholarship.json")
    if not schol or schol.get("status") == "FAILED":
        v.error("Scholarship", "All connection attempts failed (portal down)")
    elif schol.get("status") == "SCRAPED":
        v.ok("Scholarship", "Data fetched")

    return v, overrides


def generate_json_report(v, overrides, output_dir=None):
    """Save validation results as JSON."""
    out = output_dir or OUTPUT_DIR
    report = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "score_pct": v.score(),
        "summary": {
            "errors": len(v.errors),
            "warnings": len(v.warnings),
            "passed": len(v.passes),
            "total_checks": len(v.checks),
        },
        "overrides_applied": overrides,
        "checks": v.checks,
    }
    path = os.path.join(out, "validation_report.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return path


def generate_html_report(v, overrides, output_dir=None):
    """Save validation results as a styled HTML report."""
    out = output_dir or OUTPUT_DIR
    score = v.score()
    color = "#00E676" if score >= 70 else "#FFD740" if score >= 50 else "#FF5252"

    rows = ""
    for c in v.checks:
        lvl = c["level"]
        icon = {"PASS":"✅","WARN":"⚠️","ERROR":"❌","INFO":"ℹ️"}.get(lvl, "")
        bg = {"PASS":"#1a3a1a","WARN":"#3a3a1a","ERROR":"#3a1a1a","INFO":"#1a2a3a"}.get(lvl,"#222")
        rows += f'<tr style="background:{bg}"><td>{icon} {lvl}</td><td>{c["scraper"]}</td><td>{c["message"]}</td></tr>\n'

    override_html = ""
    if overrides:
        override_html = "<h2>🔧 Auto-Applied Fixes</h2><ul>"
        for k, val in overrides.items():
            override_html += f"<li><b>{k}</b>: {val}</li>"
        override_html += "</ul>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Data Validation Report</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:'Segoe UI',sans-serif; background:#0d1117; color:#c9d1d9; padding:2rem; }}
  h1 {{ color:#f0f6fc; margin-bottom:0.5rem; }}
  h2 {{ color:#f0f6fc; margin:1.5rem 0 0.5rem; }}
  .score {{ font-size:3rem; font-weight:bold; color:{color}; }}
  .summary {{ display:flex; gap:1.5rem; margin:1rem 0; flex-wrap:wrap; }}
  .card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:1rem 1.5rem; min-width:140px; }}
  .card .num {{ font-size:1.8rem; font-weight:bold; }}
  .card .label {{ color:#8b949e; font-size:0.85rem; }}
  .err .num {{ color:#FF5252; }}
  .wrn .num {{ color:#FFD740; }}
  .ok .num {{ color:#00E676; }}
  table {{ width:100%; border-collapse:collapse; margin-top:0.5rem; }}
  th {{ background:#21262d; text-align:left; padding:0.6rem 1rem; color:#f0f6fc; }}
  td {{ padding:0.5rem 1rem; border-bottom:1px solid #21262d; font-size:0.9rem; }}
  .ts {{ color:#8b949e; font-size:0.85rem; margin-bottom:1rem; }}
</style>
</head>
<body>
<h1>📊 Scraped Data Validation Report</h1>
<div class="ts">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} IST</div>

<div class="score">{score}%</div>
<div class="summary">
  <div class="card err"><div class="num">{len(v.errors)}</div><div class="label">Errors</div></div>
  <div class="card wrn"><div class="num">{len(v.warnings)}</div><div class="label">Warnings</div></div>
  <div class="card ok"><div class="num">{len(v.passes)}</div><div class="label">Passed</div></div>
</div>

{override_html}

<h2>Detailed Checks</h2>
<table>
<tr><th>Status</th><th>Scraper</th><th>Detail</th></tr>
{rows}
</table>
</body>
</html>"""

    path = os.path.join(out, "validation_report.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def print_console_summary(v, overrides):
    """Print concise validation to console."""
    score = v.score()
    icon = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"

    print(f"\n  ── DATA VALIDATION ──")
    print(f"  {icon} Score: {score}% | {len(v.errors)} errors | {len(v.warnings)} warnings | {len(v.passes)} passed")

    if v.errors:
        print(f"  🔴 Errors:")
        for e in v.errors:
            print(f"     ❌ [{e['scraper']}] {e['message']}")
    if v.warnings:
        print(f"  🟡 Warnings:")
        for w in v.warnings:
            print(f"     ⚠️  [{w['scraper']}] {w['message']}")
    if overrides:
        print(f"  🔧 Auto-fixes applied:")
        for k, val in overrides.items():
            print(f"     → {k}: {val}")


def run_validation(scraped_results=None):
    """Full validation: validate → fix → report. Called by pipeline."""
    v, overrides = validate_scraped_data(scraped_results)
    print_console_summary(v, overrides)

    json_path = generate_json_report(v, overrides)
    html_path = generate_html_report(v, overrides)
    print(f"  📄 JSON report: {json_path}")
    print(f"  🌐 HTML report: {html_path}")

    return v, overrides


if __name__ == "__main__":
    run_validation()
