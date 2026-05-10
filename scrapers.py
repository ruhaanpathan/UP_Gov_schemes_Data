"""
═══════════════════════════════════════════════════════════════
  UP GOVERNMENT SCHEMES — LIVE DATA PIPELINE v2.0
  Stage 1: Scrape & Fetch from Government Portals
═══════════════════════════════════════════════════════════════
FIXES:
  - SSL verification disabled for govt sites with expired certs
  - Corrected URLs for PMAY, ODOP, SSPY
  - Added fallback URLs for each scraper
  - HTTP fallback when HTTPS fails
"""

import requests, json, csv, os, time, re, warnings
from datetime import datetime
from bs4 import BeautifulSoup

# Suppress SSL warnings (govt sites have expired certs)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Config ──
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = 25
DELAY = 2
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scraped_data")
os.makedirs(OUT_DIR, exist_ok=True)

LOG = []

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    icon = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERR": "❌", "FETCH": "🌐"}.get(level, "")
    line = f"[{ts}] {icon} {msg}"
    print(line)
    LOG.append(line)

def safe_get(url, **kwargs):
    """Safe HTTP GET with retry + SSL bypass."""
    log(f"Fetching: {url}", "FETCH")
    for attempt in range(3):
        try:
            # Try with verify=False (many govt sites have expired SSL)
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False, **kwargs)
            r.raise_for_status()
            time.sleep(DELAY)
            log(f"  ✓ Got {len(r.text)} bytes", "OK")
            return r
        except requests.exceptions.HTTPError as e:
            log(f"  Attempt {attempt+1}: HTTP {r.status_code} - {e}", "WARN")
            time.sleep(2)
        except Exception as e:
            log(f"  Attempt {attempt+1}: {str(e)[:80]}", "WARN")
            time.sleep(3)
    log(f"  All attempts failed for {url}", "ERR")
    return None

def try_urls(urls):
    """Try multiple URLs, return first successful response."""
    for url in urls:
        r = safe_get(url)
        if r:
            return r, url
    return None, None

def save_json(data, filename):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"  Saved → {filename}", "OK")

def extract_numbers_from_page(soup):
    """Extract all meaningful numbers and stats from a page."""
    stats = {}
    for el in soup.find_all(["span", "div", "h2", "h3", "strong", "p", "td", "th", "li"]):
        text = el.get_text(strip=True)
        if len(text) > 5 and len(text) < 200:
            if re.search(r"[\d,]+\.?\d*\s*(lakh|crore|करोड़|लाख|beneficiar|hospital|card|claim|house|school|student|farmer|pension|family|district|women|girl)", text, re.I):
                stats[text[:120]] = True
    return list(stats.keys())

def extract_tables(soup, max_tables=5):
    """Extract table data from HTML."""
    all_tables = []
    for table in soup.find_all("table")[:max_tables]:
        rows = []
        for tr in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if cells and any(c.strip() for c in cells):
                rows.append(cells)
        if rows:
            all_tables.append(rows)
    return all_tables


# ══════════════════════════════════════════════════════════════
#  SCRAPER 1: SSPY Pension Portal
# ══════════════════════════════════════════════════════════════
def scrape_sspy_pensions():
    log("━━━ Scraper 1: SSPY Pension Portal ━━━")
    results = {"source": "sspy-up.gov.in", "scraped_at": str(datetime.now()), "schemes": []}

    # Try multiple URL patterns (govt sites frequently change paths)
    base_urls = [
        "https://sspy-up.gov.in",
        "http://sspy-up.gov.in",
    ]
    pension_paths = {
        "Old Age Pension": [
            "/oap/Index", "/OldAgePension", "/OldAgePension/Index",
            "/oap", "/pension/old-age",
        ],
        "Widow Pension": [
            "/widow/Index", "/WidowPension", "/WidowPension/Index",
            "/widow", "/pension/widow",
        ],
        "Divyang Pension": [
            "/handicap/Index", "/HandicapPension", "/HandicapPension/Index",
            "/divyang", "/pension/divyang",
        ],
    }

    # First try to get the main page
    r, used_url = try_urls(base_urls)
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        results["main_page"] = {
            "status": "SCRAPED",
            "url": used_url,
            "page_size": len(r.text),
            "title": soup.title.string if soup.title else "",
            "stats": extract_numbers_from_page(soup),
            "tables": extract_tables(soup),
        }
        # Extract pension links from the main page
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if any(kw in text.lower() or kw in href.lower()
                   for kw in ["pension", "old age", "widow", "divyang", "handicap", "वृद्धा", "विधवा", "दिव्यांग"]):
                results.setdefault("found_links", []).append({"text": text, "href": href})
    else:
        results["main_page"] = {"status": "FAILED"}

    # Try individual pension pages
    base = used_url or "https://sspy-up.gov.in"
    for scheme, paths in pension_paths.items():
        urls = [f"{base}{p}" for p in paths]
        r2, url2 = try_urls(urls)
        data = {"name": scheme, "url": url2 or urls[0]}
        if r2:
            soup2 = BeautifulSoup(r2.text, "lxml")
            data["status"] = "SCRAPED"
            data["page_size"] = len(r2.text)
            data["stats"] = extract_numbers_from_page(soup2)
            data["tables"] = extract_tables(soup2, 3)
        else:
            data["status"] = "FAILED"
        results["schemes"].append(data)

    save_json(results, "01_sspy_pensions.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 2: Kanya Sumangala (mksy.up.gov.in)
# ══════════════════════════════════════════════════════════════
def scrape_mksy():
    log("━━━ Scraper 2: Kanya Sumangala Yojana ━━━")
    results = {"source": "mksy.up.gov.in", "scraped_at": str(datetime.now())}

    r, url = try_urls(["https://mksy.up.gov.in", "http://mksy.up.gov.in"])
    if not r:
        results["status"] = "FAILED"
        save_json(results, "02_mksy.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["page_size"] = len(r.text)
    results["title"] = soup.title.string if soup.title else ""
    results["stats"] = extract_numbers_from_page(soup)
    results["tables"] = extract_tables(soup)

    # Look for dashboard counters
    for div in soup.find_all(["div", "span"], class_=re.compile(r"count|number|stat|dashboard", re.I)):
        results.setdefault("counters", []).append(div.get_text(strip=True))

    save_json(results, "02_mksy.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 3: PMAY Rural (rhreporting.nic.in - WORKING!)
# ══════════════════════════════════════════════════════════════
def scrape_pmay_rural():
    log("━━━ Scraper 3: PMAY Rural Housing ━━━")
    results = {"source": "rhreporting.nic.in", "scraped_at": str(datetime.now())}

    # rhreporting.nic.in is the WORKING reporting portal for PMAY-G
    urls = [
        "https://rhreporting.nic.in/netiay/newreport.aspx",
        "https://rhreporting.nic.in/netiay/home.aspx",
        "https://pmayg.nic.in/netiay/home.aspx",
        "http://pmayg.nic.in/netiay/home.aspx",
    ]
    r, url = try_urls(urls)
    if not r:
        results["status"] = "FAILED"
        save_json(results, "03_pmay_rural.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(r.text)
    results["title"] = soup.title.string if soup.title else ""
    results["stats"] = extract_numbers_from_page(soup)
    results["tables"] = extract_tables(soup)

    # Try to find UP-specific data
    for el in soup.find_all(text=re.compile(r"uttar pradesh|UP|उत्तर प्रदेश", re.I)):
        parent = el.parent
        if parent:
            row = parent.find_parent("tr")
            if row:
                cells = [c.get_text(strip=True) for c in row.find_all(["td","th"])]
                results["up_row"] = cells

    save_json(results, "03_pmay_rural.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 4: PMAY Urban (pmaymis.gov.in - WORKING!)
# ══════════════════════════════════════════════════════════════
def scrape_pmay_urban():
    log("━━━ Scraper 4: PMAY Urban Housing ━━━")
    results = {"source": "pmaymis.gov.in", "scraped_at": str(datetime.now())}

    # pmaymis.gov.in is the WORKING portal for PMAY Urban
    urls = [
        "https://pmaymis.gov.in",
        "https://pmay-urban.gov.in",
        "http://pmay-urban.gov.in",
    ]
    r, url = try_urls(urls)
    if not r:
        results["status"] = "FAILED"
        save_json(results, "04_pmay_urban.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(r.text)
    results["title"] = soup.title.string if soup.title else ""
    results["stats"] = extract_numbers_from_page(soup)
    results["tables"] = extract_tables(soup)

    save_json(results, "04_pmay_urban.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 5: ODOP (odopup.in via HTTP - WORKING!)
# ══════════════════════════════════════════════════════════════
def scrape_odop():
    log("━━━ Scraper 5: ODOP Portal ━━━")
    results = {"source": "odopup.in", "scraped_at": str(datetime.now())}

    # HTTP works, HTTPS has expired cert
    urls = [
        "http://odopup.in",
        "https://odopup.in",
        "http://www.odopup.in",
        "http://odopmart.up.gov.in",
    ]
    r, url = try_urls(urls)
    if not r:
        results["status"] = "FAILED"
        save_json(results, "05_odop.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(r.text)
    results["title"] = soup.title.string if soup.title else ""
    results["stats"] = extract_numbers_from_page(soup)
    results["tables"] = extract_tables(soup)

    # Extract district-product info
    products = []
    for el in soup.find_all(["a", "div", "li", "td"]):
        text = el.get_text(strip=True)
        if re.search(r"(brass|silk|leather|carpet|chikan|zari|pottery|glass|wood|lock|perfume|horn|textile|banana)", text, re.I):
            products.append(text[:100])
    results["products"] = list(set(products))[:50]

    save_json(results, "05_odop.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 6: Ayushman Bharat UP
# ══════════════════════════════════════════════════════════════
def scrape_ayushman():
    log("━━━ Scraper 6: Ayushman Bharat UP ━━━")
    results = {"source": "ayushmanup.in", "scraped_at": str(datetime.now())}

    urls = [
        "https://ayushmanup.in",
        "http://ayushmanup.in",
        "https://dashboard.pmjay.gov.in",
        "https://pmjay.gov.in",
    ]
    r, url = try_urls(urls)
    if not r:
        results["status"] = "FAILED"
        save_json(results, "06_ayushman.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(r.text)
    results["title"] = soup.title.string if soup.title else ""
    results["stats"] = extract_numbers_from_page(soup)
    results["tables"] = extract_tables(soup)

    save_json(results, "06_ayushman.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 7: UP Gov Portal + Multiple Sources
# ══════════════════════════════════════════════════════════════
def scrape_up_gov():
    log("━━━ Scraper 7: UP Government Portal ━━━")
    results = {"source": "up.gov.in", "scraped_at": str(datetime.now()), "schemes_found": []}

    urls = [
        "https://up.gov.in/en/page/important-government-schemes",
        "https://up.gov.in/en/page/schemes",
        "https://up.gov.in/en",
        "http://up.gov.in",
        "https://upgov.in",
    ]
    r, url = try_urls(urls)
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        results["url"] = url
        results["page_size"] = len(r.text)
        results["stats"] = extract_numbers_from_page(soup)

        # Find ALL links with scheme-related keywords
        keywords = ["yojana", "scheme", "mission", "abhiyan", "portal", "pension",
                     "awas", "shakti", "ayushman", "kanya", "odop", "kisan",
                     "shiksha", "vidyalaya", "scholarship", "rozgar", "sewayojan",
                     "योजना", "मिशन", "पेंशन"]
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if len(text) > 5 and any(kw in text.lower() or kw in a["href"].lower() for kw in keywords):
                href = a["href"]
                if not href.startswith("http"):
                    href = f"https://up.gov.in{href}"
                results["schemes_found"].append({"name": text[:100], "url": href})

    # Also try scraping the DBT portal
    log("  Also checking UP DBT portal...", "INFO")
    r2 = safe_get("https://fcs.up.gov.in")
    if r2:
        soup2 = BeautifulSoup(r2.text, "lxml")
        results["fcs_stats"] = extract_numbers_from_page(soup2)
        results["fcs_status"] = "SCRAPED"

    results["total_schemes"] = len(results["schemes_found"])
    results["status"] = "SCRAPED" if results["schemes_found"] or r else "FAILED"
    log(f"  UP.gov.in: {results['total_schemes']} scheme links found")
    save_json(results, "07_up_gov.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 8: Scholarship Portal (NEW)
# ══════════════════════════════════════════════════════════════
def scrape_scholarship():
    log("━━━ Scraper 8: UP Scholarship Portal ━━━")
    results = {"source": "scholarship.up.gov.in", "scraped_at": str(datetime.now())}

    urls = [
        "https://scholarship.up.gov.in",
        "http://scholarship.up.gov.in",
        "https://scholarship.up.nic.in",
    ]
    r, url = try_urls(urls)
    if not r:
        results["status"] = "FAILED"
        save_json(results, "08_scholarship.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(r.text)
    results["title"] = soup.title.string if soup.title else ""
    results["stats"] = extract_numbers_from_page(soup)
    results["tables"] = extract_tables(soup, 3)

    save_json(results, "08_scholarship.json")
    return results


# ══════════════════════════════════════════════════════════════
#  MASTER RUNNER
# ══════════════════════════════════════════════════════════════
def run_all_scrapers():
    log("=" * 60)
    log("  UP GOVERNMENT SCHEMES — LIVE DATA PIPELINE v2.0")
    log("  Stage 1: Scraping Government Portals")
    log("  SSL bypass enabled for expired govt certificates")
    log("=" * 60)

    all_results = {}
    scrapers = [
        ("sspy_pensions", scrape_sspy_pensions),
        ("mksy", scrape_mksy),
        ("pmay_rural", scrape_pmay_rural),
        ("pmay_urban", scrape_pmay_urban),
        ("odop", scrape_odop),
        ("ayushman", scrape_ayushman),
        ("up_gov", scrape_up_gov),
        ("scholarship", scrape_scholarship),
    ]

    for name, func in scrapers:
        try:
            all_results[name] = func()
        except Exception as e:
            log(f"  Scraper {name} crashed: {e}", "ERR")
            all_results[name] = {"status": "CRASHED", "error": str(e)}

    save_json(all_results, "00_master_scrape_results.json")

    with open(os.path.join(OUT_DIR, "scrape_log.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(LOG))

    # Summary
    log("\n" + "=" * 60)
    log("  SCRAPING SUMMARY v2.0")
    log("=" * 60)
    success = 0
    for name, res in all_results.items():
        status = res.get("status", "UNKNOWN") if isinstance(res, dict) else "UNKNOWN"
        # Also check nested main_page
        if status == "UNKNOWN" and isinstance(res, dict):
            mp = res.get("main_page", {})
            if isinstance(mp, dict) and mp.get("status") == "SCRAPED":
                status = "PARTIAL"
        icon = "✅" if status in ("SCRAPED","API_SCRAPED","PAGE_SCRAPED","PARTIAL") else "⚠️" if status == "NO_DATA" else "❌"
        if status in ("SCRAPED","API_SCRAPED","PAGE_SCRAPED","PARTIAL"):
            success += 1
        log(f"  {icon} {name:20s} → {status}")
    log(f"\n  📊 Success rate: {success}/{len(all_results)} portals scraped")
    log(f"  📁 All scraped data saved to: {os.path.abspath(OUT_DIR)}")

    return all_results


if __name__ == "__main__":
    run_all_scrapers()
