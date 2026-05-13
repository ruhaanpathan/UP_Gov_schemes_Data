"""
═══════════════════════════════════════════════════════════════
  UP GOVERNMENT SCHEMES — LIVE DATA SCRAPERS v3.0
  
  Enhanced with:
    - Proper structured data parsing from HTML
    - Selenium for JavaScript-heavy portals
    - Clean numeric output for each scraper
    - Fallback: requests → selenium → skip
═══════════════════════════════════════════════════════════════
"""
import requests, json, os, time, re, warnings, sys
from datetime import datetime
from bs4 import BeautifulSoup

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
    icon = {"INFO":"ℹ️","OK":"✅","WARN":"⚠️","ERR":"❌","FETCH":"🌐"}.get(level,"")
    line = f"[{ts}] {icon} {msg}"
    print(line)
    LOG.append(line)

def safe_get(url, **kwargs):
    """Safe HTTP GET with retry + SSL bypass."""
    log(f"Fetching: {url}", "FETCH")
    for attempt in range(3):
        try:
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

def parse_indian_number(s):
    """Parse Indian formatted numbers like '67,50,000' or '2024.37' into float."""
    if not s:
        return 0.0
    s = str(s).strip().replace(" ", "")
    s = re.sub(r"[^\d.,]", "", s)
    if not s:
        return 0.0
    s = s.replace(",", "")
    try:
        return float(s)
    except:
        return 0.0

# ── Selenium helper (optional) ──
_driver = None

def get_selenium_driver():
    global _driver
    if _driver:
        return _driver
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except:
            service = None
        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(f"user-agent={HEADERS['User-Agent']}")
        if service:
            _driver = webdriver.Chrome(service=service, options=opts)
        else:
            _driver = webdriver.Chrome(options=opts)
        log("  Selenium Chrome driver ready", "OK")
        return _driver
    except Exception as e:
        log(f"  Selenium not available: {e}", "WARN")
        return None

def selenium_get(url, wait_secs=5):
    """Fetch page with Selenium (JS rendering). Returns page source or None."""
    driver = get_selenium_driver()
    if not driver:
        return None
    try:
        log(f"  Selenium fetching: {url}", "FETCH")
        driver.get(url)
        time.sleep(wait_secs)
        src = driver.page_source
        log(f"  ✓ Selenium got {len(src)} bytes", "OK")
        return src
    except Exception as e:
        log(f"  Selenium failed: {e}", "ERR")
        return None


# ══════════════════════════════════════════════════════════════
#  SCRAPER 1: SSPY Pension Portal — PARSE REAL TABLE DATA
# ══════════════════════════════════════════════════════════════
def scrape_sspy_pensions():
    log("━━━ Scraper 1: SSPY Pension Portal ━━━")
    results = {"source": "sspy-up.gov.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    r, url = try_urls(["https://sspy-up.gov.in", "http://sspy-up.gov.in"])
    if not r:
        results["status"] = "FAILED"
        save_json(results, "01_sspy_pensions.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url

    # Parse the quarterly pension table
    pensions = []
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if cells and any(c.strip() for c in cells):
                rows.append(cells)
        
        # Look for pension data rows (they start with a number like "1", "2", "3")
        for row in rows:
            if len(row) >= 5 and row[0].strip().isdigit():
                name_hi = row[1] if len(row) > 1 else ""
                dept = row[2] if len(row) > 2 else ""
                
                # Parse quarterly data: beneficiaries and amounts
                q_data = []
                idx = 3
                while idx + 1 < len(row):
                    ben = parse_indian_number(row[idx])
                    amt = parse_indian_number(row[idx + 1]) if idx + 1 < len(row) else 0
                    if ben > 0 or amt > 0:
                        q_data.append({"beneficiaries": ben, "amount_crore": amt})
                    idx += 2
                
                # Total is last column
                total_amt = parse_indian_number(row[-1]) if len(row) > 4 else 0
                
                # Map Hindi names to English
                name_map = {
                    "वृद्धावस्था": "Old Age Pension",
                    "निराश्रित महिला": "Widow Pension", 
                    "दिव्यांग": "Divyang Pension",
                    "कुष्ठावस्था": "Leprosy Pension",
                }
                english_name = name_hi
                for hi, en in name_map.items():
                    if hi in name_hi:
                        english_name = en
                        break
                
                # Get max beneficiaries across quarters
                max_ben = max([q["beneficiaries"] for q in q_data], default=0)
                total_disbursed = total_amt
                
                pension = {
                    "name": english_name,
                    "name_hindi": name_hi,
                    "department": dept,
                    "beneficiaries": max_ben,
                    "beneficiaries_lakh": round(max_ben / 100000, 2),
                    "total_disbursed_crore": total_disbursed,
                    "quarterly_data": q_data,
                    "quarters_with_data": sum(1 for q in q_data if q["beneficiaries"] > 0),
                }
                pensions.append(pension)
            
            # Parse total row
            if len(row) >= 3 and "total" in row[0].lower():
                total_ben = parse_indian_number(row[1])
                total_amt = parse_indian_number(row[-1])
                results["parsed_data"]["grand_total"] = {
                    "total_beneficiaries": total_ben,
                    "total_beneficiaries_lakh": round(total_ben / 100000, 2),
                    "total_disbursed_crore": total_amt,
                }

    results["parsed_data"]["pensions"] = pensions
    if pensions:
        log(f"  📊 Parsed {len(pensions)} pension schemes with real numbers", "OK")
        for p in pensions:
            log(f"     • {p['name']}: {p['beneficiaries']:,.0f} beneficiaries, ₹{p['total_disbursed_crore']:,.2f} Cr", "INFO")

    save_json(results, "01_sspy_pensions.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 2: Kanya Sumangala (MKSY)
# ══════════════════════════════════════════════════════════════
def scrape_mksy():
    log("━━━ Scraper 2: Kanya Sumangala Yojana ━━━")
    results = {"source": "mksy.up.gov.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    # Try requests first
    r, url = try_urls(["https://mksy.up.gov.in", "http://mksy.up.gov.in"])
    page_html = r.text if r else None
    
    # Try Selenium for JS content
    if not page_html or len(page_html) < 1000:
        sel_html = selenium_get("https://mksy.up.gov.in", wait_secs=8)
        if sel_html and len(sel_html) > len(page_html or ""):
            page_html = sel_html
            url = "https://mksy.up.gov.in (selenium)"
    
    if not page_html:
        results["status"] = "FAILED"
        save_json(results, "02_mksy.json")
        return results

    soup = BeautifulSoup(page_html, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(page_html)
    results["title"] = soup.title.string if soup.title else ""

    # Extract any counter/stat elements
    for el in soup.find_all(["span", "div", "h2", "h3", "strong", "p"]):
        text = el.get_text(strip=True)
        nums = re.findall(r"[\d,]+\.?\d*", text)
        if nums and any(kw in text.lower() for kw in ["beneficiar", "application", "लाभार्थी", "आवेदन", "total", "girls", "कन्या"]):
            results["parsed_data"].setdefault("counters", []).append({"text": text, "numbers": nums})
    
    # Extract tables
    for table in soup.find_all("table"):
        rows = [[c.get_text(strip=True) for c in tr.find_all(["td","th"])] for tr in table.find_all("tr")]
        if rows and len(rows) > 1:
            results["parsed_data"].setdefault("tables", []).append(rows)

    save_json(results, "02_mksy.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 3: PMAY Rural Housing
# ══════════════════════════════════════════════════════════════
def scrape_pmay_rural():
    log("━━━ Scraper 3: PMAY Rural Housing ━━━")
    results = {"source": "rhreporting.nic.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    # Try Selenium first (JS-heavy portal)
    page_html = selenium_get("https://rhreporting.nic.in/netiay/newreport.aspx", wait_secs=10)
    url = "rhreporting.nic.in (selenium)"
    
    if not page_html:
        r, url = try_urls([
            "https://rhreporting.nic.in/netiay/newreport.aspx",
            "https://pmayg.nic.in/netiay/home.aspx",
        ])
        page_html = r.text if r else None
    
    if not page_html:
        results["status"] = "FAILED"
        save_json(results, "03_pmay_rural.json")
        return results

    soup = BeautifulSoup(page_html, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(page_html)

    # Extract numbers from page
    for el in soup.find_all(["span", "div", "td", "strong", "h2", "h3"]):
        text = el.get_text(strip=True)
        if re.search(r"[\d,]+\.?\d*\s*(lakh|crore|house|target|complet|sanction|grounded)", text, re.I):
            results["parsed_data"].setdefault("stats", []).append(text[:200])
        classes = " ".join(el.get("class", []))
        if re.search(r"count|number|stat|value|total", classes, re.I) and re.search(r"\d", text):
            results["parsed_data"].setdefault("counters", []).append({"class": classes, "text": text[:100]})

    # Extract tables
    for table in soup.find_all("table"):
        rows = [[c.get_text(strip=True) for c in tr.find_all(["td","th"])] for tr in table.find_all("tr")]
        rows = [r for r in rows if any(c.strip() for c in r)]
        if rows and len(rows) > 1:
            results["parsed_data"].setdefault("tables", []).append(rows[:20])

    save_json(results, "03_pmay_rural.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 4: PMAY Urban Housing
# ══════════════════════════════════════════════════════════════
def scrape_pmay_urban():
    log("━━━ Scraper 4: PMAY Urban Housing ━━━")
    results = {"source": "pmaymis.gov.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    page_html = selenium_get("https://pmaymis.gov.in", wait_secs=10)
    url = "pmaymis.gov.in (selenium)"
    
    if not page_html:
        r, url = try_urls(["https://pmaymis.gov.in", "https://pmay-urban.gov.in"])
        page_html = r.text if r else None

    if not page_html:
        results["status"] = "FAILED"
        save_json(results, "04_pmay_urban.json")
        return results

    soup = BeautifulSoup(page_html, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(page_html)

    for el in soup.find_all(["span", "div", "td", "strong"]):
        text = el.get_text(strip=True)
        if re.search(r"[\d,]+\.?\d*\s*(lakh|crore|house|sanctioned|complet|demand)", text, re.I):
            results["parsed_data"].setdefault("stats", []).append(text[:200])
        classes = " ".join(el.get("class", []))
        if re.search(r"count|number|stat|value", classes, re.I) and re.search(r"\d", text):
            results["parsed_data"].setdefault("counters", []).append({"class": classes, "text": text[:100]})

    save_json(results, "04_pmay_urban.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 5: ODOP
# ══════════════════════════════════════════════════════════════
def scrape_odop():
    log("━━━ Scraper 5: ODOP Portal ━━━")
    results = {"source": "odopup.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    page_html = selenium_get("http://odopup.in", wait_secs=8)
    url = "odopup.in (selenium)"
    
    if not page_html or len(page_html) < 1000:
        r, url = try_urls(["http://odopup.in", "https://odopup.in"])
        page_html = r.text if r else None

    if not page_html:
        results["status"] = "FAILED"
        save_json(results, "05_odop.json")
        return results

    soup = BeautifulSoup(page_html, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(page_html)

    # Extract product/district info
    products = []
    for el in soup.find_all(["a", "div", "li", "td"]):
        text = el.get_text(strip=True)
        if re.search(r"(brass|silk|leather|carpet|chikan|zari|pottery|glass|wood|lock|perfume|textile|banana|saree)", text, re.I):
            products.append(text[:100])
    results["parsed_data"]["products"] = list(set(products))[:50]
    results["parsed_data"]["districts_found"] = len(results["parsed_data"]["products"])

    save_json(results, "05_odop.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 6: Ayushman Bharat UP — PARSE DASHBOARD COUNTERS
# ══════════════════════════════════════════════════════════════
def scrape_ayushman():
    log("━━━ Scraper 6: Ayushman Bharat UP ━━━")
    results = {"source": "ayushmanup.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    r, url = try_urls(["https://ayushmanup.in", "http://ayushmanup.in", "https://pmjay.gov.in"])
    if not r:
        results["status"] = "FAILED"
        save_json(results, "06_ayushman.json")
        return results

    soup = BeautifulSoup(r.text, "lxml")
    results["status"] = "SCRAPED"
    results["url"] = url
    results["page_size"] = len(r.text)

    # Parse the dashboard counter text
    full_text = soup.get_text(" ", strip=True)
    
    # Extract known dashboard metrics
    # Note: page text often has NO spaces between numbers and labels
    # e.g. "95,682,304NUMBER OF BENEFICARIES" (also misspelled)
    patterns = {
        "total_beneficiaries": r"([\d,]+)\s*NUMBER\s*OF\s*BENEFI[CA]+R",
        "golden_cards_issued": r"([\d,]+)\s*GOLDEN\s*CARD\s*ISSUED",
        "empanelled_hospitals": r"([\d,]+)\s*EMPANELLED\s*HOSPITAL",
        "preauth_requests": r"([\d,]+)\s*TOTAL\s*PRE-?\s*AUTHORIZATION",
        "claims_submitted": r"([\d,]+)\s*TOTAL\s*CLAIMS?\s*SUBMITTED",
        "claims_settled_pct": r"([\d.]+)\s*CLAIMS?\s*SETTLED\s*\(?%?\)?\s*$",
    }
    
    # Also try extracting from raw concatenated text (no spaces)
    raw_text = soup.get_text("", strip=True)
    
    for key, pattern in patterns.items():
        match = re.search(pattern, full_text, re.I)
        if not match:
            match = re.search(pattern, raw_text, re.I)
        if match:
            val = parse_indian_number(match.group(1))
            results["parsed_data"][key] = val
            log(f"     • {key}: {val:,.0f}" if key != "claims_settled_pct" else f"     • {key}: {val}%", "INFO")
    
    # Fallback: estimate beneficiaries from golden cards if not found
    pd_data = results["parsed_data"]
    if not pd_data.get("total_beneficiaries") and pd_data.get("golden_cards_issued"):
        # Golden cards ≈ 60% of total beneficiaries typically
        pd_data["total_beneficiaries"] = pd_data["golden_cards_issued"]
        pd_data["beneficiaries_note"] = "Estimated from golden cards issued"
        log(f"     • total_beneficiaries (from cards): {pd_data['total_beneficiaries']:,.0f}", "INFO")
    
    # Compute derived metrics
    if pd_data.get("total_beneficiaries"):
        pd_data["beneficiaries_lakh"] = round(pd_data["total_beneficiaries"] / 100000, 2)
        pd_data["beneficiaries_crore"] = round(pd_data["total_beneficiaries"] / 10000000, 2)
    if pd_data.get("golden_cards_issued") and pd_data.get("total_beneficiaries"):
        pd_data["card_coverage_pct"] = round(pd_data["golden_cards_issued"] / pd_data["total_beneficiaries"] * 100, 1)

    save_json(results, "06_ayushman.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 7: UP Gov Portal + FCS
# ══════════════════════════════════════════════════════════════
def scrape_up_gov():
    log("━━━ Scraper 7: UP Government Portal ━━━")
    results = {"source": "up.gov.in", "scraped_at": str(datetime.now()), "parsed_data": {}, "schemes_found": []}

    r, url = try_urls([
        "https://up.gov.in/en/page/important-government-schemes",
        "https://up.gov.in/en",
    ])
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        results["url"] = url
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if len(text) > 5 and any(kw in text.lower() or kw in a["href"].lower() 
                for kw in ["yojana","scheme","mission","pension","awas","shakti","ayushman","kanya","odop","kisan","scholarship","rozgar"]):
                href = a["href"] if a["href"].startswith("http") else f"https://up.gov.in{a['href']}"
                results["schemes_found"].append({"name": text[:100], "url": href})

    # FCS portal for food/agriculture data
    log("  Also checking UP FCS portal...", "INFO")
    r2 = safe_get("https://fcs.up.gov.in")
    if r2:
        soup2 = BeautifulSoup(r2.text, "lxml")
        full_text = soup2.get_text(" ", strip=True)
        
        # Parse wheat procurement
        wheat_match = re.search(r"([\d,]+)\s*/\s*([\d.]+)\s*लाख\s*मी.*?टन.*?\((\d{4}-\d{2,4})\)", full_text)
        if wheat_match:
            results["parsed_data"]["wheat_procurement"] = {
                "farmers": parse_indian_number(wheat_match.group(1)),
                "quantity_lakh_mt": parse_indian_number(wheat_match.group(2)),
                "year": wheat_match.group(3),
            }
        
        centres_match = re.search(r"([\d,]+)\s*/\s*([\d.]+)\s*लाख.*?(?:केन्द्र|centre)", full_text, re.I)
        if centres_match:
            results["parsed_data"]["procurement_centres"] = parse_indian_number(centres_match.group(1))

    results["total_schemes"] = len(results["schemes_found"])
    results["status"] = "SCRAPED" if results["schemes_found"] or r else "FAILED"
    save_json(results, "07_up_gov.json")
    return results


# ══════════════════════════════════════════════════════════════
#  SCRAPER 8: Scholarship Portal
# ══════════════════════════════════════════════════════════════
def scrape_scholarship():
    log("━━━ Scraper 8: UP Scholarship Portal ━━━")
    results = {"source": "scholarship.up.gov.in", "scraped_at": str(datetime.now()), "parsed_data": {}}

    r, url = try_urls(["https://scholarship.up.gov.in", "http://scholarship.up.gov.in", "https://scholarship.up.nic.in"])
    if not r:
        # Try Selenium
        page_html = selenium_get("https://scholarship.up.gov.in", wait_secs=8)
        if page_html:
            soup = BeautifulSoup(page_html, "lxml")
            results["status"] = "SCRAPED"
            results["url"] = "scholarship.up.gov.in (selenium)"
            results["page_size"] = len(page_html)
        else:
            results["status"] = "FAILED"
            save_json(results, "08_scholarship.json")
            return results
    else:
        soup = BeautifulSoup(r.text, "lxml")
        results["status"] = "SCRAPED"
        results["url"] = url
        results["page_size"] = len(r.text)

    # Extract stats
    for el in soup.find_all(["span", "div", "td", "strong"]):
        text = el.get_text(strip=True)
        if re.search(r"[\d,]+.*?(student|छात्र|application|scholarship|disburs)", text, re.I):
            results["parsed_data"].setdefault("stats", []).append(text[:200])

    save_json(results, "08_scholarship.json")
    return results


# ══════════════════════════════════════════════════════════════
#  MASTER RUNNER
# ══════════════════════════════════════════════════════════════
def run_all_scrapers():
    log("=" * 60)
    log("  UP GOVERNMENT SCHEMES — LIVE DATA PIPELINE v3.0")
    log("  Stage 1: Scraping Government Portals")
    log("  Enhanced: Structured parsing + Selenium fallback")
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

    # Cleanup Selenium
    global _driver
    if _driver:
        try:
            _driver.quit()
        except:
            pass
        _driver = None

    save_json(all_results, "00_master_scrape_results.json")

    with open(os.path.join(OUT_DIR, "scrape_log.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(LOG))

    # Summary
    log("\n" + "=" * 60)
    log("  SCRAPING SUMMARY v3.0")
    log("=" * 60)
    success = 0
    for name, res in all_results.items():
        status = res.get("status", "UNKNOWN") if isinstance(res, dict) else "UNKNOWN"
        parsed = res.get("parsed_data", {}) if isinstance(res, dict) else {}
        has_data = bool(parsed and any(v for v in parsed.values() if v))
        icon = "✅" if has_data else "⚠️" if status == "SCRAPED" else "❌"
        data_note = f" ({len(parsed)} parsed fields)" if has_data else " (no usable data)"
        if status in ("SCRAPED","API_SCRAPED","PARTIAL"):
            success += 1
        log(f"  {icon} {name:20s} → {status}{data_note}")
    log(f"\n  📊 {success}/{len(all_results)} portals reached")
    log(f"  📁 Scraped data: {os.path.abspath(OUT_DIR)}")

    return all_results


if __name__ == "__main__":
    run_all_scrapers()
