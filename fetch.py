"""
Summit County, Ohio — Motivated Seller Lead Scraper
Clerk portal  : https://clerk.summitoh.net/RecordsSearch/SelectDivision.asp
Parcel data   : https://propertyaccess.summitoh.net/search/advancedsearch.aspx?mode=advanced
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── playwright (async) ────────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False
    logging.warning("playwright not installed – clerk scraping will be skipped")

# ── dbfread (parcel DBF) ───────────────────────────────────────────────────────
try:
    from dbfread import DBF
    DBFREAD_OK = True
except ImportError:
    DBFREAD_OK = False
    logging.warning("dbfread not installed – parcel lookup will be skipped")

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE    = "https://clerk.summitoh.net/RecordsSearch"
CLERK_SEARCH  = f"{CLERK_BASE}/SearchEntry.asp"
CLERK_RESULTS = f"{CLERK_BASE}/SearchResults.asp"
PARCEL_BASE   = "https://propertyaccess.summitoh.net"
PARCEL_SEARCH = f"{PARCEL_BASE}/search/advancedsearch.aspx?mode=advanced"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

# Lead categories ─────────────────────────────────────────────────────────────
LEAD_TYPES: dict[str, dict[str, str]] = {
    "LP":      {"label": "Lis Pendens",              "flag": "Lis pendens"},
    "NOFC":    {"label": "Notice of Foreclosure",    "flag": "Pre-foreclosure"},
    "TAXDEED": {"label": "Tax Deed",                 "flag": "Tax lien"},
    "JUD":     {"label": "Judgment",                 "flag": "Judgment lien"},
    "CCJ":     {"label": "Certified Judgment",       "flag": "Judgment lien"},
    "DRJUD":   {"label": "Domestic Judgment",        "flag": "Judgment lien"},
    "LNCORPTX":{"label": "Corp Tax Lien",            "flag": "Tax lien"},
    "LNIRS":   {"label": "IRS Lien",                 "flag": "Tax lien"},
    "LNFED":   {"label": "Federal Lien",             "flag": "Tax lien"},
    "LN":      {"label": "Lien",                     "flag": "Judgment lien"},
    "LNMECH":  {"label": "Mechanic Lien",            "flag": "Mechanic lien"},
    "LNHOA":   {"label": "HOA Lien",                 "flag": "Mechanic lien"},
    "MEDLN":   {"label": "Medicaid Lien",            "flag": "Judgment lien"},
    "PRO":     {"label": "Probate Document",         "flag": "Probate / estate"},
    "NOC":     {"label": "Notice of Commencement",   "flag": "Mechanic lien"},
    "RELLP":   {"label": "Release Lis Pendens",      "flag": "Lis pendens"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def retry(fn, attempts: int = 3, delay: float = 2.0):
    """Synchronous retry wrapper."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None


async def aretry(coro_fn, attempts: int = 3, delay: float = 2.0):
    """Async retry wrapper."""
    for i in range(attempts):
        try:
            return await coro_fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                await asyncio.sleep(delay * (i + 1))
    return None


def parse_amount(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.]", "", text)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def name_variants(full: str) -> list[str]:
    """Generate lookup variants for an owner name."""
    full = full.strip()
    variants = [full.upper()]
    parts = full.split(",", 1)
    if len(parts) == 2:
        last, first = parts[0].strip(), parts[1].strip()
        variants += [
            f"{first} {last}".upper(),
            f"{last} {first}".upper(),
            f"{last}, {first}".upper(),
        ]
    else:
        words = full.split()
        if len(words) >= 2:
            first = words[0]
            last  = " ".join(words[1:])
            variants += [
                f"{last}, {first}".upper(),
                f"{last} {first}".upper(),
            ]
    return list(dict.fromkeys(variants))  # deduplicate, preserve order


def compute_score(record: dict) -> tuple[int, list[str]]:
    """Return (score, flags)."""
    flags: list[str] = []
    score = 30

    cat = record.get("cat", "")
    filed_str = record.get("filed", "")
    amount = record.get("amount")

    # Assign flags from category
    cat_flag = LEAD_TYPES.get(cat, {}).get("flag")
    if cat_flag and cat_flag not in flags:
        flags.append(cat_flag)

    # LLC / corp owner
    owner = (record.get("owner") or "").upper()
    if any(kw in owner for kw in ("LLC", "INC", "CORP", "LP", "LTD", "TRUST")):
        flags.append("LLC / corp owner")

    # Lis pendens + foreclosure combo
    has_lp = cat in ("LP", "RELLP")
    has_fc = cat == "NOFC"
    # We'll check cross-record combos later at the aggregation step; for single record:
    if has_lp:
        score += 10
    if has_fc:
        score += 10
    if has_lp and has_fc:
        score += 20

    # Amount thresholds
    if amount:
        if amount > 100_000:
            score += 15
            flags.append("Judgment lien") if "Judgment lien" not in flags else None
        elif amount > 50_000:
            score += 10

    # New this week
    try:
        filed_dt = datetime.strptime(filed_str, "%Y-%m-%d")
        cutoff   = datetime.utcnow() - timedelta(days=7)
        if filed_dt >= cutoff:
            score += 5
            flags.append("New this week")
    except Exception:
        pass

    # Has address
    if record.get("prop_address"):
        score += 5
        flags.append("Has address") if "Has address" not in flags else None

    # Per-flag bonus (10 per flag, first flag already counted)
    score += max(0, len([f for f in flags if f != "New this week" and f != "Has address"]) - 1) * 10

    return min(score, 100), flags

# ─────────────────────────────────────────────────────────────────────────────
#  Parcel data loader
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self._by_owner: dict[str, dict] = {}
        self._loaded   = False

    # ── public ───────────────────────────────────────────────────────────────

    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data…")
        data = retry(self._download_parcel_dbf)
        if data:
            self._parse_dbf(data)
            log.info("Parcel index built: %d records", len(self._by_owner))
        else:
            log.warning("Parcel data unavailable – address enrichment skipped")
        self._loaded = True

    def lookup(self, owner_name: str) -> Optional[dict]:
        for variant in name_variants(owner_name):
            hit = self._by_owner.get(variant)
            if hit:
                return hit
        return None

    # ── internal ─────────────────────────────────────────────────────────────

    def _download_parcel_dbf(self) -> Optional[bytes]:
        """
        Summit County makes parcel data available as a downloadable DBF/ZIP
        through the advanced search page using __doPostBack.
        We POST to trigger the export, then follow the download link.
        """
        sess = requests.Session()
        sess.headers.update({"User-Agent": "Mozilla/5.0 SummitLeadBot/1.0"})

        # Step 1: load page to get viewstate
        resp = sess.get(PARCEL_SEARCH, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        def _vs(name):
            el = soup.find("input", {"name": name})
            return el["value"] if el else ""

        viewstate          = _vs("__VIEWSTATE")
        viewstate_gen      = _vs("__VIEWSTATEGENERATOR")
        event_validation   = _vs("__EVENTVALIDATION")

        # Step 2: try to trigger bulk download via __doPostBack
        payload = {
            "__EVENTTARGET":        "ctl00$cphMain$btnExport",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          viewstate,
            "__VIEWSTATEGENERATOR": viewstate_gen,
            "__EVENTVALIDATION":    event_validation,
            "ctl00$cphMain$SearchType": "Advanced",
        }

        dl_resp = sess.post(PARCEL_SEARCH, data=payload, timeout=60, stream=True)
        ct = dl_resp.headers.get("Content-Type", "")

        if "zip" in ct or "octet" in ct or "dbf" in ct.lower():
            raw = dl_resp.content
            if raw[:2] == b"PK":           # ZIP
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            return zf.read(name)
            return raw                     # raw DBF

        # Fallback: look for a download link in the response HTML
        soup2 = BeautifulSoup(dl_resp.text, "lxml")
        for a in soup2.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".dbf") or href.lower().endswith(".zip"):
                file_url = urljoin(PARCEL_BASE, href)
                file_resp = sess.get(file_url, timeout=60)
                file_resp.raise_for_status()
                raw = file_resp.content
                if raw[:2] == b"PK":
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        for name in zf.namelist():
                            if name.lower().endswith(".dbf"):
                                return zf.read(name)
                return raw

        log.warning("Could not locate parcel DBF download")
        return None

    def _parse_dbf(self, raw_bytes: bytes):
        if not DBFREAD_OK:
            return
        try:
            with io.BytesIO(raw_bytes) as buf:
                table = DBF(buf, lowernames=True, ignore_missing_memofile=True)
                for row in table:
                    rd = dict(row)
                    owner = (
                        rd.get("owner") or rd.get("own1") or ""
                    ).strip().upper()
                    if not owner:
                        continue
                    parcel = {
                        "prop_address": (rd.get("site_addr") or rd.get("siteaddr") or "").strip(),
                        "prop_city":    (rd.get("site_city") or "").strip(),
                        "prop_state":   "OH",
                        "prop_zip":     str(rd.get("site_zip") or rd.get("zip") or "").strip(),
                        "mail_address": (rd.get("addr_1") or rd.get("mailadr1") or "").strip(),
                        "mail_city":    (rd.get("city") or rd.get("mailcity") or "").strip(),
                        "mail_state":   (rd.get("state") or "OH").strip(),
                        "mail_zip":     str(rd.get("zip") or rd.get("mailzip") or "").strip(),
                    }
                    for variant in name_variants(owner):
                        if variant not in self._by_owner:
                            self._by_owner[variant] = parcel
        except Exception as exc:
            log.error("DBF parse error: %s", exc)

# ─────────────────────────────────────────────────────────────────────────────
#  Clerk portal scraper  (Playwright / async)
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_clerk(doc_type_code: str, start_date: str, end_date: str) -> list[dict]:
    """
    Scrape clerk portal for one document type.
    Returns list of raw record dicts.
    """
    if not PLAYWRIGHT_OK:
        return []

    records: list[dict] = []
    label = LEAD_TYPES.get(doc_type_code, {}).get("label", doc_type_code)

    async def _do_scrape():
        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(headless=True)
            ctx  = await browser.new_context(
                user_agent="Mozilla/5.0 SummitLeadBot/1.0",
                java_script_enabled=True,
            )
            page: Page = await ctx.new_page()

            # ── Step 1: division selection page ──────────────────────────────
            await page.goto(
                f"{CLERK_BASE}/SelectDivision.asp",
                wait_until="networkidle",
                timeout=30_000,
            )

            # Click "County Recorder" or the first division option
            try:
                await page.click("text=County Recorder", timeout=5_000)
            except Exception:
                # Try selecting via link that contains "Recorder"
                links = await page.query_selector_all("a")
                for lnk in links:
                    txt = (await lnk.inner_text()).strip()
                    if "recorder" in txt.lower() or "record" in txt.lower():
                        await lnk.click()
                        break

            await page.wait_for_load_state("networkidle", timeout=15_000)

            # ── Step 2: search form ───────────────────────────────────────────
            await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=30_000)

            # Fill date range
            for sel in ["#StartDate", "input[name='StartDate']", "input[name='startdate']"]:
                try:
                    await page.fill(sel, start_date, timeout=3_000)
                    break
                except Exception:
                    pass

            for sel in ["#EndDate", "input[name='EndDate']", "input[name='enddate']"]:
                try:
                    await page.fill(sel, end_date, timeout=3_000)
                    break
                except Exception:
                    pass

            # Select document type
            for sel in ["#DocType", "select[name='DocType']", "select[name='doctype']"]:
                try:
                    await page.select_option(sel, value=doc_type_code, timeout=3_000)
                    break
                except Exception:
                    pass

            # Submit
            for sel in ["input[type=submit]", "button[type=submit]", "#SearchButton"]:
                try:
                    await page.click(sel, timeout=3_000)
                    break
                except Exception:
                    pass

            await page.wait_for_load_state("networkidle", timeout=30_000)

            # ── Step 3: paginate results ──────────────────────────────────────
            page_num = 0
            while True:
                page_num += 1
                html  = await page.content()
                recs  = _parse_results_html(html, doc_type_code, label, page.url)
                records.extend(recs)
                log.info("  %s page %d → %d records (total %d)",
                         doc_type_code, page_num, len(recs), len(records))

                # Try "Next" pagination
                next_btn = await page.query_selector("a:has-text('Next'), a:has-text('>')")
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15_000)
                if page_num > 50:          # safety limit
                    break

            await browser.close()

    await aretry(_do_scrape)
    return records


def _parse_results_html(html: str, cat: str, label: str, base_url: str) -> list[dict]:
    """Parse a results page HTML into record dicts."""
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    records: list[dict] = []

    for row in rows:
        cells = [td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) < 3:
            continue
        # Skip header rows
        if cells[0].lower() in ("doc #", "doc number", "document number", "document #"):
            continue

        try:
            doc_num    = cells[0]
            doc_type   = cells[1] if len(cells) > 1 else cat
            filed      = _normalize_date(cells[2]) if len(cells) > 2 else ""
            grantor    = cells[3] if len(cells) > 3 else ""
            grantee    = cells[4] if len(cells) > 4 else ""
            legal      = cells[5] if len(cells) > 5 else ""
            amount_str = cells[6] if len(cells) > 6 else ""
        except IndexError:
            continue

        if not doc_num or doc_num.lower() == "no records":
            continue

        # Build direct URL
        link_tag = row.find("a", href=True)
        if link_tag:
            href = link_tag["href"]
            clerk_url = urljoin(base_url, href)
        else:
            clerk_url = (
                f"{CLERK_BASE}/DocumentDetail.asp?docnumber={doc_num}"
            )

        records.append({
            "doc_num":   doc_num,
            "doc_type":  doc_type or cat,
            "filed":     filed,
            "cat":       cat,
            "cat_label": label,
            "owner":     grantor,
            "grantee":   grantee,
            "amount":    parse_amount(amount_str),
            "legal":     legal,
            "clerk_url": clerk_url,
            # address fields filled later
            "prop_address": "",
            "prop_city":    "",
            "prop_state":   "OH",
            "prop_zip":     "",
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "OH",
            "mail_zip":     "",
        })

    return records


def _normalize_date(raw: str) -> str:
    """Try to parse various date formats → YYYY-MM-DD."""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%Y/%m/%d",
                "%m/%d/%y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw

# ─────────────────────────────────────────────────────────────────────────────
#  GHL CSV export
# ─────────────────────────────────────────────────────────────────────────────

GHL_HEADERS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def _split_name(full: str) -> tuple[str, str]:
    parts = full.strip().split(",", 1)
    if len(parts) == 2:
        last, first = parts[0].strip(), parts[1].strip()
        return first, last
    words = full.strip().split()
    if len(words) >= 2:
        return words[0], " ".join(words[1:])
    return full.strip(), ""


def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_HEADERS)
        writer.writeheader()
        for r in records:
            first, last = _split_name(r.get("owner") or "")
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "OH"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "OH"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", "") or "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                 "Summit County Clerk",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("GHL CSV saved → %s", path)

# ─────────────────────────────────────────────────────────────────────────────
#  Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    now       = datetime.now(timezone.utc)
    start_dt  = now - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")
    start_iso = start_dt.strftime("%Y-%m-%d")
    end_iso   = now.strftime("%Y-%m-%d")

    log.info("Summit County Lead Scraper starting")
    log.info("Date range: %s → %s", start_str, end_str)

    # ── Parcel lookup ─────────────────────────────────────────────────────────
    parcel = ParcelLookup()
    parcel.load()

    # ── Scrape clerk for each doc type ────────────────────────────────────────
    all_records: list[dict] = []

    for code in LEAD_TYPES:
        log.info("Scraping %s (%s)…", code, LEAD_TYPES[code]["label"])
        recs = await scrape_clerk(code, start_str, end_str)
        all_records.extend(recs)
        log.info("  → %d records for %s", len(recs), code)

    log.info("Total raw records: %d", len(all_records))

    # ── Deduplicate by doc_num ────────────────────────────────────────────────
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_records:
        key = r.get("doc_num", "")
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
        elif not key:
            deduped.append(r)

    # ── Enrich with parcel data + score ──────────────────────────────────────
    with_address = 0
    for r in deduped:
        owner = r.get("owner", "")
        if owner and parcel._loaded:
            hit = parcel.lookup(owner)
            if hit:
                r.update(hit)

        if r.get("prop_address"):
            with_address += 1

        score, flags = compute_score(r)
        r["score"] = score
        r["flags"] = flags

    # Sort by score descending
    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    # ── Build output payload ──────────────────────────────────────────────────
    payload = {
        "fetched_at":    now.isoformat(),
        "source":        "Summit County Clerk – summitoh.net",
        "date_range":    {"start": start_iso, "end": end_iso},
        "total":         len(deduped),
        "with_address":  with_address,
        "records":       deduped,
    }

    # ── Save JSON ─────────────────────────────────────────────────────────────
    for out_path in OUTPUT_PATHS:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s", out_path)

    # ── GHL CSV ───────────────────────────────────────────────────────────────
    export_ghl_csv(deduped, Path("data/ghl_export.csv"))

    log.info("Done. %d records (%d with address)", len(deduped), with_address)


if __name__ == "__main__":
    asyncio.run(main())
