"""
Summit County, Ohio — Motivated Seller Lead Scraper
Document search: https://eagleweb.summitoh.net/recorder/web/
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

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False
    logging.warning("playwright not installed")

try:
    from dbfread import DBF
    DBFREAD_OK = True
except ImportError:
    DBFREAD_OK = False
    logging.warning("dbfread not installed")

# ─────────────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))
EAGLEWEB_BASE  = "https://eagleweb.summitoh.net/recorder"
EAGLEWEB_GUEST = f"{EAGLEWEB_BASE}/web/loginPOST.jsp?guest=true"
EAGLEWEB_SEARCH= f"{EAGLEWEB_BASE}/eagleweb/docSearch.jsp"
PARCEL_BASE    = "https://propertyaccess.summitoh.net"
PARCEL_SEARCH  = f"{PARCEL_BASE}/search/advancedsearch.aspx?mode=advanced"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

LEAD_TYPES: dict[str, dict] = {
    "LP":       {"label": "Lis Pendens",            "flag": "Lis pendens"},
    "NOFC":     {"label": "Notice of Foreclosure",  "flag": "Pre-foreclosure"},
    "TAXDEED":  {"label": "Tax Deed",               "flag": "Tax lien"},
    "JUD":      {"label": "Judgment",               "flag": "Judgment lien"},
    "CCJ":      {"label": "Certified Judgment",     "flag": "Judgment lien"},
    "DRJUD":    {"label": "Domestic Judgment",      "flag": "Judgment lien"},
    "LNCORPTX": {"label": "Corp Tax Lien",          "flag": "Tax lien"},
    "LNIRS":    {"label": "IRS Lien",               "flag": "Tax lien"},
    "LNFED":    {"label": "Federal Lien",           "flag": "Tax lien"},
    "LN":       {"label": "Lien",                   "flag": "Judgment lien"},
    "LNMECH":   {"label": "Mechanic Lien",          "flag": "Mechanic lien"},
    "LNHOA":    {"label": "HOA Lien",               "flag": "Mechanic lien"},
    "MEDLN":    {"label": "Medicaid Lien",          "flag": "Judgment lien"},
    "PRO":      {"label": "Probate Document",       "flag": "Probate / estate"},
    "NOC":      {"label": "Notice of Commencement", "flag": "Mechanic lien"},
    "RELLP":    {"label": "Release Lis Pendens",    "flag": "Lis pendens"},
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

def retry(fn, attempts=3, delay=2.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None


async def aretry(coro_fn, attempts=3, delay=2.0):
    for i in range(attempts):
        try:
            result = await coro_fn()
            return result
        except Exception as exc:
            log.warning("Async attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                await asyncio.sleep(delay * (i + 1))
    return None


def parse_amount(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.]", "", str(text))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def normalize_date(raw: str) -> str:
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%Y/%m/%d",
                "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def name_variants(full: str) -> list[str]:
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
            first, last = words[0], " ".join(words[1:])
            variants += [f"{last}, {first}".upper(), f"{last} {first}".upper()]
    return list(dict.fromkeys(variants))


def split_name(full: str) -> tuple[str, str]:
    parts = full.strip().split(",", 1)
    if len(parts) == 2:
        return parts[1].strip(), parts[0].strip()
    words = full.strip().split()
    if len(words) >= 2:
        return words[0], " ".join(words[1:])
    return full.strip(), ""


# ─────────────────────────────────────────────────────────────────────────────
#  Scorer
# ─────────────────────────────────────────────────────────────────────────────

def compute_score(record: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30
    cat    = record.get("cat", "")
    amount = record.get("amount")

    cat_flag = LEAD_TYPES.get(cat, {}).get("flag")
    if cat_flag:
        flags.append(cat_flag)

    owner = (record.get("owner") or "").upper()
    if any(kw in owner for kw in ("LLC", "INC", "CORP", " LP", "LTD", "TRUST", "ESTATE")):
        flags.append("LLC / corp owner")

    if cat in ("LP", "RELLP"):
        score += 10
    if cat == "NOFC":
        score += 10

    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    try:
        filed_dt = datetime.strptime(record.get("filed", ""), "%Y-%m-%d")
        if filed_dt >= datetime.utcnow() - timedelta(days=7):
            score += 5
            flags.append("New this week")
    except Exception:
        pass

    if record.get("prop_address"):
        score += 5

    score += max(0, len([f for f in flags if f != "New this week"]) - 1) * 10
    return min(score, 100), list(dict.fromkeys(flags))


# ─────────────────────────────────────────────────────────────────────────────
#  EagleWeb scraper
# ─────────────────────────────────────────────────────────────────────────────

async def eagleweb_login(page) -> bool:
    """Navigate to EagleWeb and accept the guest disclaimer."""
    await page.goto(EAGLEWEB_GUEST, wait_until="networkidle", timeout=30_000)

    # Accept disclaimer button (various labels the site might use)
    for sel in [
        "input[value='Accept']",
        "input[value='I Accept']",
        "button:has-text('Accept')",
        "a:has-text('Accept')",
        "input[type=submit]",
        "button[type=submit]",
    ]:
        try:
            await page.click(sel, timeout=3_000)
            await page.wait_for_load_state("networkidle", timeout=10_000)
            log.info("EagleWeb: disclaimer accepted")
            return True
        except Exception:
            pass

    # Maybe no disclaimer — check if we're already on a search page
    current = page.url
    if "docSearch" in current or "eagleweb" in current:
        return True

    log.warning("EagleWeb: could not accept disclaimer, proceeding anyway")
    return True


async def scrape_eagleweb_type(page, code: str,
                                start_str: str, end_str: str) -> list[dict]:
    records: list[dict] = []
    label = LEAD_TYPES[code]["label"]

    await page.goto(EAGLEWEB_SEARCH, wait_until="networkidle", timeout=30_000)
    await asyncio.sleep(1)

    # ── Fill date range ───────────────────────────────────────────────────────
    date_fields = [
        ("dateRangeStart", start_str),
        ("dateRangeEnd",   end_str),
        # alternate field names
        ("startRecordingDate", start_str),
        ("endRecordingDate",   end_str),
    ]
    for name, val in date_fields:
        for sel in [f"input[name='{name}']", f"#{name}"]:
            try:
                await page.fill(sel, val, timeout=1_500)
                break
            except Exception:
                pass

    # ── Select document type ──────────────────────────────────────────────────
    # EagleWeb uses a <select> for doc type — find it and pick the right option
    filled = False
    selects = await page.query_selector_all("select")
    for sel_el in selects:
        options = await sel_el.query_selector_all("option")
        for opt in options:
            opt_val = (await opt.get_attribute("value") or "").strip().upper()
            opt_txt = (await opt.inner_text()).strip().upper()
            if opt_val == code or opt_txt.startswith(code):
                sel_name = await sel_el.get_attribute("name") or ""
                sel_id   = await sel_el.get_attribute("id") or ""
                target   = f"#{sel_id}" if sel_id else f"select[name='{sel_name}']"
                try:
                    await page.select_option(target, value=await opt.get_attribute("value"),
                                             timeout=2_000)
                    filled = True
                    break
                except Exception:
                    pass
        if filled:
            break

    if not filled:
        # Try typing into a doc type text input
        for sel in ["input[name='docType']", "#docType", "input[name='documentType']"]:
            try:
                await page.fill(sel, code, timeout=1_500)
                filled = True
                break
            except Exception:
                pass

    if not filled:
        log.warning("  %s: could not set document type filter", code)

    # ── Submit ────────────────────────────────────────────────────────────────
    for sel in ["input[value='Search']", "input[type=submit]",
                "button:has-text('Search')", "button[type=submit]"]:
        try:
            await page.click(sel, timeout=3_000)
            break
        except Exception:
            pass

    await page.wait_for_load_state("networkidle", timeout=30_000)

    # ── Paginate ──────────────────────────────────────────────────────────────
    for page_num in range(1, 51):
        html = await page.content()
        recs = _parse_eagleweb_results(html, code, label, page.url)
        records.extend(recs)
        log.info("  %s p%d → %d rows (total %d)", code, page_num, len(recs), len(records))

        if page_num == 1 and not recs:
            _log_table_debug(html, code)

        next_el = await page.query_selector(
            "a:has-text('Next'), a:has-text('>'), "
            "input[value='Next'], a[title='Next Page'], "
            "a[title='next']"
        )
        if not next_el:
            break
        try:
            await next_el.click()
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            break

    return records


def _parse_eagleweb_results(html: str, cat: str, label: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records: list[dict] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        header_cells = [td.get_text(" ", strip=True).lower()
                        for td in rows[0].find_all(["th", "td"])]
        joined = " ".join(header_cells)

        # Only process tables that look like result tables
        if not any(kw in joined for kw in
                   ("doc", "instrument", "record", "grantor", "date", "type")):
            continue

        # Map column indices
        col: dict[str, int] = {}
        for i, h in enumerate(header_cells):
            if any(k in h for k in ("doc #", "doc number", "instrument", "doc num")):
                col.setdefault("doc_num", i)
            elif "type" in h and "doc" in h:
                col.setdefault("doc_type", i)
            elif any(k in h for k in ("record date", "filed", "recording date", "rec date")):
                col.setdefault("filed", i)
            elif "grantor" in h:
                col.setdefault("grantor", i)
            elif "grantee" in h:
                col.setdefault("grantee", i)
            elif any(k in h for k in ("legal", "description", "desc")):
                col.setdefault("legal", i)
            elif any(k in h for k in ("amount", "consideration", "value")):
                col.setdefault("amount", i)

        # Fallback column positions if headers weren't recognized
        if "doc_num" not in col and len(header_cells) >= 1:
            col["doc_num"] = 0
        if "filed" not in col and len(header_cells) >= 3:
            col["filed"] = 2
        if "grantor" not in col and len(header_cells) >= 4:
            col["grantor"] = 3
        if "grantee" not in col and len(header_cells) >= 5:
            col["grantee"] = 4

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            texts = [c.get_text(" ", strip=True) for c in cells]
            if len(texts) < 2:
                continue

            def gc(key, default=""):
                idx = col.get(key)
                return texts[idx].strip() if idx is not None and idx < len(texts) else default

            doc_num = gc("doc_num") or texts[0]
            if not doc_num or doc_num.lower() in (
                "no records found", "no results", "document #",
                "instrument #", "doc #", ""
            ):
                continue

            doc_type = gc("doc_type") or cat
            filed    = normalize_date(gc("filed"))
            grantor  = gc("grantor")
            grantee  = gc("grantee")
            legal    = gc("legal")
            amount   = parse_amount(gc("amount"))

            link = row.find("a", href=True)
            clerk_url = (urljoin(base_url, link["href"]) if link
                         else f"{EAGLEWEB_BASE}/eagleweb/docView.jsp?docNum={doc_num}")

            records.append({
                "doc_num":      doc_num,
                "doc_type":     doc_type,
                "filed":        filed,
                "cat":          cat,
                "cat_label":    label,
                "owner":        grantor,
                "grantee":      grantee,
                "amount":       amount,
                "legal":        legal,
                "clerk_url":    clerk_url,
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


def _log_table_debug(html: str, code: str):
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    log.info("  DEBUG %s: %d tables on page", code, len(tables))
    for i, t in enumerate(tables[:4]):
        rows = t.find_all("tr")
        if rows:
            hdr = [td.get_text(" ", strip=True) for td in rows[0].find_all(["td","th"])]
            log.info("    table[%d] rows=%d header=%s", i, len(rows), hdr[:7])


# ─────────────────────────────────────────────────────────────────────────────
#  Parcel lookup
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self._by_owner: dict[str, dict] = {}
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data…")
        data = retry(self._download_parcel_dbf)
        if data:
            self._parse_dbf(data)
            log.info("Parcel index: %d owner keys", len(self._by_owner))
        else:
            log.warning("Parcel data unavailable — address enrichment skipped")
        self._loaded = True

    def lookup(self, owner_name: str) -> Optional[dict]:
        for v in name_variants(owner_name):
            hit = self._by_owner.get(v)
            if hit:
                return hit
        return None

    def _download_parcel_dbf(self) -> Optional[bytes]:
        sess = requests.Session()
        sess.headers["User-Agent"] = "Mozilla/5.0 SummitLeadBot/1.0"
        resp = sess.get(PARCEL_SEARCH, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        def _vs(n):
            el = soup.find("input", {"name": n})
            return el["value"] if el else ""

        payload = {
            "__EVENTTARGET":        "ctl00$cphMain$btnExport",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          _vs("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": _vs("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION":    _vs("__EVENTVALIDATION"),
        }
        dl = sess.post(PARCEL_SEARCH, data=payload, timeout=60)
        ct = dl.headers.get("Content-Type", "")
        raw = dl.content

        if raw[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".dbf"):
                        return zf.read(name)
            return None

        if "zip" in ct or "octet" in ct or raw[0:1] in (b"\x03", b"\x83"):
            return raw

        # Hunt for download link
        soup2 = BeautifulSoup(dl.text, "lxml")
        for a in soup2.find_all("a", href=True):
            if any(ext in a["href"].lower() for ext in (".dbf", ".zip")):
                fr = sess.get(urljoin(PARCEL_BASE, a["href"]), timeout=60)
                fr.raise_for_status()
                raw2 = fr.content
                if raw2[:2] == b"PK":
                    with zipfile.ZipFile(io.BytesIO(raw2)) as zf:
                        for name in zf.namelist():
                            if name.lower().endswith(".dbf"):
                                return zf.read(name)
                return raw2
        return None

    def _parse_dbf(self, raw_bytes: bytes):
        if not DBFREAD_OK:
            return
        try:
            table = DBF(io.BytesIO(raw_bytes), lowernames=True,
                        ignore_missing_memofile=True)
            for row in table:
                rd = dict(row)
                owner = (rd.get("owner") or rd.get("own1") or "").strip().upper()
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
                for v in name_variants(owner):
                    if v not in self._by_owner:
                        self._by_owner[v] = parcel
        except Exception as exc:
            log.error("DBF parse error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
#  GHL CSV
# ─────────────────────────────────────────────────────────────────────────────

GHL_HEADERS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_HEADERS)
        w.writeheader()
        for r in records:
            first, last = split_name(r.get("owner") or "")
            w.writerow({
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
                "Amount/Debt Owed":       r.get("amount") or "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                 "Summit County Recorder — EagleWeb",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("GHL CSV → %s (%d rows)", path, len(records))


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    now      = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")

    log.info("Summit County Lead Scraper — EagleWeb")
    log.info("Range: %s → %s  (%d days)", start_str, end_str, LOOKBACK_DAYS)

    parcel = ParcelLookup()
    parcel.load()

    all_records: list[dict] = []

    if not PLAYWRIGHT_OK:
        log.error("Playwright unavailable — cannot scrape")
    else:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()

            ok = await aretry(lambda: eagleweb_login(page))
            if ok:
                for code in LEAD_TYPES:
                    log.info("Scraping %s — %s", code, LEAD_TYPES[code]["label"])
                    try:
                        recs = await scrape_eagleweb_type(page, code, start_str, end_str)
                        all_records.extend(recs)
                    except Exception as exc:
                        log.error("Failed %s: %s", code, exc)

            await browser.close()

    log.info("Raw total: %d records", len(all_records))

    # Deduplicate
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_records:
        key = r.get("doc_num", "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        deduped.append(r)

    # Enrich + score
    with_address = 0
    for r in deduped:
        if r.get("owner") and parcel._loaded:
            hit = parcel.lookup(r["owner"])
            if hit:
                r.update(hit)
        if r.get("prop_address"):
            with_address += 1
        r["score"], r["flags"] = compute_score(r)

    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    payload = {
        "fetched_at":   now.isoformat(),
        "source":       "Summit County Recorder — EagleWeb",
        "date_range":   {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end":   now.strftime("%Y-%m-%d"),
        },
        "total":        len(deduped),
        "with_address": with_address,
        "records":      deduped,
    }

    for out_path in OUTPUT_PATHS:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s", out_path)

    export_ghl_csv(deduped, Path("data/ghl_export.csv"))
    log.info("Done — %d records, %d with address", len(deduped), with_address)


if __name__ == "__main__":
    asyncio.run(main())
