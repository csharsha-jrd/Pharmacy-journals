#!/usr/bin/env python3
"""
Pharmacy Journal Dataset Builder
Fetches, enriches, merges, and exports pharmacy journal metadata from DOAJ,
CrossRef, and publisher homepages.

Hidden Gems Mode
----------------
After the main build, the script optionally enriches every journal with:
  - OpenAlex  : APC (USD), citation count, works count, OA status
  - Scimago   : Scopus indexing, SJR quartile, h-index
  - WoS MJL   : Web of Science indexing (best-effort scrape)
  - Acceptance rate scraped from publisher pages

A "hidden gem" is scored and ranked on:
  +4  No APC confirmed
  +2  APC ≤ USD 500 (low-cost)
  +3  Scopus indexed
  +3  WoS indexed
  +2  Scimago Q1 or Q2
  +2  Acceptance rate > 30 %
  +1  DOAJ listed (quality signal)
  −1  per 10 000 citations (penalises over-saturated journals)

Results exported to pharmacy_journals_hidden_gems.csv / .json
"""

import argparse
import csv
import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (JournalBuilder/1.0; +https://example.org)",
    "Accept": "application/json,text/html,application/xhtml+xml"
}

PHARMACY_KEYWORDS = [
    "pharmacy", "pharmacology", "pharmaceutics", "pharmaceutical",
    "clinical pharmacy", "pharmacy practice", "drug", "medicines",
    "pharmacotherapy", "pharmacoepidemiology"
]

SEED_JOURNALS = [
    {"title": "Pharmacy Practice",                              "issn": "1886-3655", "homepage": "https://pharmacypractice.org/"},
    {"title": "International Journal of Pharmacy Practice",     "issn": "0961-7671", "homepage": "https://academic.oup.com/ijpp"},
    {"title": "Research in Social and Administrative Pharmacy", "issn": "1551-7411", "homepage": "https://www.journals.elsevier.com/research-in-social-and-administrative-pharmacy"},
    {"title": "Journal of Pharmacy and Pharmacology",           "issn": "0022-3573", "homepage": "https://academic.oup.com/jpp"},
    {"title": "Indian Journal of Pharmaceutical Sciences",      "issn": "0250-474X", "homepage": "https://www.ijpsonline.com"},
    {"title": "Indian Journal of Pharmaceutical Education and Research", "issn": "0019-5464", "homepage": "https://www.ijper.org"},
    {"title": "Journal of Pharmaceutical Policy and Practice",  "issn": "2052-3211", "homepage": "https://joppp.biomedcentral.com"},
    {"title": "International Journal of Clinical Pharmacy",     "issn": "2210-7703", "homepage": "https://link.springer.com/journal/11096"},
    {"title": "American Journal of Health-System Pharmacy",     "issn": "1079-2082", "homepage": "https://academic.oup.com/ajhp"},
]

# Structured field queries use DOAJ's exact-match syntax;
# free-text queries are plain strings — keep them separate so behaviour is predictable.
DOAJ_FIELD_QUERIES = [
    'bibjson.subject.term:"Pharmacy"',
    'bibjson.subject.term:"Pharmacology"',
]
DOAJ_TEXT_QUERIES = [
    "pharmacy practice",
    "clinical pharmacy",
    "pharmaceutical sciences",
]
DOAJ_QUERIES = DOAJ_FIELD_QUERIES + DOAJ_TEXT_QUERIES

# Explicit CSV column order — logical grouping for human readers
CSV_COLUMNS = [
    "title", "publisher", "country", "issns",
    "oa_status", "doaj",
    "apc_has", "scraped_apc", "waiver_has",
    "peer_review",
    "subjects", "keywords",
    "scope_seed", "scraped_scope",
    "homepage", "source", "relevance_score",
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
session = requests.Session()
session.headers.update(HEADERS)


def safe_get(url, params=None, timeout=30, retries=3):
    """
    GET with retry + exponential backoff.
    Retries on connection errors, timeouts, and 429/5xx responses.
    Returns the Response on success, None after all retries exhausted.
    """
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 2 ** attempt * 5          # 10 s, 20 s, 40 s
                log.warning("Rate-limited by %s — waiting %ds (attempt %d/%d)", url, wait, attempt, retries)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt
                log.warning("Server error %d from %s — waiting %ds (attempt %d/%d)", r.status_code, url, wait, attempt, retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.Timeout:
            log.warning("Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as exc:
            log.warning("Request error on %s: %s (attempt %d/%d)", url, exc, attempt, retries)
            time.sleep(2 ** attempt)
    log.error("All %d attempts failed for %s", retries, url)
    return None


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------
def norm(s):
    return re.sub(r'\s+', ' ', (s or '').strip())


def infer_relevance(text):
    t = (text or '').lower()
    return sum(1 for k in PHARMACY_KEYWORDS if k in t)


# ---------------------------------------------------------------------------
# APC extraction
# ---------------------------------------------------------------------------
# Matches: APC / article processing charge / open access fee / publication fee /
#          open access charge — followed by a currency symbol and amount.
_APC_PATTERN = re.compile(
    r'(?:APC|article\s+processing\s+(?:charge|fee)|open\s+access\s+(?:fee|charge)|'
    r'publication\s+fee|processing\s+fee)'
    r'[^\n\r]{0,150}?'
    r'(USD|US\$|\$|EUR|€|GBP|£|INR|₹|CNY|¥|SGD|AUD|CAD)\s?([0-9][0-9,]{2,})',
    re.I
)


def extract_apc(text):
    m = _APC_PATTERN.search(text)
    if m:
        return f"{m.group(1)} {m.group(2)}".strip()
    return None


# ---------------------------------------------------------------------------
# DOAJ
# ---------------------------------------------------------------------------
def fetch_doaj_journals(max_pages=3, page_size=50):
    journals = {}
    total_fetched = 0

    for query in DOAJ_QUERIES:
        log.info("DOAJ query: %s", query)
        for page in range(1, max_pages + 1):
            url = f"https://doaj.org/api/search/journals/{quote(query)}"
            r = safe_get(url, params={"page": page, "pageSize": page_size})
            if not r:
                log.warning("Skipping remaining pages for query '%s'", query)
                break
            data = r.json()
            results = data.get("results", [])
            if not results:
                break

            for item in results:
                bj = item.get("bibjson", {})
                title = bj.get("title")
                if not title:
                    continue

                subjects = ", ".join(x.get("term", "") for x in bj.get("subject", []))
                keywords = ", ".join(bj.get("keywords", []))
                scope_text = f"{subjects}. {keywords}".strip()

                if infer_relevance(title + " " + scope_text) < 1:
                    continue

                issns = [
                    ident.get("id")
                    for ident in bj.get("identifier", [])
                    if ident.get("type") in ("pissn", "eissn") and ident.get("id")
                ]
                key = (title.strip().lower(), tuple(sorted(set(issns))))

                apc = bj.get("apc") or {}
                waiver = bj.get("waiver") or {}

                # Peer review: DOAJ stores this under editorial.review_process
                editorial = bj.get("editorial") or {}
                peer_review = editorial.get("review_process") or editorial.get("review_process_url")

                journals[key] = {
                    "title": title,
                    "publisher": (bj.get("publisher") or {}).get("name") if isinstance(bj.get("publisher"), dict) else bj.get("publisher"),
                    "country": bj.get("country"),
                    "issns": sorted(set(issns)),
                    "doaj": True,
                    "oa_status": "Full OA",
                    "apc_has": apc.get("has_apc"),
                    "waiver_has": waiver.get("has_waiver"),
                    "homepage": (bj.get("link") or [{}])[0].get("url"),
                    "subjects": subjects,
                    "keywords": keywords,
                    "scope_seed": scope_text,
                    "peer_review": peer_review,
                    "source": "DOAJ",
                }
                total_fetched += 1

            log.info("  page %d — %d records so far", page, total_fetched)
            time.sleep(0.5)

    log.info("DOAJ fetch complete: %d unique relevant journals", len(journals))
    return list(journals.values())


# ---------------------------------------------------------------------------
# CrossRef
# ---------------------------------------------------------------------------
def fetch_crossref_title(issn):
    r = safe_get(f"https://api.crossref.org/journals/{issn}")
    if not r:
        return {}
    msg = r.json().get("message", {})
    return {
        "title": msg.get("title"),
        "publisher": msg.get("publisher"),
        "issns": sorted(set(msg.get("ISSN") or [])),
    }


# ---------------------------------------------------------------------------
# Publisher page scraping
# ---------------------------------------------------------------------------
def scrape_publisher_page(url):
    r = safe_get(url, timeout=30)
    if not r:
        return {}
    soup = BeautifulSoup(r.text, "html.parser")

    # Preserve original text for extraction; build lowercase copy for matching only
    fragments = [norm(x.get_text(" ", strip=True)) for x in soup.find_all(["h1", "h2", "h3", "p", "li"])[:200]]
    text = norm(" ".join(fragments))
    lower_text = text.lower()

    apc = extract_apc(text)

    # Scope: match on lowercase, but extract the same span from the original text
    scope = None
    scope_patterns = [
        r'aims?\s+and\s+scope[:\s]+(.{50,600})',
        r'about\s+(?:the\s+)?journal[:\s]+(.{50,600})',
        r'scope\s+of\s+the\s+journal[:\s]+(.{50,600})',
    ]
    for patt in scope_patterns:
        m = re.search(patt, lower_text, re.I)
        if m:
            # Use the match positions to slice from the original cased text
            scope = norm(text[m.start(1):m.end(1)])
            break
    if not scope:
        scope = norm(text[:500])

    acceptance_rate = _extract_acceptance_rate(text)

    return {
        "scraped_scope": scope,
        "scraped_apc": apc,
        "acceptance_rate_pct": acceptance_rate,
    }


_ACCEPTANCE_PATTERN = re.compile(
    r'acceptance\s+rate[:\s]+(?:is\s+|approximately\s+|about\s+|around\s+)?([0-9]{1,3})\s*%',
    re.I
)
_ACCEPTANCE_PATTERN2 = re.compile(
    r'([0-9]{1,3})\s*%\s+(?:overall\s+)?acceptance\s+rate',
    re.I
)


def _extract_acceptance_rate(text):
    """Return acceptance rate as a float (0–100) if found in page text, else None."""
    for patt in (_ACCEPTANCE_PATTERN, _ACCEPTANCE_PATTERN2):
        m = patt.search(text)
        if m:
            val = float(m.group(1))
            if 1.0 <= val <= 100.0:          # sanity-check the range
                return val
    return None


# ---------------------------------------------------------------------------
# Seed journal enrichment
# ---------------------------------------------------------------------------
def enrich_seed_journals(seed_list):
    out = []
    for i, row in enumerate(seed_list, 1):
        log.info("Enriching seed journal %d/%d: %s", i, len(seed_list), row.get("title"))
        item = dict(row)

        cr = fetch_crossref_title(row.get("issn", ""))
        # CrossRef fills gaps but does NOT override seed values
        item.setdefault("title", cr.get("title"))
        item.setdefault("publisher", cr.get("publisher"))
        item["issns"] = sorted(set(
            ([row["issn"]] if row.get("issn") else []) + cr.get("issns", [])
        ))

        page_data = scrape_publisher_page(row["homepage"]) if row.get("homepage") else {}
        item.update(page_data)
        item["source"] = "Seed+Crossref+Publisher"
        item["oa_status"] = item.get("oa_status", "Unknown")
        out.append(item)
        time.sleep(0.5)

    return out


# ---------------------------------------------------------------------------
# Transitive deduplication via union-find
# ---------------------------------------------------------------------------
class _UnionFind:
    def __init__(self):
        self._parent = {}

    def find(self, x):
        self._parent.setdefault(x, x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])   # path compression
        return self._parent[x]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra


def merge_records(seed_records, doaj_records):
    """
    Merge seed and DOAJ records with transitive deduplication.
    Seed values take priority over DOAJ values for conflicting fields.
    Records are linked if they share any ISSN or an identical normalised title.
    """
    all_records = list(enumerate(seed_records + doaj_records))
    uf = _UnionFind()

    # Index: normalised-title -> record index; issn -> record index
    title_idx = {}
    issn_idx = {}

    for idx, rec in all_records:
        t = rec.get("title", "").strip().lower()
        if t:
            if t in title_idx:
                uf.union(title_idx[t], idx)
            else:
                title_idx[t] = idx

        for issn in rec.get("issns", []):
            if not issn:
                continue
            if issn in issn_idx:
                uf.union(issn_idx[issn], idx)
            else:
                issn_idx[issn] = idx

    # Group by cluster root
    clusters: dict = {}
    for idx, _ in all_records:
        root = uf.find(idx)
        clusters.setdefault(root, []).append(idx)

    def _merge_two(base, incoming, seed_idx_set):
        """Merge `incoming` into `base`. Seed records win on conflicts."""
        for k, v in incoming.items():
            if v in (None, "", [], {}):
                continue
            if k == "issns":
                base[k] = sorted(set(base.get(k, []) + v))
            elif k == "source":
                existing = base.get(k, "")
                if existing and existing != v:
                    base[k] = existing + "; " + v
                else:
                    base[k] = v
            elif k == "scraped_scope":
                # Keep the longer scope text
                if len(str(v)) > len(str(base.get(k, ""))):
                    base[k] = v
            else:
                if not base.get(k):
                    base[k] = v
                # If both have values, seed wins — only overwrite if base came from DOAJ
                # (seed records carry "Seed+Crossref+Publisher" in source)
        return base

    seed_n = len(seed_records)
    merged = []
    for root, members in clusters.items():
        seed_members = [i for i in members if i < seed_n]
        doaj_members = [i for i in members if i >= seed_n]

        # Start with seeds, then layer in DOAJ data (seeds take priority)
        combined = {}
        for idx in seed_members + doaj_members:
            _, rec = all_records[idx]
            combined = _merge_two(combined, rec, set(range(seed_n)))

        # Relevance score across all text fields
        scope_text = " ".join(str(combined.get(x, "")) for x in
                              ["title", "subjects", "keywords", "scraped_scope", "scope_seed"])
        combined["relevance_score"] = infer_relevance(scope_text)
        merged.append(combined)

    merged.sort(key=lambda x: (-x.get("relevance_score", 0), (x.get("title") or "").lower()))
    log.info("Merge complete: %d unique journals", len(merged))
    return merged


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def export(records, prefix="pharmacy_journals", output_dir="."):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = str(out / (prefix + ".json"))
    csv_path  = str(out / (prefix + ".csv"))

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # Build ordered fieldnames: defined columns first, then any extras alphabetically
    extra = sorted(k for r in records for k in r if k not in CSV_COLUMNS)
    fieldnames = CSV_COLUMNS + list(dict.fromkeys(extra))  # deduplicate extras

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in records:
            row = dict(r)
            # Flatten lists for CSV readability
            if isinstance(row.get("issns"), list):
                row["issns"] = "; ".join(row["issns"])
            w.writerow(row)

    log.info("Exported %d records -> %s, %s", len(records), json_path, csv_path)
    return json_path, csv_path


# ===========================================================================
# HIDDEN GEMS — indexing enrichment + scoring
# ===========================================================================

# ---------------------------------------------------------------------------
# OpenAlex  (free, no API key needed; polite pool via mailto)
# Docs: https://docs.openalex.org/api-entities/sources
# ---------------------------------------------------------------------------
# Override via env var OPENALEX_EMAIL or --openalex-email CLI flag.
_OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "journalbuilder@example.org")

def fetch_openalex(issn):
    """
    Fetch source record from OpenAlex by ISSN.
    Returns dict with apc_usd, cited_by_count, works_count, is_oa, is_in_doaj.
    All fields are None on failure — never raises.
    """
    if not issn:
        return {}
    url = "https://api.openalex.org/sources"
    r = safe_get(url, params={
        "filter": f"issn:{issn}",
        "select": "id,display_name,issn_l,is_in_doaj,is_oa,apc_usd,apc_prices,cited_by_count,works_count",
        "mailto": _OPENALEX_EMAIL,
    })
    if not r:
        return {}
    try:
        results = r.json().get("results", [])
        if not results:
            return {}
        j = results[0]
        # apc_usd: direct field (integer or null)
        apc_usd = j.get("apc_usd")
        # Fallback: parse apc_prices list for a USD entry
        if apc_usd is None:
            for entry in (j.get("apc_prices") or []):
                if (entry.get("currency") or "").upper() == "USD":
                    apc_usd = entry.get("price")
                    break
        return {
            "oa_apc_usd":       apc_usd,
            "cited_by_count":   j.get("cited_by_count"),
            "works_count":      j.get("works_count"),
            "is_oa":            j.get("is_oa"),
            "openalex_doaj":    j.get("is_in_doaj"),
        }
    except Exception as exc:
        log.debug("OpenAlex parse error for ISSN %s: %s", issn, exc)
        return {}


# ---------------------------------------------------------------------------
# Scimago  (Scopus-based; no API key; JSON endpoint)
# If a journal appears in Scimago results it IS Scopus-indexed.
# Docs: https://www.scimagojr.com/
# ---------------------------------------------------------------------------

def check_scimago(issn):
    """
    Query Scimago by ISSN.
    Returns scopus_indexed=True, sjr, quartile, h_index, scimago_area on success.
    All None on failure or not-found.
    """
    if not issn:
        return {}
    r = safe_get(
        "https://api.scimagojr.com/journalsearch.php",
        params={"q": issn, "type": "issn"},
    )
    if not r:
        return {}
    try:
        data = r.json()
        # Response is a JSON array; empty list = not in Scopus
        if not data or not isinstance(data, list):
            return {"scopus_indexed": False}
        j = data[0]
        # Extract the best (highest) SJR value across years
        sjr = j.get("sjrbest") or j.get("sjr")
        # Quartile: "Q1", "Q2", etc. — stored per area; take the best overall
        quartile = None
        areas = j.get("areas") or []
        quartile_rank = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
        for area in areas:
            q = area.get("quartile")
            if q and (quartile is None or quartile_rank.get(q, 9) < quartile_rank.get(quartile, 9)):
                quartile = q
        # h-index
        h_index = j.get("hindex") or j.get("h_index")
        # Primary subject area name
        area_name = areas[0].get("name") if areas else None
        return {
            "scopus_indexed": True,
            "sjr":            sjr,
            "sjr_quartile":   quartile,
            "h_index":        h_index,
            "scimago_area":   area_name,
        }
    except Exception as exc:
        log.debug("Scimago parse error for ISSN %s: %s", issn, exc)
        return {}


# ---------------------------------------------------------------------------
# WoS Master Journal List  (Clarivate — public search, no key)
# Docs: https://mjl.clarivate.com/
# ---------------------------------------------------------------------------

def check_wos(issn, title=None):
    """
    Check the Web of Science Master Journal List for a journal.
    Tries ISSN search first, falls back to title keyword search.
    Returns wos_indexed=True/False, wos_collections (list) on success.
    Returns {} on network/parse failure — caller treats as unknown.
    """
    def _parse_wos_response(r):
        try:
            data = r.json()
        except Exception:
            return None
        # MJL API returns {"hits": {"total": N, "hits": [...]}} or {"data": [...]}
        hits = (
            data.get("hits", {}).get("hits")
            or data.get("data")
            or []
        )
        if not hits:
            return {"wos_indexed": False, "wos_collections": []}
        # Collect which WoS products/collections this journal appears in
        collections = []
        for h in hits:
            # Field varies: "products", "editions", "collection"
            for field in ("products", "editions", "collection"):
                val = h.get(field)
                if isinstance(val, list):
                    collections.extend(str(v) for v in val)
                elif val:
                    collections.append(str(val))
        return {
            "wos_indexed":     True,
            "wos_collections": sorted(set(collections)),
        }

    base = "https://mjl.clarivate.com/api/journals"

    # Attempt 1: ISSN exact match
    if issn:
        r = safe_get(base, params={"filter[issn]": issn, "count": 5})
        if r:
            result = _parse_wos_response(r)
            if result is not None:
                return result

    # Attempt 2: title keyword (less precise, only if ISSN failed)
    if title:
        r = safe_get(base, params={"filter[q]": title[:80], "count": 5})
        if r:
            result = _parse_wos_response(r)
            if result is not None:
                return result

    log.debug("WoS MJL lookup failed for ISSN=%s", issn)
    return {}   # unknown — do not penalise the gem score


# ---------------------------------------------------------------------------
# Enrichment pass — adds OpenAlex + Scimago + WoS to each merged record
# ---------------------------------------------------------------------------

def enrich_with_indexing(records):
    """
    Mutates records in-place, adding indexing and metrics fields.
    Picks the first available ISSN from each record.
    """
    n = len(records)
    for i, rec in enumerate(records, 1):
        issns = rec.get("issns") or []
        primary_issn = issns[0] if issns else None
        title = rec.get("title")

        if i % 10 == 0 or i == 1:
            log.info("  Indexing enrichment %d/%d ...", i, n)

        oa = fetch_openalex(primary_issn)
        rec.update({k: v for k, v in oa.items() if v is not None})

        scim = check_scimago(primary_issn)
        rec.update({k: v for k, v in scim.items() if v is not None})

        wos = check_wos(primary_issn, title)
        rec.update({k: v for k, v in wos.items() if v is not None})

        time.sleep(0.3)   # polite delay per journal

    return records


# ---------------------------------------------------------------------------
# Gem scoring
# ---------------------------------------------------------------------------

# APC threshold for "low-cost" category (USD)
_LOW_APC_USD_THRESHOLD = 500


def _apc_usd_from_record(rec):
    """
    Best-effort APC in USD. Uses OpenAlex integer first, falls back to
    parsing the scraped string (e.g. "USD 1,200" → 1200).
    Returns None if unknown.
    """
    usd = rec.get("oa_apc_usd")
    if isinstance(usd, (int, float)):
        return float(usd)
    scraped = rec.get("scraped_apc") or ""
    m = re.search(r'(USD|US\$|\$)\s?([\d,]+)', scraped, re.I)
    if m:
        try:
            return float(m.group(2).replace(",", ""))
        except ValueError:
            pass
    return None


def score_gem(rec):
    """
    Return a numerical gem score. Higher = better hidden gem.
    Also returns a dict of the individual point contributions for transparency.
    """
    breakdown = {}

    # ---- APC ---------------------------------------------------------------
    apc_confirmed_free = rec.get("apc_has") is False or rec.get("oa_apc_usd") == 0
    apc_usd = _apc_usd_from_record(rec)

    if apc_confirmed_free:
        breakdown["no_apc"] = 4
    elif apc_usd is not None and apc_usd <= _LOW_APC_USD_THRESHOLD:
        breakdown["low_apc"] = 2

    # ---- Indexing ----------------------------------------------------------
    if rec.get("scopus_indexed"):
        breakdown["scopus"] = 3
    if rec.get("wos_indexed"):
        breakdown["wos"] = 3

    # ---- Quartile ----------------------------------------------------------
    q = rec.get("sjr_quartile", "")
    if q in ("Q1", "Q2"):
        breakdown["quartile"] = 2 if q == "Q1" else 1

    # ---- Acceptance rate ---------------------------------------------------
    ar = rec.get("acceptance_rate_pct")
    if isinstance(ar, (int, float)) and ar > 30:
        breakdown["acceptance_rate"] = 2

    # ---- DOAJ quality signal -----------------------------------------------
    if rec.get("doaj"):
        breakdown["doaj_listed"] = 1

    # ---- Citation penalty (over-saturated journals are not hidden) ----------
    cited = rec.get("cited_by_count")
    if isinstance(cited, (int, float)) and cited > 0:
        penalty = int(cited // 10_000)
        if penalty:
            breakdown["citation_penalty"] = -penalty

    total = sum(breakdown.values())
    return total, breakdown


def find_hidden_gems(records, top_n=20, min_score=3):
    """
    Score all records, filter to those meeting min_score,
    sort descending, return top_n.
    Adds 'gem_score' and 'gem_breakdown' fields to each returned record.
    """
    scored = []
    for rec in records:
        total, breakdown = score_gem(rec)
        if total >= min_score:
            rec = dict(rec)   # don't mutate the original
            rec["gem_score"] = total
            rec["gem_breakdown"] = json.dumps(breakdown)
            scored.append(rec)

    scored.sort(key=lambda x: (-x["gem_score"], (x.get("title") or "").lower()))
    gems = scored[:top_n]
    log.info("Hidden gems: %d journals scored ≥ %d; returning top %d", len(scored), min_score, len(gems))
    return gems


# ---------------------------------------------------------------------------
# Hidden gems export
# ---------------------------------------------------------------------------

GEM_CSV_COLUMNS = [
    "gem_score", "gem_breakdown",
    "title", "publisher", "country", "issns",
    "oa_status", "doaj", "scopus_indexed", "wos_indexed", "wos_collections",
    "sjr_quartile", "sjr", "h_index", "scimago_area",
    "apc_has", "oa_apc_usd", "scraped_apc", "waiver_has",
    "acceptance_rate_pct",
    "cited_by_count", "works_count",
    "peer_review", "subjects", "keywords",
    "scraped_scope", "homepage", "source",
]


def export_hidden_gems(gems, prefix="pharmacy_journals_hidden_gems", output_dir="."):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = str(out / (prefix + ".json"))
    csv_path  = str(out / (prefix + ".csv"))

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(gems, f, indent=2, ensure_ascii=False)

    extra = sorted(k for r in gems for k in r if k not in GEM_CSV_COLUMNS)
    fieldnames = GEM_CSV_COLUMNS + list(dict.fromkeys(extra))

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in gems:
            row = dict(r)
            if isinstance(row.get("issns"), list):
                row["issns"] = "; ".join(row["issns"])
            if isinstance(row.get("wos_collections"), list):
                row["wos_collections"] = "; ".join(row["wos_collections"])
            w.writerow(row)

    log.info("Hidden gems exported -> %s, %s", json_path, csv_path)
    return json_path, csv_path


def print_gem_summary(gems):
    """Print a quick human-readable leaderboard to stdout."""
    print("\n" + "=" * 70)
    print(f"  TOP {len(gems)} HIDDEN GEMS — Pharmacy Journals")
    print("=" * 70)
    print(f"  {'#':<4} {'Score':<7} {'Scopus':<8} {'WoS':<6} {'APC (USD)':<12} {'Title'}")
    print("-" * 70)
    for i, rec in enumerate(gems, 1):
        apc_usd = _apc_usd_from_record(rec)
        apc_str = (
            "FREE" if rec.get("apc_has") is False or apc_usd == 0
            else (f"${int(apc_usd):,}" if apc_usd else "Unknown")
        )
        scopus = "Yes" if rec.get("scopus_indexed") else "No"
        wos    = "Yes" if rec.get("wos_indexed")    else "No"
        title  = (rec.get("title") or "")[:42]
        print(f"  {i:<4} {rec['gem_score']:<7} {scopus:<8} {wos:<6} {apc_str:<12} {title}")
    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Pharmacy Journal Dataset Builder")
    parser.add_argument(
        "--gems", action="store_true",
        help="After building the main dataset, enrich with Scopus/WoS/OpenAlex and export hidden gems"
    )
    parser.add_argument(
        "--gems-only", action="store_true",
        help="Load an existing pharmacy_journals.json and only run the hidden gems analysis"
    )
    parser.add_argument("--top", type=int, default=20, help="Number of top gems to export (default: 20)")
    parser.add_argument("--min-score", type=int, default=3, help="Minimum gem score to include (default: 3)")
    parser.add_argument(
        "--output-dir", default=".",
        help="Directory to write all output files into (default: current directory)"
    )
    parser.add_argument(
        "--openalex-email", default=None,
        help="Email for OpenAlex polite pool (overrides OPENALEX_EMAIL env var)"
    )
    args = parser.parse_args()

    # Allow CLI flag to override env var for OpenAlex email
    if args.openalex_email:
        global _OPENALEX_EMAIL
        _OPENALEX_EMAIL = args.openalex_email

    out_dir = args.output_dir

    if args.gems_only:
        log.info("=== Hidden Gems Mode (loading existing dataset) ===")
        existing = Path(out_dir) / "pharmacy_journals.json"
        with open(existing, encoding="utf-8") as f:
            merged = json.load(f)
        log.info("Loaded %d journals from %s", len(merged), existing)
    else:
        log.info("=== Pharmacy Journal Dataset Builder ===")
        log.info("Step 1/3 — Fetching from DOAJ")
        doaj = fetch_doaj_journals()

        log.info("Step 2/3 — Enriching %d seed journals", len(SEED_JOURNALS))
        seed = enrich_seed_journals(SEED_JOURNALS)

        log.info("Step 3/3 — Merging and deduplicating")
        merged = merge_records(seed, doaj)

        j, c = export(merged, output_dir=out_dir)
        print(f"\nMain dataset — {len(merged)} journals  →  {j}  |  {c}")

    if args.gems or args.gems_only:
        log.info("Step 4 — Enriching with indexing data (OpenAlex / Scimago / WoS MJL)")
        log.info("  This makes one HTTP request per journal — expect ~%.0f seconds for %d journals",
                 len(merged) * 1.0, len(merged))
        enrich_with_indexing(merged)

        # Re-export main dataset with enriched fields so gems-only also updates the full file
        export(merged, output_dir=out_dir)

        log.info("Step 5 — Scoring and selecting hidden gems")
        gems = find_hidden_gems(merged, top_n=args.top, min_score=args.min_score)
        gj, gc = export_hidden_gems(gems, output_dir=out_dir)
        print_gem_summary(gems)
        print(f"Hidden gems — {len(gems)} journals  →  {gj}  |  {gc}\n")


if __name__ == "__main__":
    main()
