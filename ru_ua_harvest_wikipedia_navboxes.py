#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Harvest entities from ONE specific navbox of a Wikipedia page, then optionally
expand via multilingual (EN/RU/UK) category walking, resolve to Wikidata QIDs,
and fetch labels/descriptions/sitelinks/instance_of plus attribution properties.

Output schema is unified with ru_ua_harvest_wikidata_entities.py output,
so you can run ru_ua_classify_entities.py on both JSONL files together.

Modified fixes:
- Modified: fetch rendered HTML via MediaWiki action=parse (more reliable than direct HTML)
- Modified: parse.text can be either a string OR {"*": "..."} -> handle both
- Modified: detect navboxes via broader selectors:
    table.navbox, table.vertical-navbox, div.navbox, nav.navbox
- Modified: select ONE navbox by title substring match (e.g., "Russo-Ukrainian war")
- Modified: optional --debug-save-html to inspect what HTML you actually parsed
- Modified: optional BFS/DFS category walk with multilingual support (en/ru/uk)
- Modified: keeps JSONL schema aligned with ru_ua_harvest_wikidata_entities.py

Deps:
  pip install requests beautifulsoup4 lxml SPARQLWrapper

Usage:
  python ru_ua_harvest_wikipedia_navboxes.py \
    --start-url "https://en.wikipedia.org/wiki/Russo-Ukrainian_War" \
    --navbox-title "Russo-Ukrainian war" \
    --include-categories \
    --category-strategy bfs \
    --category-depth 1 \
    --category-langs en,ru,uk \
    --out "data/navbox_ru_ua_entities.jsonl" \
    --out-report "data/navbox_report.json"

"""

import argparse
import json
import os
import sys
import time
from urllib.parse import urlparse, urljoin, unquote
from typing import Dict, List, Optional, Set
from collections import defaultdict, deque

import requests
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKI_API = "https://en.wikipedia.org/w/api.php"

# Modified: include contact-ish UA to reduce 403 risk
USER_AGENT = "ru-ua-navbox-harvester/3.3 (single-navbox; research; contact: yuanyu)"
HEADERS_JSON = {"User-Agent": USER_AGENT, "Accept": "application/json"}
HEADERS_HTML = {"User-Agent": USER_AGENT, "Accept": "text/html"}

ALLOWED_NAMESPACES = {""}  # mainspace only
SUPPORTED_WIKI_LANGS = ("en", "ru", "uk")
DEFAULT_CATEGORY_KEYWORDS = (
    "Russo,Ukraine,Ukrain,Russia,War,Invasion,"
    "росс,украин,войн,вторж,агресс,"
    "росій,україн,війн,вторг,агрес"
)

# Type anchors for category_hint
Q_HUMAN = "Q5"
Q_BATTLE = "Q178561"
Q_MILITARY_CONFLICT = "Q180684"
Q_ATTACK = "Q645883"
Q_MASS_KILLING = "Q167442"
Q_MILITARY_UNIT = "Q176799"
Q_ORGANIZATION = "Q43229"
Q_GOVERNMENT = "Q7188"
Q_LAW = "Q820655"
Q_PUBLIC_POLICY = "Q7163"
Q_ECON_SANCTION = "Q618779"
Q_RESOLUTION = "Q182994"
Q_CONSPIRACY_THEORY = "Q17379835"
Q_PROPAGANDA = "Q215080"

CATEGORY_MAP = {
    "person": {Q_HUMAN},
    "event": {Q_BATTLE, Q_MILITARY_CONFLICT, Q_ATTACK, Q_MASS_KILLING},
    "organization": {Q_MILITARY_UNIT, Q_ORGANIZATION, Q_GOVERNMENT},
    "policy": {Q_LAW, Q_PUBLIC_POLICY, Q_ECON_SANCTION, Q_RESOLUTION},
    "media_narrative": {Q_CONSPIRACY_THEORY, Q_PROPAGANDA},
}

ATTRIB_PROP_IDS = ["P27", "P17", "P495", "P159", "P131", "P276", "P19", "P740", "P551"]

ENSURE_QIDS = {
    "Q16150196",  # Donetsk People's Republic
    "Q16746854",  # Luhansk People's Republic
    "Q16912926",  # Novorossiya
    "Q15925436",  # Republic of Crimea
}


def mkparents(path: str) -> None:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)


def qid_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]


def is_internal_wiki_link(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.netloc.endswith("wikipedia.org") and u.path.startswith("/wiki/")
    except Exception:
        return False


def clean_title_from_href_path(href_path: str) -> Optional[str]:
    try:
        href_path = href_path.split("#", 1)[0]
        if not href_path.startswith("/wiki/"):
            return None
        title = unquote(href_path[len("/wiki/"):])
        if ":" in title:
            ns = title.split(":", 1)[0]
            if ns not in ALLOWED_NAMESPACES:
                return None
        if title.startswith("Main_Page"):
            return None
        return title
    except Exception:
        return None


# ---------------------------
# MediaWiki API helper with retry/backoff
# ---------------------------
def wiki_api_for_lang(lang: str) -> str:
    return f"https://{lang}.wikipedia.org/w/api.php"


def _lang_from_start_url(start_url: str) -> str:
    try:
        host = urlparse(start_url).netloc.lower()
        if host.endswith(".wikipedia.org"):
            sub = host.split(".wikipedia.org", 1)[0]
            if sub:
                return sub
    except Exception:
        pass
    return "en"


def _mw_api_get(params: Dict, sleep: float = 0.0, retries: int = 8, api_url: str = WIKI_API,
                use_post: bool = False) -> Dict:
    params = dict(params)
    params.setdefault("format", "json")
    params.setdefault("formatversion", 2)

    last_status = None
    last_head = ""
    last_exc = None

    for attempt in range(retries):
        try:
            if use_post:
                r = requests.post(api_url, data=params, headers=HEADERS_JSON, timeout=60)
            else:
                r = requests.get(api_url, params=params, headers=HEADERS_JSON, timeout=60)
            last_status = r.status_code
            last_head = r.text[:200]

            if r.status_code in (403, 429, 500, 502, 503, 504):
                time.sleep(min(60.0, (2 ** attempt) + 0.8))
                continue

            r.raise_for_status()

            if sleep > 0:
                time.sleep(sleep)

            return r.json()

        except Exception as e:
            last_exc = e
            time.sleep(min(60.0, (2 ** attempt) + 0.8))

    raise RuntimeError(
        f"MediaWiki API failed. last_status={last_status}, last_head={last_head!r}, last_error={repr(last_exc)}"
    )


def page_title_from_start_url(start_url: str) -> str:
    if start_url.startswith("http"):
        p = urlparse(start_url).path
        if p.startswith("/wiki/"):
            return unquote(p[len("/wiki/"):])
    return start_url.strip()


def _mw_query_all_pages(params: Dict, sleep: float = 0.0, continue_key: str = "continue",
                        api_url: str = WIKI_API, max_items: Optional[int] = None) -> List[Dict]:
    """
    Call MediaWiki API with continuation and return merged pages/items-like lists.
    """
    out_items: List[Dict] = []
    cont = {}
    while True:
        req = dict(params)
        req.update(cont)
        data = _mw_api_get(req, sleep=sleep, api_url=api_url)

        if "query" in data:
            q = data["query"]
            if isinstance(q.get("pages"), list):
                out_items.extend(q["pages"])
            elif isinstance(q.get("categorymembers"), list):
                out_items.extend(q["categorymembers"])

        if max_items and len(out_items) >= max_items:
            return out_items[:max_items]

        nxt = data.get(continue_key)
        if not nxt:
            break
        cont = nxt
    return out_items


def fetch_rendered_html_via_parse(page_title: str, sleep: float, api_url: str = WIKI_API) -> str:
    # Modified: redirects=1 to avoid redirect edge cases
    data = _mw_api_get(
        {
            "action": "parse",
            "page": page_title,
            "prop": "text",
            "redirects": 1,
        },
        sleep=sleep,
        api_url=api_url,
    )
    if "error" in data:
        raise RuntimeError(f"MediaWiki parse error: {data['error']}")

    text_field = (data.get("parse") or {}).get("text")
    if text_field is None:
        raise RuntimeError("MediaWiki parse returned no 'text' field.")

    # Modified: parse.text can be a string OR {"*": "..."}
    if isinstance(text_field, dict) and "*" in text_field:
        html = text_field["*"]
    elif isinstance(text_field, str):
        html = text_field
    else:
        html = str(text_field)

    if not html or len(html) < 100:
        raise RuntimeError("Parsed HTML looks too small; likely a fetch issue.")
    return html


def fetch_page_categories(page_title: str, sleep: float = 0.2, api_url: str = WIKI_API) -> Set[str]:
    """
    Fetch categories of one Wikipedia page (Category:... titles).
    """
    pages = _mw_query_all_pages(
        {
            "action": "query",
            "prop": "categories",
            "titles": page_title,
            "cllimit": "max",
            "redirects": 1,
            "clshow": "!hidden",
        },
        sleep=sleep,
        api_url=api_url,
    )
    out: Set[str] = set()
    for p in pages:
        for c in (p.get("categories") or []):
            t = c.get("title")
            ns = c.get("ns")
            if isinstance(t, str) and (ns == 14 or t.startswith("Category:")):
                out.add(t)
    return out


def fetch_category_members(category_title: str, sleep: float = 0.2, api_url: str = WIKI_API,
                           max_members: Optional[int] = None) -> List[Dict]:
    """
    Fetch members of a category: both mainspace pages and subcategories.
    """
    return _mw_query_all_pages(
        {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": category_title,
            "cmtype": "page|subcat",
            "cmlimit": "max",
        },
        sleep=sleep,
        api_url=api_url,
        max_items=max_members,
    )


def keep_subcategory_by_keywords(category_title: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    t = category_title.lower()
    return any(k.lower() in t for k in keywords)


def walk_categories_collect_titles(
    root_categories: Set[str],
    depth: int,
    strategy: str = "bfs",
    keywords: Optional[List[str]] = None,
    sleep: float = 0.2,
    api_url: str = WIKI_API,
    max_categories: int = 0,
    max_titles: int = 0,
    max_members_per_category: int = 0,
    progress_every: int = 25,
    progress_prefix: str = "",
) -> Dict[str, object]:
    """
    Walk categories and collect article titles from main namespace.
    strategy: bfs or dfs
    """
    kw = keywords or []
    visited_categories: Set[str] = set()
    article_titles: Set[str] = set()
    queued_categories: Set[str] = set()

    agenda = deque()
    for c in sorted(root_categories):
        agenda.append((c, 0))
        queued_categories.add(c)

    stop_reason = ""
    while agenda:
        if max_categories > 0 and len(visited_categories) >= max_categories:
            stop_reason = "max_categories_reached"
            break

        if strategy == "dfs":
            cat, d = agenda.pop()
        else:
            cat, d = agenda.popleft()

        if cat in visited_categories:
            continue
        visited_categories.add(cat)

        if d > depth:
            continue

        try:
            members = fetch_category_members(
                cat,
                sleep=sleep,
                api_url=api_url,
                max_members=(max_members_per_category if max_members_per_category > 0 else None),
            )
        except Exception as e:
            print(f"WARN: failed to fetch category '{cat}': {e}")
            continue

        if progress_every > 0 and (len(visited_categories) % progress_every == 0):
            pfx = f"{progress_prefix} " if progress_prefix else ""
            print(
                f"{pfx}progress: visited_categories={len(visited_categories)}, "
                f"queued={len(agenda)}, titles={len(article_titles)}"
            )

        for m in members:
            ns = m.get("ns")
            title = m.get("title")
            if not isinstance(title, str):
                continue

            # ns=14 category, ns=0 main/article
            if ns == 14:
                if d < depth and keep_subcategory_by_keywords(title, kw):
                    if title not in visited_categories and title not in queued_categories:
                        agenda.append((title, d + 1))
                        queued_categories.add(title)
            elif ns == 0:
                article_titles.add(title)
                if max_titles > 0 and len(article_titles) >= max_titles:
                    stop_reason = "max_titles_reached"
                    break

        if stop_reason:
            break

    return {
        "titles": article_titles,
        "visited_categories": visited_categories,
        "stop_reason": stop_reason or "completed",
    }


def fetch_langlinks(page_title: str, source_lang: str = "en", sleep: float = 0.2) -> Dict[str, str]:
    """
    Map language code -> linked title for one page.
    """
    api_url = wiki_api_for_lang(source_lang)
    data = _mw_api_get(
        {
            "action": "query",
            "prop": "langlinks",
            "titles": page_title,
            "lllimit": "max",
            "redirects": 1,
        },
        sleep=sleep,
        api_url=api_url,
    )
    out: Dict[str, str] = {}
    for p in data.get("query", {}).get("pages", []):
        for ll in (p.get("langlinks") or []):
            lang = ll.get("lang")
            title = ll.get("title")
            if isinstance(lang, str) and isinstance(title, str):
                out[lang] = title
    return out


def seed_page_titles_by_lang(base_page_title: str, source_lang: str, langs: List[str], sleep: float) -> Dict[str, str]:
    """
    Build start page titles for each target language using langlinks.
    """
    out: Dict[str, str] = {}
    if source_lang in langs:
        out[source_lang] = base_page_title

    langlinks = fetch_langlinks(base_page_title, source_lang=source_lang, sleep=sleep)
    for lang in langs:
        if lang == source_lang:
            continue
        t = langlinks.get(lang)
        if t:
            out[lang] = t
    return out


def soup_from_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        print("Error: lxml parser not available. Please run: pip install lxml")
        sys.exit(1)

<<<<<<< Updated upstream
def extract_links_from_navboxes(soup: BeautifulSoup, base_url: str) -> Set[str]:
    # maybe we could take not all navboxes... some look a bit unrelated in the end ("Russo-Ukrainian war" and "Russo-Ukrainian War (2022-present)" are related, but "Links to related articles" go a bit too much astray)
=======

def navbox_title_text(navbox) -> str:
    t = navbox.select_one(".navbox-title")
    if t:
        return " ".join(t.get_text(" ", strip=True).split())
    cap = navbox.select_one("caption")
    if cap:
        return " ".join(cap.get_text(" ", strip=True).split())
    return " ".join(navbox.get_text(" ", strip=True).split())[:120]


def select_top_level_navboxes(soup: BeautifulSoup) -> List[object]:
    # Modified: broader navbox selectors (table/div/nav + vertical-navbox)
    all_navboxes = soup.select("table.navbox, table.vertical-navbox, div.navbox, nav.navbox")

    top = []
    for nb in all_navboxes:
        parent_navbox = nb.find_parent(class_="navbox")
        if parent_navbox is None:
            top.append(nb)

    top2 = []
    for nb in top:
        cls = " ".join(nb.get("class", []))
        if "navbox-subgroup" in cls:
            continue
        top2.append(nb)

    return top2


def select_one_navbox(soup: BeautifulSoup, title_query: Optional[str], index: int) -> Optional[object]:
    navboxes = select_top_level_navboxes(soup)
    print(f"DEBUG: Found {len(navboxes)} TOP-LEVEL navboxes on page.")
    if not navboxes:
        return None

    if title_query and title_query.strip():
        tq = title_query.strip().lower()
        for nb in navboxes:
            t = navbox_title_text(nb).lower()
            if tq in t:
                print(f"DEBUG: Selected navbox by title match: '{navbox_title_text(nb)}'")
                return nb
        print(f"DEBUG: No navbox title matched '{title_query}'. Falling back to index={index}.")

    idx = max(0, min(index, len(navboxes) - 1))
    chosen = navboxes[idx]
    print(f"DEBUG: Selected navbox by index={idx}: '{navbox_title_text(chosen)}'")
    return chosen


def extract_links_from_one_navbox(soup: BeautifulSoup, base_url: str, navbox_title: Optional[str], navbox_index: int) -> Set[str]:
>>>>>>> Stashed changes
    links: Set[str] = set()
    navbox = select_one_navbox(soup, navbox_title, navbox_index)
    if navbox is None:
        return links

    for a in navbox.find_all("a", href=True):
        href = a["href"]
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urljoin(base_url, href)

        if is_internal_wiki_link(href):
            links.add(href)

    return links


def titles_from_urls(urls: Set[str]) -> List[str]:
    titles: List[str] = []
    for u in urls:
        if is_internal_wiki_link(u):
            t = clean_title_from_href_path(urlparse(u).path)
            if t:
                titles.append(t)
    return sorted(set(titles))


def wikipedia_titles_to_qids(titles: List[str], lang: str = "en", batch: int = 40, sleep: float = 0.2) -> Dict[str, str]:
    qmap: Dict[str, str] = {}
    api_url = wiki_api_for_lang(lang)

    def resolve_chunk(chunk: List[str]) -> Dict[str, str]:
        if not chunk:
            return {}
        try:
            data = _mw_api_get(
                {
                    "action": "query",
                    "prop": "pageprops",
                    "ppprop": "wikibase_item",
                    "titles": "|".join(chunk),
                    "redirects": 1,
                },
                sleep=sleep,
                api_url=api_url,
                use_post=True,
            )
        except RuntimeError as e:
            msg = str(e)
            # Fallback: split long chunks when request payload/URI is too large.
            if len(chunk) > 1 and (" 414 " in f" {msg} " or " 413 " in f" {msg} " or "Too Long" in msg):
                mid = len(chunk) // 2
                out = {}
                out.update(resolve_chunk(chunk[:mid]))
                out.update(resolve_chunk(chunk[mid:]))
                return out
            raise

        out: Dict[str, str] = {}
        pages = data.get("query", {}).get("pages", [])
        for page in pages:
            title = page.get("title")
            qid = (page.get("pageprops") or {}).get("wikibase_item")
            if title and qid:
                out[title] = qid
        return out

    for i in range(0, len(titles), batch):
        chunk = titles[i:i + batch]
        qmap.update(resolve_chunk(chunk))
    return qmap


def build_sparql_for_qids(qids: List[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)

    prop_selects = []
    prop_opts = []
    for pid in ATTRIB_PROP_IDS:
        prop_selects.append(f'(GROUP_CONCAT(DISTINCT STR(?{pid}val); separator="|") AS ?{pid}_vals)')
        prop_opts.append(f'OPTIONAL {{ ?item wdt:{pid} ?{pid}val . }}')

    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>

SELECT ?item
  (SAMPLE(?label_en) AS ?label_en) (SAMPLE(?label_uk) AS ?label_uk) (SAMPLE(?label_ru) AS ?label_ru)
  (SAMPLE(?desc_en) AS ?desc_en)   (SAMPLE(?desc_uk) AS ?desc_uk)   (SAMPLE(?desc_ru) AS ?desc_ru)
  (SAMPLE(?enwiki) AS ?enwiki) (SAMPLE(?ruwiki) AS ?ruwiki) (SAMPLE(?ukwiki) AS ?ukwiki)
  (SAMPLE(?en_title) AS ?en_title) (SAMPLE(?ru_title) AS ?ru_title) (SAMPLE(?uk_title) AS ?uk_title)
  (GROUP_CONCAT(DISTINCT STR(?inst); separator="|") AS ?insts)
  {" ".join(prop_selects)}
WHERE {{
  VALUES ?item {{ {values} }}

  OPTIONAL {{ ?item rdfs:label ?label_en . FILTER(LANG(?label_en) = "en") }}
  OPTIONAL {{ ?item rdfs:label ?label_uk . FILTER(LANG(?label_uk) = "uk") }}
  OPTIONAL {{ ?item rdfs:label ?label_ru . FILTER(LANG(?label_ru) = "ru") }}
  OPTIONAL {{ ?item schema:description ?desc_en . FILTER(LANG(?desc_en) = "en") }}
  OPTIONAL {{ ?item schema:description ?desc_uk . FILTER(LANG(?desc_uk) = "uk") }}
  OPTIONAL {{ ?item schema:description ?desc_ru . FILTER(LANG(?desc_ru) = "ru") }}

  OPTIONAL {{ ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?en_title . }}
  OPTIONAL {{ ?ruwiki schema:about ?item ; schema:isPartOf <https://ru.wikipedia.org/> ; schema:name ?ru_title . }}
  OPTIONAL {{ ?ukwiki schema:about ?item ; schema:isPartOf <https://uk.wikipedia.org/> ; schema:name ?uk_title . }}

  OPTIONAL {{ ?item wdt:P31 ?inst . }}
  {" ".join(prop_opts)}
}}
GROUP BY ?item
"""
    return query


def run_sparql(query: str, retries: int = 3, backoff: float = 2.0) -> dict:
    sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent=USER_AGENT)
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQL_JSON)
    sparql.setTimeout(120)

    last_err = None
    for attempt in range(retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            last_err = e
            time.sleep(backoff ** attempt)
    raise last_err


def split_concat(s: Optional[str]) -> List[str]:
    if not s:
        return []
    parts = [p for p in s.split("|") if p]
    out: List[str] = []
    seen = set()
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def infer_category_hint(instance_qids: Set[str]) -> str:
    for cat, anchors in CATEGORY_MAP.items():
        if instance_qids & anchors:
            return cat
    return "unknown"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", default="https://en.wikipedia.org/wiki/Russo-Ukrainian_War")
    ap.add_argument("--navbox-title", default="Russo-Ukrainian war")
    ap.add_argument("--navbox-index", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.3)
    ap.add_argument("--include-categories", action="store_true",
                    help="Also collect titles from categories attached to the start page.")
    ap.add_argument("--category-depth", type=int, default=1,
                    help="Category walk depth (0 means only root categories' direct pages).")
    ap.add_argument("--category-strategy", choices=["bfs", "dfs"], default="bfs",
                    help="Category traversal strategy.")
    ap.add_argument("--category-langs", default="en,ru,uk",
                    help="Comma-separated wikipedia languages for category walking.")
    ap.add_argument("--category-keywords",
                    default=DEFAULT_CATEGORY_KEYWORDS,
                    help="Comma-separated keywords to keep subcategories during category walk.")
    ap.add_argument("--category-max-categories", type=int, default=0,
                    help="Per-language hard cap for visited categories (0 = no cap).")
    ap.add_argument("--category-max-titles", type=int, default=0,
                    help="Per-language hard cap for collected titles (0 = no cap).")
    ap.add_argument("--category-max-members-per-category", type=int, default=0,
                    help="Cap categorymembers fetched per category (0 = no cap).")
    ap.add_argument("--category-progress-every", type=int, default=25,
                    help="Print progress every N visited categories (0 = silent).")

    ap.add_argument("--out", default="data/navbox_entities.jsonl")
    ap.add_argument("--out-report", default="data/navbox_report.json")

    # Modified: debug helper
    ap.add_argument("--debug-save-html", default=None, help="Save parsed HTML to file for inspection (optional).")

    args = ap.parse_args()

    mkparents(args.out)
    mkparents(args.out_report)
    if args.debug_save_html:
        mkparents(args.debug_save_html)

    source_lang = _lang_from_start_url(args.start_url)
    source_api = wiki_api_for_lang(source_lang)
    base = f"https://{source_lang}.wikipedia.org"
    page_title = page_title_from_start_url(args.start_url)

    print(f"Fetching via MediaWiki parse ({source_lang}): {page_title}")
    html = fetch_rendered_html_via_parse(page_title, sleep=args.sleep, api_url=source_api)

    if args.debug_save_html:
        with open(args.debug_save_html, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"DEBUG: saved parsed HTML -> {args.debug_save_html}")

    soup = soup_from_html(html)

    # Extra debug if needed
    print(f"DEBUG: html contains 'navbox'? {'navbox' in html.lower()}")

    print("Step: Extracting links ONLY from ONE selected navbox...")
    links = extract_links_from_one_navbox(soup, base, args.navbox_title, args.navbox_index)
    print(f"Total links collected from selected navbox: {len(links)}")

    navbox_titles = set(titles_from_urls(links))
    category_titles_by_lang: Dict[str, Set[str]] = defaultdict(set)
    root_categories_by_lang: Dict[str, Set[str]] = defaultdict(set)
    visited_categories_by_lang: Dict[str, Set[str]] = defaultdict(set)
    category_stop_reason_by_lang: Dict[str, str] = {}

    requested_category_langs = []
    for raw in args.category_langs.split(","):
        lang = raw.strip().lower()
        if not lang:
            continue
        if lang in SUPPORTED_WIKI_LANGS:
            if lang not in requested_category_langs:
                requested_category_langs.append(lang)
        else:
            print(f"WARN: unsupported category language '{lang}', skipping.")

    if not requested_category_langs:
        requested_category_langs = [source_lang if source_lang in SUPPORTED_WIKI_LANGS else "en"]

    if args.include_categories:
        print("Step: Collecting categories + BFS/DFS walk across languages...")
        start_titles = seed_page_titles_by_lang(
            base_page_title=page_title,
            source_lang=source_lang,
            langs=requested_category_langs,
            sleep=args.sleep,
        )
        keyword_list = [k.strip() for k in args.category_keywords.split(",") if k.strip()]

        for lang in requested_category_langs:
            seed_title = start_titles.get(lang)
            if not seed_title:
                print(f"WARN: no linked start page for lang={lang}; skip category walk.")
                continue

            api_url = wiki_api_for_lang(lang)
            roots = fetch_page_categories(seed_title, sleep=args.sleep, api_url=api_url)
            root_categories_by_lang[lang] = roots
            print(f"[{lang}] root categories: {len(roots)}")

            walked = walk_categories_collect_titles(
                root_categories=roots,
                depth=max(0, args.category_depth),
                strategy=args.category_strategy,
                keywords=keyword_list,
                sleep=args.sleep,
                api_url=api_url,
                max_categories=max(0, args.category_max_categories),
                max_titles=max(0, args.category_max_titles),
                max_members_per_category=max(0, args.category_max_members_per_category),
                progress_every=max(0, args.category_progress_every),
                progress_prefix=f"[{lang}]",
            )
            category_titles_by_lang[lang] = set(walked["titles"])
            visited_categories_by_lang[lang] = set(walked["visited_categories"])
            category_stop_reason_by_lang[lang] = str(walked.get("stop_reason", "completed"))
            print(
                f"[{lang}] walk done: strategy={args.category_strategy}, depth={args.category_depth}, "
                f"visited_categories={len(visited_categories_by_lang[lang])}, "
                f"collected_titles={len(category_titles_by_lang[lang])}, "
                f"stop_reason={category_stop_reason_by_lang[lang]}"
            )

    titles_by_lang: Dict[str, Set[str]] = defaultdict(set)
    titles_by_lang[source_lang].update(navbox_titles)
    for lang, ts in category_titles_by_lang.items():
        titles_by_lang[lang].update(ts)

    total_titles = sum(len(v) for v in titles_by_lang.values())
    print(f"Unique titles by lang: en={len(titles_by_lang.get('en', set()))}, "
          f"ru={len(titles_by_lang.get('ru', set()))}, uk={len(titles_by_lang.get('uk', set()))}, "
          f"total(sum per lang)={total_titles}")

    if total_titles == 0:
        print("Stopping. No titles found.")
        print("TIP: run again with --debug-save-html debug_parsed.html and open it to see what was fetched.")
        return

    title2qid_by_lang: Dict[str, Dict[str, str]] = {}
    qid2sources = defaultdict(set)
    qid2titles_by_lang = {lang: defaultdict(set) for lang in SUPPORTED_WIKI_LANGS}
    qids_set: Set[str] = set()

    for lang, tset in titles_by_lang.items():
        if not tset:
            continue
        t2q = wikipedia_titles_to_qids(sorted(tset), lang=lang, sleep=args.sleep)
        title2qid_by_lang[lang] = t2q
        qids_set |= set(t2q.values())
        print(f"[{lang}] resolved titles -> qids: {len(t2q)} / {len(tset)}")

        for t, q in t2q.items():
            qid2titles_by_lang.setdefault(lang, defaultdict(set))
            qid2titles_by_lang[lang][q].add(t)
            if lang == source_lang and t in navbox_titles:
                qid2sources[q].add("navbox")
            if t in category_titles_by_lang.get(lang, set()):
                qid2sources[q].add(f"category_{lang}")

    missing = sorted(list(ENSURE_QIDS - qids_set))
    if missing:
        print(f"Ensuring QIDs (missing -> add): {missing}")
        qids_set |= ENSURE_QIDS

    qids = sorted(qids_set)
    print(f"Resolved QIDs: {len(qids)}")

    entities: Dict[str, dict] = {}
    BATCH = 120
    for i in range(0, len(qids), BATCH):
        batch_qids = qids[i:i + BATCH]
        query = build_sparql_for_qids(batch_qids)
        data = run_sparql(query)

        for b in data["results"]["bindings"]:
            item_uri = b.get("item", {}).get("value")
            qid = qid_from_uri(item_uri)
            if not qid:
                continue

            insts = set(qid_from_uri(x) or x for x in split_concat(b.get("insts", {}).get("value")))

            raw_attrib_qids = {}
            for pid in ATTRIB_PROP_IDS:
                vals = split_concat(b.get(f"{pid}_vals", {}).get("value"))
                qset = set()
                for v in vals:
                    q = qid_from_uri(v) if v.startswith("http") else v
                    if q:
                        qset.add(q)
                raw_attrib_qids[pid] = sorted(qset)

            rec = {
                "qid": qid,
                "uri": item_uri or f"http://www.wikidata.org/entity/{qid}",
                "source": {
                    "type": "wikipedia_navboxes",
                    "page": args.start_url,
                    "hint": infer_category_hint(insts),
                    "navbox_title_query": args.navbox_title,
                    "navbox_index": args.navbox_index,
                },
                "labels": {
                    "en": b.get("label_en", {}).get("value"),
                    "uk": b.get("label_uk", {}).get("value"),
                    "ru": b.get("label_ru", {}).get("value"),
                },
                "descriptions": {
                    "en": b.get("desc_en", {}).get("value"),
                    "uk": b.get("desc_uk", {}).get("value"),
                    "ru": b.get("desc_ru", {}).get("value"),
                },
                "aliases": {"en": [], "uk": [], "ru": []},
                "sitelinks": {
                    "enwiki": b.get("enwiki", {}).get("value"),
                    "ruwiki": b.get("ruwiki", {}).get("value"),
                    "ukwiki": b.get("ukwiki", {}).get("value"),
                },
                "wiki_titles": {
                    "en": b.get("en_title", {}).get("value"),
                    "ru": b.get("ru_title", {}).get("value"),
                    "uk": b.get("uk_title", {}).get("value"),
                },
                "instance_of": sorted(insts),
                "raw_attrib_qids": raw_attrib_qids,
            }
            entities[qid] = rec

        time.sleep(0.05)

    # Fill wiki_titles from title->qid mappings gathered per language.
    for qid, rec in entities.items():
        for lang in SUPPORTED_WIKI_LANGS:
            tset = qid2titles_by_lang.get(lang, {}).get(qid, set())
            if tset:
                rec["wiki_titles"][lang] = sorted(tset)[0]
        fallback = {"ensure_qid"} if qid in ENSURE_QIDS else {"unknown"}
        rec["source"]["collection_paths"] = sorted(qid2sources.get(qid, fallback))

    with open(args.out, "w", encoding="utf-8") as f:
        for qid in sorted(entities.keys()):
            f.write(json.dumps(entities[qid], ensure_ascii=False) + "\n")
    print(f"[done] wrote {args.out} ({len(entities)} records)")

    report = {
        "start_url": args.start_url,
        "page_title": page_title,
        "navbox_title_query": args.navbox_title,
        "navbox_index": args.navbox_index,
        "total_links_from_selected_navbox": len(links),
        "source_lang": source_lang,
        "include_categories": bool(args.include_categories),
        "category_depth": int(args.category_depth),
        "category_strategy": args.category_strategy,
        "category_langs_requested": requested_category_langs,
        "category_limits": {
            "max_categories": int(args.category_max_categories),
            "max_titles": int(args.category_max_titles),
            "max_members_per_category": int(args.category_max_members_per_category),
        },
        "root_categories_by_lang": {lang: len(root_categories_by_lang.get(lang, set())) for lang in SUPPORTED_WIKI_LANGS},
        "visited_categories_by_lang": {lang: len(visited_categories_by_lang.get(lang, set())) for lang in SUPPORTED_WIKI_LANGS},
        "category_stop_reason_by_lang": {lang: category_stop_reason_by_lang.get(lang, "not_run") for lang in SUPPORTED_WIKI_LANGS},
        "unique_titles_navbox_source_lang": len(navbox_titles),
        "unique_titles_by_lang": {lang: len(titles_by_lang.get(lang, set())) for lang in SUPPORTED_WIKI_LANGS},
        "resolved_titles_by_lang": {lang: len(title2qid_by_lang.get(lang, {})) for lang in SUPPORTED_WIKI_LANGS},
        "unique_titles_total_sum_by_lang": total_titles,
        "resolved_qids": len(entities),
        "note": "Single-navbox harvest (unified schema)."
    }

    with open(args.out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"[done] wrote {args.out_report}")


if __name__ == "__main__":
    main()
