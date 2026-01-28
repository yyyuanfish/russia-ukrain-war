#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Harvest entities ONLY from bottom navboxes (footer boxes) of an EN Wikipedia page,
then resolve to Wikidata QIDs, then fetch labels/descriptions/sitelinks/instance_of
and attribution-related properties.

Scope kept equivalent to your current navbox-only script:
- Scrape only elements matching '.navbox'
- Parse with lxml
- Keep only internal /wiki/ links, mainspace only (no namespaces with ':')
- Resolve title->QID via MediaWiki API
- SPARQL fetch for core fields + attribution props

Key change:
- Output schema unified with the Wikidata harvester.
- NO Russian/Ukraine/mixed/other decision here (use ru_ua_classify_entities.py)

Deps:
  pip install requests beautifulsoup4 lxml SPARQLWrapper

Usage:
  python ru_ua_harvest_wikipedia_navboxes.py \
    --start-url https://en.wikipedia.org/wiki/Russo-Ukrainian_War \
    --out data/navbox_entities.jsonl \
    --out-report data/navbox_report.json
"""

import argparse
import json
import os
import sys
import time
from urllib.parse import urlparse, urljoin, unquote
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "ru-ua-wikipedia-navbox-harvester/3.0 (navbox-only; unified-schema)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json"}

ALLOWED_NAMESPACES = {""}  # mainspace only

# Type anchors for category_hint (same as your logic)
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

# Attribution props to keep (same keys as the Wikidata harvester)
ATTRIB_PROP_IDS = ["P27", "P17", "P495", "P159", "P131", "P276", "P19", "P740", "P551"]

# QIDs that must exist in output (only add if missing)
ENSURE_QIDS = {
    "Q16150196",  # Donetsk People's Republic
    "Q16746854",  # Luhansk People's Republic
    "Q16912926",  # Novorossiya
    "Q15925436",  # Republic of Crimea
}

def mkparents(path: str):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)

def qid_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]

def clean_title_from_href(href: str) -> Optional[str]:
    try:
        href = href.split("#", 1)[0]
        if not href.startswith("/wiki/"):
            return None
        title = href[len("/wiki/"):]
        title = unquote(title)
        if ":" in title:
            ns = title.split(":", 1)[0]
            if ns not in ALLOWED_NAMESPACES:
                return None
        if title.startswith("Main_Page"):
            return None
        return title
    except Exception:
        return None

def is_internal_wiki_link(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.netloc.endswith("wikipedia.org") and u.path.startswith("/wiki/")
    except Exception:
        return False

def fetch_html(url: str) -> BeautifulSoup:
    print(f"Fetching: {url}")
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    try:
        return BeautifulSoup(r.text, "lxml")
    except Exception:
        print("Error: lxml missing. Please run: pip install lxml")
        sys.exit(1)

def extract_links_from_navboxes(soup: BeautifulSoup, base_url: str) -> Set[str]:
    # maybe we could take not all navboxes... some look a bit unrelated in the end ("Russo-Ukrainian war" and "Russo-Ukrainian War (2022-present)" are related, but "Links to related articles" go a bit too much astray)
    links: Set[str] = set()
    navboxes = soup.select(".navbox")
    print(f"DEBUG: Found {len(navboxes)} navbox elements (footer boxes).")

    for navbox in navboxes:
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
    titles = []
    for u in urls:
        if is_internal_wiki_link(u):
            title = clean_title_from_href(urlparse(u).path)
            if title:
                titles.append(title)
    return sorted(set(titles))

def wikipedia_titles_to_qids(titles: List[str], batch: int = 40) -> Dict[str, str]:
    qmap: Dict[str, str] = {}
    api = "https://en.wikipedia.org/w/api.php"

    for i in range(0, len(titles), batch):
        chunk = titles[i:i + batch]
        params = {
            "action": "query",
            "format": "json",
            "prop": "pageprops",
            "ppprop": "wikibase_item",
            "titles": "|".join(chunk),
        }
        try:
            r = requests.get(api, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            pages = data.get("query", {}).get("pages", {})
            for _, page in pages.items():
                title = page.get("title")
                qid = page.get("pageprops", {}).get("wikibase_item")
                if title and qid:
                    qmap[title] = qid
        except Exception as e:
            print(f"Warning: title->qid batch failed: {e}")
        time.sleep(0.1)
    return qmap

def build_sparql_for_qids(qids: List[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    # Keep row explosion minimal: GROUP_CONCAT where needed
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

  OPTIONAL {{ ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . }}
  OPTIONAL {{ ?ruwiki schema:about ?item ; schema:isPartOf <https://ru.wikipedia.org/> . }}
  OPTIONAL {{ ?ukwiki schema:about ?item ; schema:isPartOf <https://uk.wikipedia.org/> . }}

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
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def infer_category_hint(instance_qids: Set[str]) -> str:
    for cat, anchors in CATEGORY_MAP.items():
        if instance_qids & anchors:
            return cat
    return "unknown"

def load_prior_qids(prior_path: Optional[str]) -> Set[str]:
    qids: Set[str] = set()
    if not prior_path or not os.path.exists(prior_path):
        return qids
    try:
        if prior_path.endswith(".jsonl"):
            with open(prior_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        qids.add(json.loads(line).get("qid"))
        else:
            data = json.load(open(prior_path, "r", encoding="utf-8"))
            if isinstance(data, list):
                for item in data:
                    qids.add(item.get("qid"))
    except Exception:
        pass
    return qids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", default="https://en.wikipedia.org/wiki/Russo-Ukrainian_War",
                    help="EN Wikipedia page URL")
    ap.add_argument("--out", default="navbox_ru_ua_entities.jsonl", help="Output JSONL")
    ap.add_argument("--out-report", default="navbox_report.json", help="Output report JSON")
    ap.add_argument("--prior-entities", default=None, help="Prior JSONL/JSON for overlap stats")
    ap.add_argument("--limit-titles", type=int, default=None, help="Debug limit titles")
    args = ap.parse_args()

    mkparents(args.out)
    mkparents(args.out_report)

    soup = fetch_html(args.start_url)
    base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(args.start_url))

    print("Step: Extracting links ONLY from bottom Navboxes...")
    links = extract_links_from_navboxes(soup, base)
    print(f"Total links collected from boxes: {len(links)}")

    titles = titles_from_urls(links)
    if args.limit_titles:
        titles = titles[:args.limit_titles]
    print(f"Unique enwiki titles: {len(titles)}")

    if not titles:
        print("Stopping. No titles found (check lxml installation or page structure).")
        return

    title2qid = wikipedia_titles_to_qids(titles)

    # use a set so we can check + add missing QIDs
    qids_set = set(title2qid.values())

    missing = sorted(list(ENSURE_QIDS - qids_set))
    if missing:
        print(f"Ensuring QIDs (missing -> add): {missing}")
        qids_set |= ENSURE_QIDS
    else:
        print("Ensuring QIDs: all present (no changes).")

    qids = sorted(qids_set)
    print(f"Resolved QIDs: {len(qids)}")

    

    # SPARQL fetch in batches
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

            # Build record (unified schema)
            rec = {
                "qid": qid,
                "uri": item_uri or f"http://www.wikidata.org/entity/{qid}",
                "source": {
                    "type": "wikipedia_navboxes",
                    "page": args.start_url,
                    "hint": infer_category_hint(insts),
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
                "aliases": {"en": [], "uk": [], "ru": []},  # navbox script does not enrich aliases (keeps scope same)
                "sitelinks": {
                    "enwiki": b.get("enwiki", {}).get("value"),
                    "ruwiki": b.get("ruwiki", {}).get("value"),
                    "ukwiki": b.get("ukwiki", {}).get("value"),
                },
                "wiki_titles": {
                    "en": None,
                    "ru": None,
                    "uk": None,
                },
                "instance_of": sorted(insts),
                "raw_attrib_qids": raw_attrib_qids,
            }
            entities[qid] = rec

        time.sleep(0.1)

    # Fill wiki_titles.en from the title2qid mapping (reverse lookup)
    qid2title = {}
    for t, q in title2qid.items():
        qid2title[q] = t
    for qid, rec in entities.items():
        rec["wiki_titles"]["en"] = qid2title.get(qid)

    # Write JSONL
    with open(args.out, "w", encoding="utf-8") as f:
        for qid in sorted(entities.keys()):
            f.write(json.dumps(entities[qid], ensure_ascii=False) + "\n")
    print(f"Wrote entities: {args.out} ({len(entities)} records)")

    # Report (kept from your style; not required by classifier, but useful)
    prior_qids = load_prior_qids(args.prior_entities)
    new_qids_set = set(entities.keys())
    overlap_count = len(new_qids_set & prior_qids)

    def lang_cov(lang: str) -> int:
        return sum(
            1 for e in entities.values()
            if (e["labels"].get(lang) or e["descriptions"].get(lang))
        )

    cov_stats = {lang: lang_cov(lang) for lang in ("en", "uk", "ru")}

    cat_counts = {}
    for e in entities.values():
        cat = e["source"]["hint"] or "unknown"
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    summary = {
        "start_url": args.start_url,
        "total_internal_links": len(links),
        "unique_titles": len(titles),
        "resolved_qids": len(entities),
        "overlap_with_prior": overlap_count,
        "overlap_rate": (overlap_count / len(entities)) if entities else 0.0,
        "language_coverage_nonempty_label_or_desc": cov_stats,
        "category_hint_counts": cat_counts,
        "note": "Navboxes-only harvest. Attribution is produced by ru_ua_classify_entities.py."
    }

    with open(args.out_report, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Summary Report:")
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
