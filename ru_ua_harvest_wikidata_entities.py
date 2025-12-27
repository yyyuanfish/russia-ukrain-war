#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Harvest Russia–Ukraine War-related entities from Wikidata (EN/UK/RU).

Scope kept equivalent to your original script:
- people: humans with P607 in {Russo-Ukrainian War, 2022 invasion}
- events: battles/attacks/mass killings/conflicts that are part of those wars (P361)
- orgs: org/unit/government that participated in or had a participant-in relation to those wars (P607/P1344)
- policies: law/policy/sanction/resolution where main subject is those wars (P921)
- media_narratives: propaganda/conspiracy where main subject is those wars (P921)

Key change:
- Output schema unified with the navbox harvester.
- Includes raw attribution properties (P27/P17/P495/P159/P131/P276/P19/P740/P551) for downstream classification.
- Attribution (Russian/Ukraine/mixed/other) is NOT decided here; use ru_ua_classify_entities.py

Deps:
  pip install SPARQLWrapper
Optional:
  pip install requests  (not required here)

Usage:
  python ru_ua_harvest_wikidata_entities.py --out data/wd_entities.jsonl --array data/wd_entities.json
  python ru_ua_harvest_wikidata_entities.py --out data/wd_entities.jsonl --no-aliases --limit 200
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from typing import Dict, List, Optional

try:
    from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON
except Exception:
    print("Please install SPARQLWrapper: pip install SPARQLWrapper", file=sys.stderr)
    raise

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "ru-ua-wikidata-harvester/3.0 (research; unified-schema)"

# Canonical war QIDs
QID_RUSSO_UKR_WAR = "Q15860072"   # Russo-Ukrainian War (2014–present)
QID_2022_INVASION = "Q113149305"  # 2022 Russian invasion of Ukraine

# Type anchors
QID_HUMAN = "Q5"
QID_BATTLE = "Q178561"
QID_MILITARY_CONFLICT = "Q180684"
QID_ATTACK = "Q645883"
QID_MASS_KILLING = "Q167442"
QID_MILITARY_UNIT = "Q176799"
QID_GOVERNMENT = "Q7188"
QID_ORGANIZATION = "Q43229"
QID_LAW = "Q820655"
QID_PUBLIC_POLICY = "Q7163"
QID_ECON_SANCTION = "Q618779"
QID_RESOLUTION = "Q182994"
QID_CONSPIRACY_THEORY = "Q17379835"
QID_PROPAGANDA = "Q215080"


# QIDs that must exist in output (only add if missing)
ENSURE_QIDS = {
    "Q16150196",  # Donetsk People's Republic
    "Q16746854",  # Luhansk People's Republic
    "Q16912926",  # Novorossiya
    "Q15925436",  # Republic of Crimea
}


# Properties we keep for downstream RU/UA attribution classification
ATTRIB_PROPS = {
    "P27": "citizenship",              # citizenship
    "P17": "country",                  # country
    "P495": "origin",                  # country of origin
    "P159": "hq",                      # headquarters location
    "P131": "located_in_admin",        # located in the administrative territorial entity
    "P276": "location",                # location
    "P19": "place_of_birth",           # place of birth
    "P740": "location_of_formation",   # location of formation
    "P551": "residence",               # residence
}

PREFIXES = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
"""

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
            sleep_s = backoff ** attempt
            time.sleep(sleep_s)
    raise last_err

def _split_concat(s: Optional[str]) -> List[str]:
    if not s:
        return []
    parts = [p for p in s.split("|") if p]
    # de-dupe while preserving order
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def _qid_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]

def collect_aliases(item_uri: str, langs=("en", "uk", "ru")) -> Dict[str, List[str]]:
    q = PREFIXES + f"""
SELECT ?alias ?lang WHERE {{
  VALUES ?item {{ <{item_uri}> }}
  ?item skos:altLabel ?alias .
  BIND(LANG(?alias) AS ?lang)
  FILTER(?lang IN ({",".join(f'"{l}"' for l in langs)}))
}}
"""
    res = run_sparql(q)
    aliases = defaultdict(list)
    for b in res["results"]["bindings"]:
        alias = b["alias"]["value"]
        lang = b["lang"]["value"]
        aliases[lang].append(alias)
    return {k: sorted(set(v)) for k, v in aliases.items()}

def build_grouped_query(where_block: str) -> str:
    """
    To avoid row explosion for multi-valued properties, we aggregate via GROUP_CONCAT.
    We keep the same language fields and sitelinks, and gather instance_of + attribution props.
    """
    # Build OPTIONAL lines for attrib props
    opt_props = []
    for pid in ATTRIB_PROPS.keys():
        opt_props.append(f'OPTIONAL {{ ?item wdt:{pid} ?{pid}val . }}')
    opt_props_str = "\n  ".join(opt_props)

    # Build SELECT aggregates
    select_aggs = []
    select_aggs.append("(SAMPLE(?label_en) AS ?label_en)")
    select_aggs.append("(SAMPLE(?label_uk) AS ?label_uk)")
    select_aggs.append("(SAMPLE(?label_ru) AS ?label_ru)")
    select_aggs.append("(SAMPLE(?desc_en) AS ?desc_en)")
    select_aggs.append("(SAMPLE(?desc_uk) AS ?desc_uk)")
    select_aggs.append("(SAMPLE(?desc_ru) AS ?desc_ru)")
    select_aggs.append("(SAMPLE(?enwiki) AS ?enwiki)")
    select_aggs.append("(SAMPLE(?ruwiki) AS ?ruwiki)")
    select_aggs.append("(SAMPLE(?ukwiki) AS ?ukwiki)")
    select_aggs.append("(SAMPLE(?en_title) AS ?en_title)")
    select_aggs.append("(SAMPLE(?ru_title) AS ?ru_title)")
    select_aggs.append("(SAMPLE(?uk_title) AS ?uk_title)")
    select_aggs.append('(GROUP_CONCAT(DISTINCT STR(?inst); separator="|") AS ?insts)')

    for pid in ATTRIB_PROPS.keys():
        select_aggs.append(f'(GROUP_CONCAT(DISTINCT STR(?{pid}val); separator="|") AS ?{pid}_vals)')

    return PREFIXES + f"""
SELECT ?item
  {" ".join(select_aggs)}
WHERE {{
  {where_block}

  # Labels/descriptions
  ?item rdfs:label ?label_en . FILTER(LANG(?label_en) = "en")
  OPTIONAL {{ ?item rdfs:label ?label_uk . FILTER(LANG(?label_uk) = "uk") }}
  OPTIONAL {{ ?item rdfs:label ?label_ru . FILTER(LANG(?label_ru) = "ru") }}
  OPTIONAL {{ ?item schema:description ?desc_en . FILTER(LANG(?desc_en) = "en") }}
  OPTIONAL {{ ?item schema:description ?desc_uk . FILTER(LANG(?desc_uk) = "uk") }}
  OPTIONAL {{ ?item schema:description ?desc_ru . FILTER(LANG(?desc_ru) = "ru") }}

  # Sitelinks + titles
  OPTIONAL {{ ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?en_title . }}
  OPTIONAL {{ ?ruwiki schema:about ?item ; schema:isPartOf <https://ru.wikipedia.org/> ; schema:name ?ru_title . }}
  OPTIONAL {{ ?ukwiki schema:about ?item ; schema:isPartOf <https://uk.wikipedia.org/> ; schema:name ?uk_title . }}

  # instance-of
  OPTIONAL {{ ?item wdt:P31 ?inst . }}

  # attribution props
  {opt_props_str}
}}
GROUP BY ?item
"""

def query_people(limit: Optional[int] = None) -> dict:
    where_block = f"""
  ?item wdt:P31 wd:{QID_HUMAN} .
  VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
  ?item wdt:P607 ?war .
"""
    q = build_grouped_query(where_block)
    if limit:
        q += f"\nLIMIT {int(limit)}"
    return run_sparql(q)

def query_events(limit: Optional[int] = None) -> dict:
    where_block = f"""
  VALUES ?type {{ wd:{QID_BATTLE} wd:{QID_MILITARY_CONFLICT} wd:{QID_ATTACK} wd:{QID_MASS_KILLING} }}
  ?item wdt:P31/wdt:P279* ?type .
  VALUES ?parent {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
  ?item wdt:P361 ?parent .
"""
    q = build_grouped_query(where_block)
    if limit:
        q += f"\nLIMIT {int(limit)}"
    return run_sparql(q)

def query_orgs(limit: Optional[int] = None) -> dict:
    where_block = f"""
  VALUES ?otype {{ wd:{QID_MILITARY_UNIT} wd:{QID_ORGANIZATION} wd:{QID_GOVERNMENT} }}
  ?item wdt:P31/wdt:P279* ?otype .
  VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
  {{ ?item wdt:P607 ?war }} UNION {{ ?item wdt:P1344 ?war }} .
"""
    q = build_grouped_query(where_block)
    if limit:
        q += f"\nLIMIT {int(limit)}"
    return run_sparql(q)

def query_policies(limit: Optional[int] = None) -> dict:
    where_block = f"""
  VALUES ?ptype {{ wd:{QID_LAW} wd:{QID_PUBLIC_POLICY} wd:{QID_ECON_SANCTION} wd:{QID_RESOLUTION} }}
  ?item wdt:P31/wdt:P279* ?ptype .
  VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
  ?item wdt:P921 ?war .
"""
    q = build_grouped_query(where_block)
    if limit:
        q += f"\nLIMIT {int(limit)}"
    return run_sparql(q)

def query_media_narratives(limit: Optional[int] = None) -> dict:
    where_block = f"""
  VALUES ?ntype {{ wd:{QID_CONSPIRACY_THEORY} wd:{QID_PROPAGANDA} }}
  ?item wdt:P31/wdt:P279* ?ntype .
  VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
  ?item wdt:P921 ?war .
"""
    q = build_grouped_query(where_block)
    if limit:
        q += f"\nLIMIT {int(limit)}"
    return run_sparql(q)

# 新增一个“按固定 QID 拉取”的 query 函数
def query_by_qids(qids: List[str], limit: Optional[int] = None) -> dict:
    values = " ".join(f"wd:{q}" for q in qids)
    where_block = f"""
  VALUES ?item {{ {values} }}
"""
    q = build_grouped_query(where_block)
    if limit:
        q += f"\nLIMIT {int(limit)}"
    return run_sparql(q)

def bindings_to_records(bindings: List[dict], category_hint: str) -> List[dict]:
    out = []
    for b in bindings:
        item_uri = b.get("item", {}).get("value")
        if not item_uri:
            continue
        qid = _qid_from_uri(item_uri)
        insts = _split_concat(b.get("insts", {}).get("value"))

        raw_attrib_qids = {}
        for pid in ATTRIB_PROPS.keys():
            vals = _split_concat(b.get(f"{pid}_vals", {}).get("value"))
            # normalize to QIDs where possible
            qids = []
            for v in vals:
                q = _qid_from_uri(v) if v.startswith("http") else v
                if q:
                    qids.append(q)
            raw_attrib_qids[pid] = sorted(set(qids))

        rec = {
            "qid": qid,
            "uri": item_uri,
            "source": {
                "type": "wikidata_sparql",
                "page": WIKIDATA_SPARQL,
                "hint": category_hint,
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
            "aliases": {"en": [], "uk": [], "ru": []},  # filled later unless --no-aliases
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
            "instance_of": sorted(set(_qid_from_uri(x) or x for x in insts)),
            "raw_attrib_qids": raw_attrib_qids,
        }
        out.append(rec)
    return out

def dedupe_by_qid(records: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for r in records:
        if r["qid"] in seen:
            continue
        seen.add(r["qid"])
        out.append(r)
    return out

def enrich_aliases(records: List[dict], langs=("en", "uk", "ru")) -> None:
    for i, rec in enumerate(records, 1):
        try:
            rec["aliases"] = collect_aliases(rec["uri"], langs=langs)
        except Exception:
            rec["aliases"] = {"en": [], "uk": [], "ru": []}
        # gentle pacing
        if i % 25 == 0:
            time.sleep(0.2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="wd_ru_ua_entities.jsonl", help="Output JSONL path")
    ap.add_argument("--array", default="wd_ru_ua_entities.json", help="Output JSON array path")
    ap.add_argument("--limit", type=int, default=None, help="Optional per-query LIMIT (debug)")
    ap.add_argument("--no-aliases", action="store_true", help="Skip alias enrichment (faster)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    all_records: List[dict] = []

    print("Querying people...")
    people = query_people(limit=args.limit)
    all_records += bindings_to_records(people["results"]["bindings"], "person")

    print("Querying events...")
    events = query_events(limit=args.limit)
    all_records += bindings_to_records(events["results"]["bindings"], "event")

    print("Querying organizations...")
    orgs = query_orgs(limit=args.limit)
    all_records += bindings_to_records(orgs["results"]["bindings"], "organization")

    print("Querying policies...")
    policies = query_policies(limit=args.limit)
    all_records += bindings_to_records(policies["results"]["bindings"], "policy")

    print("Querying media narratives...")
    narratives = query_media_narratives(limit=args.limit)
    all_records += bindings_to_records(narratives["results"]["bindings"], "media_narrative")

    all_records = dedupe_by_qid(all_records)
    print(f"Deduped total: {len(all_records)}")


    # Ensure specific QIDs exist in output (only add if missing)
    present = {r["qid"] for r in all_records}
    missing = sorted(list(ENSURE_QIDS - present))
    if missing:
        print(f"Ensuring QIDs (missing -> add): {missing}")
        extra = query_by_qids(missing, limit=None)
        # use a reasonable category_hint; these are political entities / org-like
        all_records += bindings_to_records(extra["results"]["bindings"], "organization")
        all_records = dedupe_by_qid(all_records)
        print(f"After ensure, total: {len(all_records)}")
    else:
        print("Ensuring QIDs: all present (no changes).")

    if not args.no_aliases:
        print("Enriching aliases (EN/UK/RU)...")
        enrich_aliases(all_records, langs=("en", "uk", "ru"))

    with open(args.out, "w", encoding="utf-8") as f:
        for r in all_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Wrote JSONL: {args.out} ({len(all_records)} records)")

    with open(args.array, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    print(f"Wrote JSON array: {args.array}")

if __name__ == "__main__":
    main()
