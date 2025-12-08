#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Harvest Russia–Ukraine War keywords from Wikidata in EN/UK/RU.
Categories: people, events, organizations, policies, media_narratives.
Outputs: JSONL + JSON with fields (qid, category, labels, descriptions, aliases, sitelinks, claims).
Requires: SPARQLWrapper, requests (optional).

Usage:
  python ru_ua_wikidata_harvest.py --out data/ru_ua_keywords.jsonl --array data/ru_ua_keywords.json

Example to install deps:
  pip install -r requirements.txt
"""

import argparse
import json
import os
import sys
from collections import defaultdict, OrderedDict

try:
    from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON
except Exception as e:
    print("Please install SPARQLWrapper: pip install SPARQLWrapper", file=sys.stderr)
    raise

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

# Canonical QIDs (verified as of 2025-10):
QID_RUSSO_UKR_WAR = "Q15860072"      # Russo-Ukrainian War (2014–present)
QID_2022_INVASION = "Q113149305"     # 2022 Russian invasion of Ukraine (full-scale)
# 限定相关性（确保选出的内容确实属于这些战争）

# Useful instance-of QIDs:
# —— Canonical “instance-of / subclass-of” QIDs used for filtering entity types ——
# Tip: queries usually use `wdt:P31/wdt:P279* ?type` to include subclasses

QID_HUMAN = "Q5"                      # Human
# Meaning: Any individual person (human beings, biographies, politicians, journalists, etc.)
# Usage: Base type for "people" category; combine with P607 (conflict participated in).
# Example: wd:Q42 (Douglas Adams), wd:Q298 (Vladimir Putin)

QID_BATTLE = "Q178561"                # Battle
QID_MILITARY_CONFLICT = "Q180684"     # Military conflict
QID_ATTACK = "Q645883"                # Violent attack
QID_MASS_KILLING = "Q167442"          # Mass killing / Massacre
QID_MILITARY_UNIT = "Q176799"         # Military unit
QID_GOVERNMENT = "Q7188"              # Government
QID_ORGANIZATION = "Q43229"           # Organization
QID_LAW = "Q820655"                   # Law
QID_PUBLIC_POLICY = "Q7163"           # Public policy
QID_ECON_SANCTION = "Q618779"         # Economic sanction
QID_RESOLUTION = "Q182994"            # Resolution
QID_CONSPIRACY_THEORY = "Q17379835"   # Conspiracy theory
QID_PROPAGANDA = "Q215080"            # Propaganda


PREFIXES = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
"""
# SPARQL 前缀：写查询时可以用缩写（比如 wd:Q5 表示 Q5 这个实体

def run_sparql(query: str, limit: int = None):
    sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent="ru-ua-keywords-harvester/1.0 (research)")
    if limit is not None:
        query += f"\nLIMIT {int(limit)}"
    print(query)
    sparql.setQuery(query)

    sparql.setReturnFormat(SPARQL_JSON) #set return format json
    return sparql.query().convert()

def normalize_binding(b):
    def get(v):
        return b.get(v, {}).get("value")
    return get

def collect_aliases(item_uri: str, langs=("en","uk","ru")):
    """
    In SPARQL we can fetch skos:altLabel per language using a separate query for an item.
    altlabel: 备用名称、俗称、缩写、历史名称等；跟主名（常说的 label / skos:prefLabel）相对
    """
    q = PREFIXES + """
    SELECT ?alias ?lang WHERE {
      VALUES ?item { %s }
      ?item skos:altLabel ?alias .
      BIND(LANG(?alias) AS ?lang)
      FILTER(?lang IN (%s))
    }
    """ % (f"<{item_uri}>", ",".join(f'"{l}"' for l in langs))
    res = run_sparql(q)
    aliases = defaultdict(list)
    for b in res["results"]["bindings"]:
        alias = b["alias"]["value"]
        lang = b["lang"]["value"]
        aliases[lang].append(alias)
    return {k: sorted(set(v)) for k, v in aliases.items()}

def base_select_block():
    return """
    ?item rdfs:label ?label_en . FILTER(LANG(?label_en) = "en")
    OPTIONAL { ?item rdfs:label ?label_uk . FILTER(LANG(?label_uk) = "uk") }
    OPTIONAL { ?item rdfs:label ?label_ru . FILTER(LANG(?label_ru) = "ru") }
    OPTIONAL { ?item schema:description ?desc_en . FILTER(LANG(?desc_en) = "en") }
    OPTIONAL { ?item schema:description ?desc_uk . FILTER(LANG(?desc_uk) = "uk") }
    OPTIONAL { ?item schema:description ?desc_ru . FILTER(LANG(?desc_ru) = "ru") }
    OPTIONAL { ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?en_title . }
    OPTIONAL { ?ruwiki schema:about ?item ; schema:isPartOf <https://ru.wikipedia.org/> ; schema:name ?ru_title . }
    OPTIONAL { ?ukwiki schema:about ?item ; schema:isPartOf <https://uk.wikipedia.org/> ; schema:name ?uk_title . }
    OPTIONAL { ?inst rdfs:label ?inst_en . FILTER(LANG(?inst_en) = "en") }
    """
# 这段在所有查询里都会复用：
# 英/乌/俄 标签（label）
# 英/乌/俄 描述（description）
# 三种语言的 维基百科 sitelink + 标题（通过 schema:about ）
# ?inst：用于拿到条目的一个“实例（instance of）”的英文名（若有）


def select_vars():
    return """
    ?item ?label_en ?label_uk ?label_ru ?desc_en ?desc_uk ?desc_ru
    ?enwiki ?ruwiki ?ukwiki ?en_title ?ru_title ?uk_title ?inst ?inst_en
    """

def query_people(limit=None):
    q = PREFIXES + f"""
    SELECT {select_vars()} WHERE {{
      ?item wdt:P31 wd:{QID_HUMAN} . #要求这个人参与过我们定义的两场战争之一 → 把人物与战争强绑定
      VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}  #声明一个“战争集合”变量 ?war，只允许是俄乌战争（2014–）或2022 全面入侵这两项之一
      ?item wdt:P607 ?war .
      OPTIONAL {{ ?item wdt:P106 ?occupation . }}
      OPTIONAL {{ ?item wdt:P31 ?inst . }}
      {base_select_block()}
    }}
    """
    return run_sparql(q, limit)
# 关键条件：wdt:P31 wd:Q5（人），且 wdt:P607 指向我们的战争 QID（参与的冲突）

def query_events(limit=None):
    # Battles, attacks, mass killings, general conflicts that are part of the war(s)
    # 声明一个事件类型集合 ?type：包括“战役/军事冲突/袭击/大规模杀戮”四类的 QID
    q = PREFIXES + f"""
    SELECT {select_vars()} WHERE {{
      VALUES ?type {{ wd:{QID_BATTLE} wd:{QID_MILITARY_CONFLICT} wd:{QID_ATTACK} wd:{QID_MASS_KILLING} }}
      ?item wdt:P31/wdt:P279* ?type .
      VALUES ?parent {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
      ?item wdt:P361 ?parent .
      OPTIONAL {{ ?item wdt:P31 ?inst . }}
      {base_select_block()}
    }}
    """
    return run_sparql(q, limit)

def query_orgs(limit=None):
    # Organizations & units that participated in the war(s)
    q = PREFIXES + f"""
    SELECT {select_vars()} WHERE {{
      VALUES ?otype {{ wd:{QID_MILITARY_UNIT} wd:{QID_ORGANIZATION} wd:{QID_GOVERNMENT} }}
      ?item wdt:P31/wdt:P279* ?otype .
      VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
      {{ ?item wdt:P607 ?war }} UNION {{ ?item wdt:P1344 ?war }}  # conflict OR participant of
      OPTIONAL {{ ?item wdt:P31 ?inst . }}
      {base_select_block()}
    }}
    """
    return run_sparql(q, limit)

def query_policies(limit=None):
    # Laws, policies, sanctions, resolutions with main subject the war(s)
    q = PREFIXES + f"""
    SELECT {select_vars()} WHERE {{
      VALUES ?ptype {{ wd:{QID_LAW} wd:{QID_PUBLIC_POLICY} wd:{QID_ECON_SANCTION} wd:{QID_RESOLUTION} }}
      ?item wdt:P31/wdt:P279* ?ptype .
      VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
      ?item wdt:P921 ?war .   # main subject
      OPTIONAL {{ ?item wdt:P31 ?inst . }}
      {base_select_block()}
    }}
    """
    return run_sparql(q, limit)

def query_media_narratives(limit=None):
    # Propaganda, conspiracy theories, narratives whose main subject is the war(s)
    q = PREFIXES + f"""
    SELECT {select_vars()} WHERE {{
      VALUES ?ntype {{ wd:{QID_CONSPIRACY_THEORY} wd:{QID_PROPAGANDA} }}
      ?item wdt:P31/wdt:P279* ?ntype .
      VALUES ?war {{ wd:{QID_RUSSO_UKR_WAR} wd:{QID_2022_INVASION} }}
      ?item wdt:P921 ?war .
      OPTIONAL {{ ?item wdt:P31 ?inst . }}
      {base_select_block()}
    }}
    """
    return run_sparql(q, limit)

def bindings_to_records(bindings, category):
    # 把 SPARQL 结果里的 bindings（每一行）转成统一结构的记录
    rows = []
    for b in bindings:
        get = lambda k: b.get(k, {}).get("value")
        item_uri = get("item")
        if not item_uri:
            continue
        qid = item_uri.rsplit("/", 1)[-1]
        rec = OrderedDict()
        rec["qid"] = qid
        rec["uri"] = item_uri
        rec["category"] = category
        labels = OrderedDict()
        labels["en"] = get("label_en")
        labels["uk"] = get("label_uk")
        labels["ru"] = get("label_ru")
        rec["labels"] = labels
        desc = OrderedDict()
        desc["en"] = get("desc_en")
        desc["uk"] = get("desc_uk")
        desc["ru"] = get("desc_ru")
        rec["descriptions"] = desc
        sitelinks = OrderedDict()
        sitelinks["enwiki"] = get("enwiki")
        sitelinks["ruwiki"] = get("ruwiki")
        sitelinks["ukwiki"] = get("ukwiki")
        # Optional human-readable titles if present
        titles = OrderedDict()
        titles["en"] = get("en_title")
        titles["ru"] = get("ru_title")
        titles["uk"] = get("uk_title")
        rec["sitelinks"] = sitelinks
        rec["wiki_titles"] = titles
        inst_uri = get("inst")
        inst_label_en = get("inst_en")
        rec["instance_of"] = {"uri": inst_uri, "label_en": inst_label_en} if inst_uri else None
        rows.append(rec)
    return rows

def enrich_with_aliases(records, langs=("en","uk","ru")):
    # slangs and spoken vocabulary: not too much in wiki padia
    # Zelenskyy → “Volodymyr Oleksandrovych Zelenskyy”, “Володимир Зеленський”
    # Russia → “RF”, “Российская Федерация”, “РФ” 

    for rec in records:
        try:
            aliases = collect_aliases(rec["uri"], langs=langs)
        except Exception as e:
            aliases = {}
        rec["aliases"] = aliases
    return records

def dedupe(records):
    seen = set()
    out = []
    for r in records:
        if r["qid"] in seen:
            continue
        seen.add(r["qid"])
        out.append(r)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="ru_ua_keywords.jsonl", help="Output JSONL path")
    ap.add_argument("--array", default="ru_ua_keywords.json", help="Output JSON array path")
    ap.add_argument("--limit", type=int, default=None, help="Optional per-query LIMIT for debugging")
    ap.add_argument("--no-aliases", action="store_true", help="Skip alias enrichment (faster)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    all_records = []

    print("Querying people...")
    people = query_people(limit=args.limit)
    all_records += bindings_to_records(people["results"]["bindings"], category="person")

    print("Querying events...")
    events = query_events(limit=args.limit)
    all_records += bindings_to_records(events["results"]["bindings"], category="event")

    print("Querying organizations...")
    orgs = query_orgs(limit=args.limit)
    all_records += bindings_to_records(orgs["results"]["bindings"], category="organization")

    print("Querying policies...")
    policies = query_policies(limit=args.limit)
    all_records += bindings_to_records(policies["results"]["bindings"], category="policy")

    print("Querying media narratives...")
    narratives = query_media_narratives(limit=args.limit)
    all_records += bindings_to_records(narratives["results"]["bindings"], category="media_narrative")

    # Dedupe by QID
    all_records = dedupe(all_records)

    # Alias enrichment
    if not args.no_aliases:
        print("Enriching aliases (EN/UK/RU)...")
        all_records = enrich_with_aliases(all_records, langs=("en","uk","ru"))

    # Write JSONL
    with open(args.out, "w", encoding="utf-8") as f:
        for r in all_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Wrote JSONL: {args.out}  ({len(all_records)} records)")

    # Write JSON array
    with open(args.array, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    print(f"Wrote JSON array: {args.array}")

if __name__ == "__main__":
    main()
