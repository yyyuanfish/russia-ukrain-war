#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: pipeline_common.py

Main purpose:
- Provide shared utilities used by all pipeline scripts:
  JSON/JSONL IO, schema normalization, record merge-by-QID, and config helpers.
- Guarantee that outputs from different harvest scripts follow one consistent schema.

This module is not intended to be run directly.
It is imported by:
- harvest_wikidata.py
- harvest_navboxes.py
- harvest_categories.py
- attribution.py
- visualization.py
"""

import json
import os
import re
import time
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

ATTRIB_PROP_IDS = ["P27", "P17", "P495", "P159", "P131", "P276", "P19", "P740", "P551"]
DEFAULT_LANG_KEYS = ("en",)
DEFAULT_SITE_KEYS = ("enwiki",)
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_USER_AGENT = "conflict-pipeline/1.0 (config-driven research)"


def normalize_langs(langs: Optional[List[str]]) -> List[str]:
    out: List[str] = []
    if not isinstance(langs, list):
        return out
    for v in langs:
        if not isinstance(v, str):
            continue
        vv = v.strip().lower()
        if vv and vv not in out:
            out.append(vv)
    return out


def site_keys_for_langs(langs: List[str]) -> List[str]:
    return [f"{l}wiki" for l in langs]


def _safe_lang_var(lang: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in lang.lower())


def normalize_prop_ids(prop_ids: Optional[List[str]], default: Optional[List[str]] = None) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    vals = prop_ids if isinstance(prop_ids, list) else []
    for v in vals:
        if not isinstance(v, str):
            continue
        p = v.strip().upper()
        if not re.fullmatch(r"P\d+", p):
            continue
        if p in seen:
            continue
        seen.add(p)
        out.append(p)

    if out:
        return out

    dvals = default if isinstance(default, list) else []
    for v in dvals:
        if not isinstance(v, str):
            continue
        p = v.strip().upper()
        if not re.fullmatch(r"P\d+", p):
            continue
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def attribution_prop_ids_from_config(config: dict) -> List[str]:
    ccfg = config.get("classification") if isinstance(config.get("classification"), dict) else {}
    return normalize_prop_ids(ccfg.get("attribution_properties"), default=ATTRIB_PROP_IDS)


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)


def read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_jsonl(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_json(path: str, payload: dict) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_jsonl(path: str, rows: List[dict]) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def qid_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]


def _norm_lang_dict(d: Optional[dict], keys: Tuple[str, ...]) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {k: None for k in keys}
    if isinstance(d, dict):
        for k in keys:
            v = d.get(k)
            out[k] = v if isinstance(v, str) and v else None
    return out


def _norm_aliases(d: Optional[dict], lang_keys: Tuple[str, ...]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {k: [] for k in lang_keys}
    if isinstance(d, dict):
        for k in lang_keys:
            vals = d.get(k)
            if isinstance(vals, list):
                cleaned = [str(x) for x in vals if isinstance(x, str) and x.strip()]
                out[k] = sorted(set(cleaned))
    return out


def _norm_qid_list(vals) -> List[str]:
    out: Set[str] = set()
    if isinstance(vals, list):
        for v in vals:
            if not isinstance(v, str):
                continue
            q = qid_from_uri(v) if v.startswith("http") else v
            if q.startswith("Q"):
                out.add(q)
    return sorted(out)


def normalize_record(
    rec: dict,
    source_type: Optional[str] = None,
    source_hint: Optional[str] = None,
    source_page: Optional[str] = None,
    collection_paths: Optional[List[str]] = None,
    lang_keys: Optional[List[str]] = None,
    site_keys: Optional[List[str]] = None,
    attrib_prop_ids: Optional[List[str]] = None,
) -> Optional[dict]:
    qid = rec.get("qid")
    if not isinstance(qid, str) or not qid.startswith("Q"):
        uri = rec.get("uri")
        if isinstance(uri, str) and "/Q" in uri:
            qid = qid_from_uri(uri)
    if not isinstance(qid, str) or not qid.startswith("Q"):
        return None

    source = rec.get("source") if isinstance(rec.get("source"), dict) else {}
    if source_type:
        source["type"] = source_type
    source.setdefault("type", "unknown_source")
    if source_hint and not source.get("hint"):
        source["hint"] = source_hint
    source.setdefault("hint", "unknown")
    if source_page and not source.get("page"):
        source["page"] = source_page

    cp = source.get("collection_paths")
    cp_set: Set[str] = set()
    if isinstance(cp, list):
        for x in cp:
            if isinstance(x, str) and x.strip():
                cp_set.add(x.strip())
    if collection_paths:
        for x in collection_paths:
            if isinstance(x, str) and x.strip():
                cp_set.add(x.strip())
    if cp_set:
        source["collection_paths"] = sorted(cp_set)

    lk = tuple(normalize_langs(lang_keys) or list(DEFAULT_LANG_KEYS))
    sk = tuple(site_keys or site_keys_for_langs(list(lk)))

    out = {
        "qid": qid,
        "uri": rec.get("uri") if isinstance(rec.get("uri"), str) else f"http://www.wikidata.org/entity/{qid}",
        "source": source,
        "labels": _norm_lang_dict(rec.get("labels"), lk),
        "descriptions": _norm_lang_dict(rec.get("descriptions"), lk),
        "aliases": _norm_aliases(rec.get("aliases"), lk),
        "sitelinks": _norm_lang_dict(rec.get("sitelinks"), sk),
        "wiki_titles": _norm_lang_dict(rec.get("wiki_titles"), lk),
        "instance_of": _norm_qid_list(rec.get("instance_of") or []),
        "raw_attrib_qids": {},
    }

    apids = normalize_prop_ids(attrib_prop_ids, default=ATTRIB_PROP_IDS)
    raw = rec.get("raw_attrib_qids") if isinstance(rec.get("raw_attrib_qids"), dict) else {}
    for pid in apids:
        out["raw_attrib_qids"][pid] = _norm_qid_list(raw.get(pid) or [])

    return out


def merge_records_by_qid(
    records: List[dict],
    lang_keys: Optional[List[str]] = None,
    site_keys: Optional[List[str]] = None,
    attrib_prop_ids: Optional[List[str]] = None,
) -> List[dict]:
    merged: Dict[str, dict] = {}
    lk = normalize_langs(lang_keys) or list(DEFAULT_LANG_KEYS)
    sk = site_keys or site_keys_for_langs(lk)
    apids = normalize_prop_ids(attrib_prop_ids, default=ATTRIB_PROP_IDS)

    for r in records:
        nr = normalize_record(r, lang_keys=lk, site_keys=sk, attrib_prop_ids=apids)
        if not nr:
            continue
        qid = nr["qid"]

        if qid not in merged:
            item = nr
            item["_sources"] = [nr.get("source", {})]
            merged[qid] = item
            continue

        cur = merged[qid]
        cur.setdefault("_sources", []).append(nr.get("source", {}))

        for field in ("labels", "descriptions", "sitelinks", "wiki_titles"):
            for k, v in nr.get(field, {}).items():
                if not cur[field].get(k) and v:
                    cur[field][k] = v

        for lang in lk:
            cur_alias = set(cur.get("aliases", {}).get(lang) or [])
            new_alias = set(nr.get("aliases", {}).get(lang) or [])
            cur["aliases"][lang] = sorted(cur_alias | new_alias)

        cur["instance_of"] = sorted(set(cur.get("instance_of") or []) | set(nr.get("instance_of") or []))

        for pid in apids:
            cur_set = set((cur.get("raw_attrib_qids") or {}).get(pid) or [])
            new_set = set((nr.get("raw_attrib_qids") or {}).get(pid) or [])
            cur["raw_attrib_qids"][pid] = sorted(cur_set | new_set)

        cur_src = cur.get("source") if isinstance(cur.get("source"), dict) else {}
        new_src = nr.get("source") if isinstance(nr.get("source"), dict) else {}
        cur_paths = set(cur_src.get("collection_paths") or [])
        new_paths = set(new_src.get("collection_paths") or [])
        if cur_paths or new_paths:
            cur_src["collection_paths"] = sorted(cur_paths | new_paths)
        cur["source"] = cur_src

    return [merged[k] for k in sorted(merged.keys())]


def config_languages(config: dict) -> List[str]:
    langs: List[str] = []
    lang_map = config.get("languages") if isinstance(config.get("languages"), dict) else {}

    def _add_lang(v: str) -> None:
        vv = v.strip().lower()
        if vv and vv not in langs:
            langs.append(vv)

    all_langs = lang_map.get("all")
    if isinstance(all_langs, list):
        for v in all_langs:
            if isinstance(v, str):
                _add_lang(v)

    if not langs:
        for _, v in lang_map.items():
            if isinstance(v, str):
                _add_lang(v)

    if not langs:
        langs = ["en"]
    return langs


def run_wikidata_sparql(query: str, retries: int = 3, backoff: float = 2.0) -> dict:
    try:
        from SPARQLWrapper import JSON as SPARQL_JSON
        from SPARQLWrapper import SPARQLWrapper
    except Exception as exc:
        raise RuntimeError("SPARQLWrapper is required for Wikidata queries. Install with: pip install SPARQLWrapper") from exc

    sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent=WIKIDATA_USER_AGENT)
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQL_JSON)
    sparql.setTimeout(120)

    last_err = None
    for attempt in range(retries):
        try:
            return sparql.query().convert()
        except Exception as exc:
            last_err = exc
            time.sleep(backoff ** attempt)
    raise last_err


def split_concat(s: Optional[str]) -> List[str]:
    if not s:
        return []
    out: List[str] = []
    seen = set()
    for p in s.split("|"):
        p = p.strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def build_item_enrichment_query(qids: List[str], langs: List[str], attrib_prop_ids: Optional[List[str]] = None) -> str:
    if not qids:
        raise ValueError("qids is empty")

    langs = normalize_langs(langs) or list(DEFAULT_LANG_KEYS)
    values = " ".join(f"wd:{q}" for q in qids if isinstance(q, str) and q.startswith("Q"))
    if not values:
        raise ValueError("no valid QIDs for enrichment query")

    label_selects: List[str] = []
    desc_selects: List[str] = []
    sitelink_selects: List[str] = []
    title_selects: List[str] = []
    opt_lang_blocks: List[str] = []
    opt_sitelink_blocks: List[str] = []

    for lang in langs:
        sfx = _safe_lang_var(lang)
        label_selects.append(f"(SAMPLE(?label_{sfx}) AS ?label_{sfx})")
        desc_selects.append(f"(SAMPLE(?desc_{sfx}) AS ?desc_{sfx})")
        sitelink_selects.append(f"(SAMPLE(?{sfx}wiki) AS ?{sfx}wiki)")
        title_selects.append(f"(SAMPLE(?{sfx}_title) AS ?{sfx}_title)")

        opt_lang_blocks.append(f'OPTIONAL {{ ?item rdfs:label ?label_{sfx} . FILTER(LANG(?label_{sfx}) = "{lang}") }}')
        opt_lang_blocks.append(f'OPTIONAL {{ ?item schema:description ?desc_{sfx} . FILTER(LANG(?desc_{sfx}) = "{lang}") }}')
        opt_sitelink_blocks.append(
            f"OPTIONAL {{ ?{sfx}wiki schema:about ?item ; schema:isPartOf <https://{lang}.wikipedia.org/> ; schema:name ?{sfx}_title . }}"
        )

    apids = normalize_prop_ids(attrib_prop_ids, default=ATTRIB_PROP_IDS)

    prop_selects = []
    prop_opts = []
    for pid in apids:
        prop_selects.append(f'(GROUP_CONCAT(DISTINCT STR(?{pid}val); separator="|") AS ?{pid}_vals)')
        prop_opts.append(f'OPTIONAL {{ ?item wdt:{pid} ?{pid}val . }}')

    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>

SELECT ?item
  {" ".join(label_selects)}
  {" ".join(desc_selects)}
  {" ".join(sitelink_selects)}
  {" ".join(title_selects)}
  (GROUP_CONCAT(DISTINCT STR(?inst); separator="|") AS ?insts)
  {" ".join(prop_selects)}
WHERE {{
  VALUES ?item {{ {values} }}

  {" ".join(opt_lang_blocks)}
  {" ".join(opt_sitelink_blocks)}
  OPTIONAL {{ ?item wdt:P31 ?inst . }}
  {" ".join(prop_opts)}
}}
GROUP BY ?item
"""
    return query


def binding_to_enriched_record(binding: dict, langs: List[str], attrib_prop_ids: Optional[List[str]] = None) -> Optional[dict]:
    item_uri = binding.get("item", {}).get("value")
    qid = qid_from_uri(item_uri)
    if not qid:
        return None

    langs = normalize_langs(langs) or list(DEFAULT_LANG_KEYS)

    labels: Dict[str, Optional[str]] = {}
    descriptions: Dict[str, Optional[str]] = {}
    aliases: Dict[str, List[str]] = {}
    sitelinks: Dict[str, Optional[str]] = {}
    wiki_titles: Dict[str, Optional[str]] = {}

    for lang in langs:
        sfx = _safe_lang_var(lang)
        labels[lang] = binding.get(f"label_{sfx}", {}).get("value")
        descriptions[lang] = binding.get(f"desc_{sfx}", {}).get("value")
        aliases[lang] = []
        sitelinks[f"{lang}wiki"] = binding.get(f"{sfx}wiki", {}).get("value")
        wiki_titles[lang] = binding.get(f"{sfx}_title", {}).get("value")

    insts = set()
    for x in split_concat(binding.get("insts", {}).get("value")):
        q = qid_from_uri(x) if x.startswith("http") else x
        if isinstance(q, str) and q.startswith("Q"):
            insts.add(q)

    apids = normalize_prop_ids(attrib_prop_ids, default=ATTRIB_PROP_IDS)
    raw_attrib_qids: Dict[str, List[str]] = {}
    for pid in apids:
        qset: Set[str] = set()
        for v in split_concat(binding.get(f"{pid}_vals", {}).get("value")):
            q = qid_from_uri(v) if v.startswith("http") else v
            if isinstance(q, str) and q.startswith("Q"):
                qset.add(q)
        raw_attrib_qids[pid] = sorted(qset)

    return {
        "qid": qid,
        "uri": item_uri or f"http://www.wikidata.org/entity/{qid}",
        "source": {},
        "labels": labels,
        "descriptions": descriptions,
        "aliases": aliases,
        "sitelinks": sitelinks,
        "wiki_titles": wiki_titles,
        "instance_of": sorted(insts),
        "raw_attrib_qids": raw_attrib_qids,
    }


def party_sets(config: dict) -> Tuple[Set[str], Set[str]]:
    parties = config.get("conflicting_parties") if isinstance(config.get("conflicting_parties"), dict) else {}
    p1 = parties.get("party1") if isinstance(parties.get("party1"), dict) else {}
    p2 = parties.get("party2") if isinstance(parties.get("party2"), dict) else {}

    def build(p: dict) -> Set[str]:
        out: Set[str] = set()
        pid = p.get("ID")
        if isinstance(pid, str) and pid.startswith("Q"):
            out.add(pid)
        allies = p.get("allies") if isinstance(p.get("allies"), list) else []
        for a in allies:
            if isinstance(a, str) and a.startswith("Q"):
                out.add(a)
        return out

    return build(p1), build(p2)
