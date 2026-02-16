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
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

ATTRIB_PROP_IDS = ["P27", "P17", "P495", "P159", "P131", "P276", "P19", "P740", "P551"]
LANG_KEYS = ("en", "ru", "uk")
SITE_KEYS = ("enwiki", "ruwiki", "ukwiki")


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


def _norm_aliases(d: Optional[dict]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {k: [] for k in LANG_KEYS}
    if isinstance(d, dict):
        for k in LANG_KEYS:
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

    out = {
        "qid": qid,
        "uri": rec.get("uri") if isinstance(rec.get("uri"), str) else f"http://www.wikidata.org/entity/{qid}",
        "source": source,
        "labels": _norm_lang_dict(rec.get("labels"), LANG_KEYS),
        "descriptions": _norm_lang_dict(rec.get("descriptions"), LANG_KEYS),
        "aliases": _norm_aliases(rec.get("aliases")),
        "sitelinks": _norm_lang_dict(rec.get("sitelinks"), SITE_KEYS),
        "wiki_titles": _norm_lang_dict(rec.get("wiki_titles"), LANG_KEYS),
        "instance_of": _norm_qid_list(rec.get("instance_of") or []),
        "raw_attrib_qids": {},
    }

    raw = rec.get("raw_attrib_qids") if isinstance(rec.get("raw_attrib_qids"), dict) else {}
    for pid in ATTRIB_PROP_IDS:
        out["raw_attrib_qids"][pid] = _norm_qid_list(raw.get(pid) or [])

    return out


def merge_records_by_qid(records: List[dict]) -> List[dict]:
    merged: Dict[str, dict] = {}

    for r in records:
        nr = normalize_record(r)
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

        for lang in LANG_KEYS:
            cur_alias = set(cur.get("aliases", {}).get(lang) or [])
            new_alias = set(nr.get("aliases", {}).get(lang) or [])
            cur["aliases"][lang] = sorted(cur_alias | new_alias)

        cur["instance_of"] = sorted(set(cur.get("instance_of") or []) | set(nr.get("instance_of") or []))

        for pid in ATTRIB_PROP_IDS:
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
    for _, v in lang_map.items():
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv and vv not in langs:
                langs.append(vv)
    if "en" not in langs:
        langs.append("en")
    return langs


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
