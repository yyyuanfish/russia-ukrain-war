#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classify RU-UA entities into:
  Russian | Ukraine | mixed | other

Design goal (your requirement):
- Make "other" as clean as possible: ideally only entities that are neither Russia nor Ukraine
  (e.g., USA/China/UN/NATO/etc.)
- If an entity is war-related but attribution is uncertain, do NOT dump it into other.
  Prefer "mixed" when uncertain.

Inputs:
- One or more JSONL files produced by:
  - ru_ua_harvest_wikidata_entities.py
  - ru_ua_harvest_wikipedia_navboxes.py

Outputs:
- JSONL with added fields:
  - ru_ua_attribution: Russian|Ukraine|mixed|other
  - ru_ua_attribution_detail: evidence + scoring + triggered rules

Core evidence sources (multiple, not limited to one):
1) Direct structured properties on the entity (highest weight):
   - P27 (citizenship), P17 (country), P495 (origin), P159 (HQ), P131 (located in admin),
     P276 (location), P19 (place of birth), P740 (formation), P551 (residence)
2) Indirect structured evidence:
   - If a property points to a place/admin entity, resolve its country via SPARQL:
     place wdt:P17 OR place wdt:P131*/wdt:P17 => Russia/Ukraine/other
3) Text fallback (lower weight):
   - labels/descriptions/aliases in en/ru/uk, with robust regex patterns
4) "Other" strong evidence:
   - explicit third-country values (USA, China, etc.) on country/citizenship/origin/HQ/location
   - but only if NO RU/UA evidence exists

Usage:
    python ru_ua_classify_entities.py \
    --in navbox_ru_ua_entities.jsonl wd_ru_ua_entities.jsonl \
    --out classified_ru_ua_entities.jsonl \
    --report classified_report.json
"""

import argparse
import json
import re
import time
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set, Tuple

from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "ru-ua-classifier/1.0 (research; unified-schema)"

# Target country QIDs
Q_RUSSIA = "Q159"
Q_UKRAINE = "Q212"

# Some common "other" country QIDs you likely care about (extend freely)
Q_USA = "Q30"
Q_CHINA = "Q148"
Q_GERMANY = "Q183"
Q_UK = "Q145"
Q_FRANCE = "Q142"
Q_POLAND = "Q36"
Q_BELARUS = "Q184"
Q_EU = "Q458"  # European Union (not a country but useful "other" anchor)

# Properties used as structured evidence
ATTRIB_PROP_IDS = ["P27", "P17", "P495", "P159", "P131", "P276", "P19", "P740", "P551"]

# Strong vs medium evidence weights
WEIGHT_STRONG = 4   # direct country/citizenship
WEIGHT_MED = 2      # location/HQ/birth/residence
WEIGHT_WEAK = 1     # text matches

# Regex patterns (text fallback)
TEXT_PATTERNS = {
    "Russian": {
        "en": [r"\brussian\b", r"\brussia\b", r"\brf\b", r"\brussian[- ]backed\b"],
        "ru": [r"\bросси", r"\bрусск", r"\bрф\b"],
        "uk": [r"\bросі", r"\bросійськ", r"\bрф\b"],
    },
    "Ukraine": {
        "en": [r"\bukrain", r"\bukraine\b"],
        "ru": [r"\bукраин", r"\bукраинец", r"\bукраинка"],
        "uk": [r"\bукраїн", r"\bукраїнець", r"\bукраїнка"],
    },
    # optional “other” demonyms/anchors to reduce false-mixed
    "American": {
        "en": [r"\bamerican\b", r"\bu\.?s\.?\b", r"\bunited states\b"],
        "ru": [r"\bамерикан"],
        "uk": [r"\bамерикан"],
    },
    "Chinese": {
        "en": [r"\bchinese\b", r"\bchina\b", r"\bprc\b"],
        "ru": [r"\bкита", r"\bкнр\b"],
        "uk": [r"\bкита", r"\bкнр\b"],
    },
}

OTHER_COUNTRY_HINTS = {Q_USA, Q_CHINA, Q_GERMANY, Q_UK, Q_FRANCE, Q_POLAND, Q_BELARUS, Q_EU}


def run_sparql(query: str, retries: int = 3, backoff: float = 2.0) -> dict:
    sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent=USER_AGENT)
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQL_JSON)

    # reduce 504s: allow longer server response time
    sparql.setTimeout(120)

    last_err = None
    for attempt in range(retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            last_err = e
            time.sleep(backoff ** attempt)
    raise last_err


def read_jsonl(path: str) -> List[dict]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def write_jsonl(path: str, rows: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def merge_records_by_qid(records: List[dict]) -> List[dict]:
    """
    Merge duplicates across sources. Keep the richest fields and aggregate source info.
    """
    merged: Dict[str, dict] = {}
    for r in records:
        qid = r.get("qid")
        if not qid:
            continue
        if qid not in merged:
            merged[qid] = r
            # ensure list-like fields
            merged[qid].setdefault("instance_of", [])
            merged[qid].setdefault("raw_attrib_qids", {})
            merged[qid].setdefault("aliases", {"en": [], "uk": [], "ru": []})
            merged[qid]["_sources"] = [r.get("source")]
        else:
            m = merged[qid]
            m["_sources"].append(r.get("source"))
            # prefer non-empty labels/descriptions
            for k in ("labels", "descriptions", "sitelinks", "wiki_titles"):
                m.setdefault(k, {})
                for lang, val in (r.get(k, {}) or {}).items():
                    if not m[k].get(lang) and val:
                        m[k][lang] = val
            # merge instance_of
            m_inst = set(m.get("instance_of") or [])
            r_inst = set(r.get("instance_of") or [])
            m["instance_of"] = sorted(m_inst | r_inst)
            # merge raw_attrib_qids by property
            m_raw = m.get("raw_attrib_qids") or {}
            r_raw = r.get("raw_attrib_qids") or {}
            for pid in set(m_raw.keys()) | set(r_raw.keys()):
                mset = set(m_raw.get(pid) or [])
                rset = set(r_raw.get(pid) or [])
                m_raw[pid] = sorted(mset | rset)
            m["raw_attrib_qids"] = m_raw
            # merge aliases
            m_alias = m.get("aliases") or {"en": [], "uk": [], "ru": []}
            r_alias = r.get("aliases") or {"en": [], "uk": [], "ru": []}
            for lang in ("en", "uk", "ru"):
                m_alias[lang] = sorted(set((m_alias.get(lang) or []) + (r_alias.get(lang) or [])))
            m["aliases"] = m_alias
    return list(merged.values())


def build_place_country_map(place_qids: List[str]) -> Dict[str, Set[str]]:
    """
    For a list of QIDs (often cities/regions), resolve their country via:
      - direct: place wdt:P17 ?country
      - indirect: place -> P131 (up to 3 levels) -> P17 ?country
    Returns: place_qid -> set(country_qids)

    Avoid expensive wdt:P131* path (often triggers 504). Use bounded depth (1..3).
    """
    place_qids = [q for q in place_qids if q and q.startswith("Q")]
    if not place_qids:
        return {}

    out: Dict[str, Set[str]] = defaultdict(set)

    BATCH = 80
    for i in range(0, len(place_qids), BATCH):
        chunk = place_qids[i:i + BATCH]
        values = " ".join(f"wd:{q}" for q in chunk)

        q = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?place ?country WHERE {{
  VALUES ?place {{ {values} }}
  {{
    ?place wdt:P17 ?country .
  }}
  UNION
  {{
    ?place wdt:P131 ?a1 .
    ?a1 wdt:P17 ?country .
  }}
  UNION
  {{
    ?place wdt:P131 ?a1 .
    ?a1 wdt:P131 ?a2 .
    ?a2 wdt:P17 ?country .
  }}
  UNION
  {{
    ?place wdt:P131 ?a1 .
    ?a1 wdt:P131 ?a2 .
    ?a2 wdt:P131 ?a3 .
    ?a3 wdt:P17 ?country .
  }}
}}
"""
        data = run_sparql(q)
        for b in data["results"]["bindings"]:
            place_uri = b.get("place", {}).get("value")
            country_uri = b.get("country", {}).get("value")
            if not place_uri or not country_uri:
                continue
            place = place_uri.rsplit("/", 1)[-1]
            country = country_uri.rsplit("/", 1)[-1]
            out[place].add(country)

        time.sleep(0.2)

    return out


def text_score(entity: dict) -> Tuple[int, int, int, List[str]]:
    """
    Returns (ru_score, ua_score, other_score, hits)
    """
    hits = []
    ru = ua = other = 0

    def scan(text: str, lang: str):
        nonlocal ru, ua, other, hits
        if not text:
            return
        t = text.lower()
        for pat in TEXT_PATTERNS["Russian"].get(lang, []):
            if re.search(pat, t):
                ru += WEIGHT_WEAK
                hits.append(f"text:{lang}:RU:{pat}")
        for pat in TEXT_PATTERNS["Ukraine"].get(lang, []):
            if re.search(pat, t):
                ua += WEIGHT_WEAK
                hits.append(f"text:{lang}:UA:{pat}")
        for pat in TEXT_PATTERNS["American"].get(lang, []):
            if re.search(pat, t):
                other += WEIGHT_WEAK
                hits.append(f"text:{lang}:OTHER:American:{pat}")
        for pat in TEXT_PATTERNS["Chinese"].get(lang, []):
            if re.search(pat, t):
                other += WEIGHT_WEAK
                hits.append(f"text:{lang}:OTHER:Chinese:{pat}")

    for lang in ("en", "ru", "uk"):
        scan((entity.get("labels", {}) or {}).get(lang) or "", lang)
        scan((entity.get("descriptions", {}) or {}).get(lang) or "", lang)
        for a in (entity.get("aliases", {}) or {}).get(lang) or []:
            scan(a, lang)

    return ru, ua, other, hits


def structured_score(entity: dict, place_country_map: Dict[str, Set[str]]) -> Tuple[int, int, int, List[str]]:
    """
    Structured evidence with weights.
    """
    raw = entity.get("raw_attrib_qids") or {}
    ru = ua = other = 0
    hits = []

    for pid in ATTRIB_PROP_IDS:
        vals = raw.get(pid) or []
        for v in vals:
            if v == Q_RUSSIA:
                ru += WEIGHT_STRONG
                hits.append(f"{pid}:direct:RU:{v}")
            elif v == Q_UKRAINE:
                ua += WEIGHT_STRONG
                hits.append(f"{pid}:direct:UA:{v}")
            else:
                if v in OTHER_COUNTRY_HINTS:
                    other += WEIGHT_STRONG
                    hits.append(f"{pid}:direct:OTHER:{v}")

    location_like = {"P159", "P131", "P276", "P19", "P740", "P551"}
    for pid in location_like:
        vals = raw.get(pid) or []
        for place_qid in vals:
            for c in place_country_map.get(place_qid, set()):
                if c == Q_RUSSIA:
                    ru += WEIGHT_MED
                    hits.append(f"{pid}:place_country:RU:{place_qid}->{c}")
                elif c == Q_UKRAINE:
                    ua += WEIGHT_MED
                    hits.append(f"{pid}:place_country:UA:{place_qid}->{c}")
                else:
                    if c in OTHER_COUNTRY_HINTS:
                        other += WEIGHT_MED
                        hits.append(f"{pid}:place_country:OTHER:{place_qid}->{c}")

    return ru, ua, other, hits


def decide_label(ru_score: int, ua_score: int, other_score: int, other_strict_threshold: int = 4) -> str:
    """
    Decision rules aligned to our goal.
    """
    if ru_score > 0 and ua_score > 0:
        return "mixed"
    if ru_score > 0 and ua_score == 0:
        return "Russian"
    if ua_score > 0 and ru_score == 0:
        return "Ukraine"

    if other_score >= other_strict_threshold:
        return "other"
    return "mixed"


# -------------------- [MOD] compact report helpers --------------------

def _get_source_type(rec: dict) -> Optional[str]:
    s = rec.get("source")
    if isinstance(s, dict) and s.get("type"):
        return s.get("type")
    for ss in (rec.get("_sources") or []):
        if isinstance(ss, dict) and ss.get("type"):
            return ss.get("type")
    return None


def _get_source_hint(rec: dict) -> str:
    s = rec.get("source")
    if isinstance(s, dict) and s.get("hint"):
        return s.get("hint")
    for ss in (rec.get("_sources") or []):
        if isinstance(ss, dict) and ss.get("hint"):
            return ss.get("hint")
    return "unknown"


def _get_start_url_from_records(records: List[dict]) -> Optional[str]:
    for r in records:
        s = r.get("source")
        if isinstance(s, dict) and s.get("page"):
            return s.get("page")
        for ss in (r.get("_sources") or []):
            if isinstance(ss, dict) and ss.get("page"):
                return ss.get("page")
    return None


def _label_or_desc_nonempty(rec: dict, lang: str) -> bool:
    labels = rec.get("labels") or {}
    descs = rec.get("descriptions") or {}
    return bool((labels.get(lang) if isinstance(labels, dict) else None) or (descs.get(lang) if isinstance(descs, dict) else None))


def _language_coverage_label_or_desc(records: List[dict]) -> Dict[str, int]:
    cov = {}
    for lang in ("en", "ru", "uk"):
        cov[lang] = sum(1 for r in records if _label_or_desc_nonempty(r, lang))
    return cov


def _category_counts(records: List[dict]) -> Dict[str, int]:
    c = Counter()
    for r in records:
        c[_get_source_hint(r)] += 1
    return dict(c)


# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inputs", nargs="+", required=True, help="Input JSONL(s) from harvesters")
    ap.add_argument("--out", required=True, help="Output classified JSONL")
    ap.add_argument("--report", default=None, help="Optional report JSON (compact summary)")
    ap.add_argument("--other-threshold", type=int, default=4, help="How strict to label 'other' when RU/UA evidence is absent")
    args = ap.parse_args()

    # Load + keep per-file rows for compact pre-stats
    all_rows: List[dict] = []
    per_file_rows: Dict[str, List[dict]] = {}
    for p in args.inputs:
        rows = read_jsonl(p)
        per_file_rows[p] = rows
        all_rows.extend(rows)

    merged = merge_records_by_qid(all_rows)
    print(f"Loaded {len(all_rows)} rows; merged to {len(merged)} unique QIDs")

    # Build place set for indirect inference
    place_qids = set()
    for e in merged:
        raw = e.get("raw_attrib_qids") or {}
        for pid in ("P159", "P131", "P276", "P19", "P740", "P551"):
            for q in raw.get(pid) or []:
                if q and q.startswith("Q") and q not in (Q_RUSSIA, Q_UKRAINE):
                    place_qids.add(q)

    print(f"Resolving place->country for {len(place_qids)} referenced places/admin entities...")
    place_country_map = build_place_country_map(sorted(place_qids))

    # Classify
    counts = Counter()
    for e in merged:
        s_ru, s_ua, s_other, s_hits = structured_score(e, place_country_map)
        t_ru, t_ua, t_other, t_hits = text_score(e)

        ru_score = s_ru + t_ru
        ua_score = s_ua + t_ua
        other_score = s_other + t_other

        label = decide_label(ru_score, ua_score, other_score, other_strict_threshold=args.other_threshold)

        e["ru_ua_attribution"] = label
        e["ru_ua_attribution_detail"] = {
            "scores": {"ru": ru_score, "ua": ua_score, "other": other_score},
            "structured_scores": {"ru": s_ru, "ua": s_ua, "other": s_other},
            "text_scores": {"ru": t_ru, "ua": t_ua, "other": t_other},
            "hits": (s_hits + t_hits)[:200],
            "policy": {
                "other_is_strict": True,
                "other_threshold": args.other_threshold,
                "note": "If no RU/UA evidence, label other only with strong explicit other evidence; else mixed."
            }
        }

        counts[label] += 1

    # Write output
    write_jsonl(args.out, merged)
    print(f"Wrote: {args.out}")
    print("Counts:", dict(counts))

    # -------------------- [MOD] compact report only (no examples) --------------------
    if args.report:
        # pre: per source type unique qids + category counts + language coverage
        type_to_qid: Dict[str, Dict[str, dict]] = defaultdict(dict)
        for r in all_rows:
            qid = r.get("qid")
            if not qid:
                continue
            st = _get_source_type(r) or "unknown_source"
            if qid not in type_to_qid[st]:
                type_to_qid[st][qid] = r

        pre_by_source = {}
        for st, qid_map in type_to_qid.items():
            uniq_records = list(qid_map.values())
            pre_by_source[st] = {
                "unique_qids": len(qid_map),
                "language_coverage_nonempty_label_or_desc": _language_coverage_label_or_desc(uniq_records),
                "category_hint_counts": _category_counts(uniq_records),
            }

        # overlap between inputs (by file) - report only when exactly 2 inputs
        overlap = None
        if len(args.inputs) == 2:
            a, b = args.inputs[0], args.inputs[1]
            a_q = {r.get("qid") for r in per_file_rows.get(a, []) if r.get("qid")}
            b_q = {r.get("qid") for r in per_file_rows.get(b, []) if r.get("qid")}
            inter = len(a_q & b_q)
            union = len(a_q | b_q)
            overlap = {
                "a": a,
                "b": b,
                "a_unique": len(a_q),
                "b_unique": len(b_q),
                "intersection": inter,
                "union": union,
                "jaccard": (inter / union) if union else 0.0,
                "a_only": len(a_q - b_q),
                "b_only": len(b_q - a_q),
            }

        report = {
            "start_url": _get_start_url_from_records(merged),
            "pre_classify": {
                "total_rows_loaded": len(all_rows),
                "unique_qids_merged": len(merged),
                "by_source_type": pre_by_source,
                "overlap_between_inputs": overlap,
                "merged_language_coverage_nonempty_label_or_desc": _language_coverage_label_or_desc(merged),
                "merged_category_hint_counts": _category_counts(merged),
            },
            "after_classify": {
                "attribution_counts": dict(counts),
            },
            "note": "Compact report (no examples). Use output JSONL ru_ua_attribution_detail.hits to audit evidence per entity."
        }

        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"Wrote report: {args.report}")


if __name__ == "__main__":
    main()
