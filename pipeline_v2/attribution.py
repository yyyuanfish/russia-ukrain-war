#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: attribution.py

Main purpose:
- Load and merge all `*_entities.jsonl` files from one folder.
- Assign one label per entity:
  `party1`, `party2`, `mixed`, or `other`.
- Use both structured Wikidata properties and multilingual text evidence.
- Keep a strict-`other` policy:
  `other` is assigned only when party1/party2 evidence is absent and
  `other_score` reaches the threshold.

Input:
- --config: path to config JSON (reads `conflicting_parties`, `languages`,
  and optional `classification` section).
- --entities_folder: folder with harvester outputs
  (e.g., `data/entities`).

Output:
- --output: classified JSONL
  (recommended: `data/classified_entities.jsonl`).
- --report: optional compact report JSON
  (recommended: `data/classified_report.json`).

How to run:
  python attribution.py \
    --config config.json \
    --entities_folder data/entities \
    --output data/classified_entities.jsonl \
    --report data/classified_report.json

Pipeline step:
- Step 4 (merge + classify).
"""

import argparse
import os
import re
import time
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set, Tuple

from SPARQLWrapper import JSON as SPARQL_JSON
from SPARQLWrapper import SPARQLWrapper

from pipeline_common import (
    ATTRIB_PROP_IDS,
    config_languages,
    merge_records_by_qid,
    party_sets,
    read_json,
    read_jsonl,
    write_json,
    write_jsonl,
)

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "conflict-attribution/1.0 (research; config-driven)"

WEIGHT_STRONG = 4
WEIGHT_MED = 2
WEIGHT_WEAK = 1

# fallback third-country anchors (can be overridden/extended in config.classification.other_country_hints)
DEFAULT_OTHER_HINTS = {
    "Q30",   # USA
    "Q148",  # China
    "Q183",  # Germany
    "Q145",  # UK
    "Q142",  # France
    "Q36",   # Poland
    "Q184",  # Belarus
    "Q458",  # EU
}


def run_sparql(query: str, retries: int = 3, backoff: float = 2.0) -> dict:
    sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent=USER_AGENT)
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


def _compile_pat_map(d: dict) -> Dict[str, List[re.Pattern]]:
    out: Dict[str, List[re.Pattern]] = {"en": [], "ru": [], "uk": []}
    if not isinstance(d, dict):
        return out
    for lang in ("en", "ru", "uk"):
        vals = d.get(lang)
        if not isinstance(vals, list):
            continue
        compiled = []
        for p in vals:
            if not isinstance(p, str) or not p.strip():
                continue
            compiled.append(re.compile(p.strip(), flags=re.IGNORECASE))
        out[lang] = compiled
    return out


def _default_party_patterns(party1_ids: Set[str], party2_ids: Set[str]) -> Tuple[Dict[str, List[re.Pattern]], Dict[str, List[re.Pattern]]]:
    p1 = {"en": [], "ru": [], "uk": []}
    p2 = {"en": [], "ru": [], "uk": []}

    # keep strong defaults for RU-UA case
    if "Q159" in party1_ids:
        p1 = _compile_pat_map(
            {
                "en": [r"\brussian\b", r"\brussia\b", r"\brf\b", r"\brussian[- ]backed\b"],
                "ru": [r"\bросси", r"\bрусск", r"\bрф\b"],
                "uk": [r"\bросі", r"\bросійськ", r"\bрф\b"],
            }
        )

    if "Q212" in party2_ids:
        p2 = _compile_pat_map(
            {
                "en": [r"\bukrain", r"\bukraine\b"],
                "ru": [r"\bукраин", r"\bукраинец", r"\bукраинка"],
                "uk": [r"\bукраїн", r"\bукраїнець", r"\bукраїнка"],
            }
        )

    return p1, p2


def _default_other_patterns() -> Dict[str, List[re.Pattern]]:
    return _compile_pat_map(
        {
            "en": [r"\bamerican\b", r"\bunited states\b", r"\bu\.?s\.?\b", r"\bchinese\b", r"\bchina\b"],
            "ru": [r"\bамерикан", r"\bкита", r"\bкнр\b"],
            "uk": [r"\bамерикан", r"\bкита", r"\bкнр\b"],
        }
    )


def discover_entity_files(folder: str) -> List[str]:
    if not os.path.isdir(folder):
        raise SystemExit(f"entities folder does not exist: {folder}")

    files = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith("_entities.jsonl"):
            continue
        files.append(os.path.join(folder, name))
    if not files:
        raise SystemExit(f"no *_entities.jsonl files found in: {folder}")
    return files


def build_place_country_map(place_qids: List[str]) -> Dict[str, Set[str]]:
    place_qids = [q for q in place_qids if isinstance(q, str) and q.startswith("Q")]
    if not place_qids:
        return {}

    out: Dict[str, Set[str]] = defaultdict(set)
    batch = 80

    for i in range(0, len(place_qids), batch):
        chunk = place_qids[i : i + batch]
        values = " ".join(f"wd:{q}" for q in chunk)

        query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?place ?country WHERE {{
  VALUES ?place {{ {values} }}
  {{ ?place wdt:P17 ?country . }}
  UNION {{ ?place wdt:P131 ?a1 . ?a1 wdt:P17 ?country . }}
  UNION {{ ?place wdt:P131 ?a1 . ?a1 wdt:P131 ?a2 . ?a2 wdt:P17 ?country . }}
  UNION {{ ?place wdt:P131 ?a1 . ?a1 wdt:P131 ?a2 . ?a2 wdt:P131 ?a3 . ?a3 wdt:P17 ?country . }}
}}
"""
        data = run_sparql(query)
        for b in data.get("results", {}).get("bindings", []):
            place_uri = b.get("place", {}).get("value")
            country_uri = b.get("country", {}).get("value")
            if not place_uri or not country_uri:
                continue
            place_q = place_uri.rsplit("/", 1)[-1]
            country_q = country_uri.rsplit("/", 1)[-1]
            if place_q.startswith("Q") and country_q.startswith("Q"):
                out[place_q].add(country_q)
        time.sleep(0.2)

    return out


def _scan_text(
    text: str,
    lang: str,
    p1_pat: Dict[str, List[re.Pattern]],
    p2_pat: Dict[str, List[re.Pattern]],
    other_pat: Dict[str, List[re.Pattern]],
    hits: List[str],
) -> Tuple[int, int, int]:
    if not text:
        return 0, 0, 0

    t = text.lower()
    s1 = s2 = so = 0

    for pat in p1_pat.get(lang, []):
        if pat.search(t):
            s1 += WEIGHT_WEAK
            hits.append(f"text:{lang}:party1:{pat.pattern}")

    for pat in p2_pat.get(lang, []):
        if pat.search(t):
            s2 += WEIGHT_WEAK
            hits.append(f"text:{lang}:party2:{pat.pattern}")

    for pat in other_pat.get(lang, []):
        if pat.search(t):
            so += WEIGHT_WEAK
            hits.append(f"text:{lang}:other:{pat.pattern}")

    return s1, s2, so


def text_score(
    entity: dict,
    scan_langs: List[str],
    p1_pat: Dict[str, List[re.Pattern]],
    p2_pat: Dict[str, List[re.Pattern]],
    other_pat: Dict[str, List[re.Pattern]],
) -> Tuple[int, int, int, List[str]]:
    s1 = s2 = so = 0
    hits: List[str] = []

    labels = entity.get("labels") if isinstance(entity.get("labels"), dict) else {}
    descs = entity.get("descriptions") if isinstance(entity.get("descriptions"), dict) else {}
    aliases = entity.get("aliases") if isinstance(entity.get("aliases"), dict) else {}

    for lang in scan_langs:
        x1, x2, xo = _scan_text(str(labels.get(lang) or ""), lang, p1_pat, p2_pat, other_pat, hits)
        s1 += x1
        s2 += x2
        so += xo

        x1, x2, xo = _scan_text(str(descs.get(lang) or ""), lang, p1_pat, p2_pat, other_pat, hits)
        s1 += x1
        s2 += x2
        so += xo

        for a in aliases.get(lang) or []:
            x1, x2, xo = _scan_text(str(a), lang, p1_pat, p2_pat, other_pat, hits)
            s1 += x1
            s2 += x2
            so += xo

    return s1, s2, so, hits


def structured_score(
    entity: dict,
    party1_ids: Set[str],
    party2_ids: Set[str],
    other_hints: Set[str],
    place_country_map: Dict[str, Set[str]],
) -> Tuple[int, int, int, List[str]]:
    raw = entity.get("raw_attrib_qids") if isinstance(entity.get("raw_attrib_qids"), dict) else {}

    s1 = s2 = so = 0
    hits: List[str] = []

    for pid in ATTRIB_PROP_IDS:
        vals = raw.get(pid) or []
        for v in vals:
            if v in party1_ids:
                s1 += WEIGHT_STRONG
                hits.append(f"{pid}:direct:party1:{v}")
            elif v in party2_ids:
                s2 += WEIGHT_STRONG
                hits.append(f"{pid}:direct:party2:{v}")
            elif v in other_hints:
                so += WEIGHT_STRONG
                hits.append(f"{pid}:direct:other:{v}")

    for pid in ("P159", "P131", "P276", "P19", "P740", "P551"):
        vals = raw.get(pid) or []
        for place_qid in vals:
            for country in place_country_map.get(place_qid, set()):
                if country in party1_ids:
                    s1 += WEIGHT_MED
                    hits.append(f"{pid}:place_country:party1:{place_qid}->{country}")
                elif country in party2_ids:
                    s2 += WEIGHT_MED
                    hits.append(f"{pid}:place_country:party2:{place_qid}->{country}")
                elif country in other_hints:
                    so += WEIGHT_MED
                    hits.append(f"{pid}:place_country:other:{place_qid}->{country}")

    return s1, s2, so, hits


def decide_label(party1_score: int, party2_score: int, other_score: int, other_threshold: int) -> str:
    if party1_score > 0 and party2_score > 0:
        return "mixed"
    if party1_score > 0 and party2_score == 0:
        return "party1"
    if party2_score > 0 and party1_score == 0:
        return "party2"

    if other_score >= other_threshold:
        return "other"
    return "mixed"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--entities_folder", required=True, help="Folder containing *_entities.jsonl")
    ap.add_argument("--output", required=True, help="Output classified JSONL")
    ap.add_argument("--report", default=None, help="Optional report JSON path")
    ap.add_argument("--other-threshold", type=int, default=None, help="Override other threshold")
    args = ap.parse_args()

    cfg = read_json(args.config)
    files = discover_entity_files(args.entities_folder)

    print("[attribution] input files:")
    for p in files:
        print(f"  - {p}")

    rows: List[dict] = []
    for p in files:
        rows.extend(read_jsonl(p))

    merged = merge_records_by_qid(rows)
    print(f"[attribution] loaded={len(rows)} merged_qids={len(merged)}")

    party1_ids, party2_ids = party_sets(cfg)
    if not party1_ids or not party2_ids:
        raise SystemExit("config.conflicting_parties.party1/party2 IDs are missing")

    ccfg = cfg.get("classification") if isinstance(cfg.get("classification"), dict) else {}

    other_threshold = args.other_threshold
    if other_threshold is None:
        try:
            other_threshold = int(ccfg.get("other_threshold", 4))
        except Exception:
            other_threshold = 4

    other_hints = set(DEFAULT_OTHER_HINTS)
    for q in (ccfg.get("other_country_hints") or []):
        if isinstance(q, str) and q.startswith("Q"):
            other_hints.add(q)

    p1_pat_cfg = _compile_pat_map(ccfg.get("party1_patterns") if isinstance(ccfg.get("party1_patterns"), dict) else {})
    p2_pat_cfg = _compile_pat_map(ccfg.get("party2_patterns") if isinstance(ccfg.get("party2_patterns"), dict) else {})
    other_pat_cfg = _compile_pat_map(ccfg.get("other_patterns") if isinstance(ccfg.get("other_patterns"), dict) else {})

    p1_default, p2_default = _default_party_patterns(party1_ids, party2_ids)
    other_default = _default_other_patterns()

    p1_pat = p1_pat_cfg if any(p1_pat_cfg[l] for l in ("en", "ru", "uk")) else p1_default
    p2_pat = p2_pat_cfg if any(p2_pat_cfg[l] for l in ("en", "ru", "uk")) else p2_default
    other_pat = other_pat_cfg if any(other_pat_cfg[l] for l in ("en", "ru", "uk")) else other_default

    scan_langs = [l for l in config_languages(cfg) if l in {"en", "ru", "uk"}]
    if not scan_langs:
        scan_langs = ["en", "ru", "uk"]

    place_qids: Set[str] = set()
    for r in merged:
        raw = r.get("raw_attrib_qids") if isinstance(r.get("raw_attrib_qids"), dict) else {}
        for pid in ("P159", "P131", "P276", "P19", "P740", "P551"):
            for q in raw.get(pid) or []:
                if isinstance(q, str) and q.startswith("Q") and q not in party1_ids and q not in party2_ids:
                    place_qids.add(q)

    print(f"[attribution] resolving place->country for {len(place_qids)} place/admin QIDs")
    place_country_map = build_place_country_map(sorted(place_qids))

    counts = Counter()
    for r in merged:
        s1, s2, so, shits = structured_score(r, party1_ids, party2_ids, other_hints, place_country_map)
        t1, t2, to, thits = text_score(r, scan_langs, p1_pat, p2_pat, other_pat)

        score1 = s1 + t1
        score2 = s2 + t2
        score_other = so + to

        label = decide_label(score1, score2, score_other, other_threshold)

        r["attribution"] = label
        r["attribution_detail"] = {
            "scores": {"party1": score1, "party2": score2, "other": score_other},
            "structured_scores": {"party1": s1, "party2": s2, "other": so},
            "text_scores": {"party1": t1, "party2": t2, "other": to},
            "hits": (shits + thits)[:200],
            "policy": {
                "other_is_strict": True,
                "other_threshold": other_threshold,
                "note": "If no party1/party2 evidence: other only when other_score >= threshold; else mixed.",
            },
            "party_qids": {
                "party1": sorted(party1_ids),
                "party2": sorted(party2_ids),
            },
        }

        # backwards-compatible labels for RU-UA case
        if "Q159" in party1_ids and "Q212" in party2_ids:
            if label == "party1":
                r["ru_ua_attribution"] = "Russian"
            elif label == "party2":
                r["ru_ua_attribution"] = "Ukraine"
            else:
                r["ru_ua_attribution"] = label

        counts[label] += 1

    write_jsonl(args.output, merged)
    print(f"[attribution] wrote {args.output} ({len(merged)} records)")
    print(f"[attribution] counts: {dict(counts)}")

    if args.report:
        report = {
            "input_files": files,
            "loaded_rows": len(rows),
            "unique_qids": len(merged),
            "counts": dict(counts),
            "party1_qids": sorted(party1_ids),
            "party2_qids": sorted(party2_ids),
            "languages_scanned": scan_langs,
            "other_threshold": other_threshold,
        }
        write_json(args.report, report)
        print(f"[attribution] wrote report {args.report}")


if __name__ == "__main__":
    main()
