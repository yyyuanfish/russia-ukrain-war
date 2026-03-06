#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: attribution.py

Main purpose:
- Load and merge all `*_entities.jsonl` files from one folder.
- Assign one label per entity:
  `party1`, `party2`, `mixed`, or `other`.
- Use both structured Wikidata properties and multilingual text evidence.
- Default to `other` when there is no party evidence:
  if both party1 and party2 evidence are absent, label the entity as `other`.

Input:
- --config: path to config JSON (reads `conflicting_parties`, `languages`,
  and optional `classification` section including regex patterns:
  `party1_patterns`, `party2_patterns`, `other_patterns`,
  plus optional output mappings `output_labels` / `legacy_output`.
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
  Country-specific output names are fully config-driven (no hardcoded RU/UA branch).
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
    attribution_prop_ids_from_config,
    config_languages,
    merge_records_by_qid,
    normalize_prop_ids,
    party_sets,
    read_json,
    read_jsonl,
    site_keys_for_langs,
    write_json,
    write_jsonl,
)

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "conflict-attribution/1.0 (research; config-driven)"

WEIGHT_STRONG = 4
WEIGHT_MED = 2
WEIGHT_WEAK = 1


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
    out: Dict[str, List[re.Pattern]] = {}
    if not isinstance(d, dict):
        return out
    for raw_lang, vals in d.items():
        if not isinstance(raw_lang, str):
            continue
        lang = raw_lang.strip().lower()
        if not lang:
            continue
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
    # Regex patterns are defined in config.json (classification.party1_patterns / party2_patterns).
    # We keep this function as a safe fallback in case config is missing patterns.
    return {}, {}


def _default_other_patterns() -> Dict[str, List[re.Pattern]]:
    # Regex patterns are defined in config.json (classification.other_patterns).
    # Keep an empty fallback to avoid hardcoding conflict-specific rules in code.
    return {}


def _resolve_output_label_config(cfg: dict, ccfg: dict, key: str, default_enabled: bool, default_field: str) -> dict:
    """
    Build config-driven optional label output mapping.

    Supported section shape (under classification.<key>):
      {
        "enabled": true/false,
        "field_name": "country_attribution",
        "party1": "CountryA",
        "party2": "CountryB",
        "mixed": "mixed",
        "other": "other"
      }
    """
    out_cfg = ccfg.get(key) if isinstance(ccfg.get(key), dict) else {}
    parties = cfg.get("conflicting_parties") if isinstance(cfg.get("conflicting_parties"), dict) else {}
    p1 = parties.get("party1") if isinstance(parties.get("party1"), dict) else {}
    p2 = parties.get("party2") if isinstance(parties.get("party2"), dict) else {}

    enabled = bool(out_cfg.get("enabled", default_enabled))
    field_name = out_cfg.get("field_name", default_field)
    if not isinstance(field_name, str) or not field_name.strip():
        field_name = default_field
    field_name = field_name.strip()

    p1_default = p1.get("label") if isinstance(p1.get("label"), str) and p1.get("label").strip() else "party1"
    p2_default = p2.get("label") if isinstance(p2.get("label"), str) and p2.get("label").strip() else "party2"

    label_map = {
        "party1": out_cfg.get("party1", p1_default),
        "party2": out_cfg.get("party2", p2_default),
        "mixed": out_cfg.get("mixed", "mixed"),
        "other": out_cfg.get("other", "other"),
    }
    for k, v in list(label_map.items()):
        if not isinstance(v, str) or not v.strip():
            label_map[k] = k
        else:
            label_map[k] = v.strip()

    return {
        "enabled": enabled,
        "field_name": field_name,
        "label_map": label_map,
    }


def _compile_other_country_text_map(items) -> List[Tuple[re.Pattern, str]]:
    """
    Parse config.classification.other_country_text_map into compiled regex rules.
    Expected item format:
      {"pattern": "<regex>", "qid": "Q..."}
    """
    out: List[Tuple[re.Pattern, str]] = []
    if not isinstance(items, list):
        return out

    for it in items:
        if not isinstance(it, dict):
            continue
        pattern = it.get("pattern")
        qid = it.get("qid")
        if not (isinstance(pattern, str) and pattern.strip()):
            continue
        if not (isinstance(qid, str) and qid.startswith("Q")):
            continue
        try:
            out.append((re.compile(pattern.strip(), flags=re.IGNORECASE), qid))
        except re.error:
            continue
    return out


def _pid_or_default(v, default_pid: str) -> str:
    if isinstance(v, str):
        pid = v.strip().upper()
        if re.fullmatch(r"P\d+", pid):
            return pid
    return default_pid


def _float_or_default(v, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _int_or_default(v, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    try:
        out = int(v)
    except Exception:
        out = default
    if min_value is not None and out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _resolve_place_resolution_config(ccfg: dict) -> dict:
    rcfg = ccfg.get("place_country_resolution") if isinstance(ccfg.get("place_country_resolution"), dict) else {}
    place_props = normalize_prop_ids(
        rcfg.get("place_properties"),
        default=["P159", "P131", "P276", "P19", "P740", "P551"],
    )
    direct_country_props = normalize_prop_ids(
        rcfg.get("direct_country_properties"),
        default=["P17", "P27", "P495"],
    )
    return {
        "country_property": _pid_or_default(rcfg.get("country_property"), "P17"),
        "admin_property": _pid_or_default(rcfg.get("admin_property"), "P131"),
        "max_admin_depth": _int_or_default(rcfg.get("max_admin_depth"), 3, min_value=0, max_value=6),
        "place_properties": place_props,
        "direct_country_properties": direct_country_props,
        "batch_size": _int_or_default(rcfg.get("batch_size"), 80, min_value=1, max_value=500),
        "sleep_seconds": _float_or_default(rcfg.get("sleep_seconds"), 0.2),
    }


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


def build_place_country_map(
    place_qids: List[str],
    country_property: str = "P17",
    admin_property: str = "P131",
    max_admin_depth: int = 3,
    batch_size: int = 80,
    sleep_seconds: float = 0.2,
) -> Dict[str, Set[str]]:
    place_qids = [q for q in place_qids if isinstance(q, str) and q.startswith("Q")]
    if not place_qids:
        return {}

    out: Dict[str, Set[str]] = defaultdict(set)
    batch = max(1, int(batch_size))
    max_depth = max(0, int(max_admin_depth))
    country_pid = _pid_or_default(country_property, "P17")
    admin_pid = _pid_or_default(admin_property, "P131")

    union_blocks: List[str] = [f"{{ ?place wdt:{country_pid} ?country . }}"]
    for depth in range(1, max_depth + 1):
        hops: List[str] = []
        prev = "?place"
        for i in range(1, depth + 1):
            cur = f"?a{i}"
            hops.append(f"{prev} wdt:{admin_pid} {cur} .")
            prev = cur
        hops.append(f"{prev} wdt:{country_pid} ?country .")
        union_blocks.append("{ " + " ".join(hops) + " }")
    union_clause = "\n  UNION ".join(union_blocks)

    for i in range(0, len(place_qids), batch):
        chunk = place_qids[i : i + batch]
        values = " ".join(f"wd:{q}" for q in chunk)

        query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?place ?country WHERE {{
  VALUES ?place {{ {values} }}
  {union_clause}
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
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

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
    attrib_prop_ids: List[str],
    place_props: List[str],
) -> Tuple[int, int, int, List[str]]:
    raw = entity.get("raw_attrib_qids") if isinstance(entity.get("raw_attrib_qids"), dict) else {}

    s1 = s2 = so = 0
    hits: List[str] = []

    for pid in attrib_prop_ids:
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

    for pid in place_props:
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


def guess_other_country(
    entity: dict,
    party1_ids: Set[str],
    party2_ids: Set[str],
    place_country_map: Dict[str, Set[str]],
    text_country_map: List[Tuple[re.Pattern, str]],
    direct_country_props: List[str],
    place_props: List[str],
    text_hits: Optional[List[str]] = None,
) -> Tuple[Optional[str], int, List[str]]:
    """
    Guess which third-party country an `other` entity most likely belongs to.

    Returns:
      (country_qid, score, evidence_snippets)

    Evidence sources (in priority / weight order):
    - Direct country-like properties from config (`classification.place_country_resolution.direct_country_properties`)
    - Place -> country inference via place_country_map (from configured place properties)
    - Configured text mapping fallback from `classification.other_country_text_map`
    """
    raw = entity.get("raw_attrib_qids") if isinstance(entity.get("raw_attrib_qids"), dict) else {}
    blocked = set(party1_ids) | set(party2_ids)

    scores: Dict[str, int] = defaultdict(int)
    evidence: Dict[str, Set[str]] = defaultdict(set)

    for pid in direct_country_props:
        for v in raw.get(pid) or []:
            if not (isinstance(v, str) and v.startswith("Q")):
                continue
            if v in blocked:
                continue
            scores[v] += WEIGHT_STRONG
            evidence[v].add(f"{pid}:direct")

    for pid in place_props:
        for place_qid in raw.get(pid) or []:
            if not (isinstance(place_qid, str) and place_qid.startswith("Q")):
                continue
            for c in place_country_map.get(place_qid, set()):
                if c in blocked:
                    continue
                scores[c] += WEIGHT_MED
                evidence[c].add(f"{pid}:place_country:{place_qid}")

    if text_hits:
        # `text_hits` entries look like: "text:en:other:<pattern>"
        for h in text_hits:
            if not (isinstance(h, str) and h.startswith("text:")):
                continue
            if ":other:" not in h:
                continue
            pat = h.split(":other:", 1)[-1]
            for rx, qid in text_country_map:
                if rx.search(pat):
                    if qid not in blocked:
                        scores[qid] += WEIGHT_WEAK
                        evidence[qid].add("text:other")

    if not scores:
        return None, 0, []

    best_qid, best_score = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[0]
    ev = sorted(evidence.get(best_qid, set()))
    return best_qid, int(best_score), ev[:12]


def fetch_qid_labels(qids: List[str], lang: str = "en") -> Dict[str, Optional[str]]:
    """
    Resolve a small list of QIDs to labels (best-effort).
    Used for report readability (e.g., top-3 third-party countries).
    """
    qids = [q for q in qids if isinstance(q, str) and q.startswith("Q")]
    if not qids:
        return {}

    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT ?item ?itemLabel WHERE {{
  VALUES ?item {{ {values} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}" . }}
}}
"""
    try:
        data = run_sparql(query)
    except Exception:
        return {}

    out: Dict[str, Optional[str]] = {}
    for b in data.get("results", {}).get("bindings", []):
        uri = b.get("item", {}).get("value")
        label = b.get("itemLabel", {}).get("value")
        if not uri:
            continue
        qid = uri.rsplit("/", 1)[-1]
        if qid.startswith("Q"):
            out[qid] = label if isinstance(label, str) and label else None
    return out


def decide_label(party1_score: int, party2_score: int, other_score: int, other_threshold: int) -> str:
    if party1_score > 0 and party2_score > 0:
        return "mixed"
    if party1_score > 0 and party2_score == 0:
        return "party1"
    if party2_score > 0 and party1_score == 0:
        return "party2"

    # No party evidence -> default to other (user requirement).
    # Note: other_score/other_threshold are kept in outputs for auditing and tuning,
    # but the label decision no longer depends on them.
    return "other"


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

    attrib_prop_ids = attribution_prop_ids_from_config(cfg)
    cfg_langs = config_languages(cfg)
    cfg_site_keys = site_keys_for_langs(cfg_langs)
    merged = merge_records_by_qid(
        rows,
        lang_keys=cfg_langs,
        site_keys=cfg_site_keys,
        attrib_prop_ids=attrib_prop_ids,
    )
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

    other_hints: Set[str] = set()
    for q in (ccfg.get("other_country_hints") or []):
        if isinstance(q, str) and q.startswith("Q"):
            other_hints.add(q)
    other_country_text_map = _compile_other_country_text_map(ccfg.get("other_country_text_map"))
    place_cfg = _resolve_place_resolution_config(ccfg)

    p1_pat_cfg = _compile_pat_map(ccfg.get("party1_patterns") if isinstance(ccfg.get("party1_patterns"), dict) else {})
    p2_pat_cfg = _compile_pat_map(ccfg.get("party2_patterns") if isinstance(ccfg.get("party2_patterns"), dict) else {})
    other_pat_cfg = _compile_pat_map(ccfg.get("other_patterns") if isinstance(ccfg.get("other_patterns"), dict) else {})

    p1_default, p2_default = _default_party_patterns(party1_ids, party2_ids)
    other_default = _default_other_patterns()

    p1_pat = p1_pat_cfg if any(p1_pat_cfg.values()) else p1_default
    p2_pat = p2_pat_cfg if any(p2_pat_cfg.values()) else p2_default
    other_pat = other_pat_cfg if any(other_pat_cfg.values()) else other_default

    scan_langs = cfg_langs
    if not scan_langs:
        scan_langs = ["en"]

    output_labels_cfg = _resolve_output_label_config(
        cfg=cfg,
        ccfg=ccfg,
        key="output_labels",
        default_enabled=False,
        default_field="country_attribution",
    )
    legacy_labels_cfg = _resolve_output_label_config(
        cfg=cfg,
        ccfg=ccfg,
        key="legacy_output",
        default_enabled=False,
        default_field="legacy_attribution",
    )

    place_qids: Set[str] = set()
    for r in merged:
        raw = r.get("raw_attrib_qids") if isinstance(r.get("raw_attrib_qids"), dict) else {}
        for pid in place_cfg["place_properties"]:
            for q in raw.get(pid) or []:
                if isinstance(q, str) and q.startswith("Q") and q not in party1_ids and q not in party2_ids:
                    place_qids.add(q)

    print(f"[attribution] resolving place->country for {len(place_qids)} place/admin QIDs")
    place_country_map = build_place_country_map(
        sorted(place_qids),
        country_property=place_cfg["country_property"],
        admin_property=place_cfg["admin_property"],
        max_admin_depth=place_cfg["max_admin_depth"],
        batch_size=place_cfg["batch_size"],
        sleep_seconds=place_cfg["sleep_seconds"],
    )

    counts = Counter()
    other_country_counts = Counter()
    other_country_unknown = 0
    for r in merged:
        s1, s2, so, shits = structured_score(
            r,
            party1_ids,
            party2_ids,
            other_hints,
            place_country_map,
            attrib_prop_ids=attrib_prop_ids,
            place_props=place_cfg["place_properties"],
        )
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
                "other_is_strict": False,
                "other_threshold": other_threshold,
                "note": "If no party1/party2 evidence: default to other.",
            },
            "party_qids": {
                "party1": sorted(party1_ids),
                "party2": sorted(party2_ids),
            },
        }

        if label == "other":
            guess_qid, guess_score, guess_ev = guess_other_country(
                r,
                party1_ids=party1_ids,
                party2_ids=party2_ids,
                place_country_map=place_country_map,
                text_country_map=other_country_text_map,
                direct_country_props=place_cfg["direct_country_properties"],
                place_props=place_cfg["place_properties"],
                text_hits=thits,
            )
            if guess_qid:
                other_country_counts[guess_qid] += 1
                r["attribution_detail"]["other_country_guess"] = {
                    "qid": guess_qid,
                    "score": guess_score,
                    "evidence": guess_ev,
                }
            else:
                other_country_unknown += 1
                r["attribution_detail"]["other_country_guess"] = None

        if output_labels_cfg["enabled"]:
            field = output_labels_cfg["field_name"]
            r[field] = output_labels_cfg["label_map"].get(label, label)

        if legacy_labels_cfg["enabled"]:
            field = legacy_labels_cfg["field_name"]
            r[field] = legacy_labels_cfg["label_map"].get(label, label)

        counts[label] += 1

    write_jsonl(args.output, merged)
    print(f"[attribution] wrote {args.output} ({len(merged)} records)")
    print(f"[attribution] counts: {dict(counts)}")

    if args.report:
        top3 = other_country_counts.most_common(3)
        top3_qids = [q for (q, _) in top3]
        report_label_lang = ccfg.get("report_label_lang")
        if not (isinstance(report_label_lang, str) and report_label_lang.strip()):
            lcfg = cfg.get("languages") if isinstance(cfg.get("languages"), dict) else {}
            report_label_lang = lcfg.get("party3")
        if not (isinstance(report_label_lang, str) and report_label_lang.strip()):
            report_label_lang = "en"
        report_label_lang = report_label_lang.strip().lower()

        top3_labels = fetch_qid_labels(top3_qids, lang=report_label_lang) if top3_qids else {}

        report = {
            "input_files": files,
            "loaded_rows": len(rows),
            "unique_qids": len(merged),
            "counts": dict(counts),
            "party1_qids": sorted(party1_ids),
            "party2_qids": sorted(party2_ids),
            "languages_scanned": scan_langs,
            "other_threshold": other_threshold,
            "attribution_properties": attrib_prop_ids,
            "place_country_resolution": place_cfg,
            "output_labels": output_labels_cfg,
            "legacy_output": legacy_labels_cfg,
            "report_label_lang": report_label_lang,
            "other_country_top3": [
                {"qid": q, "label": top3_labels.get(q), "label_lang": report_label_lang, "count": int(c)} for (q, c) in top3
            ],
            "other_country_unknown": int(other_country_unknown),
        }
        write_json(args.report, report)
        print(f"[attribution] wrote report {args.report}")


if __name__ == "__main__":
    main()
