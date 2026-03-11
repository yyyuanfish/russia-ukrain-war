#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: harvest_wikidata.py

Main purpose:
- Harvest conflict-related entities from Wikidata in a config-driven way.
- Discover conflict seed QIDs from config (and optionally from `navbox_seed_url`).
- Expand entities via generic buckets (people/events/organizations/policies/media narratives/related).
- Normalize records into the shared JSONL schema used by the whole pipeline.

Input:
- --config: path to config JSON (uses optional `wikidata` section):
  - `seed_qids`: explicit conflict seed QIDs (recommended)
  - `seed_from_navbox_page`: if true, also resolve QID from `navbox_seed_url`
  - `limit`: optional per-bucket query limit
  - `no_aliases`: skip alias enrichment
  - `ensure_qids`: additional QIDs to always include
  - `type_anchors`: optional QID anchors used by generic bucket queries
  - `relation_properties`: optional relation property IDs used by bucket queries
  - `bucket_queries`: optional full custom WHERE blocks by bucket name
  - `aliases` (optional):
    - `enabled`: enable/disable alias enrichment (default true)
    - `max_total_per_qid`: cap total aliases per QID across all languages (0 means unlimited)
    - `max_per_lang`: cap aliases per language per QID (0 means unlimited)
- Alias language control:
  - `languages.all` in config.
- Source hint control:
  - `harvest_hints.instance_of_map` in config (optional).
- Logging:
  - `pipeline.logging` in config controls log file/query logging.
  - `--log-file` can override the log file path for this run.

Output:
- --output: JSONL file (recommended: `data/entities/wikidata_entities.jsonl`).
- --array: optional pretty JSON array dump of the same records.

How to run:
  python harvest_wikidata.py --config config.json --output data/entities/wikidata_entities.jsonl

Pipeline step:
- Step 1 (first harvester). Run this before `attribution.py`.
"""

import argparse
import json
from collections import defaultdict
from typing import Dict, List, Optional, Set

import wikipedia_common as wiki_common
from pipeline_common import (
    attribution_prop_ids_from_config,
    binding_to_enriched_record,
    build_item_enrichment_query,
    config_languages,
    get_active_logger,
    log_info,
    merge_records_by_qid,
    normalize_record,
    read_json,
    run_wikidata_sparql,
    setup_script_logging,
    site_keys_for_langs,
    write_jsonl,
    ensure_parent,
)

PREFIXES = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
"""

DEFAULT_TYPE_ANCHORS = {
    "human": "Q5",
    "battle": "Q178561",
    "military_conflict": "Q180684",
    "attack": "Q645883",
    "mass_killing": "Q167442",
    "military_unit": "Q176799",
    "government": "Q7188",
    "organization": "Q43229",
    "law": "Q820655",
    "public_policy": "Q7163",
    "economic_sanction": "Q618779",
    "resolution": "Q182994",
    "conspiracy_theory": "Q17379835",
    "propaganda": "Q215080",
}

DEFAULT_RELATION_PROPERTIES = {
    "part_of": "P361",
    "conflict_participant": "P607",
    "participant_in": "P1344",
    "main_subject": "P921",
}


def _parse_optional_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        v = int(x)
        return v if v > 0 else None
    except Exception:
        return None


def _parse_nonnegative_int(x, default: int = 0) -> int:
    try:
        v = int(x)
        return v if v >= 0 else default
    except Exception:
        return default


def _resolve_alias_config(wcfg: dict) -> dict:
    acfg = wcfg.get("aliases") if isinstance(wcfg.get("aliases"), dict) else {}
    return {
        "enabled": bool(acfg.get("enabled", True)),
        "max_total_per_qid": _parse_nonnegative_int(acfg.get("max_total_per_qid"), default=0),
        "max_per_lang": _parse_nonnegative_int(acfg.get("max_per_lang"), default=0),
    }


def _normalize_qids(vals) -> List[str]:
    out: List[str] = []
    if not isinstance(vals, list):
        return out
    seen = set()
    for v in vals:
        if not (isinstance(v, str) and v.startswith("Q")):
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _normalize_type_anchors(raw: dict) -> Dict[str, str]:
    out = dict(DEFAULT_TYPE_ANCHORS)
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        key = k.strip().lower()
        if not key:
            continue
        if isinstance(v, str) and v.startswith("Q"):
            out[key] = v.strip()
    return out


def _normalize_relation_properties(raw: dict) -> Dict[str, str]:
    out = dict(DEFAULT_RELATION_PROPERTIES)
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        key = k.strip().lower()
        if not key:
            continue
        if isinstance(v, str) and v.strip().upper().startswith("P"):
            out[key] = v.strip().upper()
    return out


def _resolve_seed_qids(config: dict, sleep: float = 0.25) -> List[str]:
    wcfg = config.get("wikidata") if isinstance(config.get("wikidata"), dict) else {}
    seeds: List[str] = []
    seen = set()

    for q in _normalize_qids(wcfg.get("seed_qids") or []):
        if q not in seen:
            seen.add(q)
            seeds.append(q)

    use_navbox_seed = bool(wcfg.get("seed_from_navbox_page", True))
    if use_navbox_seed:
        start_url = config.get("navbox_seed_url")
        if isinstance(start_url, str) and start_url.strip():
            start_url = start_url.strip()
            seed_lang, seed_title = wiki_common.infer_source_lang_and_title_from_url(start_url)
            t2q = wiki_common.wikipedia_titles_to_qids([seed_title], lang=seed_lang, sleep=sleep)
            for q in t2q.values():
                if isinstance(q, str) and q.startswith("Q") and q not in seen:
                    seen.add(q)
                    seeds.append(q)

    return seeds


def _query_qids(where_block: str, limit: Optional[int] = None, query_name: str = "wikidata.bucket") -> List[str]:
    query = PREFIXES + f"""
SELECT DISTINCT ?item WHERE {{
{where_block}
}}
"""
    if isinstance(limit, int) and limit > 0:
        query += f"\nLIMIT {limit}"

    data = run_wikidata_sparql(query, query_name=query_name, log_query=True)
    out: List[str] = []
    seen = set()
    for b in data.get("results", {}).get("bindings", []):
        uri = b.get("item", {}).get("value")
        if not isinstance(uri, str) or "/Q" not in uri:
            continue
        qid = uri.rsplit("/", 1)[-1]
        if qid.startswith("Q") and qid not in seen:
            seen.add(qid)
            out.append(qid)
    return out


def _bucket_queries(seed_qids: List[str], type_anchors: Dict[str, str], rel_props: Dict[str, str]) -> Dict[str, str]:
    seed_values = " ".join(f"wd:{q}" for q in seed_qids)
    p_part_of = rel_props["part_of"]
    p_conflict_participant = rel_props["conflict_participant"]
    p_participant_in = rel_props["participant_in"]
    p_main_subject = rel_props["main_subject"]

    return {
        "person": f"""
  VALUES ?seed {{ {seed_values} }}
  ?item wdt:P31 wd:{type_anchors["human"]} .
  ?item wdt:{p_conflict_participant} ?seed .
""",
        "event": f"""
  VALUES ?seed {{ {seed_values} }}
  VALUES ?etype {{ wd:{type_anchors["battle"]} wd:{type_anchors["military_conflict"]} wd:{type_anchors["attack"]} wd:{type_anchors["mass_killing"]} }}
  ?item wdt:P31/wdt:P279* ?etype .
  {{ ?item wdt:{p_part_of} ?seed }} UNION {{ ?item wdt:{p_conflict_participant} ?seed }} UNION {{ ?item wdt:{p_participant_in} ?seed }}
""",
        "organization": f"""
  VALUES ?seed {{ {seed_values} }}
  VALUES ?otype {{ wd:{type_anchors["military_unit"]} wd:{type_anchors["organization"]} wd:{type_anchors["government"]} }}
  ?item wdt:P31/wdt:P279* ?otype .
  {{ ?item wdt:{p_conflict_participant} ?seed }} UNION {{ ?item wdt:{p_participant_in} ?seed }}
""",
        "policy": f"""
  VALUES ?seed {{ {seed_values} }}
  VALUES ?ptype {{ wd:{type_anchors["law"]} wd:{type_anchors["public_policy"]} wd:{type_anchors["economic_sanction"]} wd:{type_anchors["resolution"]} }}
  ?item wdt:P31/wdt:P279* ?ptype .
  ?item wdt:{p_main_subject} ?seed .
""",
        "media_narrative": f"""
  VALUES ?seed {{ {seed_values} }}
  VALUES ?ntype {{ wd:{type_anchors["conspiracy_theory"]} wd:{type_anchors["propaganda"]} }}
  ?item wdt:P31/wdt:P279* ?ntype .
  ?item wdt:{p_main_subject} ?seed .
""",
        "related": f"""
  VALUES ?seed {{ {seed_values} }}
  {{ ?item wdt:{p_part_of} ?seed }} UNION
  {{ ?item wdt:{p_conflict_participant} ?seed }} UNION
  {{ ?item wdt:{p_participant_in} ?seed }} UNION
  {{ ?item wdt:{p_main_subject} ?seed }}
""",
    }


def _resolve_bucket_queries(
    seed_qids: List[str],
    wcfg: dict,
    type_anchors: Dict[str, str],
    rel_props: Dict[str, str],
) -> Dict[str, str]:
    seed_values = " ".join(f"wd:{q}" for q in seed_qids)
    custom = wcfg.get("bucket_queries") if isinstance(wcfg.get("bucket_queries"), dict) else {}
    out: Dict[str, str] = {}
    for name, where_block in custom.items():
        if not (isinstance(name, str) and name.strip() and isinstance(where_block, str) and where_block.strip()):
            continue
        out[name.strip()] = where_block.replace("{seed_values}", seed_values).strip()

    if out:
        return out

    return _bucket_queries(seed_qids, type_anchors=type_anchors, rel_props=rel_props)


def _collect_qids(
    seed_qids: List[str],
    limit: Optional[int],
    wcfg: dict,
    type_anchors: Dict[str, str],
    rel_props: Dict[str, str],
) -> Dict[str, Set[str]]:
    qid_to_paths: Dict[str, Set[str]] = defaultdict(set)

    for q in seed_qids:
        qid_to_paths[q].add("wikidata_seed")

    bucket_map = _resolve_bucket_queries(seed_qids, wcfg=wcfg, type_anchors=type_anchors, rel_props=rel_props)
    for bucket, where_block in bucket_map.items():
        log_info(f"[wikidata] Querying {bucket}...")
        qids = _query_qids(where_block, limit=limit, query_name=f"wikidata.bucket.{bucket}")
        log_info(f"[wikidata] bucket={bucket} qids={len(qids)}")
        for q in qids:
            qid_to_paths[q].add(f"wikidata:{bucket}")

    return qid_to_paths


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for it in items:
        if it in seen:
            continue
        seen.add(it)
        out.append(it)
    return out


def _cap_aliases_per_qid(
    by_lang: Dict[str, List[str]],
    langs: List[str],
    max_total_per_qid: int,
) -> Dict[str, List[str]]:
    if max_total_per_qid <= 0:
        return by_lang

    capped: Dict[str, List[str]] = {lang: [] for lang in langs}
    idx: Dict[str, int] = {lang: 0 for lang in langs}
    total = 0

    while total < max_total_per_qid:
        moved = False
        for lang in langs:
            vals = by_lang.get(lang, [])
            i = idx[lang]
            if i >= len(vals):
                continue
            capped[lang].append(vals[i])
            idx[lang] = i + 1
            total += 1
            moved = True
            if total >= max_total_per_qid:
                break
        if not moved:
            break

    return capped


def _collect_aliases(
    qid: str,
    langs: List[str],
    max_total_per_qid: int = 0,
    max_per_lang: int = 0,
) -> Dict[str, List[str]]:
    if not (isinstance(qid, str) and qid.startswith("Q")):
        return {l: [] for l in langs}

    filter_langs = ",".join(f'"{l}"' for l in langs)
    limit_clause = f"\nLIMIT {max_total_per_qid}" if max_total_per_qid > 0 else ""
    query = PREFIXES + f"""
SELECT ?alias ?lang WHERE {{
  VALUES ?item {{ wd:{qid} }}
  ?item skos:altLabel ?alias .
  BIND(LANG(?alias) AS ?lang)
  FILTER(?lang IN ({filter_langs}))
}}
{limit_clause}
"""
    out: Dict[str, List[str]] = {l: [] for l in langs}
    try:
        data = run_wikidata_sparql(query, query_name=f"wikidata.aliases.{qid}", log_query=False)
    except Exception:
        return out

    for b in data.get("results", {}).get("bindings", []):
        alias = b.get("alias", {}).get("value")
        lang = b.get("lang", {}).get("value")
        if isinstance(lang, str) and isinstance(alias, str) and lang in out:
            out[lang].append(alias)

    for lang in langs:
        vals = _dedupe_preserve_order(out[lang])
        if max_per_lang > 0:
            vals = vals[:max_per_lang]
        out[lang] = vals

    return _cap_aliases_per_qid(out, langs, max_total_per_qid=max_total_per_qid)


def _enrich_aliases(
    records: List[dict],
    langs: List[str],
    max_total_per_qid: int = 0,
    max_per_lang: int = 0,
) -> None:
    total = len(records)
    for i, rec in enumerate(records, 1):
        rec["aliases"] = _collect_aliases(
            rec["qid"],
            langs,
            max_total_per_qid=max_total_per_qid,
            max_per_lang=max_per_lang,
        )
        if i % 100 == 0 or i == total:
            log_info(f"[wikidata] alias_progress={i}/{total}")
        if i % 25 == 0:
            # gentle pacing
            import time
            time.sleep(0.2)


def harvest(config: dict, limit: Optional[int], no_aliases: bool, extra_ensure_qids: Set[str]) -> List[dict]:
    wcfg = config.get("wikidata") if isinstance(config.get("wikidata"), dict) else {}
    langs = config_languages(config)
    site_keys = site_keys_for_langs(langs)
    attrib_prop_ids = attribution_prop_ids_from_config(config)
    hint_map = wiki_common.instance_hint_map_from_config(config)
    type_anchors = _normalize_type_anchors(wcfg.get("type_anchors"))
    rel_props = _normalize_relation_properties(wcfg.get("relation_properties"))
    alias_cfg = _resolve_alias_config(wcfg)

    seed_qids = _resolve_seed_qids(config)
    if not seed_qids:
        raise SystemExit(
            "No Wikidata seeds found. Set wikidata.seed_qids or provide navbox_seed_url + wikidata.seed_from_navbox_page=true."
        )
    log_info(f"[wikidata] seed_qids={seed_qids}")

    qid_to_paths = _collect_qids(
        seed_qids,
        limit=limit,
        wcfg=wcfg,
        type_anchors=type_anchors,
        rel_props=rel_props,
    )

    for q in sorted(extra_ensure_qids):
        qid_to_paths[q].add("ensure_qid")

    qids = sorted(qid_to_paths.keys())
    entities: Dict[str, dict] = {}

    batch_size = 120
    for i in range(0, len(qids), batch_size):
        batch = qids[i:i + batch_size]
        batch_id = (i // batch_size) + 1
        total_batches = ((len(qids) - 1) // batch_size) + 1 if qids else 0
        log_info(f"[wikidata] enrich_batch={batch_id}/{total_batches} batch_size={len(batch)}")
        query = build_item_enrichment_query(batch, langs, attrib_prop_ids=attrib_prop_ids)
        data = run_wikidata_sparql(
            query,
            query_name=f"wikidata.enrich.batch_{batch_id}",
            log_query=True,
        )

        for b in data.get("results", {}).get("bindings", []):
            rec = binding_to_enriched_record(b, langs, attrib_prop_ids=attrib_prop_ids)
            if not rec:
                continue

            qid = rec["qid"]
            insts = set(rec.get("instance_of") or [])
            rec["source"] = {
                "type": "wikidata_sparql",
                "page": "https://query.wikidata.org/sparql",
                "hint": wiki_common.infer_category_hint(insts, hint_map=hint_map),
                "collection_paths": sorted(qid_to_paths.get(qid, {"wikidata"})),
                "seed_qids": seed_qids,
            }
            entities[qid] = rec

    rows = [entities[q] for q in sorted(entities.keys())]

    if not no_aliases and alias_cfg.get("enabled", True):
        log_info(
            "[wikidata] Enriching aliases... "
            f"(max_total_per_qid={alias_cfg['max_total_per_qid']}, max_per_lang={alias_cfg['max_per_lang']})"
        )
        _enrich_aliases(
            rows,
            langs,
            max_total_per_qid=alias_cfg["max_total_per_qid"],
            max_per_lang=alias_cfg["max_per_lang"],
        )

    out: List[dict] = []
    for r in rows:
        nr = normalize_record(
            r,
            source_type="wikidata_sparql",
            collection_paths=(r.get("source", {}).get("collection_paths") or ["wikidata"]),
            lang_keys=langs,
            site_keys=site_keys,
            attrib_prop_ids=attrib_prop_ids,
        )
        if nr:
            out.append(nr)

    out = merge_records_by_qid(out, lang_keys=langs, site_keys=site_keys, attrib_prop_ids=attrib_prop_ids)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--array", default=None, help="Optional output JSON array path")
    ap.add_argument("--log-file", default=None, help="Optional log file path (overrides config pipeline.logging.file)")
    ap.add_argument("--limit", type=int, default=None, help="Optional per-bucket LIMIT override")
    ap.add_argument("--no-aliases", action="store_true", help="Skip alias enrichment")
    args = ap.parse_args()

    config = read_json(args.config)
    setup_script_logging(config, config_path=args.config, script_name="harvest_wikidata", override_file=args.log_file)
    if hasattr(wiki_common, "set_logger"):
        wiki_common.set_logger(get_active_logger())
    wcfg = config.get("wikidata") if isinstance(config.get("wikidata"), dict) else {}

    limit = args.limit if args.limit is not None else _parse_optional_int(wcfg.get("limit"))
    no_aliases = bool(args.no_aliases or wcfg.get("no_aliases", False))

    extra_ensure_qids: Set[str] = set(_normalize_qids(wcfg.get("ensure_qids") or []))

    rows = harvest(
        config=config,
        limit=limit,
        no_aliases=no_aliases,
        extra_ensure_qids=extra_ensure_qids,
    )

    write_jsonl(args.output, rows)
    log_info(f"[wikidata] wrote {args.output} ({len(rows)} records)")

    if args.array:
        ensure_parent(args.array)
        with open(args.array, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        log_info(f"[wikidata] wrote {args.array}")


if __name__ == "__main__":
    main()
