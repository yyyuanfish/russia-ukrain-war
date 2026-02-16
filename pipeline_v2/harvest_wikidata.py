#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: harvest_wikidata.py

Main purpose:
- Harvest conflict-related entities directly from Wikidata using SPARQL buckets
  (people, events, organizations, policies, media narratives).
- Normalize every record into the shared JSONL schema used by the whole pipeline.
- Produce the Wikidata entity source file for downstream attribution.

Input:
- --config: path to config JSON (uses optional `wikidata` section:
  `limit`, `no_aliases`, `ensure_qids`).

Output:
- --output: JSONL file (recommended:
  `data/entities/wikidata_entities.jsonl`).
- --array: optional pretty JSON array dump of the same records.

How to run:
  python harvest_wikidata.py --config config.json --output data/entities/wikidata_entities.jsonl

Pipeline step:
- Step 1 (first harvester). Run this before `attribution.py`.
"""

import argparse
import json
from typing import List, Optional, Set

import ru_ua_harvest_wikidata_entities as core
from pipeline_common import normalize_record, read_json, write_jsonl, ensure_parent


def _parse_optional_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        v = int(x)
        return v if v > 0 else None
    except Exception:
        return None


def harvest(limit: Optional[int], no_aliases: bool, extra_ensure_qids: Set[str]) -> List[dict]:
    rows: List[dict] = []

    print("[wikidata] Querying people...")
    people = core.query_people(limit=limit)
    rows += core.bindings_to_records(people["results"]["bindings"], "person")

    print("[wikidata] Querying events...")
    events = core.query_events(limit=limit)
    rows += core.bindings_to_records(events["results"]["bindings"], "event")

    print("[wikidata] Querying organizations...")
    orgs = core.query_orgs(limit=limit)
    rows += core.bindings_to_records(orgs["results"]["bindings"], "organization")

    print("[wikidata] Querying policies...")
    policies = core.query_policies(limit=limit)
    rows += core.bindings_to_records(policies["results"]["bindings"], "policy")

    print("[wikidata] Querying media narratives...")
    narratives = core.query_media_narratives(limit=limit)
    rows += core.bindings_to_records(narratives["results"]["bindings"], "media_narrative")

    rows = core.dedupe_by_qid(rows)

    ensure_qids = set(core.ENSURE_QIDS) | set(extra_ensure_qids)
    present = {r.get("qid") for r in rows if isinstance(r.get("qid"), str)}
    missing = sorted(q for q in ensure_qids if q not in present)
    if missing:
        print(f"[wikidata] Ensuring QIDs (missing -> add): {missing}")
        extra = core.query_by_qids(missing, limit=None)
        rows += core.bindings_to_records(extra["results"]["bindings"], "organization")
        rows = core.dedupe_by_qid(rows)

    if not no_aliases:
        print("[wikidata] Enriching aliases...")
        core.enrich_aliases(rows, langs=("en", "ru", "uk"))

    out: List[dict] = []
    for r in rows:
        nr = normalize_record(r, source_type="wikidata_sparql", collection_paths=["wikidata"]) 
        if nr:
            out.append(nr)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--array", default=None, help="Optional output JSON array path")
    ap.add_argument("--limit", type=int, default=None, help="Optional per-query LIMIT override")
    ap.add_argument("--no-aliases", action="store_true", help="Skip alias enrichment")
    args = ap.parse_args()

    config = read_json(args.config)
    wcfg = config.get("wikidata") if isinstance(config.get("wikidata"), dict) else {}

    limit = args.limit if args.limit is not None else _parse_optional_int(wcfg.get("limit"))
    no_aliases = bool(args.no_aliases or wcfg.get("no_aliases", False))

    extra_ensure_qids: Set[str] = set()
    for q in (wcfg.get("ensure_qids") or []):
        if isinstance(q, str) and q.startswith("Q"):
            extra_ensure_qids.add(q)

    rows = harvest(limit=limit, no_aliases=no_aliases, extra_ensure_qids=extra_ensure_qids)

    write_jsonl(args.output, rows)
    print(f"[wikidata] wrote {args.output} ({len(rows)} records)")

    if args.array:
        ensure_parent(args.array)
        with open(args.array, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"[wikidata] wrote {args.array}")


if __name__ == "__main__":
    main()
