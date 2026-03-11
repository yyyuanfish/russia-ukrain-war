#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: harvest_navboxes.py

Main purpose:
- Read `navbox_names` from `config.json`.
- Open the seed Wikipedia page (`navbox_seed_url` in config).
- Extract entities from the selected navboxes, resolve to Wikidata QIDs, and enrich
  with labels/descriptions/sitelinks/attribution properties.
- Normalize output to the same schema as other harvesters.

Input:
- --config: path to config JSON (must include `navbox_names`).
- --start-url (optional): override seed page.
  If omitted, `config.navbox_seed_url` must be provided.
- Optional source-hint mapping:
  - `harvest_hints.instance_of_map` in config.
- Logging:
  - `pipeline.logging` in config controls log file/query logging.
  - `--log-file` can override the log file path for this run.

Output:
- --output: JSONL file (recommended:
  `data/entities/navboxes_entities.jsonl`).

How to run:
  python harvest_navboxes.py --config config.json --output data/entities/navboxes_entities.jsonl

Pipeline step:
- Step 2 (Wikipedia navbox source).
"""

import argparse
from collections import defaultdict
from typing import Dict, List, Set

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
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--start-url", default=None, help="Override start wikipedia URL")
    ap.add_argument("--navbox-index", type=int, default=0, help="Fallback navbox index")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--log-file", default=None, help="Optional log file path (overrides config pipeline.logging.file)")
    ap.add_argument("--ensure-qid", nargs="*", default=[], help="Optional extra QIDs to force include")
    args = ap.parse_args()

    cfg = read_json(args.config)
    setup_script_logging(cfg, config_path=args.config, script_name="harvest_navboxes", override_file=args.log_file)
    if hasattr(wiki_common, "set_logger"):
        wiki_common.set_logger(get_active_logger())
    navbox_names = cfg.get("navbox_names") if isinstance(cfg.get("navbox_names"), list) else []
    navbox_names = [x for x in navbox_names if isinstance(x, str) and x.strip()]
    if not navbox_names:
        raise SystemExit("config.navbox_names is empty")

    start_url = args.start_url or cfg.get("navbox_seed_url")
    if not isinstance(start_url, str) or not start_url.strip():
        raise SystemExit("Missing seed page URL. Set config.navbox_seed_url or pass --start-url.")
    start_url = start_url.strip()
    langs = config_languages(cfg)
    site_keys = site_keys_for_langs(langs)
    attrib_prop_ids = attribution_prop_ids_from_config(cfg)

    hint_map = wiki_common.instance_hint_map_from_config(cfg)

    source_lang, page_title = wiki_common.infer_source_lang_and_title_from_url(start_url)
    if source_lang not in langs:
        langs.append(source_lang)
        site_keys = site_keys_for_langs(langs)
    source_api = wiki_common.wiki_api_for_lang(source_lang)
    base = f"https://{source_lang}.wikipedia.org"

    log_info(f"[navboxes] Fetching seed page: {page_title} ({source_lang})")
    html = wiki_common.fetch_rendered_html_via_parse(page_title, sleep=args.sleep, api_url=source_api)
    soup = wiki_common.soup_from_html(html)

    title_to_paths: Dict[str, Set[str]] = defaultdict(set)
    for navbox_title in navbox_names:
        links = wiki_common.extract_links_from_one_navbox(
            soup=soup,
            base_url=base,
            navbox_title=navbox_title,
            navbox_index=args.navbox_index,
        )
        titles = set(wiki_common.titles_from_urls(links))
        log_info(f"[navboxes] '{navbox_title}': titles={len(titles)}")
        for t in titles:
            title_to_paths[t].add(f"navbox:{navbox_title}")

    if not title_to_paths:
        raise SystemExit("No titles collected from configured navboxes.")

    all_titles = sorted(title_to_paths.keys())
    t2q = wiki_common.wikipedia_titles_to_qids(all_titles, lang=source_lang, sleep=args.sleep)
    log_info(f"[navboxes] resolved titles -> qids: {len(t2q)} / {len(all_titles)}")

    qid_to_paths: Dict[str, Set[str]] = defaultdict(set)
    qid_to_titles_by_lang: Dict[str, Dict[str, Set[str]]] = {lang: defaultdict(set) for lang in langs}
    qids: Set[str] = set()

    for t, q in t2q.items():
        qids.add(q)
        qid_to_paths[q].update(title_to_paths.get(t, set()))
        qid_to_titles_by_lang[source_lang][q].add(t)

    for q in args.ensure_qid:
        if isinstance(q, str) and q.startswith("Q"):
            qids.add(q)
            qid_to_paths[q].add("ensure_qid")

    entities: Dict[str, dict] = {}
    qid_list = sorted(qids)
    batch_size = 120
    for i in range(0, len(qid_list), batch_size):
        batch = qid_list[i:i + batch_size]
        batch_id = (i // batch_size) + 1
        total_batches = ((len(qid_list) - 1) // batch_size) + 1 if qid_list else 0
        log_info(f"[navboxes] enrich_batch={batch_id}/{total_batches} batch_size={len(batch)}")
        query = build_item_enrichment_query(batch, langs, attrib_prop_ids=attrib_prop_ids)
        data = run_wikidata_sparql(query, query_name=f"navboxes.enrich.batch_{batch_id}", log_query=True)

        for b in data["results"]["bindings"]:
            rec = binding_to_enriched_record(b, langs, attrib_prop_ids=attrib_prop_ids)
            if not rec:
                continue

            qid = rec["qid"]
            insts = set(rec.get("instance_of") or [])
            rec["source"] = {
                "type": "wikipedia_navboxes",
                "page": start_url,
                "hint": wiki_common.infer_category_hint(insts, hint_map=hint_map),
                "collection_paths": sorted(qid_to_paths.get(qid, {"navbox"})),
                "navbox_names": navbox_names,
            }

            if qid_to_titles_by_lang[source_lang].get(qid):
                rec["wiki_titles"][source_lang] = sorted(qid_to_titles_by_lang[source_lang][qid])[0]

            entities[qid] = rec

    rows = []
    for qid in sorted(entities.keys()):
        nr = normalize_record(
            entities[qid],
            source_type="wikipedia_navboxes",
            lang_keys=langs,
            site_keys=site_keys,
            attrib_prop_ids=attrib_prop_ids,
        )
        if nr:
            rows.append(nr)

    rows = merge_records_by_qid(rows, lang_keys=langs, site_keys=site_keys, attrib_prop_ids=attrib_prop_ids)
    write_jsonl(args.output, rows)
    log_info(f"[navboxes] wrote {args.output} ({len(rows)} records)")


if __name__ == "__main__":
    main()
