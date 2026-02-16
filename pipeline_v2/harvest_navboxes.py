#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: harvest_navboxes.py

Main purpose:
- Read `navbox_names` from `config.json`.
- Open the seed Wikipedia page (`navbox_seed_url` in config, or default RU-UA page).
- Extract entities from the selected navboxes, resolve to Wikidata QIDs, and enrich
  with labels/descriptions/sitelinks/attribution properties.
- Normalize output to the same schema as other harvesters.

Input:
- --config: path to config JSON (must include `navbox_names`).
- --start-url (optional): override seed page.

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

import ru_ua_harvest_wikipedia_navboxes as core
from pipeline_common import normalize_record, read_json, write_jsonl, merge_records_by_qid


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--start-url", default=None, help="Override start wikipedia URL")
    ap.add_argument("--navbox-index", type=int, default=0, help="Fallback navbox index")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--ensure-qid", nargs="*", default=[], help="Optional extra QIDs to force include")
    args = ap.parse_args()

    cfg = read_json(args.config)
    navbox_names = cfg.get("navbox_names") if isinstance(cfg.get("navbox_names"), list) else []
    navbox_names = [x for x in navbox_names if isinstance(x, str) and x.strip()]
    if not navbox_names:
        raise SystemExit("config.navbox_names is empty")

    start_url = args.start_url or cfg.get("navbox_seed_url") or "https://en.wikipedia.org/wiki/Russo-Ukrainian_War"

    source_lang = core._lang_from_start_url(start_url)
    source_api = core.wiki_api_for_lang(source_lang)
    page_title = core.page_title_from_start_url(start_url)
    base = f"https://{source_lang}.wikipedia.org"

    print(f"[navboxes] Fetching seed page: {page_title} ({source_lang})")
    html = core.fetch_rendered_html_via_parse(page_title, sleep=args.sleep, api_url=source_api)
    soup = core.soup_from_html(html)

    title_to_paths: Dict[str, Set[str]] = defaultdict(set)
    for navbox_title in navbox_names:
        links = core.extract_links_from_one_navbox(
            soup=soup,
            base_url=base,
            navbox_title=navbox_title,
            navbox_index=args.navbox_index,
        )
        titles = set(core.titles_from_urls(links))
        print(f"[navboxes] '{navbox_title}': titles={len(titles)}")
        for t in titles:
            title_to_paths[t].add(f"navbox:{navbox_title}")

    if not title_to_paths:
        raise SystemExit("No titles collected from configured navboxes.")

    all_titles = sorted(title_to_paths.keys())
    t2q = core.wikipedia_titles_to_qids(all_titles, lang=source_lang, sleep=args.sleep)
    print(f"[navboxes] resolved titles -> qids: {len(t2q)} / {len(all_titles)}")

    qid_to_paths: Dict[str, Set[str]] = defaultdict(set)
    qid_to_titles_by_lang = {"en": defaultdict(set), "ru": defaultdict(set), "uk": defaultdict(set)}
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
        query = core.build_sparql_for_qids(batch)
        data = core.run_sparql(query)

        for b in data["results"]["bindings"]:
            item_uri = b.get("item", {}).get("value")
            qid = core.qid_from_uri(item_uri)
            if not qid:
                continue

            insts = set(core.qid_from_uri(x) or x for x in core.split_concat(b.get("insts", {}).get("value")))

            raw_attrib_qids = {}
            for pid in core.ATTRIB_PROP_IDS:
                vals = core.split_concat(b.get(f"{pid}_vals", {}).get("value"))
                qset = set()
                for v in vals:
                    qq = core.qid_from_uri(v) if v.startswith("http") else v
                    if qq:
                        qset.add(qq)
                raw_attrib_qids[pid] = sorted(qset)

            rec = {
                "qid": qid,
                "uri": item_uri or f"http://www.wikidata.org/entity/{qid}",
                "source": {
                    "type": "wikipedia_navboxes",
                    "page": start_url,
                    "hint": core.infer_category_hint(insts),
                    "collection_paths": sorted(qid_to_paths.get(qid, {"navbox"})),
                    "navbox_names": navbox_names,
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
                "aliases": {"en": [], "uk": [], "ru": []},
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
                "instance_of": sorted(insts),
                "raw_attrib_qids": raw_attrib_qids,
            }

            if qid_to_titles_by_lang[source_lang].get(qid):
                rec["wiki_titles"][source_lang] = sorted(qid_to_titles_by_lang[source_lang][qid])[0]

            entities[qid] = rec

    rows = []
    for qid in sorted(entities.keys()):
        nr = normalize_record(entities[qid], source_type="wikipedia_navboxes")
        if nr:
            rows.append(nr)

    rows = merge_records_by_qid(rows)
    write_jsonl(args.output, rows)
    print(f"[navboxes] wrote {args.output} ({len(rows)} records)")


if __name__ == "__main__":
    main()
