#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: harvest_categories.py

Main purpose:
- Read `category_names` from `config.json`.
- Traverse Wikipedia categories (default strategy is BFS) across configured languages.
- Collect member page titles, resolve them to QIDs, enrich from Wikidata, and write
  normalized JSONL output.

Input:
- --config: path to config JSON (must include `category_names`).
- Optional overrides: `--depth`, `--strategy`, and traversal limits.

Output:
- --output: JSONL file (recommended:
  `data/entities/categories_entities.jsonl`).
- --report: optional summary report (visited categories, title counts, resolved QIDs).

How to run (BFS):
  python harvest_categories.py --config config.json --output data/entities/categories_entities.jsonl

Pipeline step:
- Step 3 (Wikipedia category source, BFS by default).
"""

import argparse
from collections import defaultdict
from typing import Dict, List, Set

import ru_ua_harvest_wikipedia_navboxes as core
from pipeline_common import config_languages, normalize_record, read_json, write_json, write_jsonl, merge_records_by_qid


def _category_title(name: str) -> str:
    n = name.strip()
    if ":" in n:
        return n
    return f"Category:{n}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--report", default=None, help="Optional report JSON path")
    ap.add_argument("--depth", type=int, default=None, help="Category depth override")
    ap.add_argument("--strategy", choices=["bfs", "dfs"], default=None, help="Traversal strategy override")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--max-categories", type=int, default=0)
    ap.add_argument("--max-titles", type=int, default=0)
    ap.add_argument("--max-members-per-category", type=int, default=0)
    ap.add_argument("--progress-every", type=int, default=25)
    args = ap.parse_args()

    cfg = read_json(args.config)
    names = cfg.get("category_names") if isinstance(cfg.get("category_names"), list) else []
    category_names = [x for x in names if isinstance(x, str) and x.strip()]
    if not category_names:
        raise SystemExit("config.category_names is empty")

    ccfg = cfg.get("categories") if isinstance(cfg.get("categories"), dict) else {}
    source_lang = str(ccfg.get("source_lang", "en")).strip().lower() or "en"
    depth = args.depth if args.depth is not None else int(ccfg.get("depth", 1))
    strategy = args.strategy if args.strategy else str(ccfg.get("strategy", "bfs")).strip().lower()
    if strategy not in {"bfs", "dfs"}:
        strategy = "bfs"

    langs = config_languages(cfg)
    if source_lang not in langs:
        langs.append(source_lang)

    kw_cfg = ccfg.get("keywords")
    if isinstance(kw_cfg, list) and kw_cfg:
        keywords = [str(x).strip() for x in kw_cfg if str(x).strip()]
    else:
        keywords = [k.strip() for k in core.DEFAULT_CATEGORY_KEYWORDS.split(",") if k.strip()]

    roots_by_lang: Dict[str, Set[str]] = defaultdict(set)

    for raw_name in category_names:
        src_title = _category_title(raw_name)
        roots_by_lang[source_lang].add(src_title)

        langlinks = core.fetch_langlinks(src_title, source_lang=source_lang, sleep=args.sleep)
        for lang in langs:
            if lang == source_lang:
                continue
            t = langlinks.get(lang)
            if t:
                roots_by_lang[lang].add(t)

    titles_by_lang: Dict[str, Set[str]] = defaultdict(set)
    visited_categories_by_lang: Dict[str, Set[str]] = defaultdict(set)

    for lang in langs:
        roots = roots_by_lang.get(lang, set())
        if not roots:
            print(f"[categories] lang={lang}: no root categories found")
            continue

        api_url = core.wiki_api_for_lang(lang)
        print(f"[categories] lang={lang}: roots={len(roots)}")

        walked = core.walk_categories_collect_titles(
            root_categories=roots,
            depth=max(0, depth),
            strategy=strategy,
            keywords=keywords,
            sleep=args.sleep,
            api_url=api_url,
            max_categories=max(0, args.max_categories),
            max_titles=max(0, args.max_titles),
            max_members_per_category=max(0, args.max_members_per_category),
            progress_every=max(0, args.progress_every),
            progress_prefix=f"[categories:{lang}]",
        )

        titles_by_lang[lang].update(walked["titles"])
        visited_categories_by_lang[lang].update(walked["visited_categories"])

        print(
            f"[categories] lang={lang}: visited_categories={len(visited_categories_by_lang[lang])}, "
            f"titles={len(titles_by_lang[lang])}, stop_reason={walked.get('stop_reason', 'completed')}"
        )

    qid_to_paths: Dict[str, Set[str]] = defaultdict(set)
    qid_to_titles_by_lang = {"en": defaultdict(set), "ru": defaultdict(set), "uk": defaultdict(set)}
    qids: Set[str] = set()

    for lang, tset in titles_by_lang.items():
        if not tset:
            continue
        t2q = core.wikipedia_titles_to_qids(sorted(tset), lang=lang, sleep=args.sleep)
        print(f"[categories] {lang}: resolved titles -> qids: {len(t2q)} / {len(tset)}")
        for t, q in t2q.items():
            qids.add(q)
            qid_to_paths[q].add(f"category:{lang}")
            qid_to_titles_by_lang.setdefault(lang, defaultdict(set))
            qid_to_titles_by_lang[lang][q].add(t)

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
                    "type": "wikipedia_categories",
                    "page": f"https://{source_lang}.wikipedia.org/wiki/{_category_title(category_names[0]).replace(' ', '_')}",
                    "hint": core.infer_category_hint(insts),
                    "collection_paths": sorted(qid_to_paths.get(qid, {"category"})),
                    "category_names": category_names,
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

            for lang in ("en", "ru", "uk"):
                tset = qid_to_titles_by_lang.get(lang, {}).get(qid, set())
                if tset:
                    rec["wiki_titles"][lang] = sorted(tset)[0]

            entities[qid] = rec

    rows = []
    for qid in sorted(entities.keys()):
        nr = normalize_record(entities[qid], source_type="wikipedia_categories")
        if nr:
            rows.append(nr)

    rows = merge_records_by_qid(rows)
    write_jsonl(args.output, rows)
    print(f"[categories] wrote {args.output} ({len(rows)} records)")

    if args.report:
        report = {
            "category_names": category_names,
            "source_lang": source_lang,
            "target_languages": langs,
            "strategy": strategy,
            "depth": depth,
            "visited_categories_by_lang": {k: len(v) for k, v in visited_categories_by_lang.items()},
            "titles_by_lang": {k: len(v) for k, v in titles_by_lang.items()},
            "resolved_qids": len(rows),
        }
        write_json(args.report, report)
        print(f"[categories] wrote report {args.report}")


if __name__ == "__main__":
    main()
