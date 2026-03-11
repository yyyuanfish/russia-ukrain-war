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
- Language control:
  - `categories.source_lang`: language of `category_names`.
  - `categories.langs`: target Wikipedia languages to crawl (e.g., `["en","ru","uk"]`).
  - `categories.use_keyword_filter` (optional, default `true`): turn subcategory keyword filter on/off.
  - `categories.keywords` (optional): global subcategory filter keywords.
  - `categories.keywords_by_lang` (optional): per-language keywords (overrides `categories.keywords` per language).
    If omitted, lightweight keywords are auto-derived per language from resolved root category titles.
- Optional source-hint mapping:
  - `harvest_hints.instance_of_map` in config.
- Logging:
  - `pipeline.logging` in config controls log file/query logging.
  - `--log-file` can override the log file path for this run.

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
    write_json,
    write_jsonl,
)


def _category_title(name: str) -> str:
    n = name.strip()
    if ":" in n:
        return n
    return f"Category:{n}"


def _auto_keywords(names: List[str]) -> List[str]:
    """
    Build lightweight keyword anchors from configured category names.
    Used only when config does not provide explicit keywords.
    """
    out: List[str] = []
    for name in names:
        raw = name.replace("Category:", " ")
        token = []
        for ch in raw:
            token.append(ch if ch.isalnum() else " ")
        for t in "".join(token).split():
            if len(t) < 4:
                continue
            tt = t.strip()
            if tt and tt not in out:
                out.append(tt)
    return out


def _keyword_list(raw) -> List[str]:
    out: List[str] = []
    if isinstance(raw, list):
        for x in raw:
            s = str(x).strip()
            if s and s not in out:
                out.append(s)
    return out


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
    ap.add_argument("--log-file", default=None, help="Optional log file path (overrides config pipeline.logging.file)")
    args = ap.parse_args()

    cfg = read_json(args.config)
    setup_script_logging(cfg, config_path=args.config, script_name="harvest_categories", override_file=args.log_file)
    if hasattr(wiki_common, "set_logger"):
        wiki_common.set_logger(get_active_logger())
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

    langs_cfg = ccfg.get("langs")
    if not isinstance(langs_cfg, list):
        langs_cfg = ccfg.get("languages")

    langs: List[str] = []
    if isinstance(langs_cfg, list):
        for x in langs_cfg:
            if isinstance(x, str):
                v = x.strip().lower()
                if v and v not in langs:
                    langs.append(v)
    if not langs:
        langs = config_languages(cfg)

    if source_lang not in langs:
        langs.append(source_lang)
    site_keys = site_keys_for_langs(langs)
    attrib_prop_ids = attribution_prop_ids_from_config(cfg)
    hint_map = wiki_common.instance_hint_map_from_config(cfg)

    roots_by_lang: Dict[str, Set[str]] = defaultdict(set)

    for raw_name in category_names:
        src_title = _category_title(raw_name)
        roots_by_lang[source_lang].add(src_title)

        langlinks = wiki_common.fetch_langlinks(src_title, source_lang=source_lang, sleep=args.sleep)
        for lang in langs:
            if lang == source_lang:
                continue
            t = langlinks.get(lang)
            if t:
                roots_by_lang[lang].add(t)

    use_keyword_filter = bool(ccfg.get("use_keyword_filter", True))
    global_keywords = _keyword_list(ccfg.get("keywords"))
    kw_by_lang_cfg = ccfg.get("keywords_by_lang") if isinstance(ccfg.get("keywords_by_lang"), dict) else {}
    keywords_by_lang: Dict[str, List[str]] = {lang: [] for lang in langs}

    if use_keyword_filter:
        for lang in langs:
            lang_keywords = _keyword_list(kw_by_lang_cfg.get(lang)) if kw_by_lang_cfg else []
            if lang_keywords:
                keywords_by_lang[lang] = lang_keywords
                continue

            if global_keywords:
                keywords_by_lang[lang] = list(global_keywords)
                continue

            auto_source = sorted(roots_by_lang.get(lang, set()))
            if not auto_source:
                auto_source = category_names
            keywords_by_lang[lang] = _auto_keywords(auto_source)

    titles_by_lang: Dict[str, Set[str]] = defaultdict(set)
    visited_categories_by_lang: Dict[str, Set[str]] = defaultdict(set)

    for lang in langs:
        roots = roots_by_lang.get(lang, set())
        if not roots:
            log_info(f"[categories] lang={lang}: no root categories found")
            continue

        lang_keywords = keywords_by_lang.get(lang, [])
        api_url = wiki_common.wiki_api_for_lang(lang)
        log_info(
            f"[categories] lang={lang}: roots={len(roots)}, keyword_filter={'on' if use_keyword_filter else 'off'}, "
            f"keywords={len(lang_keywords)}"
        )

        walked = wiki_common.walk_categories_collect_titles(
            root_categories=roots,
            depth=max(0, depth),
            strategy=strategy,
            keywords=lang_keywords,
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

        log_info(
            f"[categories] lang={lang}: visited_categories={len(visited_categories_by_lang[lang])}, "
            f"titles={len(titles_by_lang[lang])}, stop_reason={walked.get('stop_reason', 'completed')}"
        )

    qid_to_paths: Dict[str, Set[str]] = defaultdict(set)
    qid_to_titles_by_lang: Dict[str, Dict[str, Set[str]]] = {lang: defaultdict(set) for lang in langs}
    qids: Set[str] = set()

    for lang, tset in titles_by_lang.items():
        if not tset:
            continue
        t2q = wiki_common.wikipedia_titles_to_qids(sorted(tset), lang=lang, sleep=args.sleep)
        log_info(f"[categories] {lang}: resolved titles -> qids: {len(t2q)} / {len(tset)}")
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
        batch_id = (i // batch_size) + 1
        total_batches = ((len(qid_list) - 1) // batch_size) + 1 if qid_list else 0
        log_info(f"[categories] enrich_batch={batch_id}/{total_batches} batch_size={len(batch)}")
        query = build_item_enrichment_query(batch, langs, attrib_prop_ids=attrib_prop_ids)
        data = run_wikidata_sparql(query, query_name=f"categories.enrich.batch_{batch_id}", log_query=True)

        for b in data["results"]["bindings"]:
            rec = binding_to_enriched_record(b, langs, attrib_prop_ids=attrib_prop_ids)
            if not rec:
                continue

            qid = rec["qid"]
            insts = set(rec.get("instance_of") or [])
            rec["source"] = {
                "type": "wikipedia_categories",
                "page": f"https://{source_lang}.wikipedia.org/wiki/{_category_title(category_names[0]).replace(' ', '_')}",
                "hint": wiki_common.infer_category_hint(insts, hint_map=hint_map),
                "collection_paths": sorted(qid_to_paths.get(qid, {"category"})),
                "category_names": category_names,
            }

            for lang in langs:
                tset = qid_to_titles_by_lang.get(lang, {}).get(qid, set())
                if tset:
                    rec["wiki_titles"][lang] = sorted(tset)[0]

            entities[qid] = rec

    rows = []
    for qid in sorted(entities.keys()):
        nr = normalize_record(
            entities[qid],
            source_type="wikipedia_categories",
            lang_keys=langs,
            site_keys=site_keys,
            attrib_prop_ids=attrib_prop_ids,
        )
        if nr:
            rows.append(nr)

    rows = merge_records_by_qid(rows, lang_keys=langs, site_keys=site_keys, attrib_prop_ids=attrib_prop_ids)
    write_jsonl(args.output, rows)
    log_info(f"[categories] wrote {args.output} ({len(rows)} records)")

    if args.report:
        report = {
            "category_names": category_names,
            "source_lang": source_lang,
            "target_languages": langs,
            "use_keyword_filter": use_keyword_filter,
            "keywords_global": global_keywords,
            "keywords_by_lang": keywords_by_lang,
            "strategy": strategy,
            "depth": depth,
            "visited_categories_by_lang": {k: len(v) for k, v in visited_categories_by_lang.items()},
            "titles_by_lang": {k: len(v) for k, v in titles_by_lang.items()},
            "resolved_qids": len(rows),
        }
        write_json(args.report, report)
        log_info(f"[categories] wrote report {args.report}")


if __name__ == "__main__":
    main()
