# Conflict Entity Pipeline v2 (Config-Driven)

This folder provides a config-driven pipeline for conflict entity harvesting and attribution.

## What this version adds

- Uses one `config.json` to control parties, languages, navboxes, and categories.
- Splits harvesting into 3 scripts:
  - `harvest_wikidata.py`
  - `harvest_navboxes.py`
  - `harvest_categories.py`
- Uses one attribution script:
  - `attribution.py`
- Keeps one unified JSONL schema across all harvest outputs.
- Keeps your original `ru_ua_*` scripts for backward compatibility.

## Files

- `config.json`: main manual research config.
- `pipeline_common.py`: shared helpers (schema normalization, merge, IO).
- `harvest_wikidata.py`: Wikidata SPARQL harvest.
- `harvest_navboxes.py`: harvest from configured navbox titles.
- `harvest_categories.py`: BFS/DFS category harvest.
- `attribution.py`: classify into `party1 | party2 | mixed | other`.
- `visualization.py` (optional): overlap + attribution summary report and figures.

## Unified JSONL schema (all harvesters)

Each record is normalized to:

- `qid`
- `uri`
- `source`:
  - `type`
  - `page`
  - `hint`
  - `collection_paths`
- `labels`: `{en, ru, uk}`
- `descriptions`: `{en, ru, uk}`
- `aliases`: `{en, ru, uk}`
- `sitelinks`: `{enwiki, ruwiki, ukwiki}`
- `wiki_titles`: `{en, ru, uk}`
- `instance_of`: `[]`
- `raw_attrib_qids`:
  - `P27, P17, P495, P159, P131, P276, P19, P740, P551`

This guarantees all generated JSONL files can be merged/classified consistently.

## Config format

`config.json` is researcher-authored and should look like this (example included in this folder):

```json
{
  "conflicting_parties": {
    "party1": {"ID": "Q159", "allies": ["Q16150196", "Q16746854"]},
    "party2": {"ID": "Q212", "allies": []}
  },
  "languages": {"party1": "ru", "party2": "uk", "party3": "en"},
  "navbox_names": ["Russo-Ukrainian war", "Russo-Ukrainian war (2022-present)"],
  "category_names": ["Russo-Ukrainian war"]
}
```

Optional sections are also supported:

- `navbox_seed_url`
- `categories` (`source_lang`, `depth`, `strategy`, `keywords`)
- `wikidata` (`limit`, `no_aliases`, `ensure_qids`)
- `classification` (`other_threshold`, `other_country_hints`, regex pattern overrides)

## Install

```bash
cd "/Users/yuanyu/Desktop/Russia Ukrain War/pipeline_v2"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install lxml numpy matplotlib matplotlib-venn
```

## Run pipeline

Before running, create the output folder:

```bash
mkdir -p data/entities
```

### 1) Harvest from Wikidata

```bash
python harvest_wikidata.py \
  --config config.json \
  --output data/entities/wikidata_entities.jsonl
```

### 2) Run navbox harvesting

```bash
python harvest_navboxes.py \
  --config config.json \
  --output data/entities/navboxes_entities.jsonl
```

### 3) Run category harvesting (BFS)

```bash
python harvest_categories.py \
  --config config.json \
  --output data/entities/categories_entities.jsonl
```

### 4) Merge and run attribution (classification)

Recommended command (with report):

```bash
python attribution.py \
  --config config.json \
  --entities_folder data/entities \
  --output data/classified_entities.jsonl \
  --report data/classified_report.json
```

### 5) Generate comparison + visualization (optional)

```bash
python visualization.py \
  --entities_folder data/entities \
  --classified data/classified_entities.jsonl \
  --outdir data/visualization
```

### 6) Quick output checks

```bash
ls -lh data/entities
ls -lh data/classified_entities.jsonl data/classified_report.json
ls -lh data/visualization
```

## Label policy in attribution.py

Output labels are:

- `party1`
- `party2`
- `mixed`
- `other`

Decision rule:

- both sides evidence -> `mixed`
- only party1 evidence -> `party1`
- only party2 evidence -> `party2`
- no party1/party2 evidence:
  - if `other_score >= other_threshold` -> `other`
  - else -> `mixed`

This is the strict-other policy you requested.

## Debug / tuning

- Category harvest too broad:
  - lower `categories.depth` in `config.json`
  - use `--max-categories`, `--max-titles`
- Category harvest too slow:
  - keep `strategy=bfs`, `depth=1`
  - reduce keywords scope
- Attribution too many `other`:
  - increase `classification.other_threshold`
- Attribution too many `mixed`:
  - add stronger `party1_patterns` / `party2_patterns` in `config.json`

## Backward compatibility

Legacy scripts remain available:

- `ru_ua_harvest_wikidata_entities.py`
- `ru_ua_harvest_wikipedia_navboxes.py`
- `ru_ua_classify_entities.py`
- `ru_ua_harvest_visual.py`

Use them if you need exact old outputs. The new config-driven scripts are for the cleaner v2 workflow.
