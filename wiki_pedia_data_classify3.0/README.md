# Russo–Ukrainian War Knowledge Graph (RU/UA)
Entity Harvest → Unified JSONL → Attribution Classify → Overlap Visualisation

This project builds a Wikidata-backed entity inventory for the **Russo–Ukrainian War** using two complementary harvesting methods, then classifies each entity into **Russian / Ukraine / mixed / other**, and finally produces overlap & distribution visualisations.

## Why this exists
- Wikipedia navboxes provide broad, human-curated link discovery (wide coverage).
- Wikidata SPARQL provides targeted, structured discovery (high precision for specific buckets).
- A strict attribution policy keeps `other` clean and prevents “uncertain war-related” items from being dumped into `other` (they become `mixed`).

---

## Pipeline overview

1) Harvest from Wikidata (SPARQL queries)  
2) Harvest from Wikipedia (bottom navboxes only)  
3) Merge + classify entities into RU/UA/mixed/other  
4) Visualise overlap + pre/post distributions

High-level flow:

```
Wikipedia navboxes (EN) ─┐
                         ├─> unified JSONL ──> merge ──> classify ──> plots + reports
Wikidata SPARQL ─────────┘
```

---

## Repository / file layout (suggested)

```
.
├── ru_ua_harvest_wikidata_entities.py
├── ru_ua_harvest_wikipedia_navboxes.py
├── ru_ua_classify_entities.py
├── ru_ua_harvest_visual.py
└── outputs/
    ├── wd_ru_ua_entities.jsonl
    ├── wd_ru_ua_entities.json
    ├── navbox_ru_ua_entities.jsonl
    ├── navbox_report.json
    ├── classified_ru_ua_entities.jsonl
    ├── classified_report.json
    └── overlap_analysis_output/
        ├── overlap_report_v2.json
        └── *.png
```

Your run used these main files:
- `wd_ru_ua_entities.jsonl` (Wikidata harvest)
- `navbox_ru_ua_entities.jsonl` (Wikipedia navbox harvest)
- `classified_ru_ua_entities.jsonl` + `classified_report.json` (merged + classified)
- `overlap_analysis_output*/overlap_report_v2.json` + figures (visualisation)

---

## Setup

### Create a virtual environment (example)
```bash
python -m venv ru_ua
source ru_ua/bin/activate
pip install -U pip
```

### Install dependencies
Minimum:
```bash
pip install requests beautifulsoup4 lxml SPARQLWrapper numpy matplotlib
```

Optional (only if you want `--venn`):
```bash
pip install matplotlib-venn
```

---

## Script 1 — Wikidata harvest
**File:** `ru_ua_harvest_wikidata_entities.py`

### What it does
- Runs WDQS queries for several buckets (e.g., people/events/organizations/policies/media narratives).
- Deduplicates results into a single QID set.
- Enriches multilingual fields (labels/descriptions/aliases depending on script settings).
- Writes:
  - JSONL (`wd_ru_ua_entities.jsonl`)
  - JSON array (`wd_ru_ua_entities.json`)

### Run
```bash
python ru_ua_harvest_wikidata_entities.py
```

Typical console output includes:
- “Querying people... events... organizations... policies... media narratives...”
- “Deduped total: 462”
- “Ensuring QIDs ... After ensure, total: 466”
- “Wrote JSONL ... (466 records)”

---

## Script 2 — Wikipedia navbox harvest (bottom-of-page only)
**File:** `ru_ua_harvest_wikipedia_navboxes.py`

### What it does
- Fetches a Wikipedia start page (default: Russo-Ukrainian_War).
- Extracts links ONLY from **bottom navboxes** (`.navbox` elements in the footer).
- Converts enwiki titles → Wikidata QIDs.
- Enriches QIDs (labels/descriptions/sitelinks + instance-of + raw attribution properties).
- Writes:
  - JSONL (`navbox_ru_ua_entities.jsonl`)
  - Report JSON (`navbox_report.json`)

### Run
```bash
python ru_ua_harvest_wikipedia_navboxes.py
```

Example output (your run):
- Total links collected from boxes: 2085
- Unique titles: 2014
- Resolved QIDs: 1781
- Writes `navbox_ru_ua_entities.jsonl` and prints the compact report.

---

## Script 3 — Merge + classify (RU/UA/mixed/other)
**File:** `ru_ua_classify_entities.py`

### Goal
Merge duplicates across inputs and classify each entity into:
- `Russian`
- `Ukraine`
- `mixed`
- `other` (strict)

### Evidence sources (highest → lowest)
1) Structured properties (e.g., citizenship/country/origin/HQ/location)  
2) Indirect structured inference: place/admin → country (bounded depth)  
3) Text fallback: regex on labels/descriptions/aliases in EN/RU/UK  

### Strict “other” policy (important)
- If RU and UA evidence both exist → `mixed`
- If only RU evidence → `Russian`
- If only UA evidence → `Ukraine`
- If no RU/UA evidence:
  - `other` ONLY if strong explicit third-country evidence reaches the threshold
  - otherwise → `mixed` (keeps `other` clean)

### Run
```bash
python ru_ua_classify_entities.py \
  --in navbox_ru_ua_entities.jsonl wd_ru_ua_entities.jsonl \
  --out classified_ru_ua_entities.jsonl \
  --report classified_report.json
```

### Output
- `classified_ru_ua_entities.jsonl`
- `classified_report.json`

Each record gains:
- `ru_ua_attribution`
- `ru_ua_attribution_detail` (scores + evidence hits + policy metadata)

---

## Script 4 — Overlap & visualisation
**File:** `ru_ua_harvest_visual.py`

### What it does (v2 approach)
- Uses the two *pre-classify* JSONLs to compute overlap.
- Uses the *post-classify* JSONL to compute attribution distributions and “which source contributes what”.
- Generates plots + a JSON report.

### Required arguments
The script requires:
- `--wiki`
- `--wd`
- `--classified`

### Run (with QID lists)
```bash
python ru_ua_harvest_visual.py \
  --wiki navbox_ru_ua_entities.jsonl \
  --wd wd_ru_ua_entities.jsonl \
  --classified classified_ru_ua_entities.jsonl \
  --outdir overlap_analysis_output2 \
  --write-qid-lists
```

Optional Venn diagrams:
```bash
python ru_ua_harvest_visual.py \
  --wiki navbox_ru_ua_entities.jsonl \
  --wd wd_ru_ua_entities.jsonl \
  --classified classified_ru_ua_entities.jsonl \
  --outdir overlap_analysis_output3 \
  --venn
```

### Outputs
- Directory: `overlap_analysis_output*/`
- JSON: `overlap_report_v2.json`
- Multiple PNG plots (global overlap, pre distributions, post distributions, per-label overlap, per-label jaccard)
- Optional per-label QID lists (if `--write-qid-lists`)

---

## Data format (JSONL schema)

Both harvesters write a compatible unified schema so they can be merged:

Common fields (typical):
- `qid` (e.g., `"Q212"`)
- `labels` / `descriptions` (en/ru/uk)
- `aliases` (en/ru/uk; richer on Wikidata harvest)
- `sitelinks`
- `instance_of`
- `raw_attrib_qids` (property → list of QIDs)

Classifier-added fields:
- `ru_ua_attribution`: `Russian|Ukraine|mixed|other`
- `ru_ua_attribution_detail`:
  - `scores` (ru/ua/other)
  - split: structured vs text score
  - `hits`: triggered evidence rules (auditable)

---

## Reports

### navbox_report.json
A compact summary of the navbox harvest:
- start_url
- link/title/QID totals
- language coverage
- category_hint_counts

### classified_report.json
Compact “pre vs post” report:
- pre_classify totals by source
- overlap between inputs (intersection/union/Jaccard)
- merged language coverage + category hints
- after_classify attribution_counts

### overlap_report_v2.json
Saved in the visual output directory:
- global overlap
- per-label overlap composition
- per-label Jaccard
- source contribution breakdown per label

---

## Example run numbers (from your output)

### Pre-classify
- Total rows loaded: 2247
- Unique QIDs merged: 2202
- navboxes unique QIDs: 1781
- wikidata unique QIDs: 466
- intersection: 45
- union: 2202
- jaccard: ~0.0204

### After classify (label counts)
- mixed: 932
- Ukraine: 841
- Russian: 398
- other: 31

---

## Troubleshooting

### 1) “required arguments …”
- Visual script requires `--wiki --wd --classified`.
- Classifier requires `--in --out`.

### 2) FileNotFoundError
- You ran with paths like `data/wd_entities.jsonl` that didn’t exist from your current working directory.
- Fix by using correct relative paths or absolute paths.

### 3) HTTP 504 from WDQS
- WDQS can time out (gateway timeout).
- Rerun, reduce query pressure, or keep batches small.

### 4) zsh parse error near `yuanyu@MacBook-Air`
- You pasted the prompt into the command.
- Copy only the command lines.

---

## License / usage note
This repo is intended for research and dataset building. Attribution labels are heuristic and auditable via `ru_ua_attribution_detail.hits`.
