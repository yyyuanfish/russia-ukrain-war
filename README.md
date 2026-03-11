# Russo–Ukrainian War Entity Harvesting + Attribution Classification (Wikipedia + Wikidata)

<<<<<<< Updated upstream
This project builds a Wikidata-backed entity inventory for the **Russo–Ukrainian War Analysis** using two complementary harvesting methods, then classifies each entity into **Russian / Ukraine / mixed / other**, and finally produces overlap & distribution visualisations.
=======
This project folder (`/Users/yuanyu/Desktop/Russia Ukrain War`) contains a pipeline to:
>>>>>>> Stashed changes

1. **Harvest entities (QIDs)** from **Wikidata** via SPARQL queries (people/events/orgs/policies/media narratives) using `ru_ua_harvest_wikidata_entities.py` (v2 equivalent: `pipeline_v2/harvest_wikidata.py`).
2. **Harvest entities (QIDs)** from Wikipedia navbox/category sources using `ru_ua_harvest_wikipedia_navboxes.py` (v2 split: `pipeline_v2/harvest_navboxes.py` + `pipeline_v2/harvest_categories.py`).
3. **Merge + classify** all harvested QIDs into **Russian / Ukraine / mixed / other** using `ru_ua_classify_entities.py` (v2 equivalent: `pipeline_v2/attribution.py`).
4. **Analyze overlap** and generate **figures + compact report** using `ru_ua_harvest_visual.py` (v2 equivalent: `pipeline_v2/visualization.py`).

---

## What’s in this folder

### Scripts

- `ru_ua_harvest_wikidata_entities.py`  
  SPARQL-harvests Wikidata entities for several buckets (people/events/organizations/policies/media narratives), enriches labels/descriptions/sitelinks/aliases, and writes:
  - `data/wd_entities.jsonl`
  - `data/wd_entities.json`

- `ru_ua_harvest_wikipedia_navboxes.py`  
  Fetches a Wikipedia page, extracts links from a selected navbox, optionally expands categories (EN/RU/UK), resolves titles → QIDs, queries Wikidata for metadata, and writes:
  - `data/navbox_ru_ua_entities.jsonl`
  - `data/navbox_report.json`

- `ru_ua_classify_entities.py`  
  Loads one or more JSONL files from the harvesters, merges duplicates by QID, resolves place→country for referenced places/admin entities, and assigns:
  - `ru_ua_attribution` in `{Russian, Ukraine, mixed, other}`
  - `ru_ua_attribution_detail` (scores + evidence hits)

  Writes:
  - `data/classified_ru_ua_entities.jsonl`
  - `data/classified_report.json`

- `ru_ua_harvest_visual.py`  
  Compares **navbox** vs **wikidata** harvest sets and produces overlap figures + `overlap_report_v2.json`.

### Main outputs (typical run)

- `data/wd_entities.jsonl`
- `data/navbox_ru_ua_entities.jsonl`
- `data/classified_ru_ua_entities.jsonl`
- `data/classified_report.json`
- `data/overlap_analysis_output_v2*/` (plots + overlap report)

---

<<<<<<< Updated upstream
## Repository / file layout
=======
## Installation
>>>>>>> Stashed changes

### 1) Create & activate a virtual environment (recommended)

From the repository root (example from your terminal):

```bash
cd "/Users/yuanyu/Desktop/Russia Ukrain War"
python -m venv ru_ua
source ./ru_ua/bin/activate
```

### 2) Install dependencies

You already have a `requirements.txt` at the repo root. It currently contains these core packages:

- requests>=2.31.0
- beautifulsoup4>=4.12.0
- SPARQLWrapper>=2.0.0

Install them:

```bash
pip install -r requirements.txt
```

**Additional packages needed by some scripts:**

- `ru_ua_harvest_wikipedia_navboxes.py` uses BeautifulSoup with the **lxml** parser → install `lxml`
- `ru_ua_harvest_visual.py` needs `numpy`, `matplotlib`, `matplotlib-venn`

Install the extras:

```bash
pip install lxml numpy matplotlib matplotlib-venn
```

> If you prefer a single command, you can also add the extras into `requirements.txt`.

---

## Quickstart (end-to-end)

Run all commands from the project root:

```bash
cd "/Users/yuanyu/Desktop/Russia Ukrain War"
```

### Step A — Wikipedia navbox harvest (example you are using)

```bash
python3 "/Users/yuanyu/Desktop/Russia Ukrain War/ru_ua_harvest_wikipedia_navboxes.py" \
  --start-url "https://en.wikipedia.org/wiki/Russo-Ukrainian_War" \
  --navbox-title "Russo-Ukrainian war" \
  --navbox-index 0 \
  --include-categories \
  --category-strategy bfs \
  --category-depth 1 \
  --category-langs en,ru,uk \
  --out "/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_ru_ua_entities.jsonl" \
  --out-report "/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_report.json"
```

Outputs:
- `/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_ru_ua_entities.jsonl`
- `/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_report.json`

#### Navbox command parameters

- `--start-url`: seed page URL.
- `--navbox-title`: preferred navbox title substring match.
- `--navbox-index`: fallback navbox index if title match fails.
- `--include-categories`: enables category graph expansion.
- `--category-strategy`: category traversal mode (`bfs`/`dfs`), `bfs` recommended.
- `--category-depth`: category walk depth (`1` is balanced).
- `--category-langs`: languages for category walk (e.g., `en,ru,uk`).
- `--out`: output JSONL path.
- `--out-report`: summary report JSON path.

#### Extra navbox tuning/debug parameters (optional)

- `--category-keywords`: comma-separated keyword filter for subcategory expansion.
- `--category-max-categories`: per-language hard cap on visited categories.
- `--category-max-titles`: per-language hard cap on collected titles.
- `--category-max-members-per-category`: cap members fetched per category.
- `--category-progress-every`: print progress every N visited categories.
- `--sleep`: API pacing delay.
- `--debug-save-html`: save parsed HTML to debug navbox selection.

<<<<<<< Updated upstream
### Strict “other” policy (important)
- If RU and UA evidence both exist → `mixed`
- If only RU evidence → `Russian`
- If only UA evidence → `Ukraine`
- If no RU/UA evidence:
  - `other` ONLY if strong explicit third-country evidence reaches the threshold
  - otherwise → `mixed` (keeps `other` clean) # _hmm, interesting separation, but isn't it a bit far-fetched? For example, some entity would be clearly related to, say, Hungary (not mentioned in third-country codes), yet it ideally should be related to other (example being http://www.wikidata.org/entity/Q117063798 )_
=======
### Step B — Wikidata SPARQL harvest
>>>>>>> Stashed changes

```bash
python3 "/Users/yuanyu/Desktop/Russia Ukrain War/ru_ua_harvest_wikidata_entities.py" \
  --out "/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.jsonl" \
  --array "/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.json"
```

Outputs:
- `/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.jsonl`
- `/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.json`

#### Wikidata command parameters

- `--out`: output JSONL path used by classifier.
- `--array`: output pretty JSON array path (same records as JSONL).
- `--limit` (optional): debug mode, limit rows per query.
- `--no-aliases` (optional): skip alias enrichment for speed.

### Step C — Merge + classify (Russian / Ukraine / mixed / other)

```bash
python3 "/Users/yuanyu/Desktop/Russia Ukrain War/ru_ua_classify_entities.py" \
  --in "/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_ru_ua_entities.jsonl" "/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.jsonl" \
  --out "/Users/yuanyu/Desktop/Russia Ukrain War/data/classified_ru_ua_entities.jsonl" \
  --report "/Users/yuanyu/Desktop/Russia Ukrain War/data/classified_report.json"
```

#### Classify command parameters

- `--in`: one or more input JSONL files from harvesters (here: navbox + wikidata).
- `--out`: output classified JSONL.
- `--report`: compact report JSON.
- `--other-threshold` (optional): minimum `other_score` to assign `other` when RU/UA evidence is absent.

### Step D — Overlap analysis + figures (with per-label QID lists)

```bash
python3 "/Users/yuanyu/Desktop/Russia Ukrain War/ru_ua_harvest_visual.py" \
  --wiki "/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_ru_ua_entities.jsonl" \
  --wd "/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.jsonl" \
  --classified "/Users/yuanyu/Desktop/Russia Ukrain War/data/classified_ru_ua_entities.jsonl" \
  --outdir "/Users/yuanyu/Desktop/Russia Ukrain War/data/overlap_analysis_output_v2" \
  --report "overlap_report_v2.json" \
  --write-qid-lists
```

Optional Venn run:

```bash
python3 "/Users/yuanyu/Desktop/Russia Ukrain War/ru_ua_harvest_visual.py" \
  --wiki "/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_ru_ua_entities.jsonl" \
  --wd "/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.jsonl" \
  --classified "/Users/yuanyu/Desktop/Russia Ukrain War/data/classified_ru_ua_entities.jsonl" \
  --outdir "/Users/yuanyu/Desktop/Russia Ukrain War/data/overlap_analysis_output_v2_venn" \
  --report "overlap_report_v2.json" \
  --venn
```

#### Visual command parameters

- `--wiki`: navbox harvester JSONL.
- `--wd`: wikidata harvester JSONL.
- `--classified`: classifier output JSONL.
- `--outdir`: output directory for plots and report.
- `--report`: report filename inside `--outdir`.
- `--write-qid-lists` (optional): writes per-label overlap QID lists.
- `--venn` (optional): outputs Venn diagrams (requires `matplotlib-venn`).

---

## How classification works (high level)

Each entity gets scores from two evidence types:

1. **Structured evidence (higher weight)** from these properties (when present):
   - P27, P17, P495, P159, P131, P276, P19, P740, P551
2. **Text fallback (lower weight)** from labels/descriptions/aliases in EN/RU/UK (regex patterns)

It then assigns:

- **mixed**: RU evidence > 0 and UA evidence > 0  
- **Russian**: RU evidence > 0 and UA evidence == 0  
- **Ukraine**: UA evidence > 0 and RU evidence == 0  
- **other**: RU evidence == 0 and UA evidence == 0

Notes:
- `other_score` / `other_threshold` are still recorded for auditing and tuning.
- The final label decision no longer gates `other` on `other_score`.

To audit why something was labeled, inspect:
- `ru_ua_attribution_detail.hits` in `classified_ru_ua_entities.jsonl`

---

## Reports

### `navbox_report.json`
Summarizes:
- navbox selection (`navbox_title_query`, `navbox_index`)
- whether category expansion was enabled
- category settings (`depth`, `strategy`, `langs`, limit knobs)
- root/visited category counts per language
- title counts per language + resolved-title counts per language
- total resolved QIDs

### `classified_report.json`
Compact pre/post summary:
- input row counts + merged QIDs
- per-source-type stats (navboxes vs wikidata SPARQL)
- overlap between the two input files (intersection/union/jaccard)
- merged language coverage + category hints
- final attribution counts after classification

### `overlap_report_v2.json` (inside `overlap_analysis_output*/`)
Overlap stats + paths to generated figures/lists.

---

## Common issues (from your terminal logs)

### 1) `error: the following arguments are required: --in, --out`
You ran `ru_ua_classify_entities.py` without required CLI args. Use:

```bash
python3 "/Users/yuanyu/Desktop/Russia Ukrain War/ru_ua_classify_entities.py" \
  --in "/Users/yuanyu/Desktop/Russia Ukrain War/data/navbox_ru_ua_entities.jsonl" "/Users/yuanyu/Desktop/Russia Ukrain War/data/wd_entities.jsonl" \
  --out "/Users/yuanyu/Desktop/Russia Ukrain War/data/classified_ru_ua_entities.jsonl"
```

### 2) `HTTP Error 504: Gateway Timeout` (WDQS)
Wikidata Query Service can time out when a query is expensive or the service is busy.
Mitigations already added in your classifier version:
- smaller batching for place→country
- bounded-depth admin traversal instead of `P131*`
- slower pacing + longer timeout

If it still happens:
- rerun later
- reduce batch size further
- temporarily reduce the number of QIDs to resolve (debug limit)

### 3) `FileNotFoundError: ... data/wd_entities.jsonl`
That path didn’t exist from your current working directory.
Fix: `cd` into the folder where the file actually lives, or pass the correct relative/absolute path.

### 4) `414 Request-URI Too Long` (Wikipedia API title→QID)
This can happen when a language batch contains many long titles.
Current script versions mitigate this by using POST + automatic chunk splitting.
If needed, also lower expansion size:
- use `--category-depth 1`
- set caps like `--category-max-categories 150`

### 5) zsh `parse error near 'yuanyu@MacBook-Air'`
This happens when you accidentally paste your prompt text into the command.
Only paste the command lines starting with `python ...`.

---

## Data & licensing notes

- Wikipedia content is licensed under CC BY-SA; Wikidata content under CC0.
- This repo stores **QIDs and metadata** harvested from those sources; always respect the original licenses when publishing derived datasets.

---

<<<<<<< Updated upstream
## License / usage note
This repo is intended for research and dataset building. Attribution labels are heuristic and auditable via `ru_ua_attribution_detail.hits`.

---
## Todo
- [ ] extend to other languages (substitute the var names with `side_1`/`side_2` (?), put the country names as hyperparameters)
- [ ] add "historical extension", i.e. so that "Russia" would be attributed not only to the Russian Federation, but also to the Russian Empire/Soviet Union 
=======
## Minimal folder structure (example)

```
/Users/yuanyu/Desktop/Russia Ukrain War/
  ru_ua_harvest_wikidata_entities.py
  ru_ua_harvest_wikipedia_navboxes.py
  ru_ua_classify_entities.py
  ru_ua_harvest_visual.py
  data/
    wd_entities.jsonl
    navbox_ru_ua_entities.jsonl
    classified_ru_ua_entities.jsonl
    classified_report.json
    navbox_report.json
    overlap_analysis_output_v2/
      overlap_report_v2.json
      *.png
      qidlist_*.txt
```
>>>>>>> Stashed changes
