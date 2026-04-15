## Legacy RU-UA-Only Scripts

This folder stores the older Russo-Ukrainian-war-specific scripts that are not part of the main config-driven `pipeline_v2` workflow.

Files here are kept only for backward compatibility:

- `ru_ua_harvest_wikidata_entities.py`
- `ru_ua_harvest_wikipedia_navboxes.py`
- `ru_ua_classify_entities.py`
- `ru_ua_harvest_visual.py`

Use the top-level `pipeline_v2/` scripts for the current workflow:

- `harvest_wikidata.py`
- `harvest_navboxes.py`
- `harvest_categories.py`
- `attribution.py`
- `visualization.py`
- `run_pipeline.py`

Cache files such as `__pycache__/` and macOS metadata such as `.DS_Store` are also outside the workflow, but they should be deleted rather than archived here.
