#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: run_pipeline.py

Main purpose:
- Run the full config-driven pipeline with one command.
- Read all output paths and step switches from `config.json`.
- Execute harvesters -> attribution -> visualization in order.

How to run:
  python run_pipeline.py --config config.json

Config section used:
- `pipeline.paths`:
  - `entities_folder`
  - `classified_output`
  - `attribution_jsonl_output` (optional, compact scoring audit JSONL)
  - `classified_report`
  - `visualization_outdir`
  - `visualization_report` (filename inside visualization_outdir)
- `pipeline.run`:
  - `wikidata` (default true)
  - `navboxes` (default true)
  - `categories` (default true)
  - `attribution` (default true)
  - `visualization` (default true)
- `pipeline.logging`:
  - `enabled` / `file` / `append` / `log_queries` / `query_max_chars`
  - same log file is forwarded to the three harvest scripts

Notes:
- Entity output filenames come from `visualization.entity_files` in config.
- Relative paths in config are resolved relative to the config file location.
"""

import argparse
import os
import shlex
import subprocess
import sys
from typing import Dict

from pipeline_common import build_logger, read_json, resolve_logging_settings

_RUN_LOGGER = None


def _log_info(msg: str) -> None:
    if _RUN_LOGGER:
        _RUN_LOGGER.info(msg)
    else:
        print(msg)


def _as_dict(x) -> dict:
    return x if isinstance(x, dict) else {}


def _as_bool(x, default: bool) -> bool:
    if isinstance(x, bool):
        return x
    return default


def _as_str(x, default: str) -> str:
    if isinstance(x, str) and x.strip():
        return x.strip()
    return default


def _abs_path(base_dir: str, path_val: str) -> str:
    if os.path.isabs(path_val):
        return os.path.normpath(path_val)
    return os.path.normpath(os.path.join(base_dir, path_val))


def _resolve_entity_files(cfg: dict) -> Dict[str, str]:
    vis = _as_dict(cfg.get("visualization"))
    ef = _as_dict(vis.get("entity_files"))
    defaults = {
        "wikidata": "wikidata_entities.jsonl",
        "navboxes": "navboxes_entities.jsonl",
        "categories": "categories_entities.jsonl",
    }
    out = dict(defaults)
    for k in ("wikidata", "navboxes", "categories"):
        v = ef.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    return out


def _resolve_pipeline(cfg: dict, config_path: str) -> dict:
    base_dir = os.path.dirname(os.path.abspath(config_path))
    pcfg = _as_dict(cfg.get("pipeline"))
    run_cfg = _as_dict(pcfg.get("run"))
    path_cfg = _as_dict(pcfg.get("paths"))
    entity_files = _resolve_entity_files(cfg)

    entities_folder = _abs_path(base_dir, _as_str(path_cfg.get("entities_folder"), "data/entities"))
    classified_output = _abs_path(base_dir, _as_str(path_cfg.get("classified_output"), "data/classified_entities.jsonl"))
    attribution_jsonl_output = _as_str(path_cfg.get("attribution_jsonl_output"), "data/attribution_scores.jsonl")
    attribution_jsonl_output = _abs_path(base_dir, attribution_jsonl_output) if attribution_jsonl_output else ""
    classified_report = _abs_path(base_dir, _as_str(path_cfg.get("classified_report"), "data/classified_report.json"))
    visualization_outdir = _abs_path(base_dir, _as_str(path_cfg.get("visualization_outdir"), "data/visualization"))
    visualization_report = _as_str(path_cfg.get("visualization_report"), "visualization_report.json")
    if os.path.isabs(visualization_report):
        visualization_report = os.path.basename(visualization_report)

    return {
        "run": {
            "wikidata": _as_bool(run_cfg.get("wikidata"), True),
            "navboxes": _as_bool(run_cfg.get("navboxes"), True),
            "categories": _as_bool(run_cfg.get("categories"), True),
            "attribution": _as_bool(run_cfg.get("attribution"), True),
            "visualization": _as_bool(run_cfg.get("visualization"), True),
        },
        "paths": {
            "entities_folder": entities_folder,
            "wikidata_output": os.path.join(entities_folder, entity_files["wikidata"]),
            "navboxes_output": os.path.join(entities_folder, entity_files["navboxes"]),
            "categories_output": os.path.join(entities_folder, entity_files["categories"]),
            "classified_output": classified_output,
            "attribution_jsonl_output": attribution_jsonl_output,
            "classified_report": classified_report,
            "visualization_outdir": visualization_outdir,
            "visualization_report": visualization_report,
        },
    }


def _run_step(name: str, cmd: list, cwd: str) -> None:
    _log_info(f"[pipeline] {name}")
    _log_info("[pipeline] cmd: " + " ".join(shlex.quote(x) for x in cmd))
    subprocess.run(cmd, cwd=cwd, check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    args = ap.parse_args()

    config_path = os.path.abspath(args.config)
    cfg = read_json(config_path)
    log_cfg = resolve_logging_settings(
        cfg,
        config_path=config_path,
        default_file="data/logs/pipeline_run.log",
    )
    global _RUN_LOGGER
    if log_cfg["enabled"]:
        _RUN_LOGGER = build_logger("run_pipeline", log_cfg["file"], append=log_cfg["append"], to_stdout=True)
        _RUN_LOGGER.info(
            f"[run_pipeline] log file: {log_cfg['file']} | "
            f"log_queries={'on' if log_cfg['log_queries'] else 'off'} | "
            f"query_max_chars={log_cfg['query_max_chars']}"
        )

    pipeline_cfg = _resolve_pipeline(cfg, config_path)
    run_map = pipeline_cfg["run"]
    p = pipeline_cfg["paths"]

    os.makedirs(p["entities_folder"], exist_ok=True)
    os.makedirs(os.path.dirname(p["classified_output"]) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(p["classified_report"]) or ".", exist_ok=True)
    os.makedirs(p["visualization_outdir"], exist_ok=True)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    py = sys.executable
    harvest_log_args = ["--log-file", log_cfg["file"]] if log_cfg["enabled"] else []

    if run_map["wikidata"]:
        _run_step(
            "Step 1/5 harvest_wikidata",
            [
                py,
                "harvest_wikidata.py",
                "--config",
                config_path,
                "--output",
                p["wikidata_output"],
                *harvest_log_args,
            ],
            cwd=script_dir,
        )

    if run_map["navboxes"]:
        _run_step(
            "Step 2/5 harvest_navboxes",
            [
                py,
                "harvest_navboxes.py",
                "--config",
                config_path,
                "--output",
                p["navboxes_output"],
                *harvest_log_args,
            ],
            cwd=script_dir,
        )

    if run_map["categories"]:
        _run_step(
            "Step 3/5 harvest_categories",
            [
                py,
                "harvest_categories.py",
                "--config",
                config_path,
                "--output",
                p["categories_output"],
                *harvest_log_args,
            ],
            cwd=script_dir,
        )

    if run_map["attribution"]:
        _run_step(
            "Step 4/5 attribution",
            [
                py,
                "attribution.py",
                "--config",
                config_path,
                "--entities_folder",
                p["entities_folder"],
                "--output",
                p["classified_output"],
                "--attribution-jsonl",
                p["attribution_jsonl_output"],
                "--report",
                p["classified_report"],
            ],
            cwd=script_dir,
        )

    if run_map["visualization"]:
        _run_step(
            "Step 5/5 visualization",
            [
                py,
                "visualization.py",
                "--config",
                config_path,
                "--entities_folder",
                p["entities_folder"],
                "--classified",
                p["classified_output"],
                "--outdir",
                p["visualization_outdir"],
                "--report",
                p["visualization_report"],
            ],
            cwd=script_dir,
        )

    _log_info("[pipeline] completed")
    _log_info(f"[pipeline] entities_folder={p['entities_folder']}")
    _log_info(f"[pipeline] classified_output={p['classified_output']}")
    _log_info(f"[pipeline] attribution_jsonl_output={p['attribution_jsonl_output']}")
    _log_info(f"[pipeline] visualization_outdir={p['visualization_outdir']}")


if __name__ == "__main__":
    main()
