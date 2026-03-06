#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: visualization.py

Main purpose:
- Compare coverage overlap across three sources:
  Wikidata, navboxes, and categories.
- Summarize attribution distribution from classified output.
- Write a machine-readable report and charts (including config-driven Venn plots).

Input:
- --entities_folder: folder containing:
  `wikidata_entities.jsonl`, `navboxes_entities.jsonl`,
  `categories_entities.jsonl`.
- --classified: classified output from `attribution.py`.
- --config (optional): path to `config.json` for display labels
  and language order.
  Venn behavior is controlled by `visualization.venn` in config.
- Optional source file overrides:
  - `--wikidata-file`, `--navboxes-file`, `--categories-file`
  - or set `visualization.entity_files` in config.

Output:
- --outdir: output folder for `visualization_report.json` and figures.

How to run:
  python visualization.py \
    --config config.json \
    --entities_folder data/entities \
    --classified data/classified_entities.jsonl \
    --outdir data/visualization

Pipeline step:
- Step 5 (optional analysis/visualization).
"""

import argparse
import os
import re
import tempfile
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set

from pipeline_common import config_languages, read_json, read_jsonl, write_json

# Make matplotlib cache writable in restricted environments.
if "MPLCONFIGDIR" not in os.environ:
    _mpl_tmp = os.path.join(tempfile.gettempdir(), "conflict_pipeline_mpl")
    try:
        os.makedirs(_mpl_tmp, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = _mpl_tmp
    except Exception:
        pass

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib_venn import venn3
    HAS_PLOT = True
    HAS_VENN = True
except Exception:
    HAS_PLOT = False
    HAS_VENN = False


def ensure_dir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def qid_of(rec: dict) -> Optional[str]:
    q = rec.get("qid")
    if isinstance(q, str) and q.startswith("Q"):
        return q
    uri = rec.get("uri")
    if isinstance(uri, str) and "/Q" in uri:
        qq = uri.rsplit("/", 1)[-1]
        if qq.startswith("Q"):
            return qq
    return None


def load_qids(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    out: Set[str] = set()
    for r in read_jsonl(path):
        q = qid_of(r)
        if q:
            out.add(q)
    return out


def source_bucket(q: str, wd: Set[str], nav: Set[str], cat: Set[str]) -> str:
    in_wd = q in wd
    in_nav = q in nav
    in_cat = q in cat
    bits = [in_wd, in_nav, in_cat]
    if sum(bits) == 0:
        return "none"
    if bits == [True, False, False]:
        return "wd_only"
    if bits == [False, True, False]:
        return "nav_only"
    if bits == [False, False, True]:
        return "cat_only"
    if bits == [True, True, False]:
        return "wd_nav"
    if bits == [True, False, True]:
        return "wd_cat"
    if bits == [False, True, True]:
        return "nav_cat"
    return "all_three"


def _normalize_lang_list(vals) -> List[str]:
    out: List[str] = []
    if not isinstance(vals, list):
        return out
    for v in vals:
        if not isinstance(v, str):
            continue
        vv = v.strip().lower()
        if vv and vv not in out:
            out.append(vv)
    return out


def build_visual_config(cfg: Optional[dict]) -> dict:
    vis = cfg.get("visualization") if isinstance(cfg, dict) and isinstance(cfg.get("visualization"), dict) else {}
    lang_map = cfg.get("languages") if isinstance(cfg, dict) and isinstance(cfg.get("languages"), dict) else {}

    language_order = _normalize_lang_list(vis.get("language_order"))
    if not language_order and isinstance(cfg, dict) and cfg:
        language_order = config_languages(cfg)
    if not language_order:
        language_order = ["en"]

    attr_display = {
        "party1": "party1",
        "party2": "party2",
        "mixed": "mixed",
        "other": "other",
    }
    p1_lang = lang_map.get("party1")
    p2_lang = lang_map.get("party2")
    if isinstance(p1_lang, str) and p1_lang.strip():
        attr_display["party1"] = p1_lang.strip().lower()
    if isinstance(p2_lang, str) and p2_lang.strip():
        attr_display["party2"] = p2_lang.strip().lower()

    attr_override = vis.get("attribution_display_names")
    if isinstance(attr_override, dict):
        for k in ("party1", "party2", "mixed", "other"):
            v = attr_override.get(k)
            if isinstance(v, str) and v.strip():
                attr_display[k] = v.strip()

    source_display = {
        "wikidata": "Wikidata",
        "navboxes": "Wikipedia navboxes",
        "categories": "Wikipedia categories",
    }
    source_override = vis.get("source_display_names")
    if isinstance(source_override, dict):
        for k in ("wikidata", "navboxes", "categories"):
            v = source_override.get(k)
            if isinstance(v, str) and v.strip():
                source_display[k] = v.strip()

    venn_cfg = vis.get("venn") if isinstance(vis.get("venn"), dict) else {}
    source_labels = [
        source_display.get("wikidata", "Wikidata"),
        source_display.get("navboxes", "Wikipedia navboxes"),
        source_display.get("categories", "Wikipedia categories"),
    ]
    venn_source_labels = venn_cfg.get("source_labels")
    if isinstance(venn_source_labels, list):
        cleaned = [str(x).strip() for x in venn_source_labels if str(x).strip()]
        if len(cleaned) == 3:
            source_labels = cleaned

    return {
        "language_order": language_order,
        "attribution_display_names": attr_display,
        "source_display_names": source_display,
        "venn": {
            "enabled": bool(venn_cfg.get("enabled", True)),
            "global": bool(venn_cfg.get("global", True)),
            "per_label": bool(venn_cfg.get("per_label", True)),
            "source_labels": source_labels,
        },
    }


def resolve_entity_filenames(cfg: Optional[dict], overrides: Optional[dict] = None) -> Dict[str, str]:
    defaults = {
        "wikidata": "wikidata_entities.jsonl",
        "navboxes": "navboxes_entities.jsonl",
        "categories": "categories_entities.jsonl",
    }

    vis = cfg.get("visualization") if isinstance(cfg, dict) and isinstance(cfg.get("visualization"), dict) else {}
    cfg_map = vis.get("entity_files") if isinstance(vis.get("entity_files"), dict) else {}
    out = dict(defaults)

    for k in ("wikidata", "navboxes", "categories"):
        v = cfg_map.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()

    ov = overrides if isinstance(overrides, dict) else {}
    for k in ("wikidata", "navboxes", "categories"):
        v = ov.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    return out


def build_legacy_fallback(cfg: Optional[dict]) -> dict:
    """
    Build fallback mapping when `attribution` is absent in classified rows.
    """
    parties = cfg.get("conflicting_parties") if isinstance(cfg, dict) and isinstance(cfg.get("conflicting_parties"), dict) else {}
    p1 = parties.get("party1") if isinstance(parties.get("party1"), dict) else {}
    p2 = parties.get("party2") if isinstance(parties.get("party2"), dict) else {}
    p1_label = p1.get("label") if isinstance(p1.get("label"), str) and p1.get("label").strip() else "party1"
    p2_label = p2.get("label") if isinstance(p2.get("label"), str) and p2.get("label").strip() else "party2"

    out = {
        "field_name": "legacy_attribution",
        "to_internal": {
            "party1": "party1",
            "party2": "party2",
            p1_label: "party1",
            p2_label: "party2",
            "mixed": "mixed",
            "other": "other",
        },
    }
    ccfg = cfg.get("classification") if isinstance(cfg, dict) and isinstance(cfg.get("classification"), dict) else {}
    legacy = ccfg.get("legacy_output") if isinstance(ccfg.get("legacy_output"), dict) else {}
    if not legacy:
        return out

    field_name = legacy.get("field_name")
    if isinstance(field_name, str) and field_name.strip():
        out["field_name"] = field_name.strip()

    mapping = {}
    for internal in ("party1", "party2", "mixed", "other"):
        v = legacy.get(internal)
        if isinstance(v, str) and v.strip():
            mapping[v.strip()] = internal
    if mapping:
        out["to_internal"] = mapping
    return out


def _lang_nonempty_count(rows: List[dict], lang: str) -> int:
    c = 0
    for r in rows:
        labels = r.get("labels") if isinstance(r.get("labels"), dict) else {}
        descs = r.get("descriptions") if isinstance(r.get("descriptions"), dict) else {}
        if labels.get(lang) or descs.get(lang):
            c += 1
    return c


def compute_language_coverage(rows: List[dict], langs: List[str]) -> Dict[str, int]:
    return {lang: _lang_nonempty_count(rows, lang) for lang in langs}


def _venn_subsets(wd: Set[str], nav: Set[str], cat: Set[str]) -> tuple:
    wd_only = len(wd - nav - cat)
    nav_only = len(nav - wd - cat)
    cat_only = len(cat - wd - nav)
    wd_nav_only = len((wd & nav) - cat)
    wd_cat_only = len((wd & cat) - nav)
    nav_cat_only = len((nav & cat) - wd)
    all_three = len(wd & nav & cat)
    return (wd_only, nav_only, wd_nav_only, cat_only, wd_cat_only, nav_cat_only, all_three)


def _safe_slug(text: str) -> str:
    out = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return out or "label"


def make_plots(
    report: dict,
    outdir: str,
    vis_cfg: dict,
    wd_qids: Set[str],
    nav_qids: Set[str],
    cat_qids: Set[str],
    label_qids: Dict[str, Set[str]],
) -> None:
    if not HAS_PLOT:
        return

    # plot 1: source sizes
    sizes = report["sources"]["sizes"]
    source_keys = ["wikidata", "navboxes", "categories"]
    source_names = vis_cfg.get("source_display_names", {})
    labels = [source_names.get(k, k) for k in source_keys]
    vals = [sizes.get(k, 0) for k in source_keys]
    x = np.arange(len(source_keys))

    plt.figure(figsize=(7, 4))
    plt.bar(x, vals)
    plt.xticks(x, labels)
    plt.ylabel("Unique QIDs")
    plt.title("Harvest size by source")
    for i, v in enumerate(vals):
        plt.text(i, v + max(vals + [1]) * 0.01, str(v), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "source_sizes.png"))
    plt.close()

    # plot 2: attribution counts
    attrib = report["classified"]["attribution_counts"]
    a_keys = ["party1", "party2", "mixed", "other"]
    a_names = vis_cfg.get("attribution_display_names", {})
    a_labels = [a_names.get(k, k) for k in a_keys]
    a_vals = [attrib.get(k, 0) for k in a_keys]
    x2 = np.arange(len(a_labels))

    plt.figure(figsize=(7, 4))
    plt.bar(x2, a_vals)
    plt.xticks(x2, a_labels)
    plt.ylabel("Count")
    plt.title("Attribution counts")
    for i, v in enumerate(a_vals):
        plt.text(i, v + max(a_vals + [1]) * 0.01, str(v), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "attribution_counts.png"))
    plt.close()

    # plot 3: source bucket distribution within classified
    buckets = ["wd_only", "nav_only", "cat_only", "wd_nav", "wd_cat", "nav_cat", "all_three"]
    bvals = [report["classified"]["source_bucket_counts"].get(b, 0) for b in buckets]
    x3 = np.arange(len(buckets))

    plt.figure(figsize=(10, 4))
    plt.bar(x3, bvals)
    plt.xticks(x3, buckets, rotation=20, ha="right")
    plt.ylabel("Count")
    plt.title("Classified entities by source presence")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "classified_source_presence.png"))
    plt.close()

    # plot 4: language coverage from classified rows
    lang_cov = report["classified"].get("language_coverage_nonempty_label_or_desc", {})
    langs = vis_cfg.get("language_order", [])
    if langs:
        lvals = [lang_cov.get(l, 0) for l in langs]
        x4 = np.arange(len(langs))
        plt.figure(figsize=(7, 4))
        plt.bar(x4, lvals)
        plt.xticks(x4, langs)
        plt.ylabel("Count (label or desc nonempty)")
        plt.title("Classified language coverage")
        for i, v in enumerate(lvals):
            plt.text(i, v + max(lvals + [1]) * 0.01, str(v), ha="center", va="bottom", fontsize=9)
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "classified_language_coverage.png"))
        plt.close()

    venn_vis = vis_cfg.get("venn") if isinstance(vis_cfg.get("venn"), dict) else {}
    if not bool(venn_vis.get("enabled", True)):
        return
    if not HAS_VENN:
        return

    set_labels = tuple(venn_vis.get("source_labels", ["Wikidata", "Wikipedia navboxes", "Wikipedia categories"]))

    if bool(venn_vis.get("global", True)):
        plt.figure(figsize=(6, 6))
        venn3(subsets=_venn_subsets(wd_qids, nav_qids, cat_qids), set_labels=set_labels)
        plt.title("Global source overlap (Venn)")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "venn_global.png"))
        plt.close()

    if bool(venn_vis.get("per_label", True)):
        label_names = vis_cfg.get("attribution_display_names", {})
        for label_key in ("party1", "party2", "mixed", "other"):
            qset = label_qids.get(label_key, set())
            if not qset:
                continue

            wd_l = wd_qids & qset
            nav_l = nav_qids & qset
            cat_l = cat_qids & qset
            subsets = _venn_subsets(wd_l, nav_l, cat_l)
            if sum(subsets) == 0:
                continue

            display_name = label_names.get(label_key, label_key)
            plt.figure(figsize=(6, 6))
            venn3(subsets=subsets, set_labels=set_labels)
            plt.title(f"Source overlap for {display_name} (Venn)")
            plt.tight_layout()
            plt.savefig(os.path.join(outdir, f"venn_{_safe_slug(label_key)}.png"))
            plt.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="Optional config.json for display names and language order")
    ap.add_argument("--entities_folder", required=True, help="Folder with wikidata/navboxes/categories JSONL outputs")
    ap.add_argument("--classified", required=True, help="Classified JSONL output")
    ap.add_argument("--wikidata-file", default=None, help="Override Wikidata entities filename inside entities_folder")
    ap.add_argument("--navboxes-file", default=None, help="Override navboxes entities filename inside entities_folder")
    ap.add_argument("--categories-file", default=None, help="Override categories entities filename inside entities_folder")
    ap.add_argument("--outdir", required=True, help="Output folder for report/figures")
    ap.add_argument("--report", default="visualization_report.json", help="Report filename inside outdir")
    args = ap.parse_args()

    ensure_dir(args.outdir)
    cfg = read_json(args.config) if args.config else {}
    entity_files = resolve_entity_filenames(
        cfg,
        overrides={
            "wikidata": args.wikidata_file,
            "navboxes": args.navboxes_file,
            "categories": args.categories_file,
        },
    )
    wd_path = os.path.join(args.entities_folder, entity_files["wikidata"])
    nav_path = os.path.join(args.entities_folder, entity_files["navboxes"])
    cat_path = os.path.join(args.entities_folder, entity_files["categories"])

    wd_qids = load_qids(wd_path)
    nav_qids = load_qids(nav_path)
    cat_qids = load_qids(cat_path)

    vis_cfg = build_visual_config(cfg)
    legacy_cfg = build_legacy_fallback(cfg)

    union = wd_qids | nav_qids | cat_qids

    overlap = {
        "wd_nav": len(wd_qids & nav_qids),
        "wd_cat": len(wd_qids & cat_qids),
        "nav_cat": len(nav_qids & cat_qids),
        "all_three": len(wd_qids & nav_qids & cat_qids),
    }

    classified_rows = read_jsonl(args.classified)
    attrib_counts = Counter()
    source_bucket_counts = Counter()
    by_label_bucket = defaultdict(Counter)
    label_qids: Dict[str, Set[str]] = defaultdict(set)

    for r in classified_rows:
        q = qid_of(r)
        if not q:
            continue

        lab = r.get("attribution")
        if not isinstance(lab, str):
            fallback = r.get(legacy_cfg.get("field_name", "legacy_attribution"))
            if isinstance(fallback, str):
                lab = legacy_cfg.get("to_internal", {}).get(fallback, fallback)
        if lab not in {"party1", "party2", "mixed", "other"}:
            lab = "mixed"

        b = source_bucket(q, wd_qids, nav_qids, cat_qids)
        attrib_counts[lab] += 1
        source_bucket_counts[b] += 1
        by_label_bucket[lab][b] += 1
        label_qids[lab].add(q)

    lang_cov = compute_language_coverage(classified_rows, vis_cfg.get("language_order", []))

    report = {
        "sources": {
            "files": {
                "wikidata": wd_path,
                "navboxes": nav_path,
                "categories": cat_path,
            },
            "sizes": {
                "wikidata": len(wd_qids),
                "navboxes": len(nav_qids),
                "categories": len(cat_qids),
                "union": len(union),
            },
            "overlap": overlap,
        },
        "classified": {
            "file": args.classified,
            "rows": len(classified_rows),
            "attribution_counts": dict(attrib_counts),
            "source_bucket_counts": dict(source_bucket_counts),
            "by_label_source_bucket": {k: dict(v) for k, v in by_label_bucket.items()},
            "label_unique_qids": {k: len(v) for k, v in label_qids.items()},
            "language_coverage_nonempty_label_or_desc": lang_cov,
        },
        "visualization_config": vis_cfg,
        "entity_files": entity_files,
        "fallback_config": legacy_cfg,
        "note": "Uses attribution field when present; fallback field is config-driven.",
    }

    make_plots(report, args.outdir, vis_cfg, wd_qids, nav_qids, cat_qids, label_qids)

    out_report = os.path.join(args.outdir, args.report)
    write_json(out_report, report)
    print(f"[visualization] wrote {out_report}")
    if not HAS_PLOT:
        print("[visualization] matplotlib not available; JSON report only")
    if HAS_PLOT and vis_cfg.get("venn", {}).get("enabled", True) and not HAS_VENN:
        print("[visualization] matplotlib-venn not available; skipped venn plots")


if __name__ == "__main__":
    main()
