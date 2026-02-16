#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: visualization.py

Main purpose:
- Compare coverage overlap across three sources:
  Wikidata, navboxes, and categories.
- Summarize attribution distribution from classified output.
- Write a machine-readable report and (optionally) charts.

Input:
- --entities_folder: folder containing:
  `wikidata_entities.jsonl`, `navboxes_entities.jsonl`,
  `categories_entities.jsonl`.
- --classified: classified output from `attribution.py`.

Output:
- --outdir: output folder for `visualization_report.json` and figures.

How to run:
  python visualization.py \
    --entities_folder data/entities \
    --classified data/classified_entities.jsonl \
    --outdir data/visualization

Pipeline step:
- Step 5 (optional analysis/visualization).
"""

import argparse
import os
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set

from pipeline_common import read_jsonl, write_json

try:
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_PLOT = True
except Exception:
    HAS_PLOT = False


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


def make_plots(report: dict, outdir: str) -> None:
    if not HAS_PLOT:
        return

    # plot 1: source sizes
    sizes = report["sources"]["sizes"]
    labels = ["wikidata", "navboxes", "categories"]
    vals = [sizes.get(k, 0) for k in labels]
    x = np.arange(len(labels))

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
    a_labels = ["party1", "party2", "mixed", "other"]
    a_vals = [attrib.get(k, 0) for k in a_labels]
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entities_folder", required=True, help="Folder with wikidata/navboxes/categories JSONL outputs")
    ap.add_argument("--classified", required=True, help="Classified JSONL output")
    ap.add_argument("--outdir", required=True, help="Output folder for report/figures")
    ap.add_argument("--report", default="visualization_report.json", help="Report filename inside outdir")
    args = ap.parse_args()

    ensure_dir(args.outdir)

    wd_path = os.path.join(args.entities_folder, "wikidata_entities.jsonl")
    nav_path = os.path.join(args.entities_folder, "navboxes_entities.jsonl")
    cat_path = os.path.join(args.entities_folder, "categories_entities.jsonl")

    wd_qids = load_qids(wd_path)
    nav_qids = load_qids(nav_path)
    cat_qids = load_qids(cat_path)

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

    for r in classified_rows:
        q = qid_of(r)
        if not q:
            continue

        lab = r.get("attribution")
        if not isinstance(lab, str):
            lab = r.get("ru_ua_attribution")
            if lab == "Russian":
                lab = "party1"
            elif lab == "Ukraine":
                lab = "party2"
        if lab not in {"party1", "party2", "mixed", "other"}:
            lab = "mixed"

        b = source_bucket(q, wd_qids, nav_qids, cat_qids)
        attrib_counts[lab] += 1
        source_bucket_counts[b] += 1
        by_label_bucket[lab][b] += 1

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
        },
        "note": "Uses attribution field when present, fallback to ru_ua_attribution.",
    }

    make_plots(report, args.outdir)

    out_report = os.path.join(args.outdir, args.report)
    write_json(out_report, report)
    print(f"[visualization] wrote {out_report}")
    if not HAS_PLOT:
        print("[visualization] matplotlib not available; JSON report only")


if __name__ == "__main__":
    main()
