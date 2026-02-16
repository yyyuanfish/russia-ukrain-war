#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Russo-Ukrainian War Knowledge Graph: Overlap & Visualisation Suite (v2)

This version is aligned to your CURRENT pipeline outputs:

Inputs:
  A) Wikipedia navboxes harvester output JSONL  (e.g., navbox_ru_ua_entities.jsonl)
  B) Wikidata SPARQL harvester output JSONL     (e.g., wd_ru_ua_entities.jsonl)
  C) Classified merged output JSONL             (e.g., classified_ru_ua_entities.jsonl)

Key change vs your old script:
  - NO normalize / NO inference.
  - We ONLY use:
      * QID overlap between A and B
      * pre-classify distributions from A and B (category_hint + language coverage)
      * post-classify distributions from C (ru_ua_attribution + source contribution)
"""

import argparse
import json
import os
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import matplotlib.pyplot as plt

# Optional (venn)
try:
    from matplotlib_venn import venn2
    HAS_VENN = True
except Exception:
    HAS_VENN = False


# -----------------------------
# Utilities
# -----------------------------

def ensure_dir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path)


def read_jsonl(path: str) -> List[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def qid_from_record(rec: dict) -> Optional[str]:
    q = rec.get("qid") or rec.get("id")
    if not q:
        # fallback: uri like http://www.wikidata.org/entity/Qxxx
        uri = rec.get("uri")
        if isinstance(uri, str) and "/Q" in uri:
            q = uri.rsplit("/", 1)[-1]
    if isinstance(q, str) and q.startswith("Q"):
        return q
    return None


def lang_coverage_nonempty_label_or_desc(rows: List[dict], lang: str) -> int:
    c = 0
    for r in rows:
        labels = (r.get("labels") or {})
        descs = (r.get("descriptions") or {})
        if labels.get(lang) or descs.get(lang):
            c += 1
    return c


def compute_language_coverage(rows: List[dict]) -> Dict[str, int]:
    return {lang: lang_coverage_nonempty_label_or_desc(rows, lang) for lang in ("en", "ru", "uk")}


def category_hint(rec: dict) -> str:
    s = rec.get("source") or {}
    hint = s.get("hint")
    return hint or "unknown"


def compute_category_hint_counts(rows: List[dict]) -> Dict[str, int]:
    c = Counter()
    for r in rows:
        c[category_hint(r)] += 1
    return dict(c)


def load_qid_set(path: str) -> Set[str]:
    rows = read_jsonl(path)
    s = set()
    for r in rows:
        q = qid_from_record(r)
        if q:
            s.add(q)
    return s


def presence_bucket(qid: str, wiki_qids: Set[str], wd_qids: Set[str]) -> str:
    in_wiki = qid in wiki_qids
    in_wd = qid in wd_qids
    if in_wiki and in_wd:
        return "both"
    if in_wiki:
        return "wiki_only"
    if in_wd:
        return "wd_only"
    return "none"


# -----------------------------
# Venn (optional)
# -----------------------------

def generate_venn_diagram(set_a: Set[str], set_b: Set[str], title: str, out_path: str,
                         label_a: str = "Wikipedia navboxes", label_b: str = "Wikidata SPARQL") -> None:
    if not HAS_VENN:
        return
    only_a = set_a - set_b
    only_b = set_b - set_a
    inter = set_a & set_b
    if not (only_a or only_b or inter):
        return

    plt.figure(figsize=(6, 6))
    v = venn2(subsets=(len(only_a), len(only_b), len(inter)), set_labels=(label_a, label_b))
    if v.get_patch_by_id("10"):
        v.get_patch_by_id("10").set_alpha(0.5)
    if v.get_patch_by_id("01"):
        v.get_patch_by_id("01").set_alpha(0.5)
    if v.get_patch_by_id("11"):
        v.get_patch_by_id("11").set_alpha(0.7)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


# -----------------------------
# Plots
# -----------------------------

def plot_global_overlap_bar(wiki_qids: Set[str], wd_qids: Set[str], output_dir: str) -> None:
    only_wiki = len(wiki_qids - wd_qids)
    only_wd = len(wd_qids - wiki_qids)
    inter = len(wiki_qids & wd_qids)

    labels = ["Wiki-only", "Both", "WD-only"]
    values = [only_wiki, inter, only_wd]

    plt.figure(figsize=(6, 4))
    x = np.arange(len(labels))
    plt.bar(x, values)
    plt.xticks(x, labels)
    plt.ylabel("Number of QIDs")
    plt.title("Global QID Overlap: Wikipedia navboxes vs Wikidata SPARQL")

    maxv = max(values) if values else 0
    for i, v in enumerate(values):
        plt.text(i, v + maxv * 0.01, str(v), ha="center", va="bottom", fontsize=9)

    out_path = os.path.join(output_dir, "global_overlap_bar.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_category_hint_distribution(hints_wiki: Dict[str, int], hints_wd: Dict[str, int], output_dir: str) -> None:
    cats = sorted(set(hints_wiki.keys()) | set(hints_wd.keys()))
    wiki_vals = [hints_wiki.get(c, 0) for c in cats]
    wd_vals = [hints_wd.get(c, 0) for c in cats]

    x = np.arange(len(cats))
    width = 0.35

    plt.figure(figsize=(9, 4))
    plt.bar(x - width / 2, wiki_vals, width, label="Wikipedia navboxes")
    plt.bar(x + width / 2, wd_vals, width, label="Wikidata SPARQL")
    plt.xticks(x, cats, rotation=20, ha="right")
    plt.ylabel("Count")
    plt.title("Pre-classify: category_hint distribution by source")
    plt.legend()

    out_path = os.path.join(output_dir, "pre_category_hint_distribution.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_language_coverage(cov_wiki: Dict[str, int], cov_wd: Dict[str, int], output_dir: str) -> None:
    langs = ["en", "ru", "uk"]
    wiki_vals = [cov_wiki.get(l, 0) for l in langs]
    wd_vals = [cov_wd.get(l, 0) for l in langs]

    x = np.arange(len(langs))
    width = 0.35

    plt.figure(figsize=(7, 4))
    plt.bar(x - width / 2, wiki_vals, width, label="Wikipedia navboxes")
    plt.bar(x + width / 2, wd_vals, width, label="Wikidata SPARQL")
    plt.xticks(x, langs)
    plt.ylabel("Count (label or desc nonempty)")
    plt.title("Pre-classify: language coverage by source")
    plt.legend()

    out_path = os.path.join(output_dir, "pre_language_coverage.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_attribution_counts(attrib_counts: Dict[str, int], output_dir: str) -> None:
    labels = ["Russian", "Ukraine", "mixed", "other"]
    vals = [attrib_counts.get(l, 0) for l in labels]

    plt.figure(figsize=(7, 4))
    x = np.arange(len(labels))
    plt.bar(x, vals)
    plt.xticks(x, labels)
    plt.ylabel("Count")
    plt.title("Post-classify: ru_ua_attribution counts (merged)")

    maxv = max(vals) if vals else 0
    for i, v in enumerate(vals):
        plt.text(i, v + maxv * 0.01, str(v), ha="center", va="bottom", fontsize=9)

    out_path = os.path.join(output_dir, "post_attribution_counts.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_attribution_by_source_presence(by_label_presence: Dict[str, Dict[str, int]], output_dir: str) -> None:
    labels = ["Russian", "Ukraine", "mixed", "other"]
    buckets = ["wiki_only", "both", "wd_only"]

    x = np.arange(len(labels))
    width = 0.25

    plt.figure(figsize=(9, 4))
    for j, b in enumerate(buckets):
        vals = [by_label_presence.get(l, {}).get(b, 0) for l in labels]
        plt.bar(x + (j - 1) * width, vals, width, label=b)

    plt.xticks(x, labels)
    plt.ylabel("Count")
    plt.title("Post-classify: attribution by source contribution (wiki_only / both / wd_only)")
    plt.legend()

    out_path = os.path.join(output_dir, "post_attribution_by_source_presence.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_per_label_overlap_composition(per_label_stats: Dict[str, Dict[str, float]], output_dir: str) -> None:
    labels = ["Russian", "Ukraine", "mixed", "other"]
    wiki_only = [per_label_stats.get(l, {}).get("wiki_only", 0) for l in labels]
    both = [per_label_stats.get(l, {}).get("inter", 0) for l in labels]
    wd_only = [per_label_stats.get(l, {}).get("wd_only", 0) for l in labels]

    x = np.arange(len(labels))
    width = 0.25

    plt.figure(figsize=(9, 4))
    plt.bar(x - width, wiki_only, width, label="Wiki-only")
    plt.bar(x, both, width, label="Both")
    plt.bar(x + width, wd_only, width, label="WD-only")

    plt.xticks(x, labels)
    plt.ylabel("Count")
    plt.title("Post-classify: overlap composition per attribution label")
    plt.legend()

    out_path = os.path.join(output_dir, "post_per_label_overlap_composition.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_per_label_jaccard(per_label_stats: Dict[str, Dict[str, float]], output_dir: str) -> None:
    labels = ["Russian", "Ukraine", "mixed", "other"]
    jacc = [per_label_stats.get(l, {}).get("jaccard", 0.0) for l in labels]

    x = np.arange(len(labels))
    plt.figure(figsize=(7, 4))
    plt.bar(x, jacc)
    plt.xticks(x, labels)
    plt.ylim(0, 1.0)
    plt.ylabel("Jaccard")
    plt.title("Post-classify: Jaccard per attribution label (Wiki vs WD)")

    for i, v in enumerate(jacc):
        plt.text(i, v + 0.02, f"{v:.2f}", ha="center", va="bottom", fontsize=9)

    out_path = os.path.join(output_dir, "post_per_label_jaccard.png")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wiki", required=True, help="Pre-classify Wikipedia navboxes JSONL")
    ap.add_argument("--wd", required=True, help="Pre-classify Wikidata SPARQL JSONL")
    ap.add_argument("--classified", required=True, help="Post-classify merged JSONL (with ru_ua_attribution)")
    ap.add_argument("--outdir", default="overlap_analysis_output_v2", help="Output directory")
    ap.add_argument("--report", default="overlap_report_v2.json", help="Output JSON report filename inside outdir")
    ap.add_argument("--write-qid-lists", action="store_true", help="Write per-label QID lists for manual QA")
    ap.add_argument("--venn", action="store_true", help="Also save Venn diagrams (requires matplotlib-venn)")
    args = ap.parse_args()

    ensure_dir(args.outdir)

    # -------- pre-classify load --------
    wiki_rows = read_jsonl(args.wiki)
    wd_rows = read_jsonl(args.wd)
    wiki_qids = {qid_from_record(r) for r in wiki_rows}
    wd_qids = {qid_from_record(r) for r in wd_rows}
    wiki_qids.discard(None)
    wd_qids.discard(None)

    inter = wiki_qids & wd_qids
    union = wiki_qids | wd_qids
    jaccard = (len(inter) / len(union)) if union else 0.0

    cov_wiki = compute_language_coverage(wiki_rows)
    cov_wd = compute_language_coverage(wd_rows)
    hints_wiki = compute_category_hint_counts(wiki_rows)
    hints_wd = compute_category_hint_counts(wd_rows)

    # -------- post-classify load --------
    cls_rows = read_jsonl(args.classified)
    cls_qids = []
    attrib_counts = Counter()
    by_label_presence = defaultdict(lambda: Counter())

    for r in cls_rows:
        q = qid_from_record(r)
        if not q:
            continue
        cls_qids.append(q)
        lab = r.get("ru_ua_attribution") or "unknown"
        attrib_counts[lab] += 1
        by_label_presence[lab][presence_bucket(q, wiki_qids, wd_qids)] += 1

    # per-label overlap stats (Wiki vs WD), using post-classify labels
    per_label_stats: Dict[str, Dict[str, float]] = {}
    for lab in ("Russian", "Ukraine", "mixed", "other"):
        qids_lab = {qid_from_record(r) for r in cls_rows if (r.get("ru_ua_attribution") == lab)}
        qids_lab.discard(None)
        a = qids_lab & wiki_qids
        b = qids_lab & wd_qids
        inter_lab = a & b
        union_lab = a | b
        per_label_stats[lab] = {
            "wiki": len(a),
            "wd": len(b),
            "inter": len(inter_lab),
            "wiki_only": len(a - b),
            "wd_only": len(b - a),
            "union": len(union_lab),
            "jaccard": (len(inter_lab) / len(union_lab)) if union_lab else 0.0,
        }

    # -------- plots --------
    plot_global_overlap_bar(wiki_qids, wd_qids, args.outdir)
    plot_category_hint_distribution(hints_wiki, hints_wd, args.outdir)
    plot_language_coverage(cov_wiki, cov_wd, args.outdir)

    plot_attribution_counts(dict(attrib_counts), args.outdir)
    plot_attribution_by_source_presence(by_label_presence, args.outdir)
    plot_per_label_overlap_composition(per_label_stats, args.outdir)
    plot_per_label_jaccard(per_label_stats, args.outdir)

    # optional venns
    if args.venn and HAS_VENN:
        generate_venn_diagram(wiki_qids, wd_qids,
                              "Global overlap (pre-classify)",
                              os.path.join(args.outdir, "venn_global.png"))
        for lab in ("Russian", "Ukraine", "mixed", "other"):
            qids_lab = {qid_from_record(r) for r in cls_rows if (r.get("ru_ua_attribution") == lab)}
            qids_lab.discard(None)
            a = qids_lab & wiki_qids
            b = qids_lab & wd_qids
            generate_venn_diagram(a, b,
                                  f"Overlap by post-classify label: {lab}",
                                  os.path.join(args.outdir, f"venn_{lab}.png"))

    # optional qid lists for QA
    if args.write_qid_lists:
        for lab in ("Russian", "Ukraine", "mixed", "other"):
            qids_lab = {qid_from_record(r) for r in cls_rows if (r.get("ru_ua_attribution") == lab)}
            qids_lab.discard(None)
            a = qids_lab & wiki_qids
            b = qids_lab & wd_qids
            inter_lab = a & b
            with open(os.path.join(args.outdir, f"qidlist_{lab}.txt"), "w", encoding="utf-8") as f:
                f.write(f"[{lab}] inter ({len(inter_lab)})\n")
                for q in sorted(inter_lab):
                    f.write(q + "\n")
                f.write(f"\n[{lab}] wiki_only ({len(a - b)})\n")
                for q in sorted(a - b):
                    f.write(q + "\n")
                f.write(f"\n[{lab}] wd_only ({len(b - a)})\n")
                for q in sorted(b - a):
                    f.write(q + "\n")

    # -------- json report --------
    report = {
        "pre_classify": {
            "wiki": {
                "file": args.wiki,
                "unique_qids": len(wiki_qids),
                "language_coverage_nonempty_label_or_desc": cov_wiki,
                "category_hint_counts": hints_wiki,
            },
            "wd": {
                "file": args.wd,
                "unique_qids": len(wd_qids),
                "language_coverage_nonempty_label_or_desc": cov_wd,
                "category_hint_counts": hints_wd,
            },
            "overlap": {
                "intersection": len(inter),
                "union": len(union),
                "wiki_only": len(wiki_qids - wd_qids),
                "wd_only": len(wd_qids - wiki_qids),
                "jaccard": jaccard,
            },
        },
        "post_classify": {
            "classified_file": args.classified,
            "unique_qids_in_classified": len(set(cls_qids)),
            "attribution_counts": dict(attrib_counts),
            "attribution_by_source_presence": {
                lab: dict(by_label_presence[lab]) for lab in by_label_presence
            },
            "per_label_overlap_stats": per_label_stats,
        },
        "note": "No normalization/inference. Uses raw JSONL fields: source.hint and ru_ua_attribution."
    }

    out_report_path = os.path.join(args.outdir, args.report)
    with open(out_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("Done. Outputs saved in:", args.outdir)
    print("Report:", out_report_path)
    if args.venn and not HAS_VENN:
        print("NOTE: --venn requested but matplotlib-venn not installed. Install: pip install matplotlib-venn")


if __name__ == "__main__":
    main()
