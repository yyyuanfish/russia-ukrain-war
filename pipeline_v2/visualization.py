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
import csv
import os
import re
import tempfile
from collections import Counter, defaultdict
from typing import Callable, Dict, List, Optional, Set

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

DEFAULT_HINT_ORDER = ["person", "event", "organization", "policy", "media_narrative", "unknown"]
SOURCE_KEYS = ["wikidata", "navboxes", "categories"]
SOURCE_TYPE_TO_KEY = {
    "wikidata_sparql": "wikidata",
    "wikipedia_navboxes": "navboxes",
    "wikipedia_categories": "categories",
}
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
VIS_USER_AGENT = "conflict-visualization/1.0 (research; class-summary)"
UNKNOWN_HINT_TOP_N = 10
UNKNOWN_HINT_HEATMAP_ITEMS = 6
UNKNOWN_HINT_LABEL_MAX_CHARS = 34
FALLBACK_CLASS_LABELS = {
    "en": {
        "Q5": "human",
        "Q43229": "organization",
        "Q645883": "battle",
        "Q7278": "political party",
        "Q11424": "film",
        "Q79913": "non-governmental organization",
        "Q191067": "article",
        "Q273120": "protest",
        "Q350604": "armed conflict",
        "Q467011": "invasion",
        "Q678146": "bombardment",
        "Q686984": "civil disorder",
        "Q188686": "military occupation",
        "Q1190554": "occurrence",
        "Q1330251": "United Nations General Assembly resolution",
        "Q13406463": "Wikimedia list article",
        "Q17928402": "blog post",
        "Q17524420": "aspect of history",
        "Q18340550": "Wikimedia timeline",
        "Q18487055": "missile model",
        "Q2001676": "offensive",
        "Q21191270": "television series episode",
        "Q3002150": "aircraft crash",
        "Q4071928": "cyberattack",
        "Q5176896": "counteroffensive",
        "Q5791104": "international crisis",
        "Q104841013": "hromada",
        "Q111034471": "missile strike",
    }
}


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
    parties = cfg.get("conflicting_parties") if isinstance(cfg, dict) and isinstance(cfg.get("conflicting_parties"), dict) else {}
    p1 = parties.get("party1") if isinstance(parties.get("party1"), dict) else {}
    p2 = parties.get("party2") if isinstance(parties.get("party2"), dict) else {}

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
    p1_label = p1.get("label")
    p2_label = p2.get("label")
    if isinstance(p1_label, str) and p1_label.strip():
        attr_display["party1"] = p1_label.strip()
    if isinstance(p2_label, str) and p2_label.strip():
        attr_display["party2"] = p2_label.strip()

    p1_lang = lang_map.get("party1")
    p2_lang = lang_map.get("party2")
    if attr_display["party1"] == "party1" and isinstance(p1_lang, str) and p1_lang.strip():
        attr_display["party1"] = p1_lang.strip().lower()
    if attr_display["party2"] == "party2" and isinstance(p2_lang, str) and p2_lang.strip():
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


def pick_language_overlap_langs(vis_cfg: dict) -> List[str]:
    ordered: List[str] = []
    for lang in vis_cfg.get("language_order", []):
        if isinstance(lang, str):
            ll = lang.strip().lower()
            if ll in {"en", "ru", "uk"} and ll not in ordered:
                ordered.append(ll)
    for lang in ("en", "ru", "uk"):
        if lang not in ordered:
            ordered.append(lang)
    return ordered[:3]


def collect_language_presence_sets(rows: List[dict], langs: List[str], field: str) -> Dict[str, Set[str]]:
    if field not in {"pages", "labels"}:
        raise ValueError(f"unsupported language-presence field: {field}")

    out: Dict[str, Set[str]] = {lang: set() for lang in langs}
    for rec in rows:
        qid = qid_of(rec)
        if not qid:
            continue

        if field == "pages":
            values = rec.get("sitelinks") if isinstance(rec.get("sitelinks"), dict) else {}
            for lang in langs:
                if values.get(f"{lang}wiki"):
                    out[lang].add(qid)
        else:
            values = rec.get("labels") if isinstance(rec.get("labels"), dict) else {}
            for lang in langs:
                if values.get(lang):
                    out[lang].add(qid)
    return out


def build_language_overlap_report(lang_sets: Dict[str, Set[str]], langs: List[str]) -> dict:
    if len(langs) != 3:
        raise ValueError("language overlap report requires exactly three languages")

    a, b, c = langs
    aset = set(lang_sets.get(a, set()))
    bset = set(lang_sets.get(b, set()))
    cset = set(lang_sets.get(c, set()))
    subsets = _venn_subsets(aset, bset, cset)

    return {
        "langs": list(langs),
        "set_sizes": {
            a: len(aset),
            b: len(bset),
            c: len(cset),
        },
        "venn_subsets": {
            f"{a}_only": subsets[0],
            f"{b}_only": subsets[1],
            f"{a}_{b}_only": subsets[2],
            f"{c}_only": subsets[3],
            f"{a}_{c}_only": subsets[4],
            f"{b}_{c}_only": subsets[5],
            "all_three": subsets[6],
        },
        "entity_count_with_any_language": len(aset | bset | cset),
    }


def resolve_qid_labels(qids: List[str], lang: str = "en") -> Dict[str, Optional[str]]:
    qids = [q for q in qids if isinstance(q, str) and q.startswith("Q")]
    if not qids:
        return {}

    try:
        from SPARQLWrapper import JSON as SPARQL_JSON
        from SPARQLWrapper import SPARQLWrapper
    except Exception:
        return {}

    values = " ".join(f"wd:{q}" for q in sorted(set(qids)))
    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT ?item ?itemLabel WHERE {{
  VALUES ?item {{ {values} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}" . }}
}}
"""
    try:
        sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent=VIS_USER_AGENT)
        sparql.setQuery(query)
        sparql.setReturnFormat(SPARQL_JSON)
        sparql.setTimeout(60)
        data = sparql.query().convert()
    except Exception:
        return {}

    out: Dict[str, Optional[str]] = {}
    for b in data.get("results", {}).get("bindings", []):
        uri = b.get("item", {}).get("value")
        label = b.get("itemLabel", {}).get("value")
        if not uri:
            continue
        qid = uri.rsplit("/", 1)[-1]
        if qid.startswith("Q"):
            out[qid] = label if isinstance(label, str) and label else None
    return out


def fallback_class_labels(qids: List[str], lang: str = "en") -> Dict[str, Optional[str]]:
    lang_map = FALLBACK_CLASS_LABELS.get(lang, {})
    return {qid: lang_map[qid] for qid in qids if qid in lang_map}


def pick_report_label_lang(cfg: Optional[dict]) -> str:
    ccfg = cfg.get("classification") if isinstance(cfg, dict) and isinstance(cfg.get("classification"), dict) else {}
    report_label_lang = ccfg.get("report_label_lang")
    if isinstance(report_label_lang, str) and report_label_lang.strip():
        return report_label_lang.strip().lower()

    lcfg = cfg.get("languages") if isinstance(cfg, dict) and isinstance(cfg.get("languages"), dict) else {}
    party3 = lcfg.get("party3")
    if isinstance(party3, str) and party3.strip():
        return party3.strip().lower()
    return "en"


def _valid_instance_of_qids(rec: dict) -> List[str]:
    insts = rec.get("instance_of")
    if not isinstance(insts, list):
        return []

    out: List[str] = []
    seen: Set[str] = set()
    for raw in insts:
        if not isinstance(raw, str):
            continue
        qid = raw.rsplit("/", 1)[-1] if "/Q" in raw else raw
        if qid.startswith("Q") and qid not in seen:
            out.append(qid)
            seen.add(qid)
    return out


def _pack_unknown_class_summary(
    counter: Counter,
    unknown_entity_count: int,
    missing_instance_of_count: int,
    label_map: Dict[str, Optional[str]],
    top_n: int,
) -> dict:
    top_classes = []
    for qid, count in counter.most_common(max(1, int(top_n))):
        label = label_map.get(qid)
        display = f"{label} ({qid})" if isinstance(label, str) and label.strip() else qid
        top_classes.append(
            {
                "qid": qid,
                "label": label,
                "display": display,
                "count": int(count),
            }
        )

    return {
        "unknown_entity_count": int(unknown_entity_count),
        "entities_with_no_instance_of": int(missing_instance_of_count),
        "unique_class_count": int(len(counter)),
        "instance_assignment_total": int(sum(counter.values())),
        "top_classes": top_classes,
    }


def build_unknown_hint_class_report(
    rows: List[dict],
    top_n: int = UNKNOWN_HINT_TOP_N,
    label_lang: str = "en",
    label_resolver: Optional[Callable[[List[str], str], Dict[str, Optional[str]]]] = None,
) -> dict:
    overall_counts = Counter()
    overall_unknown_entities = 0
    overall_missing_instance = 0
    by_source_counts = defaultdict(Counter)
    by_source_unknown_entities = Counter()
    by_source_missing_instance = Counter()
    all_qids: Set[str] = set()

    for rec in rows:
        inst_qids = _valid_instance_of_qids(rec)

        entity_profile = _entity_hint_profile(rec)
        if entity_profile.get("effective_hint") == "unknown":
            overall_unknown_entities += 1
            if inst_qids:
                for qid in inst_qids:
                    overall_counts[qid] += 1
                    all_qids.add(qid)
            else:
                overall_missing_instance += 1

        for sk in SOURCE_KEYS:
            source_profile = _source_hint_profile(rec, sk)
            if not bool(source_profile.get("present")):
                continue
            if source_profile.get("effective_hint") != "unknown":
                continue

            by_source_unknown_entities[sk] += 1
            if inst_qids:
                for qid in inst_qids:
                    by_source_counts[sk][qid] += 1
                    all_qids.add(qid)
            else:
                by_source_missing_instance[sk] += 1

    label_map = fallback_class_labels(sorted(all_qids), label_lang) if all_qids else {}
    resolver = label_resolver if callable(label_resolver) else resolve_qid_labels
    if all_qids:
        resolved = resolver(sorted(all_qids), label_lang)
        if isinstance(resolved, dict):
            for qid, label in resolved.items():
                if isinstance(label, str) and label.strip():
                    label_map[qid] = label.strip()

    return {
        "label_lang": label_lang,
        "top_n": int(top_n),
        "overall": _pack_unknown_class_summary(
            overall_counts,
            overall_unknown_entities,
            overall_missing_instance,
            label_map,
            top_n,
        ),
        "by_source": {
            sk: _pack_unknown_class_summary(
                by_source_counts.get(sk, Counter()),
                int(by_source_unknown_entities.get(sk, 0)),
                int(by_source_missing_instance.get(sk, 0)),
                label_map,
                top_n,
            )
            for sk in SOURCE_KEYS
        },
    }


def _shorten_note_label(text: str, max_chars: int = UNKNOWN_HINT_LABEL_MAX_CHARS) -> str:
    if not isinstance(text, str):
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def format_unknown_class_note(summary: Optional[dict], max_items: int = UNKNOWN_HINT_HEATMAP_ITEMS) -> Optional[str]:
    if not isinstance(summary, dict):
        return None

    unknown_entity_count = int(summary.get("unknown_entity_count", 0))
    top_classes = summary.get("top_classes") if isinstance(summary.get("top_classes"), list) else []
    missing_instance = int(summary.get("entities_with_no_instance_of", 0))

    if unknown_entity_count <= 0:
        return None

    lines = [f"Unknown hint classes (n={unknown_entity_count})"]
    shown = 0
    for item in top_classes:
        if shown >= max(1, int(max_items)):
            break
        if not isinstance(item, dict):
            continue
        label = item.get("display") or item.get("label") or item.get("qid") or "unknown class"
        count = int(item.get("count", 0))
        lines.append(f"{shown + 1}. {_shorten_note_label(str(label))} ({count})")
        shown += 1

    if shown == 0:
        lines.append("No instance_of labels available")
    if missing_instance > 0:
        lines.append(f"No instance_of: {missing_instance}")
    return "\n".join(lines)


def _counter_dict(c) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if isinstance(c, Counter):
        for k, v in c.items():
            out[str(k)] = int(v)
    return out


def _nested_counter_dict(d) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        if isinstance(v, Counter):
            out[str(k)] = _counter_dict(v)
    return out


def _normalize_hint(value) -> str:
    if isinstance(value, str):
        v = value.strip().lower()
        if v:
            return v
    return "unknown"


def _source_key_from_type(source_type) -> Optional[str]:
    if not isinstance(source_type, str):
        return None
    return SOURCE_TYPE_TO_KEY.get(source_type.strip())


def _iter_source_entries(rec: dict) -> List[dict]:
    out: List[dict] = []
    src = rec.get("source")
    if isinstance(src, dict):
        out.append(src)
    srcs = rec.get("_sources")
    if isinstance(srcs, list):
        for s in srcs:
            if isinstance(s, dict):
                out.append(s)
    return out


def _pick_effective_hint(values: List[str]) -> str:
    if not values:
        return "unknown"
    non_unknown = [h for h in values if h != "unknown"]
    if not non_unknown:
        return values[0]
    counts = Counter(non_unknown)
    best = non_unknown[0]
    for h in non_unknown:
        if counts[h] > counts[best]:
            best = h
    return best


def _hint_profile_from_entries(entries: List[dict]) -> dict:
    present = bool(entries)
    has_hint_key = False
    nonempty_hints: List[str] = []

    for s in entries:
        if not isinstance(s, dict):
            continue
        if "hint" in s:
            has_hint_key = True
        hv = s.get("hint")
        if isinstance(hv, str) and hv.strip():
            nonempty_hints.append(_normalize_hint(hv))

    unique_hints = sorted(set(nonempty_hints))
    effective_hint = _pick_effective_hint(nonempty_hints)

    if not present:
        reason = "missing_source_metadata"
    elif not has_hint_key:
        reason = "missing_hint_metadata"
    elif not nonempty_hints:
        reason = "blank_hint_value"
    elif all(h == "unknown" for h in nonempty_hints):
        reason = "explicit_unknown_only"
    elif any(h == "unknown" for h in nonempty_hints):
        reason = "mixed_known_and_unknown"
    else:
        reason = "has_known_hint_only"

    return {
        "present": present,
        "has_hint_key": has_hint_key,
        "nonempty_hints": nonempty_hints,
        "unique_hints": unique_hints,
        "effective_hint": effective_hint,
        "reason": reason,
    }


def _entity_hint_profile(rec: dict) -> dict:
    return _hint_profile_from_entries(_iter_source_entries(rec))


def _source_hint_profile(rec: dict, source_key: str) -> dict:
    picked: List[dict] = []
    for s in _iter_source_entries(rec):
        if _source_key_from_type(s.get("type")) == source_key:
            picked.append(s)
    return _hint_profile_from_entries(picked)


def _unknown_reason_group(reason: str) -> str:
    if reason in {"missing_source_metadata", "missing_hint_metadata", "blank_hint_value"}:
        return "missing_info"
    if reason == "explicit_unknown_only":
        return "true_unknown"
    if reason == "mixed_known_and_unknown":
        return "mixed_known_unknown"
    if reason == "has_known_hint_only":
        return "known_hint"
    return "other"


def build_unknown_hint_report(
    overall_reason_counts: Counter,
    by_label_reason_counts: Dict[str, Counter],
    by_source_reason_counts: Dict[str, Counter],
    by_source_label_reason_counts: Dict[str, Dict[str, Counter]],
) -> dict:
    group_overall = Counter()
    group_by_label = defaultdict(Counter)
    group_by_source = defaultdict(Counter)
    group_by_source_label = defaultdict(lambda: defaultdict(Counter))

    for reason, n in overall_reason_counts.items():
        group_overall[_unknown_reason_group(str(reason))] += int(n)
    for lab, cnt in by_label_reason_counts.items():
        for reason, n in cnt.items():
            group_by_label[str(lab)][_unknown_reason_group(str(reason))] += int(n)
    for sk, cnt in by_source_reason_counts.items():
        for reason, n in cnt.items():
            group_by_source[str(sk)][_unknown_reason_group(str(reason))] += int(n)
    for sk, lab_map in by_source_label_reason_counts.items():
        for lab, cnt in lab_map.items():
            for reason, n in cnt.items():
                group_by_source_label[str(sk)][str(lab)][_unknown_reason_group(str(reason))] += int(n)

    out = {
        "reason_counts_overall": _counter_dict(overall_reason_counts),
        "reason_counts_by_attribution": _nested_counter_dict(by_label_reason_counts),
        "reason_counts_by_source": {
            k: _counter_dict(v) for k, v in by_source_reason_counts.items()
        },
        "reason_counts_by_source_and_attribution": {
            sk: _nested_counter_dict(by_source_label_reason_counts.get(sk, {}))
            for sk in SOURCE_KEYS
        },
        "group_counts_overall": _counter_dict(group_overall),
        "group_counts_by_attribution": _nested_counter_dict(group_by_label),
        "group_counts_by_source": {
            k: _counter_dict(v) for k, v in group_by_source.items()
        },
        "group_counts_by_source_and_attribution": {
            sk: _nested_counter_dict(group_by_source_label.get(sk, {}))
            for sk in SOURCE_KEYS
        },
        "reason_definitions": {
            "missing_source_metadata": "No source metadata dictionaries were found for the entity.",
            "missing_hint_metadata": "Source metadata exists but 'hint' key is absent.",
            "blank_hint_value": "Hint key exists but values are blank/null.",
            "explicit_unknown_only": "Hint values exist and all normalize to 'unknown'.",
            "mixed_known_and_unknown": "Both known hints and unknown hints are present.",
            "has_known_hint_only": "Known hints are present and no unknown hint values are used.",
        },
        "group_definitions": {
            "missing_info": "Unknown is caused by missing source/hint metadata.",
            "true_unknown": "Unknown is explicitly provided by source metadata.",
            "mixed_known_unknown": "Known and unknown hint values appear together.",
            "known_hint": "Known hint values only (normally not counted as unknown).",
            "other": "Any reason not covered above.",
        },
        "focus_for_interpretation": [
            "Use missing_info vs true_unknown to separate metadata gaps from real unknown entities.",
            "Source-level group counts help identify which harvester introduces unknown dilution.",
        ],
    }
    return out


def _ordered_hints(observed: Set[str]) -> List[str]:
    out: List[str] = []
    for h in DEFAULT_HINT_ORDER:
        if h in observed:
            out.append(h)
    rest = sorted(h for h in observed if h not in set(DEFAULT_HINT_ORDER))
    out.extend(rest)
    return out


def build_hint_report(
    by_label_hint: Dict[str, Counter],
    labels: List[str],
) -> dict:
    observed: Set[str] = set()
    for lab in labels:
        observed.update((by_label_hint.get(lab) or {}).keys())
    if not observed:
        observed = {"unknown"}

    hints = _ordered_hints(observed)
    label_order = [lab for lab in labels if lab in {"party1", "party2", "mixed", "other"}]

    col_totals: Dict[str, int] = {h: 0 for h in hints}
    row_totals: Dict[str, int] = {lab: 0 for lab in label_order}
    counts: Dict[str, Dict[str, int]] = {}
    pct_of_hint: Dict[str, Dict[str, float]] = {}
    pct_of_row: Dict[str, Dict[str, float]] = {}

    for lab in label_order:
        row = {}
        row_sum = 0
        for h in hints:
            n = int((by_label_hint.get(lab) or {}).get(h, 0))
            row[h] = n
            row_sum += n
            col_totals[h] += n
        counts[lab] = row
        row_totals[lab] = row_sum

    for lab in label_order:
        pct_of_hint[lab] = {}
        pct_of_row[lab] = {}
        for h in hints:
            n = counts[lab][h]
            denom = col_totals.get(h, 0)
            pct_of_hint[lab][h] = (100.0 * n / denom) if denom > 0 else 0.0
            row_denom = row_totals.get(lab, 0)
            pct_of_row[lab][h] = (100.0 * n / row_denom) if row_denom > 0 else 0.0

    return {
        "labels": label_order,
        "hints": hints,
        "counts": counts,
        "column_totals_by_hint": col_totals,
        "row_totals_by_attribution": row_totals,
        "cell_percent_of_hint_total": pct_of_hint,
        "cell_percent_of_row_total": pct_of_row,
        "hint_extraction": {
            "method": "effective_hint",
            "description": "Uses source.hint and _sources[*].hint; prefers most frequent non-unknown hint per entity.",
        },
        "note": "Provides both column-normalized and row-normalized cell percentages.",
    }


def write_hint_table_csv(
    hint_report: dict,
    outdir: str,
    vis_cfg: dict,
    pct_mode: str = "column",
    filename: str = "hint_attribution_table.csv",
) -> str:
    ensure_dir(outdir)
    out_path = os.path.join(outdir, filename)
    labels = hint_report.get("labels", [])
    hints = hint_report.get("hints", [])
    counts = hint_report.get("counts", {})
    if pct_mode == "row":
        pct = hint_report.get("cell_percent_of_row_total", {})
        pct_desc = "row%"
    else:
        pct = hint_report.get("cell_percent_of_hint_total", {})
        pct_desc = "column%"
    col_totals = hint_report.get("column_totals_by_hint", {})
    row_totals = hint_report.get("row_totals_by_attribution", {})

    name_map = vis_cfg.get("attribution_display_names", {})
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        header = ["attribution"]
        header.extend([f"{h} (total={int(col_totals.get(h, 0))}) [{pct_desc}]" for h in hints])
        header.append("row_total")
        w.writerow(header)

        for lab in labels:
            display = name_map.get(lab, lab)
            row = [display]
            for h in hints:
                n = int((counts.get(lab) or {}).get(h, 0))
                p = float((pct.get(lab) or {}).get(h, 0.0))
                row.append(f"{n} ({p:.1f}%)")
            row.append(int(row_totals.get(lab, 0)))
            w.writerow(row)
    return out_path


def plot_hint_heatmap(
    hint_report: dict,
    outdir: str,
    vis_cfg: dict,
    pct_mode: str = "column",
    filename: str = "hint_attribution_heatmap.png",
    title_prefix: str = "Attribution by Harvest Hint",
    unknown_class_summary: Optional[dict] = None,
) -> None:
    if not HAS_PLOT:
        return
    labels = hint_report.get("labels", [])
    hints = hint_report.get("hints", [])
    counts = hint_report.get("counts", {})
    if pct_mode == "row":
        pct = hint_report.get("cell_percent_of_row_total", {})
        pct_desc = "row%"
    else:
        pct = hint_report.get("cell_percent_of_hint_total", {})
        pct_desc = "column%"
    if not labels or not hints:
        return

    data = np.array([[int((counts.get(lab) or {}).get(h, 0)) for h in hints] for lab in labels], dtype=float)
    name_map = vis_cfg.get("attribution_display_names", {})
    ylabels = [name_map.get(lab, lab) for lab in labels]
    unknown_note = format_unknown_class_note(unknown_class_summary)

    fig_w = max(8, 1.4 * len(hints))
    fig_h = max(4, 1.1 * len(labels) + 2.0)
    if unknown_note:
        fig_w += 4.8

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    vmax = max(float(data.max()), 1.0)
    im = ax.imshow(data, cmap="YlGnBu", aspect="auto", vmin=0, vmax=vmax)
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Count")

    ax.set_xticks(np.arange(len(hints)))
    ax.set_xticklabels(hints, rotation=35, ha="right")
    ax.set_yticks(np.arange(len(labels)))
    ax.set_yticklabels(ylabels)
    ax.set_title(f"{title_prefix} (count and {pct_desc})")

    for i, lab in enumerate(labels):
        for j, h in enumerate(hints):
            n = int((counts.get(lab) or {}).get(h, 0))
            p = float((pct.get(lab) or {}).get(h, 0.0))
            color = "white" if data[i, j] > (0.55 * vmax) else "black"
            ax.text(j, i, f"{n}\n({p:.1f}%)", ha="center", va="center", fontsize=8, color=color)

    if unknown_note:
        fig.tight_layout(rect=[0, 0, 0.70, 1])
        fig.text(
            0.73,
            0.5,
            unknown_note,
            ha="left",
            va="center",
            fontsize=8,
            wrap=True,
            bbox={"boxstyle": "round,pad=0.4", "facecolor": "#fff8dc", "edgecolor": "#999999"},
        )
    else:
        fig.tight_layout()

    fig.savefig(os.path.join(outdir, filename), bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)


def plot_unknown_hint_breakdown(unknown_report: dict, outdir: str) -> None:
    if not HAS_PLOT:
        return
    overall = unknown_report.get("reason_counts_overall", {}) if isinstance(unknown_report, dict) else {}
    if not isinstance(overall, dict) or not overall:
        return

    keys = sorted(overall.keys())
    vals = [int(overall.get(k, 0)) for k in keys]
    x = np.arange(len(keys))

    plt.figure(figsize=(max(8, 1.1 * len(keys)), 4.5))
    plt.bar(x, vals)
    plt.xticks(x, keys, rotation=35, ha="right")
    plt.ylabel("Count")
    plt.title("Unknown hint diagnostics (overall)")
    for i, v in enumerate(vals):
        plt.text(i, v + max(vals + [1]) * 0.01, str(v), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "unknown_hint_reason_counts.png"))
    plt.close()


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


def plot_language_overlap_venn(
    lang_sets: Dict[str, Set[str]],
    langs: List[str],
    set_labels: List[str],
    title: str,
    out_path: str,
) -> None:
    if not HAS_PLOT or not HAS_VENN:
        return
    if len(langs) != 3 or len(set_labels) != 3:
        return

    a, b, c = langs
    plt.figure(figsize=(6, 6))
    venn3(
        subsets=_venn_subsets(
            set(lang_sets.get(a, set())),
            set(lang_sets.get(b, set())),
            set(lang_sets.get(c, set())),
        ),
        set_labels=tuple(set_labels),
    )
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def make_plots(
    report: dict,
    outdir: str,
    vis_cfg: dict,
    wd_qids: Set[str],
    nav_qids: Set[str],
    cat_qids: Set[str],
    label_qids: Dict[str, Set[str]],
    page_lang_sets: Dict[str, Set[str]],
    label_lang_sets: Dict[str, Set[str]],
    overlap_langs: List[str],
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

    if len(overlap_langs) == 3:
        plot_language_overlap_venn(
            page_lang_sets,
            overlap_langs,
            [f"{lang}wiki" for lang in overlap_langs],
            "Wikipedia page overlap by language (Venn)",
            os.path.join(outdir, "venn_wikipedia_pages_by_language.png"),
        )
        plot_language_overlap_venn(
            label_lang_sets,
            overlap_langs,
            [f"labels.{lang}" for lang in overlap_langs],
            "Wikidata label overlap by language (Venn)",
            os.path.join(outdir, "venn_wikidata_labels_by_language.png"),
        )

    hint_report = report.get("classified", {}).get("hint_attribution")
    unknown_class_report = report.get("classified", {}).get("unknown_hint_class_analysis")
    overall_unknown_class_summary = None
    if isinstance(unknown_class_report, dict):
        overall_unknown_class_summary = unknown_class_report.get("overall")
    if isinstance(hint_report, dict):
        plot_hint_heatmap(
            hint_report,
            outdir,
            vis_cfg,
            pct_mode="column",
            filename="hint_attribution_heatmap.png",
            title_prefix="Attribution by Harvest Hint",
            unknown_class_summary=overall_unknown_class_summary,
        )
        plot_hint_heatmap(
            hint_report,
            outdir,
            vis_cfg,
            pct_mode="row",
            filename="hint_attribution_heatmap_row_normalized.png",
            title_prefix="Attribution by Harvest Hint",
            unknown_class_summary=overall_unknown_class_summary,
        )

    by_source = report.get("classified", {}).get("hint_attribution_by_source", {})
    if isinstance(by_source, dict):
        source_names = vis_cfg.get("source_display_names", {})
        for sk in SOURCE_KEYS:
            hrep = by_source.get(sk)
            if not isinstance(hrep, dict):
                continue
            source_label = source_names.get(sk, sk)
            source_unknown_summary = None
            if isinstance(unknown_class_report, dict):
                source_unknown_summary = (unknown_class_report.get("by_source") or {}).get(sk)
            plot_hint_heatmap(
                hrep,
                outdir,
                vis_cfg,
                pct_mode="column",
                filename=f"hint_attribution_heatmap_{_safe_slug(sk)}.png",
                title_prefix=f"{source_label}: Attribution by Hint",
                unknown_class_summary=source_unknown_summary,
            )
            plot_hint_heatmap(
                hrep,
                outdir,
                vis_cfg,
                pct_mode="row",
                filename=f"hint_attribution_heatmap_{_safe_slug(sk)}_row_normalized.png",
                title_prefix=f"{source_label}: Attribution by Hint",
                unknown_class_summary=source_unknown_summary,
            )

    unknown_report = report.get("classified", {}).get("unknown_hint_analysis")
    if isinstance(unknown_report, dict):
        plot_unknown_hint_breakdown(unknown_report, outdir)


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
    by_label_hint = defaultdict(Counter)
    by_source_label_hint = defaultdict(lambda: defaultdict(Counter))
    unknown_reason_counts = Counter()
    unknown_reason_by_label = defaultdict(Counter)
    unknown_reason_by_source = defaultdict(Counter)
    unknown_reason_by_source_label = defaultdict(lambda: defaultdict(Counter))
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
        hint_profile = _entity_hint_profile(r)
        hint = hint_profile.get("effective_hint", "unknown")
        hint_reason = hint_profile.get("reason", "other")
        attrib_counts[lab] += 1
        source_bucket_counts[b] += 1
        by_label_bucket[lab][b] += 1
        by_label_hint[lab][hint] += 1
        label_qids[lab].add(q)

        if hint == "unknown":
            unknown_reason_counts[hint_reason] += 1
            unknown_reason_by_label[lab][hint_reason] += 1

        for sk in SOURCE_KEYS:
            source_profile = _source_hint_profile(r, sk)
            if not bool(source_profile.get("present")):
                continue
            shint = source_profile.get("effective_hint", "unknown")
            sreason = source_profile.get("reason", "other")
            by_source_label_hint[sk][lab][shint] += 1
            if shint == "unknown":
                unknown_reason_by_source[sk][sreason] += 1
                unknown_reason_by_source_label[sk][lab][sreason] += 1

    lang_cov = compute_language_coverage(classified_rows, vis_cfg.get("language_order", []))
    overlap_langs = pick_language_overlap_langs(vis_cfg)
    page_lang_sets = collect_language_presence_sets(classified_rows, overlap_langs, field="pages")
    label_lang_sets = collect_language_presence_sets(classified_rows, overlap_langs, field="labels")
    page_overlap_report = build_language_overlap_report(page_lang_sets, overlap_langs)
    label_overlap_report = build_language_overlap_report(label_lang_sets, overlap_langs)
    hint_report = build_hint_report(by_label_hint, labels=["party1", "party2", "mixed", "other"])
    hint_csv = write_hint_table_csv(
        hint_report,
        args.outdir,
        vis_cfg,
        pct_mode="column",
        filename="hint_attribution_table.csv",
    )
    hint_csv_row = write_hint_table_csv(
        hint_report,
        args.outdir,
        vis_cfg,
        pct_mode="row",
        filename="hint_attribution_table_row_normalized.csv",
    )

    hint_report_by_source: Dict[str, dict] = {}
    hint_csv_by_source: Dict[str, dict] = {}
    for sk in SOURCE_KEYS:
        lab_hint = by_source_label_hint.get(sk)
        if not lab_hint:
            continue
        sr = build_hint_report(lab_hint, labels=["party1", "party2", "mixed", "other"])
        hint_report_by_source[sk] = sr
        hint_csv_by_source[sk] = {
            "column": write_hint_table_csv(
                sr,
                args.outdir,
                vis_cfg,
                pct_mode="column",
                filename=f"hint_attribution_table_{_safe_slug(sk)}.csv",
            ),
            "row": write_hint_table_csv(
                sr,
                args.outdir,
                vis_cfg,
                pct_mode="row",
                filename=f"hint_attribution_table_{_safe_slug(sk)}_row_normalized.csv",
            ),
        }

    unknown_report = build_unknown_hint_report(
        overall_reason_counts=unknown_reason_counts,
        by_label_reason_counts=unknown_reason_by_label,
        by_source_reason_counts=unknown_reason_by_source,
        by_source_label_reason_counts=unknown_reason_by_source_label,
    )
    unknown_class_report = build_unknown_hint_class_report(
        classified_rows,
        top_n=UNKNOWN_HINT_TOP_N,
        label_lang=pick_report_label_lang(cfg),
    )

    heatmap_outputs = {
        "column": os.path.join(args.outdir, "hint_attribution_heatmap.png"),
        "row": os.path.join(args.outdir, "hint_attribution_heatmap_row_normalized.png"),
    }
    heatmap_by_source = {
        sk: {
            "column": os.path.join(args.outdir, f"hint_attribution_heatmap_{_safe_slug(sk)}.png"),
            "row": os.path.join(args.outdir, f"hint_attribution_heatmap_{_safe_slug(sk)}_row_normalized.png"),
        }
        for sk in hint_report_by_source.keys()
    }

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
            "wikipedia_page_language_overlap": page_overlap_report,
            "wikidata_label_language_overlap": label_overlap_report,
            "hint_attribution": hint_report,
            "hint_attribution_by_source": hint_report_by_source,
            "unknown_hint_analysis": unknown_report,
            "unknown_hint_class_analysis": unknown_class_report,
        },
        "visualization_config": vis_cfg,
        "entity_files": entity_files,
        "fallback_config": legacy_cfg,
        "outputs": {
            "hint_attribution_csv": {
                "column": hint_csv,
                "row": hint_csv_row,
            },
            "hint_attribution_csv_by_source": hint_csv_by_source,
            "hint_attribution_heatmap": heatmap_outputs,
            "hint_attribution_heatmap_by_source": heatmap_by_source,
            "language_overlap_venn": {
                "wikipedia_pages": os.path.join(args.outdir, "venn_wikipedia_pages_by_language.png"),
                "wikidata_labels": os.path.join(args.outdir, "venn_wikidata_labels_by_language.png"),
            },
            "unknown_hint_plot": os.path.join(args.outdir, "unknown_hint_reason_counts.png"),
        },
        "note": "Uses attribution field when present; fallback field is config-driven.",
    }

    make_plots(
        report,
        args.outdir,
        vis_cfg,
        wd_qids,
        nav_qids,
        cat_qids,
        label_qids,
        page_lang_sets,
        label_lang_sets,
        overlap_langs,
    )

    out_report = os.path.join(args.outdir, args.report)
    write_json(out_report, report)
    print(f"[visualization] wrote {out_report}")
    if not HAS_PLOT:
        print("[visualization] matplotlib not available; JSON report only")
    if HAS_PLOT and vis_cfg.get("venn", {}).get("enabled", True) and not HAS_VENN:
        print("[visualization] matplotlib-venn not available; skipped venn plots")


if __name__ == "__main__":
    main()
