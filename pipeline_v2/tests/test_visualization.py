import pathlib
import sys
import unittest


PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import visualization


class VisualizationHelpersTests(unittest.TestCase):
    def test_source_bucket_distinguishes_all_overlap_shapes(self):
        # Verify overlap bucketing covers every source-combination case.
        wd = {"Q1", "Q2", "Q4", "Q7"}
        nav = {"Q2", "Q3", "Q5", "Q7"}
        cat = {"Q4", "Q5", "Q6", "Q7"}

        self.assertEqual(visualization.source_bucket("Q0", wd, nav, cat), "none")
        self.assertEqual(visualization.source_bucket("Q1", wd, nav, cat), "wd_only")
        self.assertEqual(visualization.source_bucket("Q3", wd, nav, cat), "nav_only")
        self.assertEqual(visualization.source_bucket("Q6", wd, nav, cat), "cat_only")
        self.assertEqual(visualization.source_bucket("Q2", wd, nav, cat), "wd_nav")
        self.assertEqual(visualization.source_bucket("Q4", wd, nav, cat), "wd_cat")
        self.assertEqual(visualization.source_bucket("Q5", wd, nav, cat), "nav_cat")
        self.assertEqual(visualization.source_bucket("Q7", wd, nav, cat), "all_three")

    def test_compute_language_coverage_counts_label_or_description_presence(self):
        # Verify coverage counts a language when either label or description is present.
        rows = [
            {"labels": {"en": "A"}, "descriptions": {"ru": "описание"}},
            {"labels": {"en": ""}, "descriptions": {"en": "desc"}},
            {"labels": {"ru": "Б"}, "descriptions": {}},
            {"labels": {}, "descriptions": {}},
        ]

        coverage = visualization.compute_language_coverage(rows, ["en", "ru", "uk"])

        self.assertEqual(coverage, {"en": 2, "ru": 2, "uk": 0})

    def test_collect_language_presence_sets_separates_pages_and_labels(self):
        # Verify page sitelinks and label presence produce different language membership sets.
        rows = [
            {"qid": "Q1", "sitelinks": {"enwiki": "u1", "ruwiki": "u2"}, "labels": {"en": "Alpha"}},
            {"qid": "Q2", "sitelinks": {"ukwiki": "u3"}, "labels": {"ru": "Beta", "uk": "Бета"}},
            {"qid": "Q3", "sitelinks": {"enwiki": "u4", "ukwiki": "u5"}, "labels": {"en": "Gamma", "uk": ""}},
            {"uri": "http://www.wikidata.org/entity/Q4", "sitelinks": {}, "labels": {"uk": "Delta"}},
        ]

        page_sets = visualization.collect_language_presence_sets(rows, ["en", "ru", "uk"], field="pages")
        label_sets = visualization.collect_language_presence_sets(rows, ["en", "ru", "uk"], field="labels")

        self.assertEqual(page_sets, {"en": {"Q1", "Q3"}, "ru": {"Q1"}, "uk": {"Q2", "Q3"}})
        self.assertEqual(label_sets, {"en": {"Q1", "Q3"}, "ru": {"Q2"}, "uk": {"Q2", "Q4"}})

    def test_build_language_overlap_report_counts_all_venn_regions(self):
        # Verify the language-overlap report tracks all singleton, pairwise, and triple intersections.
        lang_sets = {
            "en": {"Q1", "Q2", "Q4", "Q7"},
            "ru": {"Q2", "Q3", "Q5", "Q7"},
            "uk": {"Q4", "Q5", "Q6", "Q7"},
        }

        report = visualization.build_language_overlap_report(lang_sets, ["en", "ru", "uk"])

        self.assertEqual(report["set_sizes"], {"en": 4, "ru": 4, "uk": 4})
        self.assertEqual(
            report["venn_subsets"],
            {
                "en_only": 1,
                "ru_only": 1,
                "en_ru_only": 1,
                "uk_only": 1,
                "en_uk_only": 1,
                "ru_uk_only": 1,
                "all_three": 1,
            },
        )
        self.assertEqual(report["entity_count_with_any_language"], 7)

    def test_build_visual_config_prefers_explicit_display_names(self):
        # Verify visualization config respects explicit labels and Venn display overrides.
        cfg = {
            "languages": {"all": ["en", "ru", "uk"]},
            "conflicting_parties": {
                "party1": {"label": "Russia"},
                "party2": {"label": "Ukraine"},
            },
            "visualization": {
                "language_order": ["uk", "en"],
                "attribution_display_names": {"mixed": "contested"},
                "source_display_names": {"navboxes": "Navbox graph"},
                "venn": {
                    "enabled": True,
                    "global": False,
                    "per_label": True,
                    "source_labels": ["WD", "NAV", "CAT"],
                },
            },
        }

        visual_cfg = visualization.build_visual_config(cfg)

        self.assertEqual(visual_cfg["language_order"], ["uk", "en"])
        self.assertEqual(visual_cfg["attribution_display_names"]["party1"], "Russia")
        self.assertEqual(visual_cfg["attribution_display_names"]["party2"], "Ukraine")
        self.assertEqual(visual_cfg["attribution_display_names"]["mixed"], "contested")
        self.assertEqual(visual_cfg["source_display_names"]["navboxes"], "Navbox graph")
        self.assertEqual(visual_cfg["venn"]["source_labels"], ["WD", "NAV", "CAT"])
        self.assertFalse(visual_cfg["venn"]["global"])

    def test_build_legacy_fallback_uses_configured_mapping_when_present(self):
        # Verify legacy output labels can still be mapped back to internal labels.
        cfg = {
            "conflicting_parties": {
                "party1": {"label": "Russia"},
                "party2": {"label": "Ukraine"},
            },
            "classification": {
                "legacy_output": {
                    "field_name": "ru_ua_attribution",
                    "party1": "Russian",
                    "party2": "Ukraine",
                    "mixed": "mixed",
                    "other": "other",
                }
            },
        }

        legacy = visualization.build_legacy_fallback(cfg)

        self.assertEqual(legacy["field_name"], "ru_ua_attribution")
        self.assertEqual(
            legacy["to_internal"],
            {
                "Russian": "party1",
                "Ukraine": "party2",
                "mixed": "mixed",
                "other": "other",
            },
        )

    def test_build_unknown_hint_class_report_summarizes_top_classes_overall_and_by_source(self):
        # Verify unknown-hint entities can be summarized by instance_of class globally and per source.
        rows = [
            {
                "instance_of": ["Q5", "Q43229"],
                "source": {"type": "wikidata_sparql", "hint": "unknown"},
            },
            {
                "instance_of": ["Q5"],
                "source": {"type": "wikipedia_navboxes", "hint": "unknown"},
            },
            {
                "instance_of": [],
                "source": {"type": "wikipedia_categories", "hint": "unknown"},
            },
            {
                "instance_of": ["Q645883"],
                "source": {"type": "wikidata_sparql", "hint": "person"},
                "_sources": [{"type": "wikipedia_navboxes", "hint": "unknown"}],
            },
        ]

        report = visualization.build_unknown_hint_class_report(
            rows,
            top_n=3,
            label_lang="en",
            label_resolver=lambda qids, lang: {
                "Q5": "human",
                "Q43229": "organization",
                "Q645883": "battle",
            },
        )

        self.assertEqual(report["label_lang"], "en")
        self.assertEqual(report["overall"]["unknown_entity_count"], 3)
        self.assertEqual(report["overall"]["entities_with_no_instance_of"], 1)
        self.assertEqual(report["overall"]["top_classes"][0]["display"], "human (Q5)")
        self.assertEqual(report["overall"]["top_classes"][0]["count"], 2)

        nav_counts = {item["qid"]: item["count"] for item in report["by_source"]["navboxes"]["top_classes"]}
        self.assertEqual(report["by_source"]["wikidata"]["unknown_entity_count"], 1)
        self.assertEqual(report["by_source"]["navboxes"]["unknown_entity_count"], 2)
        self.assertEqual(report["by_source"]["categories"]["entities_with_no_instance_of"], 1)
        self.assertEqual(nav_counts, {"Q5": 1, "Q645883": 1})

    def test_build_unknown_hint_class_report_uses_local_fallback_labels(self):
        # Verify common class QIDs still render readable labels when online resolution is unavailable.
        rows = [
            {
                "instance_of": ["Q13406463", "Q2001676"],
                "source": {"type": "wikipedia_categories", "hint": "unknown"},
            }
        ]

        report = visualization.build_unknown_hint_class_report(
            rows,
            top_n=2,
            label_lang="en",
            label_resolver=lambda qids, lang: {},
        )

        self.assertEqual(
            report["overall"]["top_classes"],
            [
                {
                    "qid": "Q13406463",
                    "label": "Wikimedia list article",
                    "display": "Wikimedia list article (Q13406463)",
                    "count": 1,
                },
                {
                    "qid": "Q2001676",
                    "label": "offensive",
                    "display": "offensive (Q2001676)",
                    "count": 1,
                },
            ],
        )

    def test_format_unknown_class_note_lists_top_classes_and_missing_instance_count(self):
        # Verify the heatmap side-note renders the top unknown classes in a compact readable format.
        note = visualization.format_unknown_class_note(
            {
                "unknown_entity_count": 7,
                "entities_with_no_instance_of": 2,
                "top_classes": [
                    {"display": "human (Q5)", "count": 4},
                    {"display": "battle (Q645883)", "count": 2},
                ],
            },
            max_items=2,
        )

        self.assertIn("Unknown hint classes (n=7)", note)
        self.assertIn("1. human (Q5) (4)", note)
        self.assertIn("2. battle (Q645883) (2)", note)
        self.assertIn("No instance_of: 2", note)


if __name__ == "__main__":
    unittest.main()
