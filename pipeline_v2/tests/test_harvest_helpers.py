import pathlib
import sys
import unittest
from unittest import mock


PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import harvest_categories
import harvest_wikidata


class HarvestHelperTests(unittest.TestCase):
    def test_resolve_alias_config_normalizes_limits(self):
        # Verify alias settings coerce to non-negative integers and preserve enable/disable.
        cfg = {
            "aliases": {
                "enabled": False,
                "max_total_per_qid": "7",
                "max_per_lang": "-1",
            }
        }

        resolved = harvest_wikidata._resolve_alias_config(cfg)

        self.assertEqual(
            resolved,
            {
                "enabled": False,
                "max_total_per_qid": 7,
                "max_per_lang": 0,
            },
        )

    def test_resolve_seed_qids_merges_explicit_and_navbox_seed_qids(self):
        # Verify explicit seeds are preserved and navbox-derived seeds are appended without duplication.
        cfg = {
            "wikidata": {
                "seed_qids": ["Q1", "Q2", "Q1"],
                "seed_from_navbox_page": True,
            },
            "navbox_seed_url": "https://en.wikipedia.org/wiki/Russo-Ukrainian_War",
        }

        with mock.patch(
            "harvest_wikidata.wiki_common.wikipedia_titles_to_qids",
            return_value={"Russo-Ukrainian_War": "Q9"},
        ):
            seeds = harvest_wikidata._resolve_seed_qids(cfg, sleep=0.0)

        self.assertEqual(seeds, ["Q1", "Q2", "Q9"])

    def test_resolve_bucket_queries_prefers_custom_blocks_and_substitutes_seed_values(self):
        # Verify custom bucket queries override defaults and expand the seed placeholder.
        seed_qids = ["Q10", "Q20"]
        wcfg = {
            "bucket_queries": {
                "custom_bucket": "VALUES ?seed { {seed_values} } ?item wdt:P31 wd:Q5 ."
            }
        }

        queries = harvest_wikidata._resolve_bucket_queries(
            seed_qids,
            wcfg=wcfg,
            type_anchors=harvest_wikidata.DEFAULT_TYPE_ANCHORS,
            rel_props=harvest_wikidata.DEFAULT_RELATION_PROPERTIES,
        )

        self.assertEqual(list(queries.keys()), ["custom_bucket"])
        self.assertIn("wd:Q10 wd:Q20", queries["custom_bucket"])

    def test_category_helper_functions_normalize_titles_and_keywords(self):
        # Verify category helper functions construct titles and derive stable keyword lists.
        self.assertEqual(harvest_categories._category_title("Russo-Ukrainian war"), "Category:Russo-Ukrainian war")
        self.assertEqual(harvest_categories._category_title("Category:Russo-Ukrainian war"), "Category:Russo-Ukrainian war")
        self.assertEqual(
            harvest_categories._auto_keywords(["Category:Russo-Ukrainian war", "Category:2022 invasion"]),
            ["Russo", "Ukrainian", "2022", "invasion"],
        )
        self.assertEqual(harvest_categories._keyword_list([" war ", "", "war", "Ukraine"]), ["war", "Ukraine"])


if __name__ == "__main__":
    unittest.main()
