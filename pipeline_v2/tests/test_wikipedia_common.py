import pathlib
import sys
import unittest
from unittest import mock

from bs4 import BeautifulSoup


PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import wikipedia_common as wiki_common


class WikipediaCommonTests(unittest.TestCase):
    def test_instance_hint_map_from_config_uses_valid_custom_qids_and_fallbacks(self):
        # Verify hint mapping keeps valid custom QIDs while ignoring malformed entries.
        cfg = {
            "harvest_hints": {
                "instance_of_map": {
                    "person": ["Q5", "bad"],
                    "policy": ["Q820655"],
                    "empty": [],
                }
            }
        }

        hint_map = wiki_common.instance_hint_map_from_config(cfg)

        self.assertEqual(hint_map["person"], {"Q5"})
        self.assertEqual(hint_map["policy"], {"Q820655"})
        self.assertNotIn("empty", hint_map)

    def test_infer_source_lang_and_title_from_url_parses_wikipedia_urls(self):
        # Verify seed URLs are split into their source language and page title.
        lang, title = wiki_common.infer_source_lang_and_title_from_url(
            "https://uk.wikipedia.org/wiki/%D0%91%D0%BE%D1%97_%D0%B7%D0%B0_%D0%9A%D0%B8%D1%97%D0%B2_(2022)"
        )

        self.assertEqual(lang, "uk")
        self.assertEqual(title, "Бої_за_Київ_(2022)")

    def test_clean_title_from_href_path_rejects_non_mainspace_titles(self):
        # Verify title cleanup only keeps mainspace article links.
        self.assertEqual(wiki_common.clean_title_from_href_path("/wiki/Battle_of_Kyiv"), "Battle_of_Kyiv")
        self.assertIsNone(wiki_common.clean_title_from_href_path("/wiki/Category:Russo-Ukrainian_war"))
        self.assertIsNone(wiki_common.clean_title_from_href_path("/w/index.php?title=Battle_of_Kyiv"))

    def test_select_one_navbox_prefers_title_match_and_extracts_article_titles(self):
        # Verify navbox selection prefers title text and only returns internal article links.
        soup = BeautifulSoup(
            """
            <html><body>
              <table class="navbox">
                <tr><th class="navbox-title">Other topic</th></tr>
                <tr><td><a href="/wiki/Not_Selected">Not Selected</a></td></tr>
              </table>
              <table class="navbox">
                <tr><th class="navbox-title">Russo-Ukrainian war</th></tr>
                <tr><td>
                  <a href="/wiki/Battle_of_Kyiv">Battle of Kyiv</a>
                  <a href="/wiki/Category:Russo-Ukrainian_war">Category page</a>
                  <a href="https://example.com/outside">Outside</a>
                </td></tr>
              </table>
            </body></html>
            """,
            "html.parser",
        )

        links = wiki_common.extract_links_from_one_navbox(
            soup=soup,
            base_url="https://en.wikipedia.org",
            navbox_title="Russo-Ukrainian war",
            navbox_index=0,
        )
        titles = wiki_common.titles_from_urls(links)

        self.assertEqual(titles, ["Battle_of_Kyiv"])

    def test_walk_categories_collect_titles_respects_keywords_and_limits(self):
        # Verify category traversal filters subcategories by keyword and reports stop conditions.
        members = {
            "Category:Root": [
                {"ns": 0, "title": "Alpha"},
                {"ns": 14, "title": "Category:Conflict events"},
                {"ns": 14, "title": "Category:Ignore me"},
            ],
            "Category:Conflict events": [
                {"ns": 0, "title": "Beta"},
            ],
            "Category:Ignore me": [
                {"ns": 0, "title": "Gamma"},
            ],
        }

        with mock.patch(
            "wikipedia_common.fetch_category_members",
            side_effect=lambda title, **kwargs: members.get(title, []),
        ):
            walked = wiki_common.walk_categories_collect_titles(
                root_categories={"Category:Root"},
                depth=1,
                strategy="bfs",
                keywords=["Conflict"],
                max_titles=2,
            )

        self.assertEqual(walked["titles"], {"Alpha", "Beta"})
        self.assertEqual(walked["stop_reason"], "max_titles_reached")
        self.assertIn("Category:Root", walked["visited_categories"])
        self.assertIn("Category:Conflict events", walked["visited_categories"])
        self.assertNotIn("Category:Ignore me", walked["visited_categories"])


if __name__ == "__main__":
    unittest.main()
