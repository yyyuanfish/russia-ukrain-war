import json
import pathlib
import sys
import unittest


PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import attribution
import pipeline_common
import run_pipeline


CONFIG_PATH = PIPELINE_DIR / "config.json"


class ConfigValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    def test_config_declares_required_top_level_sections(self):
        # Verify the researcher-authored config exposes the sections the pipeline expects.
        for key in ("conflicting_parties", "languages", "navbox_names", "category_names", "pipeline"):
            with self.subTest(key=key):
                self.assertIn(key, self.cfg)

    def test_config_languages_and_party_sets_are_well_formed(self):
        # Verify party IDs and language configuration can be parsed by the shared helpers.
        langs = pipeline_common.config_languages(self.cfg)
        party1_ids, party2_ids = pipeline_common.party_sets(self.cfg)

        self.assertEqual(langs, ["en", "ru", "uk"])
        self.assertIn("Q159", party1_ids)
        self.assertIn("Q212", party2_ids)
        self.assertTrue(set(langs) >= {"en", "ru", "uk"})

    def test_config_pattern_sections_compile_without_invalid_regexes(self):
        # Verify text-pattern sections in config compile into regex maps cleanly.
        ccfg = self.cfg.get("classification") if isinstance(self.cfg.get("classification"), dict) else {}
        for key in ("party1_patterns", "party2_patterns", "other_patterns"):
            with self.subTest(section=key):
                compiled = attribution._compile_pat_map(ccfg.get(key) if isinstance(ccfg.get(key), dict) else {})
                self.assertIsInstance(compiled, dict)

    def test_config_pipeline_paths_resolve_to_expected_locations(self):
        # Verify the real config resolves to the current pipeline data layout.
        resolved = run_pipeline._resolve_pipeline(self.cfg, str(CONFIG_PATH))

        self.assertTrue(resolved["paths"]["entities_folder"].endswith("pipeline_v2/data/entities"))
        self.assertTrue(resolved["paths"]["classified_output"].endswith("pipeline_v2/data/classified_entities.jsonl"))
        self.assertTrue(resolved["paths"]["classified_report"].endswith("pipeline_v2/data/classified_report.json"))
        self.assertTrue(resolved["paths"]["visualization_outdir"].endswith("pipeline_v2/data/visualization"))


if __name__ == "__main__":
    unittest.main()
