import json
import pathlib
import re
import sys
import unittest

# Add pipeline_v2 to sys.path so the local attribution module can be imported in tests.
PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import attribution

CONFIG_PATH = PIPELINE_DIR / "config.json"
CLASSIFIED_FIXTURE_PATH = PIPELINE_DIR / "data" / "classified_entities.jsonl"

# These examples come from manually curated researcher judgments.
# The "supported" set already matches current classifier behavior.
# The "known gap" set documents desired labels that the current rules still miss.
SUPPORTED_REFERENCE_EXAMPLES = {
    "Q105038767": "party1",
    "Q100605773": "party2",
    "Q112898425": "party1",
    "Q113687154": "party1",
    "Q111014380": "mixed",
    "Q111014904": "mixed",
    "Q111015252": "mixed",
    "Q111015860": "party2",
    "Q114738445": "party2",
    "Q108306133": "mixed",
    "Q110254389": "mixed",
    "Q111011730": "mixed",
    "Q111195201": "other",
    "Q111362750": "other",
    "Q125208615": "mixed",
    "Q128686808": "mixed",
    "Q114344966": "mixed",
    "Q114565965": "mixed",
    "Q114825840": "mixed",
    "Q111104576": "party2",
    "Q111134609": "mixed",
    "Q111148178": "mixed",
}

KNOWN_GAP_REFERENCE_EXAMPLES = {
    "Q107356545": "party1",
    "Q111103866": "party1",
    "Q111449823": "party1",
    "Q111699282": "other",
    "Q113182796": "party1",
    "Q114436433": "party2",
    "Q116838551": "party2",
    "Q131399616": "party1",
    "Q16389280": "party1",
    "Q19356545": "party2",
    "Q19887803": "mixed",
}

ALL_REFERENCE_EXAMPLES = {
    **SUPPORTED_REFERENCE_EXAMPLES,
    **KNOWN_GAP_REFERENCE_EXAMPLES,
}


class PatternCompilationTests(unittest.TestCase):
    def test_compile_pat_map_normalizes_lang_keys_and_skips_invalid_items(self):
        # Verify config regexes are lowercased by language and invalid entries are ignored.
        compiled = attribution._compile_pat_map(
            {
                " EN ": ["russia", "", None],
                "ru": "not-a-list",
                5: ["ignored"],
            }
        )

        self.assertEqual(sorted(compiled.keys()), ["en"])
        self.assertEqual(len(compiled["en"]), 1)
        self.assertTrue(compiled["en"][0].search("RUSSIA"))


class ScoringTests(unittest.TestCase):
    def test_text_score_counts_label_description_and_alias_hits(self):
        # Verify text evidence is counted across labels, descriptions, and aliases.
        entity = {
            "labels": {"en": "Russia-backed entity"},
            "descriptions": {"en": "Operating in Ukraine"},
            "aliases": {"en": ["Poland corridor"]},
        }
        party1 = {"en": [re.compile("russia", re.IGNORECASE)]}
        party2 = {"en": [re.compile("ukraine", re.IGNORECASE)]}
        other = {"en": [re.compile("poland", re.IGNORECASE)]}

        s1, s2, so, hits = attribution.text_score(entity, ["en"], party1, party2, other)

        self.assertEqual((s1, s2, so), (1, 1, 1))
        self.assertEqual(len(hits), 3)
        self.assertIn("text:en:party1:russia", hits)
        self.assertIn("text:en:party2:ukraine", hits)
        self.assertIn("text:en:other:poland", hits)

    def test_structured_score_uses_direct_and_place_country_evidence(self):
        # Verify structured evidence uses both direct country props and place->country lookup.
        entity = {
            "raw_attrib_qids": {
                "P17": ["Q_RU", "Q_PL"],
                "P276": ["Q_KYIV", "Q_WARSAW"],
            }
        }

        s1, s2, so, hits = attribution.structured_score(
            entity,
            party1_ids={"Q_RU"},
            party2_ids={"Q_UA"},
            other_hints={"Q_PL"},
            place_country_map={"Q_KYIV": {"Q_UA"}, "Q_WARSAW": {"Q_PL"}},
            attrib_prop_ids=["P17", "P495"],
            place_props=["P276"],
        )

        self.assertEqual((s1, s2, so), (4, 2, 6))
        self.assertIn("P17:direct:party1:Q_RU", hits)
        self.assertIn("P17:direct:other:Q_PL", hits)
        self.assertIn("P276:place_country:party2:Q_KYIV->Q_UA", hits)
        self.assertIn("P276:place_country:other:Q_WARSAW->Q_PL", hits)

    def test_guess_other_country_prefers_best_non_party_candidate(self):
        # Verify "other country" inference picks the strongest non-party candidate.
        entity = {
            "raw_attrib_qids": {
                "P17": ["Q_PL"],
                "P276": ["Q_HELSINKI", "Q_WARSAW"],
            }
        }

        qid, score, evidence = attribution.guess_other_country(
            entity,
            party1_ids={"Q_RU"},
            party2_ids={"Q_UA"},
            place_country_map={"Q_HELSINKI": {"Q_FI"}, "Q_WARSAW": {"Q_PL"}},
            text_country_map=[
                (re.compile("poland", re.IGNORECASE), "Q_PL"),
                (re.compile("finland", re.IGNORECASE), "Q_FI"),
            ],
            direct_country_props=["P17"],
            place_props=["P276"],
            text_hits=["text:en:other:poland"],
        )

        self.assertEqual(qid, "Q_PL")
        self.assertEqual(score, 7)
        self.assertEqual(evidence, ["P17:direct", "P276:place_country:Q_WARSAW", "text:other"])


class LabelDecisionTests(unittest.TestCase):
    def test_decide_label_covers_all_supported_outcomes(self):
        # Verify the final label logic covers mixed, party1, party2, and other.
        cases = [
            ((2, 2, 0, 3), "mixed"),
            ((2, 0, 0, 3), "party1"),
            ((0, 2, 0, 3), "party2"),
            ((0, 0, 9, 3), "other"),
        ]

        for args, expected in cases:
            with self.subTest(args=args):
                self.assertEqual(attribution.decide_label(*args), expected)


class ReferenceExampleRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        ccfg = cls.cfg.get("classification") if isinstance(cls.cfg.get("classification"), dict) else {}

        cls.party1_ids, cls.party2_ids = attribution.party_sets(cls.cfg)
        cls.attrib_prop_ids = attribution.attribution_prop_ids_from_config(cls.cfg)
        cls.scan_langs = attribution.config_languages(cls.cfg) or ["en"]
        cls.other_threshold = int(ccfg.get("other_threshold", 4))
        cls.place_props = attribution._resolve_place_resolution_config(ccfg)["place_properties"]
        cls.other_hints = {
            qid
            for qid in (ccfg.get("other_country_hints") or [])
            if isinstance(qid, str) and qid.startswith("Q")
        }

        p1_cfg = attribution._compile_pat_map(ccfg.get("party1_patterns") if isinstance(ccfg.get("party1_patterns"), dict) else {})
        p2_cfg = attribution._compile_pat_map(ccfg.get("party2_patterns") if isinstance(ccfg.get("party2_patterns"), dict) else {})
        other_cfg = attribution._compile_pat_map(ccfg.get("other_patterns") if isinstance(ccfg.get("other_patterns"), dict) else {})
        p1_default, p2_default = attribution._default_party_patterns(cls.party1_ids, cls.party2_ids)
        other_default = attribution._default_other_patterns()

        cls.p1_pat = p1_cfg if any(p1_cfg.values()) else p1_default
        cls.p2_pat = p2_cfg if any(p2_cfg.values()) else p2_default
        cls.other_pat = other_cfg if any(other_cfg.values()) else other_default
        cls.fixture_rows = cls._load_reference_fixture_rows()

    @classmethod
    def _load_reference_fixture_rows(cls):
        rows = {}
        with CLASSIFIED_FIXTURE_PATH.open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                qid = row.get("qid")
                if qid in ALL_REFERENCE_EXAMPLES:
                    rows[qid] = row

        missing = sorted(set(ALL_REFERENCE_EXAMPLES) - set(rows))
        if missing:
            raise AssertionError(f"missing reference fixture rows for qids: {missing}")

        return rows

    def _build_place_country_map(self, row):
        place_country_map = {}
        hits = (row.get("attribution_detail") or {}).get("hits") or []
        for hit in hits:
            match = re.match(r"^P\d+:place_country:(?:party1|party2|other):(Q\d+)->(Q\d+)$", hit)
            if not match:
                continue
            place_qid, country_qid = match.groups()
            place_country_map.setdefault(place_qid, set()).add(country_qid)
        return place_country_map

    def _minimal_entity_input(self, row):
        return {
            "labels": row.get("labels") or {},
            "descriptions": row.get("descriptions") or {},
            "aliases": row.get("aliases") or {},
            "raw_attrib_qids": row.get("raw_attrib_qids") or {},
        }

    def _classify_reference_row(self, qid):
        row = self.fixture_rows[qid]
        entity = self._minimal_entity_input(row)
        place_country_map = self._build_place_country_map(row)

        s1, s2, so, _ = attribution.structured_score(
            entity,
            party1_ids=self.party1_ids,
            party2_ids=self.party2_ids,
            other_hints=self.other_hints,
            place_country_map=place_country_map,
            attrib_prop_ids=self.attrib_prop_ids,
            place_props=self.place_props,
        )
        t1, t2, to, _ = attribution.text_score(
            entity,
            self.scan_langs,
            self.p1_pat,
            self.p2_pat,
            self.other_pat,
        )
        return attribution.decide_label(s1 + t1, s2 + t2, so + to, self.other_threshold)

    def test_supported_reference_examples_match_current_classifier(self):
        for qid, expected in SUPPORTED_REFERENCE_EXAMPLES.items():
            with self.subTest(qid=qid):
                self.assertEqual(self._classify_reference_row(qid), expected)

    @unittest.expectedFailure
    def test_known_gap_reference_examples_document_current_misclassifications(self):
        mismatches = []
        for qid, expected in KNOWN_GAP_REFERENCE_EXAMPLES.items():
            actual = self._classify_reference_row(qid)
            if actual != expected:
                mismatches.append((qid, expected, actual))

        self.assertEqual(mismatches, [])


if __name__ == "__main__":
    unittest.main()
