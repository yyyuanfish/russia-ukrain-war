import pathlib
import sys
import unittest


PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import pipeline_common


class NormalizeRecordTests(unittest.TestCase):
    def test_normalize_record_extracts_qid_and_normalizes_fields(self):
        # Verify a noisy harvested record is normalized into the shared schema.
        record = {
            "uri": "http://www.wikidata.org/entity/Q42",
            "source": {"collection_paths": ["seed", "seed", " "]},
            "labels": {"en": "Douglas Adams", "ru": ""},
            "descriptions": {"en": "Writer"},
            "aliases": {"en": ["DNA", "DNA", "  ", 7], "ru": ["Дуглас Адамс"]},
            "sitelinks": {"enwiki": "https://en.wikipedia.org/wiki/Douglas_Adams"},
            "wiki_titles": {"en": "Douglas Adams"},
            "instance_of": ["Q5", "http://www.wikidata.org/entity/Q5", "bad"],
            "raw_attrib_qids": {
                "P17": ["Q145", "http://www.wikidata.org/entity/Q145", "invalid"],
                "P27": ["Q145", "Q42"],
            },
        }

        normalized = pipeline_common.normalize_record(
            record,
            source_type="wikidata_sparql",
            source_hint="person",
            collection_paths=["derived", "seed"],
            lang_keys=["EN", "ru", "en"],
            site_keys=["enwiki", "ruwiki"],
            attrib_prop_ids=["p17", "bad", "P27"],
        )

        # The normalized output should salvage the QID and deduplicate lists.
        self.assertEqual(normalized["qid"], "Q42")
        self.assertEqual(normalized["source"]["type"], "wikidata_sparql")
        self.assertEqual(normalized["source"]["hint"], "person")
        self.assertEqual(normalized["source"]["collection_paths"], ["derived", "seed"])
        self.assertEqual(normalized["labels"], {"en": "Douglas Adams", "ru": None})
        self.assertEqual(normalized["aliases"]["en"], ["DNA"])
        self.assertEqual(normalized["instance_of"], ["Q5"])
        self.assertEqual(normalized["raw_attrib_qids"]["P17"], ["Q145"])
        self.assertEqual(normalized["raw_attrib_qids"]["P27"], ["Q145", "Q42"])

    def test_merge_records_by_qid_combines_text_aliases_sources_and_props(self):
        # Verify duplicate QIDs from different harvesters merge into one record.
        records = [
            {
                "qid": "Q1",
                "source": {"type": "wikidata_sparql", "collection_paths": ["wd"]},
                "labels": {"en": "Alpha"},
                "aliases": {"en": ["A1"]},
                "raw_attrib_qids": {"P17": ["Q10"]},
            },
            {
                "qid": "Q1",
                "source": {"type": "wikipedia_navboxes", "collection_paths": ["nav"]},
                "labels": {"ru": "Альфа"},
                "descriptions": {"en": "Entity from navbox"},
                "aliases": {"en": ["A1", "A2"]},
                "instance_of": ["Q5"],
                "raw_attrib_qids": {"P17": ["Q20"], "P27": ["Q30"]},
            },
            {
                "qid": "Q2",
                "labels": {"en": "Beta"},
            },
        ]

        merged = pipeline_common.merge_records_by_qid(
            records,
            lang_keys=["en", "ru"],
            site_keys=["enwiki", "ruwiki"],
            attrib_prop_ids=["P17", "P27"],
        )

        self.assertEqual([row["qid"] for row in merged], ["Q1", "Q2"])

        # The merged record should keep the best text fields plus all source evidence.
        first = merged[0]
        self.assertEqual(first["labels"]["en"], "Alpha")
        self.assertEqual(first["labels"]["ru"], "Альфа")
        self.assertEqual(first["descriptions"]["en"], "Entity from navbox")
        self.assertEqual(first["aliases"]["en"], ["A1", "A2"])
        self.assertEqual(first["instance_of"], ["Q5"])
        self.assertEqual(first["raw_attrib_qids"]["P17"], ["Q10", "Q20"])
        self.assertEqual(first["raw_attrib_qids"]["P27"], ["Q30"])
        self.assertEqual(first["source"]["collection_paths"], ["nav", "wd"])
        self.assertEqual(len(first["_sources"]), 2)


class EnrichmentHelpersTests(unittest.TestCase):
    def test_build_item_enrichment_query_includes_requested_langs_and_props(self):
        # Verify the SPARQL builder includes exactly the requested languages and props.
        query = pipeline_common.build_item_enrichment_query(
            ["Q1", "Q2"],
            ["en", "ru"],
            attrib_prop_ids=["P17", "P495"],
        )

        self.assertIn('VALUES ?item { wd:Q1 wd:Q2 }', query)
        self.assertIn('FILTER(LANG(?label_en) = "en")', query)
        self.assertIn('FILTER(LANG(?label_ru) = "ru")', query)
        self.assertIn('GROUP_CONCAT(DISTINCT STR(?P17val); separator="|") AS ?P17_vals', query)
        self.assertIn('GROUP_CONCAT(DISTINCT STR(?P495val); separator="|") AS ?P495_vals', query)

    def test_binding_to_enriched_record_maps_fields_from_binding(self):
        # Verify one SPARQL binding row is converted into the normalized entity shape.
        binding = {
            "item": {"value": "http://www.wikidata.org/entity/Q42"},
            "label_en": {"value": "Douglas Adams"},
            "desc_en": {"value": "English writer"},
            "enwiki": {"value": "https://en.wikipedia.org/wiki/Douglas_Adams"},
            "en_title": {"value": "Douglas Adams"},
            "insts": {"value": "http://www.wikidata.org/entity/Q5|Q36180"},
            "P17_vals": {"value": "http://www.wikidata.org/entity/Q145|Q145"},
        }

        record = pipeline_common.binding_to_enriched_record(
            binding,
            ["en"],
            attrib_prop_ids=["P17"],
        )

        # The output record should expose clean labels, types, sitelinks, and QID lists.
        self.assertEqual(record["qid"], "Q42")
        self.assertEqual(record["labels"]["en"], "Douglas Adams")
        self.assertEqual(record["descriptions"]["en"], "English writer")
        self.assertEqual(record["sitelinks"]["enwiki"], "https://en.wikipedia.org/wiki/Douglas_Adams")
        self.assertEqual(record["wiki_titles"]["en"], "Douglas Adams")
        self.assertEqual(record["instance_of"], ["Q36180", "Q5"])
        self.assertEqual(record["raw_attrib_qids"]["P17"], ["Q145"])


if __name__ == "__main__":
    unittest.main()
