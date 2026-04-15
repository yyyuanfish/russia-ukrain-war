import pathlib
import sys
import tempfile
import unittest
from unittest import mock


PIPELINE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import run_pipeline


class PipelineResolutionTests(unittest.TestCase):
    def test_resolve_entity_files_applies_visualization_overrides(self):
        # Verify entity file names come from visualization.entity_files when configured.
        cfg = {
            "visualization": {
                "entity_files": {
                    "wikidata": "wd_custom.jsonl",
                    "categories": "cat_custom.jsonl",
                }
            }
        }

        resolved = run_pipeline._resolve_entity_files(cfg)

        self.assertEqual(
            resolved,
            {
                "wikidata": "wd_custom.jsonl",
                "navboxes": "navboxes_entities.jsonl",
                "categories": "cat_custom.jsonl",
            },
        )

    def test_resolve_pipeline_resolves_relative_paths_and_run_flags(self):
        # Verify relative pipeline paths are anchored at the config location and run flags are respected.
        cfg = {
            "visualization": {
                "entity_files": {
                    "navboxes": "nav_custom.jsonl",
                }
            },
            "pipeline": {
                "run": {
                    "wikidata": False,
                    "navboxes": True,
                    "categories": False,
                    "attribution": True,
                    "visualization": True,
                },
                "paths": {
                    "entities_folder": "outputs/entities",
                    "classified_output": "outputs/classified.jsonl",
                    "attribution_jsonl_output": "outputs/audit.jsonl",
                    "classified_report": "outputs/classified_report.json",
                    "visualization_outdir": "outputs/figures",
                    "visualization_report": "/tmp/custom_report.json",
                },
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = pathlib.Path(tmpdir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")

            resolved = run_pipeline._resolve_pipeline(cfg, str(config_path))

        expected_base = pathlib.Path(tmpdir) / "outputs"
        self.assertEqual(
            resolved["run"],
            {
                "wikidata": False,
                "navboxes": True,
                "categories": False,
                "attribution": True,
                "visualization": True,
            },
        )
        self.assertEqual(str(expected_base / "entities"), resolved["paths"]["entities_folder"])
        self.assertEqual(str(expected_base / "entities" / "wikidata_entities.jsonl"), resolved["paths"]["wikidata_output"])
        self.assertEqual(str(expected_base / "entities" / "nav_custom.jsonl"), resolved["paths"]["navboxes_output"])
        self.assertEqual(str(expected_base / "entities" / "categories_entities.jsonl"), resolved["paths"]["categories_output"])
        self.assertEqual("custom_report.json", resolved["paths"]["visualization_report"])

    def test_run_step_invokes_subprocess_with_cwd_and_check(self):
        # Verify step execution forwards cwd and always runs subprocesses in checked mode.
        with mock.patch("run_pipeline.subprocess.run") as run_mock:
            run_pipeline._run_step("Step X", ["python", "script.py"], cwd="/tmp/pipeline")

        run_mock.assert_called_once_with(["python", "script.py"], cwd="/tmp/pipeline", check=True)


if __name__ == "__main__":
    unittest.main()
