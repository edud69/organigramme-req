import csv
import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

os.environ["APP_SKIP_BOOTSTRAP"] = "1"
import app as app_module


def write_csv(rows):
    buffer = io.StringIO()
    fieldnames = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


class ReqAppTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        app_module.DATABASE_URL = ""
        app_module.DATA_DIR = tmp_path
        app_module.DATA_ZIP_PATH = tmp_path / "req-dataset.zip"
        app_module.DB_PATH = tmp_path / "req_cache.sqlite3"
        app_module.reset_engine()
        app_module.sync_state.update(
            {
                "is_running": False,
                "phase": "idle",
                "last_started_at": None,
                "last_completed_at": None,
                "last_success_at": None,
                "last_error": None,
                "last_result": None,
            }
        )
        app_module.init_db()
        with zipfile.ZipFile(app_module.DATA_ZIP_PATH, "w") as archive:
            archive.writestr(
                "Nom.csv",
                write_csv(
                    [
                        {"NEQ": "111", "NOM_ASSUJ": "Alpha Inc."},
                        {"NEQ": "222", "NOM_ASSUJ": "Beta LLC"},
                        {"NEQ": "333", "NOM_ASSUJ": "Gamma SA"},
                    ]
                ),
            )
            archive.writestr(
                "FusionScission.csv",
                write_csv(
                    [
                        {"NEQ_ASSUJ_REL": "111", "NEQ": "222", "COD_RELA_ASSUJ": "FUS"},
                        {"NEQ_ASSUJ_REL": "111", "NEQ": "333", "COD_RELA_ASSUJ": "SCI"},
                        {"NEQ_ASSUJ_REL": "", "NEQ": "333", "DENOMN_SOC": "Holding Delta inc.", "COD_RELA_ASSUJ": "FO"},
                        {"NEQ_ASSUJ_REL": "", "NEQ": "222", "DENOMN_SOC": "Alpha Inc.", "COD_RELA_ASSUJ": "FO"},
                    ]
                ),
            )
        app_module.ingest_archive(app_module.DATA_ZIP_PATH)
        self.client = app_module.app.test_client()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_search_returns_company(self):
        response = self.client.get("/api/search?q=Alpha")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload[0]["label"], "Alpha Inc.")
        self.assertEqual(payload[0]["neq"], "111")

    def test_network_returns_relations(self):
        response = self.client.get("/api/network?neq=111")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload["nodes"]), 3)
        self.assertEqual(len(payload["links"]), 3)

    def test_network_includes_named_company_relation_without_related_neq(self):
        response = self.client.get("/api/network?neq=333")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        labels = {node["label"] for node in payload["nodes"]}
        self.assertIn("Holding Delta inc.", labels)
        self.assertGreaterEqual(len(payload["links"]), 2)

    def test_named_relation_reuses_existing_company_when_alias_matches(self):
        response = self.client.get("/api/network?neq=222")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        labels = [node["label"] for node in payload["nodes"]]
        self.assertEqual(labels.count("Alpha Inc."), 1)
        self.assertFalse(any(node["id"].startswith("company-name:") and node["label"] == "Alpha Inc." for node in payload["nodes"]))

    def test_summary_has_counts(self):
        response = self.client.get("/api/summary")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["companies"], 4)
        self.assertEqual(payload["people"], 0)
        self.assertEqual(payload["relations"], 4)

    def test_parse_public_registry_relations_supports_people_and_companies(self):
        relations = app_module.parse_public_registry_relations(
            [
                {
                    "heading": "Administrateurs",
                    "headers": ["Nom", "Fonction"],
                    "rows": [["Jane Doe", "Présidente"]],
                },
                {
                    "heading": "Actionnaires",
                    "headers": ["Nom", "NEQ", "Type"],
                    "rows": [["GESTION ALPHA INC.", "1170000001", "Personne morale"]],
                },
            ]
        )
        self.assertEqual(len(relations), 2)
        self.assertEqual(relations[0]["target_type"], app_module.PERSON_NODE)
        self.assertEqual(relations[0]["relation_label"], "Présidente")
        self.assertEqual(relations[1]["target_type"], app_module.COMPANY_NODE)
        self.assertEqual(relations[1]["target_neq"], "1170000001")


if __name__ == "__main__":
    unittest.main()
