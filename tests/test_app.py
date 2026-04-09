import csv
import io
import tempfile
import unittest
import zipfile
from pathlib import Path

import app as app_module


def write_csv(rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


class ReqAppTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        app_module.DATA_DIR = tmp_path
        app_module.DATA_ZIP_PATH = tmp_path / "req-dataset.zip"
        app_module.DB_PATH = tmp_path / "req_cache.sqlite3"
        app_module.sync_state.update(
            {
                "is_running": False,
                "last_started_at": None,
                "last_completed_at": None,
                "last_success_at": None,
                "last_error": None,
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
                    ]
                ),
            )
        app_module.ingest_tables(app_module.read_archive_tables(app_module.DATA_ZIP_PATH))
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
        self.assertEqual(len(payload["links"]), 2)

    def test_summary_has_counts(self):
        response = self.client.get("/api/summary")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["companies"], 3)
        self.assertEqual(payload["people"], 0)
        self.assertEqual(payload["relations"], 2)


if __name__ == "__main__":
    unittest.main()
