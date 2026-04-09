import csv
import datetime as dt
import hashlib
import io
import json
import os
import sqlite3
import threading
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_ZIP_PATH = DATA_DIR / "req-dataset.zip"
DB_PATH = DATA_DIR / "req_cache.sqlite3"

CKAN_PACKAGE_URL = os.environ.get(
    "REQ_CKAN_PACKAGE_URL",
    "https://www.donneesquebec.ca/recherche/api/3/action/package_show"
    "?id=6f710997-b5f9-4347-893b-1a47ddb61437",
)
REQ_DATASET_ZIP_URL = os.environ.get("REQ_DATASET_ZIP_URL", "").strip()
UPDATE_INTERVAL_SECONDS = int(os.environ.get("UPDATE_INTERVAL_SECONDS", str(24 * 3600)))
AUTO_SYNC_ENABLED = os.environ.get("AUTO_SYNC_ENABLED", "1") == "1"
MAX_SEARCH_RESULTS = int(os.environ.get("MAX_SEARCH_RESULTS", "20"))
MAX_GRAPH_EDGES = int(os.environ.get("MAX_GRAPH_EDGES", "250"))
SYNC_TIMEOUT_SECONDS = int(os.environ.get("SYNC_TIMEOUT_SECONDS", "120"))
ADMIN_SYNC_TOKEN = os.environ.get("ADMIN_SYNC_TOKEN", "")

COMPANY_NODE = "company"
PERSON_NODE = "person"

app = Flask(__name__)
data_lock = threading.Lock()
sync_state = {
    "is_running": False,
    "last_started_at": None,
    "last_completed_at": None,
    "last_success_at": None,
    "last_error": None,
}
sync_thread: Optional[threading.Thread] = None


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def local_now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat()


def open_db() -> sqlite3.Connection:
    ensure_data_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with open_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS entities (
                node_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                label TEXT NOT NULL,
                normalized_label TEXT NOT NULL,
                neq TEXT,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS entity_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                alias TEXT NOT NULL,
                normalized_alias TEXT NOT NULL,
                source TEXT NOT NULL,
                UNIQUE(node_id, normalized_alias)
            );

            CREATE INDEX IF NOT EXISTS idx_entity_aliases_normalized
            ON entity_aliases(normalized_alias);

            CREATE TABLE IF NOT EXISTS relations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_node_id TEXT NOT NULL,
                target_node_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                relation_label TEXT NOT NULL,
                source_dataset TEXT NOT NULL,
                source_detail TEXT,
                updated_at TEXT NOT NULL,
                UNIQUE(source_node_id, target_node_id, relation_type, source_dataset, source_detail)
            );

            CREATE INDEX IF NOT EXISTS idx_relations_source
            ON relations(source_node_id);

            CREATE INDEX IF NOT EXISTS idx_relations_target
            ON relations(target_node_id);
            """
        )


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def normalize_node_id(entity_type: str, raw_value: str) -> str:
    if entity_type == COMPANY_NODE and raw_value:
        return f"company:{raw_value.strip()}"
    digest = hashlib.sha1(normalize_text(raw_value).encode("utf-8")).hexdigest()[:16]
    return f"{entity_type}:{digest}"


def fetch_json(url: str, timeout: int = 30) -> Optional[dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def fetch_metadata() -> Optional[dict]:
    payload = fetch_json(CKAN_PACKAGE_URL)
    if payload and payload.get("success"):
        return payload.get("result")
    return None


def find_zip_resource(metadata: dict) -> Optional[dict]:
    for res in metadata.get("resources", []):
        url = (res.get("url") or "").lower()
        name = (res.get("name") or "").lower()
        format_name = (res.get("format") or "").lower()
        if ".zip" in url or ".zip" in name or format_name == "zip":
            return res
    return None


def parse_remote_date(date_str: str) -> Optional[dt.datetime]:
    if not date_str:
        return None
    try:
        parsed = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except ValueError:
        return None


def download_file(url: str, dest: Path) -> bool:
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    try:
        with urllib.request.urlopen(url, timeout=SYNC_TIMEOUT_SECONDS) as response, tmp_path.open("wb") as out:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                out.write(chunk)
        os.replace(tmp_path, dest)
        return True
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False


def read_archive_tables(zip_path: Path) -> Dict[str, List[Dict[str, str]]]:
    tables: Dict[str, List[Dict[str, str]]] = {}
    if not zip_path.exists():
        return tables

    with zipfile.ZipFile(zip_path, "r") as archive:
        for filename in archive.namelist():
            if not filename.lower().endswith(".csv"):
                continue
            with archive.open(filename) as handle:
                raw = handle.read()
            text = None
            for encoding in ("utf-8", "latin-1"):
                try:
                    text = raw.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            if text is None:
                continue
            reader = csv.DictReader(io.StringIO(text))
            basename = Path(filename).name.lower()
            tables[basename] = [{(key or "").strip(): (value or "").strip() for key, value in row.items()} for row in reader]
    return tables


def choose_company_name(row: Dict[str, str]) -> Optional[str]:
    for key in (
        "NOM_ASSUJ",
        "DENOMN_SOC",
        "NOM_ASSUJ_LANG_ETRNG",
        "NOM_ASSUJ_ETRNG",
        "NOM",
    ):
        value = row.get(key, "").strip()
        if value:
            return value
    return None


def dataset_label_from_filename(filename: str) -> str:
    label = Path(filename).stem.replace("_", " ").replace("-", " ").strip()
    return label or filename


def upsert_company(
    conn: sqlite3.Connection,
    neq: str,
    label: str,
    source: str,
    aliases: Optional[Iterable[str]] = None,
) -> str:
    node_id = normalize_node_id(COMPANY_NODE, neq)
    normalized_label = normalize_text(label or neq)
    updated_at = utc_now_iso()
    conn.execute(
        """
        INSERT INTO entities (node_id, entity_type, label, normalized_label, neq, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            label = excluded.label,
            normalized_label = excluded.normalized_label,
            neq = excluded.neq,
            source = excluded.source,
            updated_at = excluded.updated_at
        """,
        (node_id, COMPANY_NODE, label or neq, normalized_label, neq, source, updated_at),
    )
    alias_values = set(aliases or [])
    alias_values.add(label or neq)
    for alias in alias_values:
        normalized_alias = normalize_text(alias)
        if not normalized_alias:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO entity_aliases (node_id, alias, normalized_alias, source)
            VALUES (?, ?, ?, ?)
            """,
            (node_id, alias, normalized_alias, source),
        )
    return node_id


def upsert_person(conn: sqlite3.Connection, name: str, source: str) -> str:
    node_id = normalize_node_id(PERSON_NODE, name)
    normalized_label = normalize_text(name)
    updated_at = utc_now_iso()
    conn.execute(
        """
        INSERT INTO entities (node_id, entity_type, label, normalized_label, neq, source, updated_at)
        VALUES (?, ?, ?, ?, NULL, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            label = excluded.label,
            normalized_label = excluded.normalized_label,
            source = excluded.source,
            updated_at = excluded.updated_at
        """,
        (node_id, PERSON_NODE, name, normalized_label, source, updated_at),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO entity_aliases (node_id, alias, normalized_alias, source)
        VALUES (?, ?, ?, ?)
        """,
        (node_id, name, normalized_label, source),
    )
    return node_id


def insert_relation(
    conn: sqlite3.Connection,
    source_node_id: str,
    target_node_id: str,
    relation_type: str,
    relation_label: str,
    source_dataset: str,
    source_detail: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO relations
        (source_node_id, target_node_id, relation_type, relation_label, source_dataset, source_detail, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_node_id,
            target_node_id,
            relation_type,
            relation_label,
            source_dataset,
            source_detail,
            utc_now_iso(),
        ),
    )


def ingest_tables(tables: Dict[str, List[Dict[str, str]]]) -> Dict[str, int]:
    stats = {
        "tables": len(tables),
        "companies": 0,
        "people": 0,
        "relations": 0,
    }
    with open_db() as conn:
        conn.execute("DELETE FROM relations")
        conn.execute("DELETE FROM entity_aliases")
        conn.execute("DELETE FROM entities")

        company_names: Dict[str, set] = {}
        for filename, rows in tables.items():
            if "nom" in filename:
                for row in rows:
                    neq = row.get("NEQ", "").strip()
                    if not neq:
                        continue
                    aliases = {
                        value.strip()
                        for key, value in row.items()
                        if key
                        in {
                            "NOM_ASSUJ",
                            "DENOMN_SOC",
                            "NOM_ASSUJ_LANG_ETRNG",
                            "NOM_ASSUJ_ETRNG",
                        }
                        and value
                    }
                    label = choose_company_name(row) or neq
                    company_names.setdefault(neq, set()).update(aliases or {label})

        for neq, aliases in company_names.items():
            upsert_company(conn, neq, sorted(aliases)[0], "req-open-data", aliases=aliases)

        relation_files = 0
        for filename, rows in tables.items():
            if not rows:
                continue
            sample_keys = {key for key in rows[0].keys() if key}

            if {"NEQ", "NEQ_ASSUJ_REL"}.issubset(sample_keys):
                relation_files += 1
                dataset_label = dataset_label_from_filename(filename)
                for row in rows:
                    src_neq = row.get("NEQ_ASSUJ_REL", "").strip()
                    dst_neq = row.get("NEQ", "").strip()
                    if not src_neq or not dst_neq:
                        continue
                    src_name = company_names.get(src_neq, {src_neq})
                    dst_name = company_names.get(dst_neq, {dst_neq})
                    src_node_id = upsert_company(conn, src_neq, sorted(src_name)[0], "req-open-data", aliases=src_name)
                    dst_node_id = upsert_company(conn, dst_neq, sorted(dst_name)[0], "req-open-data", aliases=dst_name)
                    relation_code = row.get("COD_RELA_ASSUJ", "").strip() or filename
                    insert_relation(
                        conn,
                        src_node_id,
                        dst_node_id,
                        f"company_relation:{relation_code}",
                        dataset_label,
                        filename,
                        relation_code,
                    )

            # This supports future enrichment sources where persons are present.
            if {"NEQ", "NOM_PRENOM"}.issubset(sample_keys):
                dataset_label = dataset_label_from_filename(filename)
                for row in rows:
                    neq = row.get("NEQ", "").strip()
                    full_name = row.get("NOM_PRENOM", "").strip()
                    if not neq or not full_name:
                        continue
                    company_aliases = company_names.get(neq, {neq})
                    company_id = upsert_company(conn, neq, sorted(company_aliases)[0], "req-open-data", aliases=company_aliases)
                    person_id = upsert_person(conn, full_name, "enrichment")
                    role = row.get("ROLE", "").strip() or row.get("FONCTION", "").strip() or "personne liée"
                    insert_relation(
                        conn,
                        company_id,
                        person_id,
                        f"person_role:{normalize_text(role)}",
                        role,
                        filename,
                    )

        stats["companies"] = conn.execute(
            "SELECT COUNT(*) AS total FROM entities WHERE entity_type = ?",
            (COMPANY_NODE,),
        ).fetchone()["total"]
        stats["people"] = conn.execute(
            "SELECT COUNT(*) AS total FROM entities WHERE entity_type = ?",
            (PERSON_NODE,),
        ).fetchone()["total"]
        stats["relations"] = conn.execute("SELECT COUNT(*) AS total FROM relations").fetchone()["total"]
        conn.execute("REPLACE INTO metadata (key, value) VALUES (?, ?)", ("last_ingest_at", utc_now_iso()))
        conn.execute(
            "REPLACE INTO metadata (key, value) VALUES (?, ?)",
            ("last_table_count", str(relation_files)),
        )
    return stats


def sync_dataset(force: bool = False) -> Dict[str, object]:
    with data_lock:
        sync_state["is_running"] = True
        sync_state["last_started_at"] = local_now_iso()
        sync_state["last_error"] = None

        try:
            ensure_data_dir()
            metadata = fetch_metadata()
            zip_resource = find_zip_resource(metadata or {})

            if not zip_resource and REQ_DATASET_ZIP_URL:
                zip_resource = {
                    "url": REQ_DATASET_ZIP_URL,
                    "last_modified": None,
                    "name": "REQ dataset ZIP (manual override)",
                }

            if not zip_resource:
                raise RuntimeError(
                    "Impossible de trouver la ressource ZIP du REQ. "
                    "Définis REQ_DATASET_ZIP_URL si le portail CKAN répond 403."
                )

            remote_ts = parse_remote_date(zip_resource.get("last_modified", ""))
            local_ts = None
            if DATA_ZIP_PATH.exists():
                local_ts = dt.datetime.fromtimestamp(DATA_ZIP_PATH.stat().st_mtime, tz=dt.timezone.utc)

            should_download = force or not DATA_ZIP_PATH.exists()
            if remote_ts and local_ts and remote_ts > local_ts:
                should_download = True

            if should_download:
                url = zip_resource.get("url")
                if not url:
                    raise RuntimeError("La ressource ZIP ne contient pas d'URL téléchargeable.")
                if not download_file(url, DATA_ZIP_PATH):
                    raise RuntimeError("Le téléchargement du jeu de données a échoué.")

            tables = read_archive_tables(DATA_ZIP_PATH)
            if not tables:
                raise RuntimeError("Aucun fichier CSV exploitable n'a été trouvé dans l'archive.")

            stats = ingest_tables(tables)
            result = {
                "downloaded": should_download,
                "remote_last_modified": zip_resource.get("last_modified"),
                "stats": stats,
                "source_note": (
                    "Le jeu de données ouvert du REQ exclut les noms et prénoms des personnes physiques. "
                    "Le graphe des personnes est donc prêt pour de l'enrichissement, mais restera vide "
                    "sans source complémentaire autorisée."
                ),
            }
            sync_state["last_success_at"] = local_now_iso()
            return result
        except Exception as exc:
            sync_state["last_error"] = str(exc)
            raise
        finally:
            sync_state["is_running"] = False
            sync_state["last_completed_at"] = local_now_iso()


def start_sync_in_background(force: bool = False) -> bool:
    global sync_thread

    if sync_state["is_running"] and sync_thread and sync_thread.is_alive():
        return False

    def _runner() -> None:
        try:
            sync_dataset(force=force)
        except Exception:
            pass

    sync_thread = threading.Thread(
        target=_runner,
        daemon=True,
        name=f"manual-sync-{'force' if force else 'normal'}",
    )
    sync_thread.start()
    return True


def get_metadata_value(key: str) -> Optional[str]:
    with open_db() as conn:
        row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def get_summary() -> Dict[str, object]:
    with open_db() as conn:
        company_total = conn.execute(
            "SELECT COUNT(*) AS total FROM entities WHERE entity_type = ?",
            (COMPANY_NODE,),
        ).fetchone()["total"]
        person_total = conn.execute(
            "SELECT COUNT(*) AS total FROM entities WHERE entity_type = ?",
            (PERSON_NODE,),
        ).fetchone()["total"]
        relation_total = conn.execute("SELECT COUNT(*) AS total FROM relations").fetchone()["total"]

    return {
        "companies": company_total,
        "people": person_total,
        "relations": relation_total,
        "last_ingest_at": get_metadata_value("last_ingest_at"),
        "sync": dict(sync_state),
        "constraints": [
            "Les données ouvertes REQ couvrent toutes les entreprises, mais anonymisent les personnes physiques.",
            "Les liens vers actionnaires, administrateurs et bénéficiaires ultimes exigent une source complémentaire autorisée.",
        ],
    }


def search_entities(query: str) -> List[Dict[str, str]]:
    needle = normalize_text(query)
    if not needle:
        return []

    with open_db() as conn:
        rows = conn.execute(
            """
            SELECT
                e.node_id,
                e.entity_type,
                e.label,
                e.neq,
                MIN(a.alias) AS matched_alias
            FROM entity_aliases a
            JOIN entities e ON e.node_id = a.node_id
            WHERE a.normalized_alias LIKE ?
               OR (e.neq IS NOT NULL AND e.neq LIKE ?)
            GROUP BY e.node_id, e.entity_type, e.label, e.neq
            ORDER BY
                CASE WHEN e.entity_type = 'company' THEN 0 ELSE 1 END,
                LENGTH(e.label),
                e.label
            LIMIT ?
            """,
            (f"%{needle}%", f"%{query.strip()}%", MAX_SEARCH_RESULTS),
        ).fetchall()

    return [
        {
            "id": row["node_id"],
            "type": row["entity_type"],
            "label": row["label"],
            "neq": row["neq"] or "",
            "matched_alias": row["matched_alias"] or row["label"],
        }
        for row in rows
    ]


def fetch_graph(node_id: str) -> Dict[str, List[Dict[str, object]]]:
    with open_db() as conn:
        nodes = {}
        relations = conn.execute(
            """
            SELECT *
            FROM relations
            WHERE source_node_id = ? OR target_node_id = ?
            LIMIT ?
            """,
            (node_id, node_id, MAX_GRAPH_EDGES),
        ).fetchall()
        if not relations:
            entity = conn.execute("SELECT * FROM entities WHERE node_id = ?", (node_id,)).fetchone()
            if not entity:
                return {"nodes": [], "links": []}
            return {
                "nodes": [
                    {
                        "id": entity["node_id"],
                        "label": entity["label"],
                        "type": entity["entity_type"],
                        "neq": entity["neq"] or "",
                    }
                ],
                "links": [],
            }

        links = []
        related_ids = {node_id}
        for relation in relations:
            related_ids.add(relation["source_node_id"])
            related_ids.add(relation["target_node_id"])
            links.append(
                {
                    "source": relation["source_node_id"],
                    "target": relation["target_node_id"],
                    "label": relation["relation_label"],
                    "type": relation["relation_type"],
                    "dataset": relation["source_dataset"],
                }
            )

        placeholders = ",".join("?" for _ in related_ids)
        entity_rows = conn.execute(
            f"SELECT * FROM entities WHERE node_id IN ({placeholders})",
            tuple(related_ids),
        ).fetchall()
        for entity in entity_rows:
            nodes[entity["node_id"]] = {
                "id": entity["node_id"],
                "label": entity["label"],
                "type": entity["entity_type"],
                "neq": entity["neq"] or "",
            }
        return {"nodes": list(nodes.values()), "links": links}


def sync_worker_loop() -> None:
    while True:
        try:
            sync_dataset(force=False)
        except Exception:
            pass
        time.sleep(UPDATE_INTERVAL_SECONDS)


def maybe_start_sync_loop() -> None:
    if not AUTO_SYNC_ENABLED:
        return
    thread = threading.Thread(target=sync_worker_loop, daemon=True, name="req-sync")
    thread.start()


def check_admin_token(req) -> bool:
    if not ADMIN_SYNC_TOKEN:
        return True
    provided = req.headers.get("X-Admin-Sync-Token", "")
    return provided == ADMIN_SYNC_TOKEN


@app.route("/")
def index():
    return render_template("index.html", summary=get_summary())


@app.route("/api/summary")
def api_summary():
    return jsonify(get_summary())


@app.route("/api/search")
def api_search():
    query = request.args.get("q", "")
    return jsonify(search_entities(query))


@app.route("/api/network")
def api_network():
    node_id = request.args.get("id", "").strip()
    neq = request.args.get("neq", "").strip()
    if not node_id and neq:
        node_id = normalize_node_id(COMPANY_NODE, neq)
    if not node_id:
        return jsonify({"nodes": [], "links": []})
    return jsonify(fetch_graph(node_id))


@app.route("/api/sync", methods=["GET", "POST"])
def api_sync():
    if request.method == "POST":
        if not check_admin_token(request):
            return jsonify({"error": "Unauthorized"}), 401
        force = request.args.get("force", "0") == "1"
        wait_for_completion = request.args.get("wait", "0") == "1"
        if wait_for_completion:
            try:
                result = sync_dataset(force=force)
                return jsonify({"ok": True, "mode": "sync", **result})
            except Exception as exc:
                return jsonify({"ok": False, "error": str(exc), "sync": dict(sync_state)}), 500

        started = start_sync_in_background(force=force)
        status_code = 202 if started else 200
        return (
            jsonify(
                {
                    "ok": True,
                    "mode": "async",
                    "started": started,
                    "sync": dict(sync_state),
                }
            ),
            status_code,
        )
    return jsonify({"ok": True, "sync": dict(sync_state), "summary": get_summary()})


def bootstrap() -> None:
    init_db()
    if not get_metadata_value("last_ingest_at") and DATA_ZIP_PATH.exists():
        try:
            ingest_tables(read_archive_tables(DATA_ZIP_PATH))
        except Exception:
            pass
    maybe_start_sync_loop()


def main() -> None:
    bootstrap()
    mode = os.environ.get("APP_MODE", "").strip().lower()
    if len(os.sys.argv) > 1:
        mode = os.sys.argv[1].strip().lower()

    if mode == "sync":
        result = sync_dataset(force=True)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    port = int(os.environ.get("PORT", "5000"))
    app.run(debug=debug, host="0.0.0.0", port=port)


bootstrap()


if __name__ == "__main__":
    main()
