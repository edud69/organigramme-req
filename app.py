import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import shutil
import threading
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from flask import Flask, jsonify, render_template, request
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine


BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DEFAULT_DATA_DIR = Path("/tmp/organigramme-req") if DATABASE_URL else BASE_DIR / "data"
DATA_DIR = Path(os.environ.get("REQ_DATA_DIR", str(DEFAULT_DATA_DIR)))
DATA_ZIP_PATH = DATA_DIR / "req-dataset.zip"
DB_PATH = DATA_DIR / "req_cache.sqlite3"
APP_SKIP_BOOTSTRAP = os.environ.get("APP_SKIP_BOOTSTRAP", "0") == "1"

CKAN_PACKAGE_URL = os.environ.get(
    "REQ_CKAN_PACKAGE_URL",
    "https://www.donneesquebec.ca/recherche/api/3/action/package_show"
    "?id=6f710997-b5f9-4347-893b-1a47ddb61437",
)
REQ_DATASET_ZIP_URL = os.environ.get("REQ_DATASET_ZIP_URL", "").strip()
REQ_DOWNLOAD_MODE = os.environ.get("REQ_DOWNLOAD_MODE", "http").strip().lower()
REQ_BROWSER_HEADLESS = os.environ.get("REQ_BROWSER_HEADLESS", "0") == "1"
REQ_BROWSER_CHANNEL = os.environ.get("REQ_BROWSER_CHANNEL", "chrome").strip() or None
REQ_BROWSER_PRIME_URL = os.environ.get(
    "REQ_BROWSER_PRIME_URL",
    "https://www.donneesquebec.ca/recherche/dataset/registre-des-entreprises",
).strip()
UPDATE_INTERVAL_SECONDS = int(os.environ.get("UPDATE_INTERVAL_SECONDS", str(24 * 3600)))
AUTO_SYNC_ENABLED = os.environ.get("AUTO_SYNC_ENABLED", "1") == "1"
MAX_SEARCH_RESULTS = int(os.environ.get("MAX_SEARCH_RESULTS", "20"))
MAX_GRAPH_EDGES = int(os.environ.get("MAX_GRAPH_EDGES", "250"))
SYNC_TIMEOUT_SECONDS = int(os.environ.get("SYNC_TIMEOUT_SECONDS", "120"))
ADMIN_SYNC_TOKEN = os.environ.get("ADMIN_SYNC_TOKEN", "")
ENTITY_BATCH_SIZE = int(os.environ.get("ENTITY_BATCH_SIZE", "1000"))
RELATION_BATCH_SIZE = int(os.environ.get("RELATION_BATCH_SIZE", "2000"))

COMPANY_NODE = "company"
PERSON_NODE = "person"
RELEVANT_ARCHIVE_FILES = {
    "nom.csv",
    "fusionscissions.csv",
    "continuationstransformations.csv",
}

app = Flask(__name__)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("organigramme-req")
HTTP_USER_AGENT = os.environ.get(
    "HTTP_USER_AGENT",
    "Mozilla/5.0 (compatible; OrganigrammeREQ/1.0; +https://github.com/edud69/organigramme-req)",
)
data_lock = threading.Lock()
sync_state = {
    "is_running": False,
    "phase": "idle",
    "last_started_at": None,
    "last_completed_at": None,
    "last_success_at": None,
    "last_error": None,
    "last_result": None,
}
sync_thread: Optional[threading.Thread] = None
db_engine: Optional[Engine] = None


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def local_now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat()


def get_database_url() -> str:
    if DATABASE_URL:
        if DATABASE_URL.startswith("postgres://"):
            return DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
        if DATABASE_URL.startswith("postgresql://"):
            return DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
        return DATABASE_URL
    ensure_data_dir()
    return f"sqlite:///{DB_PATH}"


def get_engine() -> Engine:
    global db_engine
    if db_engine is None:
        engine_kwargs = {
            "pool_pre_ping": True,
            "future": True,
        }
        if DATABASE_URL:
            engine_kwargs["connect_args"] = {
                "options": "-c statement_timeout=0 -c lock_timeout=0"
            }
        db_engine = create_engine(get_database_url(), **engine_kwargs)
    return db_engine


def reset_engine() -> None:
    global db_engine
    if db_engine is not None:
        db_engine.dispose()
    db_engine = None


def is_postgres() -> bool:
    return get_engine().dialect.name == "postgresql"


def open_db() -> Connection:
    return get_engine().connect()


def open_db_tx():
    return get_engine().begin()


def init_db() -> None:
    entity_aliases_id = "BIGSERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    relations_id = "BIGSERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    use_optional_indexes = not is_postgres()
    with open_db_tx() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    node_id TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    label TEXT NOT NULL,
                    normalized_label TEXT NOT NULL,
                    neq TEXT,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS entity_aliases (
                    id {entity_aliases_id},
                    node_id TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    normalized_alias TEXT NOT NULL,
                    source TEXT NOT NULL,
                    UNIQUE(node_id, normalized_alias)
                )
                """
            )
        )
        if use_optional_indexes:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_entity_aliases_normalized ON entity_aliases(normalized_alias)"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS relations (
                    id {relations_id},
                    source_node_id TEXT NOT NULL,
                    target_node_id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    relation_label TEXT NOT NULL,
                    source_dataset TEXT NOT NULL,
                    source_detail TEXT,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_node_id, target_node_id, relation_type, source_dataset, source_detail)
                )
                """
            )
        )
        if use_optional_indexes:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_relations_source ON relations(source_node_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_relations_target ON relations(target_node_id)"))


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def normalize_node_id(entity_type: str, raw_value: str) -> str:
    if entity_type == COMPANY_NODE and raw_value:
        return f"company:{raw_value.strip()}"
    digest = hashlib.sha1(normalize_text(raw_value).encode("utf-8")).hexdigest()[:16]
    return f"{entity_type}:{digest}"


def fetch_json(url: str, timeout: int = 30) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
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
    if REQ_DOWNLOAD_MODE == "browser":
        return download_file_with_browser(url, dest)

    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": HTTP_USER_AGENT,
                "Accept": "*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=SYNC_TIMEOUT_SECONDS) as response, tmp_path.open("wb") as out:
            logger.info(
                "REQ download response status=%s content_type=%s final_url=%s",
                getattr(response, "status", "unknown"),
                response.headers.get("Content-Type"),
                response.geturl(),
            )
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                out.write(chunk)
        os.replace(tmp_path, dest)
        return True
    except Exception:
        logger.exception("REQ download failed for %s", url)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False


def download_file_with_browser(url: str, dest: Path) -> bool:
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    browser_profile_dir = DATA_DIR / "browser-profile"
    ensure_data_dir()
    browser_profile_dir.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception:
        logger.exception("Playwright is not installed or not available")
        return False

    try:
        with sync_playwright() as p:
            browser_launcher = p.chromium
            launch_kwargs = {
                "headless": REQ_BROWSER_HEADLESS,
                "accept_downloads": True,
                "user_agent": HTTP_USER_AGENT,
            }
            if REQ_BROWSER_CHANNEL:
                launch_kwargs["channel"] = REQ_BROWSER_CHANNEL

            context = browser_launcher.launch_persistent_context(
                str(browser_profile_dir),
                **launch_kwargs,
            )
            try:
                page = context.new_page()
                page.set_default_timeout(SYNC_TIMEOUT_SECONDS * 1000)
                page.set_extra_http_headers(
                    {
                        "Accept": "*/*",
                        "Upgrade-Insecure-Requests": "1",
                    }
                )

                if REQ_BROWSER_PRIME_URL:
                    logger.info("REQ browser priming on %s", REQ_BROWSER_PRIME_URL)
                    page.goto(REQ_BROWSER_PRIME_URL, wait_until="domcontentloaded")
                    page.wait_for_timeout(1500)

                logger.info("REQ browser downloading archive from %s", url)
                with page.expect_download(timeout=SYNC_TIMEOUT_SECONDS * 1000) as download_info:
                    try:
                        page.goto(url, wait_until="domcontentloaded")
                    except Exception as exc:
                        # Playwright raises when navigation turns into a file download.
                        if "Download is starting" not in str(exc):
                            raise

                download = download_info.value
                logger.info("REQ browser download suggested filename=%s", download.suggested_filename)
                download.save_as(str(tmp_path))
            finally:
                context.close()

        if not tmp_path.exists() or tmp_path.stat().st_size == 0:
            logger.error("REQ browser download created no usable file")
            tmp_path.unlink(missing_ok=True)
            return False

        shutil.move(str(tmp_path), str(dest))
        return True
    except PlaywrightTimeoutError:
        logger.exception("REQ browser download timed out")
    except Exception:
        logger.exception("REQ browser download failed for %s", url)

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
            tables[basename] = [
                {
                    (key or "").replace("\ufeff", "").strip(): (value or "").strip()
                    for key, value in row.items()
                }
                for row in reader
            ]
    return tables


def iter_archive_rows(zip_path: Path, target_files: Optional[set[str]] = None):
    if not zip_path.exists():
        return

    with zipfile.ZipFile(zip_path, "r") as archive:
        for filename in archive.namelist():
            basename = Path(filename).name.lower()
            if not basename.endswith(".csv"):
                continue
            if target_files and basename not in target_files:
                continue
            with archive.open(filename) as handle:
                try:
                    wrapper = io.TextIOWrapper(handle, encoding="utf-8-sig", newline="")
                    reader = csv.DictReader(wrapper)
                    for row in reader:
                        yield basename, {
                            (key or "").replace("\ufeff", "").strip(): (value or "").strip()
                            for key, value in row.items()
                        }
                except UnicodeDecodeError:
                    handle.seek(0)
                    wrapper = io.TextIOWrapper(handle, encoding="latin-1", newline="")
                    reader = csv.DictReader(wrapper)
                    for row in reader:
                        yield basename, {
                            (key or "").replace("\ufeff", "").strip(): (value or "").strip()
                            for key, value in row.items()
                        }


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
    conn: Connection,
    neq: str,
    label: str,
    source: str,
    aliases: Optional[Iterable[str]] = None,
) -> str:
    node_id = normalize_node_id(COMPANY_NODE, neq)
    normalized_label = normalize_text(label or neq)
    updated_at = utc_now_iso()
    conn.execute(
        text(
            """
        INSERT INTO entities (node_id, entity_type, label, normalized_label, neq, source, updated_at)
        VALUES (:node_id, :entity_type, :label, :normalized_label, :neq, :source, :updated_at)
        ON CONFLICT(node_id) DO UPDATE SET
            label = excluded.label,
            normalized_label = excluded.normalized_label,
            neq = excluded.neq,
            source = excluded.source,
            updated_at = excluded.updated_at
        """
        ),
        {
            "node_id": node_id,
            "entity_type": COMPANY_NODE,
            "label": label or neq,
            "normalized_label": normalized_label,
            "neq": neq,
            "source": source,
            "updated_at": updated_at,
        },
    )
    alias_values = set(aliases or [])
    alias_values.add(label or neq)
    for alias in alias_values:
        normalized_alias = normalize_text(alias)
        if not normalized_alias:
            continue
        conn.execute(
            text(
                """
                INSERT INTO entity_aliases (node_id, alias, normalized_alias, source)
                VALUES (:node_id, :alias, :normalized_alias, :source)
                ON CONFLICT(node_id, normalized_alias) DO NOTHING
                """
            ),
            {
                "node_id": node_id,
                "alias": alias,
                "normalized_alias": normalized_alias,
                "source": source,
            },
    )
    return node_id


def chunked(items: List[dict], size: int = 1000) -> Iterable[List[dict]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def upsert_person(conn: Connection, name: str, source: str) -> str:
    node_id = normalize_node_id(PERSON_NODE, name)
    normalized_label = normalize_text(name)
    updated_at = utc_now_iso()
    conn.execute(
        text(
            """
        INSERT INTO entities (node_id, entity_type, label, normalized_label, neq, source, updated_at)
        VALUES (:node_id, :entity_type, :label, :normalized_label, NULL, :source, :updated_at)
        ON CONFLICT(node_id) DO UPDATE SET
            label = excluded.label,
            normalized_label = excluded.normalized_label,
            source = excluded.source,
            updated_at = excluded.updated_at
        """
        ),
        {
            "node_id": node_id,
            "entity_type": PERSON_NODE,
            "label": name,
            "normalized_label": normalized_label,
            "source": source,
            "updated_at": updated_at,
        },
    )
    conn.execute(
        text(
            """
            INSERT INTO entity_aliases (node_id, alias, normalized_alias, source)
            VALUES (:node_id, :alias, :normalized_alias, :source)
            ON CONFLICT(node_id, normalized_alias) DO NOTHING
            """
        ),
        {
            "node_id": node_id,
            "alias": name,
            "normalized_alias": normalized_label,
            "source": source,
        },
    )
    return node_id


def insert_relation(
    conn: Connection,
    source_node_id: str,
    target_node_id: str,
    relation_type: str,
    relation_label: str,
    source_dataset: str,
    source_detail: Optional[str] = None,
) -> None:
    conn.execute(
        text(
            """
        INSERT INTO relations
        (source_node_id, target_node_id, relation_type, relation_label, source_dataset, source_detail, updated_at)
        VALUES (:source_node_id, :target_node_id, :relation_type, :relation_label, :source_dataset, :source_detail, :updated_at)
        ON CONFLICT(source_node_id, target_node_id, relation_type, source_dataset, source_detail) DO NOTHING
        """
        ),
        {
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "relation_type": relation_type,
            "relation_label": relation_label,
            "source_dataset": source_dataset,
            "source_detail": source_detail,
            "updated_at": utc_now_iso(),
        },
    )


def ingest_archive(zip_path: Path) -> Dict[str, int]:
    stats = {
        "tables": 0,
        "companies": 0,
        "people": 0,
        "relations": 0,
    }
    now = utc_now_iso()
    company_names: Dict[str, set] = {}
    seen_files = set()
    for filename, row in iter_archive_rows(zip_path, {"nom.csv"}):
        seen_files.add(filename)
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

    stats["tables"] = len(seen_files)
    logger.info("REQ ingest company_names=%s", len(company_names))
    with open_db() as conn:
        if is_postgres():
            conn.execute(text("TRUNCATE TABLE relations, entity_aliases, entities"))
        else:
            conn.execute(text("DELETE FROM relations"))
            conn.execute(text("DELETE FROM entity_aliases"))
            conn.execute(text("DELETE FROM entities"))
        conn.commit()

        entity_batch: List[dict] = []
        alias_batch: List[dict] = []
        inserted_entities = 0
        inserted_aliases = 0
        for neq, aliases in company_names.items():
            clean_aliases = sorted({alias.strip() for alias in aliases if alias and alias.strip()})
            label = clean_aliases[0] if clean_aliases else neq
            node_id = normalize_node_id(COMPANY_NODE, neq)
            entity_batch.append(
                {
                    "node_id": node_id,
                    "entity_type": COMPANY_NODE,
                    "label": label,
                    "normalized_label": normalize_text(label),
                    "neq": neq,
                    "source": "req-open-data",
                    "updated_at": now,
                }
            )
            seen_normalized_aliases = set()
            for alias in clean_aliases or [label]:
                normalized_alias = normalize_text(alias)
                if not normalized_alias or normalized_alias in seen_normalized_aliases:
                    continue
                seen_normalized_aliases.add(normalized_alias)
                alias_batch.append(
                    {
                        "node_id": node_id,
                        "alias": alias,
                        "normalized_alias": normalized_alias,
                        "source": "req-open-data",
                    }
                )
            if len(entity_batch) >= ENTITY_BATCH_SIZE:
                conn.execute(
                    text(
                        """
                        INSERT INTO entities (node_id, entity_type, label, normalized_label, neq, source, updated_at)
                        VALUES (:node_id, :entity_type, :label, :normalized_label, :neq, :source, :updated_at)
                        """
                    ),
                    entity_batch,
                )
                inserted_entities += len(entity_batch)
                entity_batch.clear()
                conn.commit()
                logger.info("REQ ingest inserted_entities=%s", inserted_entities)
            if len(alias_batch) >= RELATION_BATCH_SIZE:
                conn.execute(
                    text(
                        """
                        INSERT INTO entity_aliases (node_id, alias, normalized_alias, source)
                        VALUES (:node_id, :alias, :normalized_alias, :source)
                        ON CONFLICT(node_id, normalized_alias) DO NOTHING
                        """
                    ),
                    alias_batch,
                )
                inserted_aliases += len(alias_batch)
                alias_batch.clear()
                conn.commit()
                logger.info("REQ ingest inserted_aliases=%s", inserted_aliases)

        if entity_batch:
            conn.execute(
                text(
                    """
                    INSERT INTO entities (node_id, entity_type, label, normalized_label, neq, source, updated_at)
                    VALUES (:node_id, :entity_type, :label, :normalized_label, :neq, :source, :updated_at)
                    """
                ),
                entity_batch,
            )
            inserted_entities += len(entity_batch)
        if alias_batch:
            conn.execute(
                text(
                    """
                    INSERT INTO entity_aliases (node_id, alias, normalized_alias, source)
                    VALUES (:node_id, :alias, :normalized_alias, :source)
                    ON CONFLICT(node_id, normalized_alias) DO NOTHING
                    """
                ),
                alias_batch,
            )
            inserted_aliases += len(alias_batch)
        conn.commit()
        logger.info("REQ ingest inserted_entities=%s", inserted_entities)
        logger.info("REQ ingest inserted_aliases=%s", inserted_aliases)

        relation_files = set()
        direct_relations = 0
        relation_batch: List[dict] = []
        relation_keys = set()
        inserted_relations = 0
        for filename, row in iter_archive_rows(zip_path, RELEVANT_ARCHIVE_FILES - {"nom.csv"}):
            seen_files.add(filename)
            if row.get("NEQ", "").strip() and row.get("NEQ_ASSUJ_REL", "").strip():
                relation_files.add(filename)
                src_neq = row.get("NEQ_ASSUJ_REL", "").strip()
                dst_neq = row.get("NEQ", "").strip()
                direct_relations += 1
                relation_code = row.get("COD_RELA_ASSUJ", "").strip() or filename
                relation_key = (
                    normalize_node_id(COMPANY_NODE, src_neq),
                    normalize_node_id(COMPANY_NODE, dst_neq),
                    f"company_relation:{relation_code}",
                    filename,
                    relation_code,
                )
                if relation_key in relation_keys:
                    continue
                relation_keys.add(relation_key)
                relation_batch.append(
                    {
                        "source_node_id": relation_key[0],
                        "target_node_id": relation_key[1],
                        "relation_type": relation_key[2],
                        "relation_label": dataset_label_from_filename(filename),
                        "source_dataset": filename,
                        "source_detail": relation_code,
                        "updated_at": now,
                    }
                )
                if len(relation_batch) >= RELATION_BATCH_SIZE:
                    conn.execute(
                        text(
                            """
                            INSERT INTO relations
                            (source_node_id, target_node_id, relation_type, relation_label, source_dataset, source_detail, updated_at)
                            VALUES (:source_node_id, :target_node_id, :relation_type, :relation_label, :source_dataset, :source_detail, :updated_at)
                            ON CONFLICT(source_node_id, target_node_id, relation_type, source_dataset, source_detail) DO NOTHING
                            """
                        ),
                        relation_batch,
                    )
                    inserted_relations += len(relation_batch)
                    relation_batch.clear()
                    relation_keys.clear()
                    conn.commit()
                    logger.info("REQ ingest inserted_relations=%s", inserted_relations)

        if relation_batch:
            conn.execute(
                text(
                    """
                    INSERT INTO relations
                    (source_node_id, target_node_id, relation_type, relation_label, source_dataset, source_detail, updated_at)
                    VALUES (:source_node_id, :target_node_id, :relation_type, :relation_label, :source_dataset, :source_detail, :updated_at)
                    ON CONFLICT(source_node_id, target_node_id, relation_type, source_dataset, source_detail) DO NOTHING
                    """
                ),
                relation_batch,
            )
            inserted_relations += len(relation_batch)
            conn.commit()
        logger.info("REQ ingest inserted_relations=%s", inserted_relations)

        logger.info("REQ ingest relation_files=%s direct_relations=%s", len(relation_files), direct_relations)
        stats["tables"] = len(seen_files)

        stats["companies"] = conn.execute(
            text("SELECT COUNT(*) AS total FROM entities WHERE entity_type = :entity_type"),
            {"entity_type": COMPANY_NODE},
        ).mappings().one()["total"]
        stats["people"] = conn.execute(
            text("SELECT COUNT(*) AS total FROM entities WHERE entity_type = :entity_type"),
            {"entity_type": PERSON_NODE},
        ).mappings().one()["total"]
        stats["relations"] = conn.execute(text("SELECT COUNT(*) AS total FROM relations")).mappings().one()["total"]
        conn.execute(
            text(
                """
                INSERT INTO metadata (key, value)
                VALUES (:key, :value)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """
            ),
            {"key": "last_ingest_at", "value": utc_now_iso()},
        )
        conn.execute(
            text(
                """
                INSERT INTO metadata (key, value)
                VALUES (:key, :value)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """
            ),
            {"key": "last_table_count", "value": str(len(relation_files))},
        )
        conn.commit()
    return stats


def sync_dataset(force: bool = False) -> Dict[str, object]:
    with data_lock:
        logger.info("REQ sync started force=%s", force)
        sync_state["is_running"] = True
        sync_state["phase"] = "starting"
        sync_state["last_started_at"] = local_now_iso()
        sync_state["last_error"] = None

        try:
            ensure_data_dir()
            sync_state["phase"] = "fetching_metadata"
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
                sync_state["phase"] = "downloading_archive"
                url = zip_resource.get("url")
                if not url:
                    raise RuntimeError("La ressource ZIP ne contient pas d'URL téléchargeable.")
                logger.info("REQ sync downloading archive from %s", url)
                if not download_file(url, DATA_ZIP_PATH):
                    raise RuntimeError("Le téléchargement du jeu de données a échoué.")

            sync_state["phase"] = "reading_archive"
            if not DATA_ZIP_PATH.exists():
                raise RuntimeError("Aucun fichier CSV exploitable n'a été trouvé dans l'archive.")

            sync_state["phase"] = "ingesting"
            stats = ingest_archive(DATA_ZIP_PATH)
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
            sync_state["phase"] = "completed"
            sync_state["last_result"] = result
            sync_state["last_success_at"] = local_now_iso()
            logger.info("REQ sync completed: %s", result)
            return result
        except Exception as exc:
            sync_state["last_error"] = str(exc)
            sync_state["phase"] = "failed"
            logger.exception("REQ sync failed")
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
            logger.exception("Background sync crashed")

    sync_thread = threading.Thread(
        target=_runner,
        daemon=True,
        name=f"manual-sync-{'force' if force else 'normal'}",
    )
    sync_thread.start()
    return True


def get_metadata_value(key: str) -> Optional[str]:
    with open_db() as conn:
        row = conn.execute(
            text("SELECT value FROM metadata WHERE key = :key"),
            {"key": key},
        ).mappings().first()
    return row["value"] if row else None


def get_summary() -> Dict[str, object]:
    with open_db() as conn:
        company_total = conn.execute(
            text("SELECT COUNT(*) AS total FROM entities WHERE entity_type = :entity_type"),
            {"entity_type": COMPANY_NODE},
        ).mappings().one()["total"]
        person_total = conn.execute(
            text("SELECT COUNT(*) AS total FROM entities WHERE entity_type = :entity_type"),
            {"entity_type": PERSON_NODE},
        ).mappings().one()["total"]
        relation_total = conn.execute(text("SELECT COUNT(*) AS total FROM relations")).mappings().one()["total"]

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
            text(
                """
            SELECT
                e.node_id,
                e.entity_type,
                e.label,
                e.neq,
                MIN(a.alias) AS matched_alias
            FROM entity_aliases a
            JOIN entities e ON e.node_id = a.node_id
            WHERE a.normalized_alias LIKE :normalized_query
               OR (e.neq IS NOT NULL AND e.neq LIKE :raw_query)
            GROUP BY e.node_id, e.entity_type, e.label, e.neq
            ORDER BY
                CASE WHEN e.entity_type = 'company' THEN 0 ELSE 1 END,
                LENGTH(e.label),
                e.label
            LIMIT :limit
            """
            ),
            {
                "normalized_query": f"%{needle}%",
                "raw_query": f"%{query.strip()}%",
                "limit": MAX_SEARCH_RESULTS,
            },
        ).mappings().all()

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
            text(
                """
            SELECT *
            FROM relations
            WHERE source_node_id = :node_id OR target_node_id = :node_id
            LIMIT :limit
            """
            ),
            {"node_id": node_id, "limit": MAX_GRAPH_EDGES},
        ).mappings().all()
        if not relations:
            entity = conn.execute(
                text("SELECT * FROM entities WHERE node_id = :node_id"),
                {"node_id": node_id},
            ).mappings().first()
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

        entity_rows = conn.execute(
            text("SELECT * FROM entities WHERE node_id IN :node_ids").bindparams(
                bindparam("node_ids", expanding=True)
            ),
            {"node_ids": list(related_ids)},
        ).mappings().all()
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
            ingest_archive(DATA_ZIP_PATH)
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


if not APP_SKIP_BOOTSTRAP:
    bootstrap()


if __name__ == "__main__":
    main()
