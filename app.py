import datetime
import os
import threading
import zipfile
from functools import lru_cache
from typing import Dict, List, Optional

"""
Application Flask pour visualiser les relations entre entités inscrites au
registre des entreprises du Québec (REQ). Cette version ne dépend pas de
bibliothèques externes comme pandas ou networkx afin de faciliter son
déploiement sur des plateformes disposant de ressources limitées.

Le jeu de données du REQ est distribué sous forme d'une archive ZIP qui
contient plusieurs fichiers CSV. Seuls deux fichiers sont utilisés ici :
  - ``Nom.csv`` : liste des noms et numéros d'entreprise (NEQ) ;
  - ``FusionScission.csv`` : relations entre entreprises (NEQ) impliquées
    dans des fusions ou scissions.

Le code télécharge automatiquement l'archive si une nouvelle version est
publiée, puis charge les fichiers nécessaires en mémoire à l'aide du module
CSV de la bibliothèque standard.

Cela permet d'exécuter l'application sur des environnements où il est
impossible d'installer des paquets supplémentaires.
"""

import csv
from flask import Flask, jsonify, render_template, request
import json
import urllib.request

app = Flask(__name__)

# Chemin vers l'archive ZIP contenant les données du registre. Elle sera
# automatiquement téléchargée et mise à jour depuis le portail Données Québec.
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DATA_ZIP_PATH = os.path.join(DATA_DIR, "req-dataset.zip")

# URL de l'API CKAN utilisée pour récupérer les métadonnées du jeu de données.
# Voir https://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.get.package_show
CKAN_PACKAGE_URL = (
    "https://www.donneesquebec.ca/recherche/api/3/action/package_show"
    "?id=6f710997-b5f9-4347-893b-1a47ddb61437"
)

# Intervalle entre deux vérifications des mises à jour (en secondes).
UPDATE_INTERVAL = 24 * 3600  # 24 heures

# Verrou pour protéger l'accès concurrentiel au fichier de données lors de la
# mise à jour. Sans cela, une requête API pourrait tenter de lire des données
# pendant qu'elles sont en cours de téléchargement.
data_lock = threading.Lock()


def ensure_data_dir() -> None:
    """Crée le dossier des données s'il n'existe pas."""
    os.makedirs(DATA_DIR, exist_ok=True)


def fetch_metadata() -> Optional[dict]:
    """Récupère les métadonnées du jeu de données via l'API CKAN.

    Returns:
        Un dictionnaire représentant la clé ``result`` de la réponse JSON, ou
        ``None`` en cas d'erreur.
    """
    try:
        with urllib.request.urlopen(CKAN_PACKAGE_URL, timeout=30) as resp:
            # Le corps renvoyé est une chaîne JSON. On le charge via json.loads.
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            if data.get("success"):
                return data.get("result")
    except Exception:
        # La récupération a échoué (problème réseau, 403, etc.)
        return None
    return None


def find_zip_resource(metadata: dict) -> Optional[dict]:
    """Identifie la ressource ZIP dans la liste des ressources du jeu de données.

    Args:
        metadata: Dictionnaire ``result`` renvoyé par l'API CKAN.

    Returns:
        Le dictionnaire de la ressource ZIP si trouvé, ``None`` sinon.
    """
    resources = metadata.get("resources", []) if metadata else []
    for res in resources:
        # On recherche une ressource dont l'URL se termine par .zip ou dont le
        # format déclaré est ZIP. Certaines ressources peuvent être renommées,
        # donc on vérifie également le nom.
        url = res.get("url", "").lower()
        name = res.get("name", "").lower()
        format_field = res.get("format", "").lower()
        if ".zip" in url or ".zip" in name or format_field == "zip":
            return res
    return None


def parse_remote_date(date_str: str) -> Optional[datetime.datetime]:
    """Convertit une chaîne ISO 8601 en objet datetime.

    Args:
        date_str: Chaîne au format ``YYYY-MM-DDTHH:MM:SS(.ffffffff)``.

    Returns:
        Un ``datetime`` ou ``None`` si la chaîne est invalide.
    """
    if not date_str:
        return None
    try:
        # Certains champs ``last_modified`` contiennent des fractions de
        # secondes. On les prend en compte si présents.
        return datetime.datetime.fromisoformat(date_str.rstrip("Z"))
    except ValueError:
        return None


def download_file(url: str, dest: str) -> bool:
    """Télécharge un fichier via HTTP en écrivant les octets dans ``dest``.

    Args:
        url: L'URL à télécharger.
        dest: Chemin du fichier de destination.

    Returns:
        ``True`` si le téléchargement s'est bien déroulé, ``False`` sinon.
    """
    try:
        # Télécharge le fichier dans un fichier temporaire.
        tmp_path = dest + ".tmp"
        with urllib.request.urlopen(url, timeout=60) as response, open(tmp_path, "wb") as out:
            # Lire en blocs pour gérer de gros fichiers.
            block_size = 8192
            while True:
                chunk = response.read(block_size)
                if not chunk:
                    break
                out.write(chunk)
        # Remplacement atomique du fichier
        os.replace(tmp_path, dest)
        return True
    except Exception:
        return False


def update_dataset() -> None:
    """Vérifie la présence d'une nouvelle version et télécharge si nécessaire."""
    ensure_data_dir()
    metadata = fetch_metadata()
    if not metadata:
        # Impossible de récupérer les métadonnées ; on abandonne la mise à jour
        return
    zip_res = find_zip_resource(metadata)
    if not zip_res:
        # Aucune ressource ZIP trouvée ; rien à faire
        return

    # Date de mise à jour distante
    remote_ts = parse_remote_date(zip_res.get("last_modified"))
    # Date de fichier local
    local_ts = None
    if os.path.exists(DATA_ZIP_PATH):
        local_ts = datetime.datetime.fromtimestamp(os.path.getmtime(DATA_ZIP_PATH))
    # S'il n'y a pas de fichier local, on doit télécharger ; si les dates
    # diffèrent et que la distante est plus récente, on télécharge aussi.
    should_download = False
    if not local_ts:
        should_download = True
    elif remote_ts and remote_ts > local_ts:
        should_download = True
    if should_download:
        url = zip_res.get("url")
        if url:
            success = download_file(url, DATA_ZIP_PATH)
            if success:
                # On recharge les données en purgeant le cache
                with data_lock:
                    get_data.cache_clear()
                    # Appel immédiat pour précharger
                    get_data()


def schedule_update() -> None:
    """Planifie l'exécution périodique de la mise à jour."""
    def _run():
        update_dataset()
        # Planifie la prochaine exécution
        timer = threading.Timer(UPDATE_INTERVAL, _run)
        timer.daemon = True
        timer.start()

    # Lancement immédiat de la première exécution en arrière‑plan
    timer0 = threading.Timer(0, _run)
    timer0.daemon = True
    timer0.start()


def load_dataset() -> Dict[str, list]:
    """Charge et parse ``Nom.csv`` et ``FusionScission.csv`` depuis l'archive ZIP.

    Au lieu d'utiliser pandas, on s'appuie sur le module ``csv`` de la
    bibliothèque standard pour lire les fichiers. Chaque fichier est
    transformé en une liste de dictionnaires, où les clés correspondent
    aux noms de colonnes et les valeurs sont des chaînes.

    Returns:
        Un dictionnaire ayant deux clés : ``nom.csv`` et ``fusionscission.csv``.
        Chacune mappe vers une liste de lignes (dictionnaires). Si un fichier
        est manquant, la liste sera vide.
    """
    if not os.path.exists(DATA_ZIP_PATH):
        return {}
    datasets: Dict[str, list] = {"nom.csv": [], "fusionscission.csv": []}
    with data_lock:
        try:
            with zipfile.ZipFile(DATA_ZIP_PATH, "r") as archive:
                for filename in archive.namelist():
                    lower = filename.lower()
                    base = os.path.basename(lower)
                    if base == "nom.csv" or base == "fusionscission.csv":
                        with archive.open(filename) as f:
                            # Certaines lignes peuvent comporter un encodage latin‑1.
                            # On essaie UTF‑è puis latin‑1.
                            content = f.read()
                            for encoding in ("utf-8", "latin-1"):
                                try:
                                    text = content.decode(encoding)
                                    reader = csv.DictReader(text.splitlines())
                                    datasets[base] = [ {k: (v or "").strip() for k, v in row.items()} for row in reader ]
                                    break
                                except Exception:
                                    continue
        except zipfile.BadZipFile:
            return {}
    return datasets


@lru_cache(maxsize=1)
def get_data():
    """Retourne les données et un dictionnaire de noms, mis en cache.

    Le chargement des fichiers CSV peut être coûteux ; nous le faisons
    uniquement lors du premier appel et stockons le résultat en cache. Les
    données sont renvoyées sous forme de deux listes de dictionnaires ainsi
    qu'un mapping ``NEQ → nom``.

    Returns:
        nom_list (List[Dict[str, str]]): lignes de ``Nom.csv``.
        fusion_list (List[Dict[str, str]]): lignes de ``FusionScission.csv``.
        names_map (Dict[str, str]): dictionnaire NEQ → nom de l'entreprise.
    """
    datasets = load_dataset()
    nom_list: List[Dict[str, str]] = datasets.get("nom.csv", [])
    fusion_list: List[Dict[str, str]] = datasets.get("fusionscission.csv", [])
    names_map: Dict[str, str] = {}
    # Extraire les noms à partir des différentes colonnes du fichier Nom.csv
    for row in nom_list:
        neq = row.get("NEQ", "").strip()
        if not neq:
            continue
        # Les colonnes de noms possibles sont listées ici.
        for col in ("NOM_ASSUJ", "DENOMN_SOC", "NOM_ASSUJ_LANG_ETRNG", "NOM_ASSUJ_ETRNG"):
            val = row.get(col, "").strip()
            if val:
                names_map.setdefault(neq, val)
                break
    # Associer les NEQ présents uniquement dans FusionScission
    for row in fusion_list:
        src = row.get("NEQ_ASSUJ_REL", "").strip()
        dst = row.get("NEQ", "").strip()
        for neq in (src, dst):
            if neq and neq not in names_map:
                names_map[neq] = neq
    return nom_list, fusion_list, names_map


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search")
def api_search():
    query = request.args.get("q", "").strip().lower()
    results: List[Dict[str, str]] = []
    _, _, names_map = get_data()
    if not query:
        return jsonify(results)
    for neq, name in names_map.items():
        if query in neq.lower() or query in name.lower():
            results.append({"neq": neq, "name": name})
            if len(results) >= 20:
                break
    results.sort(key=lambda x: x["name"])
    return jsonify(results)


@app.route("/api/network")
def api_network():
    target_neq = request.args.get("neq", "").strip()
    if not target_neq:
        return jsonify({"nodes": [], "links": []})
    _, fusion_list, names_map = get_data()
    if not fusion_list:
        return jsonify({"nodes": [], "links": []})
    nodes_set: set = set()
    links: List[Dict[str, str]] = []
    # Filtrer toutes les lignes où target_neq apparaît soit comme NEQ, soit
    # comme NEQ_ASSUJ_REL. On ne récupère que les relations directes pour
    # éviter de surcharger la visualisation.
    for row in fusion_list:
        src = row.get("NEQ_ASSUJ_REL", "").strip()
        dst = row.get("NEQ", "").strip()
        relation = row.get("COD_RELA_ASSUJ", "").strip()
        if not src or not dst:
            continue
        if src == target_neq or dst == target_neq:
            links.append({"source": src, "target": dst, "relation": relation})
            nodes_set.update([src, dst])
    nodes = [{"id": n, "name": names_map.get(n, n)} for n in nodes_set]
    return jsonify({"nodes": nodes, "links": links})


if __name__ == "__main__":
    # Démarre la tâche de mise à jour en arrière‑plan
    schedule_update()
    # Précharge les données lors du démarrage pour réduire le délai de la
    # première requête. Si le fichier n'existe pas encore, la tâche de mise à
    # jour le téléchargera et remplira le cache.
    get_data()
    app.run(debug=True, host="0.0.0.0")
