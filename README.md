# Organigramme REQ Québec

Application Flask pour rechercher les entreprises du Québec et visualiser leurs relations sous forme de graphe interactif à partir du jeu de données ouvert du Registre des entreprises du Québec (REQ).

## Ce que le projet fait maintenant

- Télécharge automatiquement la dernière archive ZIP du REQ depuis Données Québec.
- Indexe les CSV dans une base SQLite locale pour rendre la recherche rapide.
- Affiche un graphe visuel des relations directes entre entités juridiques trouvées dans les fichiers REQ.
- Expose un endpoint de sync manuel `POST /api/sync`.
- Lance un sync automatique toutes les 24 heures tant que le service reste actif.

## Limite importante sur les personnes physiques

Le jeu de données ouvert du REQ anonymise les personnes physiques et les personnes liées. La page officielle du dataset précise que :

- les noms, prénoms et adresses des personnes physiques sont absents;
- les noms, prénoms et adresses des personnes liées comme les administrateurs sont absents.

Conséquence : l’application prépare déjà le modèle de données pour relier des personnes physiques aux compagnies, mais ces nœuds ne peuvent pas être remplis à grande échelle à partir du seul dataset ouvert. Pour afficher actionnaires, administrateurs et bénéficiaires ultimes, il faut une source complémentaire autorisée.

## Lancer le projet

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Application disponible sur `http://localhost:5000`.

## Forcer une synchronisation

```bash
python app.py sync
```

Ou via HTTP :

```bash
curl -X POST http://localhost:5000/api/sync
```

Si `ADMIN_SYNC_TOKEN` est défini, ajoute l'en-tête `X-Admin-Sync-Token`.

## Variables d'environnement

- `PORT` : port HTTP, par défaut `5000`
- `FLASK_DEBUG` : `1` pour activer le mode debug
- `AUTO_SYNC_ENABLED` : `1` ou `0`
- `UPDATE_INTERVAL_SECONDS` : fréquence du sync, par défaut `86400`
- `MAX_SEARCH_RESULTS` : taille de la liste de résultats, par défaut `20`
- `MAX_GRAPH_EDGES` : limite de liens par vue graphe, par défaut `250`
- `ADMIN_SYNC_TOKEN` : jeton simple pour protéger `POST /api/sync`
- `REQ_CKAN_PACKAGE_URL` : surcharge de l'URL CKAN si nécessaire
- `REQ_DATASET_ZIP_URL` : URL ZIP directe à utiliser si l'API CKAN renvoie `403`

## Déploiement

Le repo inclut :

- un `Procfile` pour lancer `gunicorn`
- un `render.yaml` pour préparer un déploiement Render

## Workflow conseillé

1. Déployer l'app web.
2. Définir `ADMIN_SYNC_TOKEN` sur l'hébergeur.
3. Si Données Québec bloque l’API CKAN côté serveur, définir aussi `REQ_DATASET_ZIP_URL`.
4. Mettre en place un cron côté hébergeur ou laisser l’auto-sync interne tourner toutes les 24 heures.

Cette approche est robuste pour le refresh du dataset ouvert. Pour les personnes physiques, il faudra décider de la source d’enrichissement avant de promettre un graphe complet entreprise/personne à l’échelle du Québec.
