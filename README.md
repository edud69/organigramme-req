# Organigramme REQ Québec

Application Flask pour rechercher les entreprises du Québec et visualiser leurs relations sous forme de graphe interactif à partir du jeu de données ouvert du Registre des entreprises du Québec (REQ).

## Ce que le projet fait maintenant

- Télécharge automatiquement la dernière archive ZIP du REQ depuis Données Québec.
- Indexe les CSV dans une base persistante via `DATABASE_URL` quand elle est disponible.
- Affiche un graphe visuel des relations directes entre entités juridiques trouvées dans les fichiers REQ.
- Peut enrichir progressivement certaines entreprises à partir de la consultation publique officielle pour récupérer administrateurs, dirigeants, actionnaires et bénéficiaires ultimes.
- Expose un endpoint de sync manuel `POST /api/sync`.
- Lance un sync automatique toutes les 24 heures tant que le service reste actif.

## Limite importante sur les personnes physiques

Le jeu de données ouvert du REQ anonymise les personnes physiques et les personnes liées. La page officielle du dataset précise que :

- les noms, prénoms et adresses des personnes physiques sont absents;
- les noms, prénoms et adresses des personnes liées comme les administrateurs sont absents.

Conséquence : l’application prépare déjà le modèle de données pour relier des personnes physiques aux compagnies, mais ces nœuds ne peuvent pas être remplis à grande échelle à partir du seul dataset ouvert. Pour afficher actionnaires, administrateurs et bénéficiaires ultimes, il faut une source complémentaire autorisée.

## Enrichissement public incrémental

Le projet sait maintenant tenter un enrichissement supplémentaire via la consultation publique officielle du registre.

Principe :

- le ZIP REQ reste la source de base pour toutes les entreprises;
- une seconde passe visite un nombre limité de fiches publiques;
- les personnes physiques extraites sont ajoutées comme nœuds `person`;
- les liens `administrateur`, `dirigeant`, `actionnaire` et `bénéficiaire ultime` sont ajoutés quand ils sont détectés.

Limite importante :

- le site officiel est protégé par Cloudflare;
- l’enrichissement automatique peut demander une vérification humaine ponctuelle dans Chrome;
- il faut donc traiter cet enrichissement comme un processus incrémental et opportuniste, pas comme une garantie de couverture complète de tout le Québec chaque nuit.

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

Par défaut, `POST /api/sync` lance le sync en arrière-plan et répond tout de suite. Pour attendre la fin côté client, ajoute `?wait=1`.

## Variables d'environnement

- `PORT` : port HTTP, par défaut `5000`
- `FLASK_DEBUG` : `1` pour activer le mode debug
- `AUTO_SYNC_ENABLED` : `1` ou `0`
- `UPDATE_INTERVAL_SECONDS` : fréquence du sync, par défaut `86400`
- `MAX_SEARCH_RESULTS` : taille de la liste de résultats, par défaut `20`
- `MAX_GRAPH_EDGES` : limite de liens par vue graphe, par défaut `250`
- `ADMIN_SYNC_TOKEN` : jeton simple pour protéger `POST /api/sync`
- `DATABASE_URL` : base de données persistante. Recommandé en production, notamment avec Render Postgres
- `REQ_CKAN_PACKAGE_URL` : surcharge de l'URL CKAN si nécessaire
- `REQ_DATASET_ZIP_URL` : URL ZIP directe à utiliser si l'API CKAN renvoie `403`
- `REQ_DATA_DIR` : dossier local temporaire pour l'archive ZIP, par défaut `/tmp/organigramme-req` quand `DATABASE_URL` est défini
- `REQ_PUBLIC_ENRICH_ENABLED` : `1` pour activer l’enrichissement depuis le registre public
- `REQ_PUBLIC_ENRICH_LIMIT` : nombre de fiches publiques à enrichir par sync, par défaut `25`
- `REQ_PUBLIC_HEADLESS` : `0` recommandé pour pouvoir résoudre un défi Cloudflare si nécessaire
- `REQ_PUBLIC_BROWSER_CHANNEL` : canal navigateur Playwright pour l’enrichissement public, par défaut `chrome`
- `REQ_PUBLIC_CHALLENGE_TIMEOUT_SECONDS` : temps d’attente d’une éventuelle vérification Cloudflare avant échec

## Déploiement

Le repo inclut :

- un `Procfile` pour lancer `gunicorn`
- un `render.yaml` pour préparer un déploiement Render
- un workflow GitHub Actions pour exécuter le sync directement dans Supabase/Postgres

## Workflow conseillé

1. Déployer l'app web.
2. Créer une base Postgres persistante, par exemple Supabase.
3. Définir `DATABASE_URL` sur le web service avec la chaîne de connexion de cette base.
4. Définir `ADMIN_SYNC_TOKEN` sur l'hébergeur si tu veux garder le endpoint `/api/sync` pour debug manuel.
5. Définir `REQ_DATASET_ZIP_URL`.
6. Utiliser GitHub Actions pour exécuter `python app.py sync` chaque nuit directement vers Postgres.

Cette approche est robuste pour le refresh du dataset ouvert. Pour les personnes physiques, il faudra décider de la source d’enrichissement avant de promettre un graphe complet entreprise/personne à l’échelle du Québec.

Pour activer l’enrichissement public incrémental sur ton Mac, ajoute par exemple :

```bash
REQ_PUBLIC_ENRICH_ENABLED=1
REQ_PUBLIC_ENRICH_LIMIT=10
REQ_PUBLIC_HEADLESS=0
```

Recommandation pratique :

- commence avec une petite limite, par exemple `5` ou `10`;
- laisse Chrome visible;
- si une page Cloudflare apparaît, résous-la une fois dans le navigateur;
- les syncs suivants réutiliseront le profil navigateur persistant.

## GitHub Actions

Le workflow nocturne n'appelle plus Render. Il exécute directement le job d'ingestion et écrit dans `DATABASE_URL`.

Secrets GitHub à définir :

- `DATABASE_URL` : chaîne Postgres Supabase ou autre base persistante
- `REQ_DATASET_ZIP_URL` : URL directe du ZIP REQ

## Automatisation locale macOS

Quand le REQ bloque les téléchargements depuis des IP cloud, la solution la plus fiable est d'exécuter le sync chaque nuit depuis ton Mac, puis d'écrire dans Supabase.

Fichiers fournis :

- script : [scripts/run_req_sync.sh](/Users/matt/Documents/Codex/organigramme-req/scripts/run_req_sync.sh)
- exemple d'environnement : [.env.local.example](/Users/matt/Documents/Codex/organigramme-req/.env.local.example)
- job `launchd` : [launchd/com.organigramme.req-sync.plist](/Users/matt/Documents/Codex/organigramme-req/launchd/com.organigramme.req-sync.plist)

Étapes :

1. Créer `.env.local` à la racine du projet à partir de `.env.local.example`.
2. Y mettre au minimum :
   - `DATABASE_URL` pour l'app web
   - `SYNC_DATABASE_URL` pour le job local de sync
   - `REQ_DATASET_ZIP_URL`
   - `REQ_DOWNLOAD_MODE=browser`
3. Installer les dépendances Python et Playwright :

```bash
cd /Users/matt/Documents/Codex/organigramme-req
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chrome
```

4. Rendre le script exécutable :

```bash
chmod +x /Users/matt/Documents/Codex/organigramme-req/scripts/run_req_sync.sh
```

5. Créer le dossier de logs :

```bash
mkdir -p /Users/matt/Documents/Codex/organigramme-req/logs
```

6. Installer le job `launchd` :

```bash
cp /Users/matt/Documents/Codex/organigramme-req/launchd/com.organigramme.req-sync.plist ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/com.organigramme.req-sync.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.organigramme.req-sync.plist
```

7. Lancer un test manuel :

```bash
launchctl start com.organigramme.req-sync
```

8. Consulter les logs :

```bash
tail -f /Users/matt/Documents/Codex/organigramme-req/logs/req-sync.log
```

Le job est configuré pour tourner tous les jours à `03:15` heure locale. Tu peux changer l'heure dans le fichier plist.

Notes :

- `REQ_DOWNLOAD_MODE=browser` force l’usage d’un vrai navigateur piloté par Playwright.
- `REQ_BROWSER_HEADLESS=0` garde le navigateur visible, ce qui se rapproche davantage d’un usage humain.
- si Chrome n'est pas installé, tu peux utiliser Chromium via `python -m playwright install chromium` et vider `REQ_BROWSER_CHANNEL`.
- tu peux utiliser le pooler Supabase dans `DATABASE_URL` et aussi dans `SYNC_DATABASE_URL`.
- si la connexion directe Supabase fonctionne sur ta machine, tu peux aussi l'utiliser pour `SYNC_DATABASE_URL`, mais ce n'est pas obligatoire.
- pour l’enrichissement public, `REQ_PUBLIC_HEADLESS=0` est fortement recommandé.
