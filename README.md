# Organigramme des entreprises du Québec

Cette application Flask permet de visualiser les relations d'entreprises du Registre des entreprises du Québec (REQ) sous la forme d'un organigramme interactif. Elle charge les fichiers `Nom.csv` et `FusionScission.csv` du jeu de données du REQ, extrait les noms des entreprises et construit un graphe des relations de fusion/scission entre NEQ.

## Mise à jour des données

Le script `update_dataset` télécharge automatiquement la dernière version du registre via l'API CKAN de Données Québec et planifie une vérification toutes les 24 heures. Les fichiers CSV sont extraits et rechargés sans redémarrage.

> **Licence** : Les données du Registre des entreprises sont publiques et anonymisées (les noms et adresses des personnes physiques ne figurent pas dans les fichiers), et sont diffusées sous licence CC BY‑NC‑SA 4.0【122191560308607†L150-L156】【122191560308607†L176-L183】. Elles sont mises à jour deux fois par mois【122191560308607†L160-L170】.

## Déploiement

Pour déployer l'application sur une plateforme d'hébergement, installez les dépendances avec :

```【】bash
pip install -r requirements.txt
```

puis lancez le serveur Flask :

```【】bash
python app.py
```

L'application écoute par défaut sur le port 5000.
