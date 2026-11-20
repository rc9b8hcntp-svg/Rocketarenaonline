# Rocket Arena 3D – serveur de matchmaking

Serveur Node.js (>= 18) **sans dépendance** : file d'attente Casual / Compétitif (par MMR), **comptes avec vraie liste d'amis**, **groupes** (4 joueurs, recherche de match en équipe) et **matchs privés réservés aux groupes**, relais temps réel des matchs. Il sert aussi le jeu (`public/index.html`), donc un seul service suffit.

## Tester en local
```
node server.js        # puis ouvre http://localhost:8080 dans 2 onglets
```
Onglet 1 et 2 : choisis Soccar 1v1 → JOUER : les deux joueurs sont mis dans le même match.
Les amis et les groupes demandent un compte (connexion Firebase du jeu) : un invité peut jouer en ligne mais n'a ni amis ni groupe.

## Mettre en ligne (Render, gratuit)
1. Mets ce dossier dans un dépôt GitHub.
2. render.com → New → Blueprint (il lit `render.yaml`) ou New → Web Service : Build `true`, Start `node server.js`.
3. Ouvre l'URL Render : le jeu s'y charge et se connecte tout seul au serveur.

Si tu héberges le jeu ailleurs (GitHub Pages…), ouvre-le une fois avec `?mm=wss://TON-APP.onrender.com` (l'adresse est mémorisée), ou écris-la dans `MM_DEFAULT_URL` en haut du bloc « EN LIGNE » du HTML.

## Amis, groupes et matchs privés
- **Compte** : le jeu envoie le jeton de connexion Firebase ; le serveur le vérifie (signature RS256, certificats Google, projet `FIREBASE_PROJECT_ID`, par défaut `rocketarena`). Chaque compte reçoit un **code ami** (ex. `K7M2-QX4P`).
- **Amis** : on s'ajoute avec le code, l'autre accepte ou refuse. Si deux joueurs s'ajoutent mutuellement, l'amitié est directe. La présence (en ligne, en groupe, en recherche, en match, hors ligne) est envoyée aux amis en temps réel.
- **Groupe** : on invite un ami en ligne (60 s pour répondre), 4 joueurs maximum. Le chef lance la recherche de match : le groupe entre dans la file **ensemble et reste dans la même équipe** (le serveur l'assemble avec des joueurs seuls ou d'autres groupes). Si un membre part, la recherche s'annule.
- **Match privé** : uniquement depuis un groupe de **2 joueurs ou plus**. Le chef choisit mode / format / durée, chaque joueur choisit son équipe (Bleu / Orange). Les salons à code et les liens d'invitation n'existent plus.
- **Sauvegarde** : les comptes et les listes d'amis sont écrits dans `DATA_DIR/accounts.json` (par défaut `./data`). **Sur un hébergeur à disque éphémère (Render gratuit, conteneur sans volume), les amis seraient perdus à chaque redémarrage : monte un disque ou un volume persistant sur `DATA_DIR`.**

## Variables d'environnement (facultatif)
- `DATA_DIR` (`./data`) : dossier des comptes / amis.
- `FIREBASE_PROJECT_ID` (`rocketarena`) : projet Firebase dont les jetons sont acceptés.
- `FILL_SECONDS` (20) : attente avant de compléter avec des bots en Casual (le classé n'est jamais complété).
- `ALLOWED_ORIGINS` : liste d'origines autorisées, séparées par des virgules.
- `MAX_CONNECTIONS` (1000).

`/health` et `/stats` (joueurs en ligne, files, salons, groupes, comptes) sont disponibles en HTTP.

## Limites connues
- Le serveur doit pouvoir joindre `googleapis.com` (certificats de vérification des jetons) ; sinon personne ne peut se connecter à son compte côté serveur (le jeu en ligne reste possible en invité).
- Un seul appareil par compte à la fois : une nouvelle connexion déconnecte l'ancienne.
- Le joueur « hôte » du match arbitre (buts, chrono) : un tricheur hôte pourrait tricher. Si l'hôte part, un autre joueur prend le relais automatiquement.
- Render gratuit s'endort après ~15 min sans visite (premier chargement ~50 s).
