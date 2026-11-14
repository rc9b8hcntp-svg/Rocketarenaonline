# Rocket Arena 3D – serveur de matchmaking

Serveur Node.js (>= 18) **sans dépendance** : file d'attente Casual / Compétitif (par MMR), salons privés entre amis (code ou lien), relais temps réel des matchs. Il sert aussi le jeu (`public/index.html`), donc un seul service suffit.

## Tester en local
```
node server.js        # puis ouvre http://localhost:8080 dans 2 onglets
```
Onglet 1 et 2 : choisis Soccar 1v1 → JOUER : les deux joueurs sont mis dans le même match.

## Mettre en ligne (Render, gratuit)
1. Mets ce dossier dans un dépôt GitHub.
2. render.com → New → Blueprint (il lit `render.yaml`) ou New → Web Service : Build `true`, Start `node server.js`.
3. Ouvre l'URL Render : le jeu s'y charge et se connecte tout seul au serveur.

Si tu héberges le jeu ailleurs (GitHub Pages…), ouvre-le une fois avec `?mm=wss://TON-APP.onrender.com` (l'adresse est mémorisée), ou écris-la dans `MM_DEFAULT_URL` en haut du bloc « EN LIGNE » du HTML.

## Variables d'environnement (facultatif)
- `FILL_SECONDS` (20) : attente avant de compléter avec des bots en Casual (le classé n'est jamais complété).
- `ALLOWED_ORIGINS` : liste d'origines autorisées, séparées par des virgules.
- `MAX_CONNECTIONS` (1000).

`/health` et `/stats` (joueurs en ligne, files, salons) sont disponibles en HTTP.

## Limites connues
- Le joueur « hôte » du match arbitre (buts, chrono) : un tricheur hôte pourrait tricher. Si l'hôte part, un autre joueur prend le relais automatiquement.
- Render gratuit s'endort après ~15 min sans visite (premier chargement ~50 s).
