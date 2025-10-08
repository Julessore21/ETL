# ETL Nutrition & Alimentation

## Commandes Majeures (Windows/PowerShell)
- Se placer dans le sous‑projet
  - `cd C:\Users\sore-larregain\Desktop\Workspace\Ynov\ETL\ETL\etl-nutrition`

- Docker Postgres
  - `docker compose up -d`
  - `docker compose ps`
  - `docker compose logs -f db`
  - `docker compose down`

- Dépendances Python
  - `pip install -r requirements.txt`

- Variables d’environnement (exemples)
  - ` $env:DB_URL = "postgresql+psycopg://user:pass@localhost:5432/food" `
  - ` $env:OFF_PAGE_SIZE = "1000" `
  - ` $env:OFF_MAX_PAGES = "50" `
  - ` $env:SMOKE_FAIL_ON_DQ = "1" `

- Test rapide (logs détaillés + rapport DQ JSON)
  - `python -m tests.run_pipeline_smoke`
  - Capturer logs: `python -m tests.run_pipeline_smoke *>&1 | Tee-Object -FilePath logs\smoke_$(Get-Date -Format yyyyMMdd_HHmmss).log`
  - Rapport: `logs\last_run_report_YYYYMMDD_HHMMSS.json`

- Flow Prefect (end‑to‑end)
  - `python flows/etl_daily.py`

- Étapes individuelles
  - Init schéma: `python -m database.init_db`
  - Extraction OFF: `python -m scripts.extract.openfoodfacts`
  - Consolidation: `python -m scripts.transform.consolidate_off`
  - Harmonisation: `python -m scripts.transform.harmonize_units`
  - Chargement DB: `python -m scripts.load.load_fact_tables`

- Lookup produit (scan code‑barres)
  - `python -m scripts.query.product_lookup <barcode>`

- Emplacements de données
  - Raw: `data\raw\YYYYMMDD\openfoodfacts\off_p*.json`
  - Intermédiaire: `data\processed\tmp_products.jsonl`
  - Harmonisé: `data\processed\products_harmonized.jsonl`

Ce dépôt contient le **pipeline ETL** (Extract–Transform–Load) du projet fil rouge *Application Data Nutrition & Alimentation* (Masters Data & IA – Ynov 2025–2026).  
Objectif : produire un **jeu de données propre, structuré et centralisé** à partir de sources variées (Open Food Facts, Agribalyse, sites de recettes) pour la visualisation et l’IA.

---

## 🚀 Pile technique
- **Python 3.11** (pandas, requests, pydantic, SQLAlchemy)
- **Orchestration** : Prefect
- **Base** : PostgreSQL 15 (ou SQLite pour développement rapide)
- **Qualité de données** : Great Expectations (ou Pandera)
- **Conteneurisation** : Docker + docker-compose
- **Logs** : logging Python + métadonnées d’exécution

---

## 🗂️ Arborescence
```
etl-nutrition/
├─ README.md
├─ requirements.txt
├─ docker-compose.yml
├─ .env.example
├─ configs/
│  ├─ sources.yaml
│  └─ mappings.yaml
├─ data/
│  ├─ raw/
│  └─ processed/
├─ database/
│  ├─ schema.sql
│  ├─ seeds/
│  └─ loaders/
├─ scripts/
│  ├─ extract/
│  │  └─ openfoodfacts.py
│  ├─ transform/
│  │  └─ harmonize_units.py
│  └─ load/
│     └─ load_fact_tables.py
├─ flows/
│  └─ etl_daily.py
└─ tests/
   └─ expectations/
```
> Placez **exactement** chaque fichier comme indiqué ci-dessus.

---

## 📥 Étape 1 — Extraction
- Sources cibles : **Open Food Facts (OFF)**, **Agribalyse**, sites de recettes (Marmiton, 750g).
- Bonnes pratiques : respect de *robots.txt*, *rate limiting*, versionnement des schémas, stockage brut dans `data/raw/YYYYMMDD/` avec **manifest.json**.

Script fourni : `scripts/extract/openfoodfacts.py`  
- Paramétrable (taille de page, champs).  
- Sauvegarde des pages JSON dans `data/raw/<date>/openfoodfacts/` + `manifest.json`.

---

## 🧹 Étape 2 — Transformation
Objectifs : nettoyage, standardisation des unités (g, kcal, kJ, mg), normalisation des colonnes (snake_case), enrichissement (scores).  
Script fourni : `scripts/transform/harmonize_units.py`  
- Utilise `configs/mappings.yaml` pour convertir les unités et fixer les cibles (`nutrient_targets`).  
- Enregistre un fichier harmonisé dans `data/processed/products_harmonized.jsonl`.

---

## 🗄️ Étape 3 — Chargement
- Schéma **étoile** minimal : dimensions (produit, nutriment, source) + faits (valeurs nutritionnelles, impacts).  
- DDL : `database/schema.sql`  
- Loader : `scripts/load/load_fact_tables.py` (SQLAlchemy) qui **insère** les dimensions et pivote les nutriments vers la table de faits.

---

## 🧭 Orchestration
`flows/etl_daily.py` orchestre **extract → transform → load** via Prefect (avec *retries*).

Exécuter localement :
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DB_URL="postgresql+psycopg://user:pass@localhost:5432/food"
python flows/etl_daily.py
```

Avec Docker :
```bash
cp .env.example .env   # éditez les variables
docker-compose up -d   # lance Postgres + Prefect UI (optionnel)
```

---

## 🔎 Qualité & tests
- Règles d’exemple : unicité de `code`, valeurs plausibles (kcal, sodium), couverture de nutriments clés.
- Ajoutez vos suites **Great Expectations** dans `tests/expectations/` et exécutez-les sur `data/processed/` et après *load* (requêtes SQL).

---

## 🔐 Sécurité & conformité
- Respect des licences et conditions d’utilisation des sources.
- Secrets dans `.env` (jamais commit).
- Traçabilité : chaque run génère un **manifest** (date, URL, hash, volumes).

---

## 🗓️ Planning (rappel)
- S1 : setup repo, extraction OFF.
- S2 : Agribalyse/mapping, transformations v1, DDL.
- S3 : data quality, orchestration Prefect.
- S4 : gel des données, doc & répétition.

---

## 👣 Commandes utiles
```bash
# Install
pip install -r requirements.txt

# Lancer un run ETL
python flows/etl_daily.py

# Inspecter la DB (exemple psql)
psql postgresql://user:pass@localhost:5432/food -c "\dt"
```
