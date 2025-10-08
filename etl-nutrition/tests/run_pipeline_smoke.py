import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime

from sqlalchemy import create_engine, text

# Import pipeline modules
from database.init_db import main as init_db
from scripts.extract.openfoodfacts import main as extract_off
from scripts.transform.consolidate_off import main as consolidate_off
from scripts.transform.harmonize_units import main as harmonize
from scripts.load.load_fact_tables import main as load_db


def _log(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


def _count_lines(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0


def _db_counts(db_url: str) -> dict:
    engine = create_engine(db_url, future=True)
    with engine.connect() as con:
        res = {}
        for table in ["dim_product", "dim_nutrient", "fact_product_nutrient"]:
            try:
                res[table] = con.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
            except Exception:
                res[table] = None
        return res


def main():
    start = time.time()
    db_url = os.getenv("DB_URL", "postgresql+psycopg://user:pass@localhost:5432/food")

    # Tune extraction for a quick smoke test
    os.environ.setdefault("OFF_PAGE_SIZE", "200")

    _log("Démarrage du smoke test ETL")
    _log(f"DB_URL = {db_url}")

    # 1) Init DB
    t0 = time.time()
    _log("[1/5] Initialisation de la base (schema.sql)")
    init_db()
    _log(f"→ Base initialisée en {time.time()-t0:.2f}s")

    # 2) Extract OFF
    t0 = time.time()
    _log("[2/5] Extraction OpenFoodFacts (1-2 pages)")
    manifest = extract_off(max_pages=1)
    raw_dir = None
    try:
        date_prefix = datetime.now().strftime("%Y%m%d")
        raw_dir = Path(f"data/raw/{date_prefix}/openfoodfacts")
        page_files = sorted(raw_dir.glob("off_p*.json"))
        _log(f"→ {len(page_files)} fichier(s) page dans {raw_dir}")
    except Exception:
        pass
    _log(f"→ Manifest: {json.dumps(manifest, ensure_ascii=False)}")
    _log(f"→ Extraction terminée en {time.time()-t0:.2f}s")

    # 3) Consolidate/flatten
    t0 = time.time()
    _log("[3/5] Consolidation & flatten OFF → tmp_products.jsonl")
    tmp_path = Path(consolidate_off())
    tmp_lines = _count_lines(tmp_path)
    _log(f"→ Fichier: {tmp_path} ({tmp_lines} lignes)")
    _log(f"→ Consolidation terminée en {time.time()-t0:.2f}s")

    # 4) Harmonize units
    t0 = time.time()
    _log("[4/5] Harmonisation des unités → products_harmonized.jsonl")
    out_path = Path(harmonize(in_path=str(tmp_path)))
    out_lines = _count_lines(out_path)
    _log(f"→ Fichier: {out_path} ({out_lines} lignes)")
    # Afficher un échantillon de 2 lignes
    try:
        with out_path.open("r", encoding="utf-8") as f:
            for i in range(2):
                ln = f.readline().strip()
                if ln:
                    _log(f"exemple[{i}] {ln[:500]}")
    except Exception:
        pass
    _log(f"→ Harmonisation terminée en {time.time()-t0:.2f}s")

    # 5) Load into DB
    t0 = time.time()
    _log("[5/5] Chargement DB (upsert dims + facts)")
    load_db(in_path=str(out_path))
    counts = _db_counts(db_url)
    _log(f"→ Comptes DB: {counts}")
    _log(f"→ Chargement terminé en {time.time()-t0:.2f}s")

    _log(f"Durée totale: {time.time()-start:.2f}s")
    _log("Smoke test ETL terminé")


if __name__ == "__main__":
    sys.exit(main())

