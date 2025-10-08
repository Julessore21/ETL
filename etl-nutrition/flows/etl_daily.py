import os
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from prefect import flow, task
from scripts.extract.openfoodfacts import main as extract_off
from scripts.transform.consolidate_off import main as consolidate_off
from scripts.transform.harmonize_units import main as harmonize
from scripts.load.load_fact_tables import main as load_db
from database.init_db import main as init_db


@task(retries=2, retry_delay_seconds=60)
def extract_task():
    # paramètres pris depuis configs/sources.yaml (env possible)
    max_pages_env = os.getenv("OFF_MAX_PAGES")
    max_pages = int(max_pages_env) if max_pages_env else None
    return extract_off(max_pages=max_pages)


@task
def consolidate_task():
    return consolidate_off()


@task
def transform_task(in_path: str):
    return harmonize(in_path=in_path)


@task
def load_task():
    return load_db(in_path="data/processed/products_harmonized.jsonl")


@flow(name="ETL Nutrition Daily")
def run():
    init_db()
    extract_task()
    tmp = consolidate_task()
    transform_task(tmp)
    load_task()


if __name__ == "__main__":
    run()
