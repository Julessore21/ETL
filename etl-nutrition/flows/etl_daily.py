from prefect import flow, task
from scripts.extract.openfoodfacts import main as extract_off
from scripts.transform.harmonize_units import main as harmonize
from scripts.load.load_fact_tables import main as load_db

@task(retries=2, retry_delay_seconds=60)
def extract_task():
    return extract_off(page_size=1000, max_pages=2)  # limiter pour les tests

@task
def transform_task():
    # Ici, supposez que vous avez préalablement consolidé un jsonl intermédiaire
    # Pour la démonstration, on pointe directement vers 'data/processed/tmp_products.jsonl'
    return harmonize(in_path="data/processed/tmp_products.jsonl")

@task
def load_task():
    return load_db(in_path="data/processed/products_harmonized.jsonl")

@flow(name="ETL Nutrition Daily")
def run():
    extract_task()
    transform_task()
    load_task()

if __name__ == "__main__":
    run()
