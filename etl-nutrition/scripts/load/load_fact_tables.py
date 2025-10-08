import os, json
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg://user:pass@localhost:5432/food")

def upsert_dim_product(df: pd.DataFrame, con):
    cols_map = {
        "code":"code",
        "product_name":"name",
        "brands":"brand",
        "categories":"category",
        "nutriscore_grade":"nutriscore_grade",
    }
    dim = df[[c for c in cols_map if c in df.columns]].rename(columns=cols_map).drop_duplicates("code")
    # naive upsert: try insert; on conflict do nothing (requires psql COPY or manual merge for performance)
    dim.to_sql("dim_product", con=con, if_exists="append", index=False, method="multi")

def ensure_nutrients(nutrient_names: list[str], con):
    # insert if not exists
    existing = pd.read_sql("SELECT name, nutrient_id FROM dim_nutrient", con)
    to_add = [n for n in nutrient_names if n not in set(existing["name"])]
    if to_add:
        df = pd.DataFrame({"name": to_add, "unit": ["per_100g"]*len(to_add)})
        df.to_sql("dim_nutrient", con=con, if_exists="append", index=False, method="multi")
    return pd.read_sql("SELECT name, nutrient_id FROM dim_nutrient", con)

def pivot_nutrients(df: pd.DataFrame) -> pd.DataFrame:
    # supposer colonnes *_100g sont des nutriments
    nut_cols = [c for c in df.columns if c.endswith("_100g")]
    melted = df[["code"] + nut_cols].melt(id_vars="code", var_name="name", value_name="value_per_100g")
    return melted

def main(in_path: str = "data/processed/products_harmonized.jsonl"):
    engine = create_engine(DB_URL, future=True)
    df = pd.read_json(in_path, lines=True)
    with engine.begin() as con:
        upsert_dim_product(df, con)
        m = pivot_nutrients(df)
        nutrient_index = ensure_nutrients(sorted(m["name"].unique()), con)
        m = m.merge(nutrient_index, on="name", how="left")
        facts = m[["code","nutrient_id","value_per_100g"]].dropna(subset=["value_per_100g"])
        facts.to_sql("fact_product_nutrient", con=con, if_exists="append", index=False, method="multi")
    print(f"Chargement terminé ({len(facts)} lignes dans fact_product_nutrient).")
    return True

if __name__ == "__main__":
    main()
