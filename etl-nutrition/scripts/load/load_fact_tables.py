import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg://user:pass@localhost:5432/food")


def upsert_dim_product(df: pd.DataFrame, con):
    cols_map = {
        "code": "code",
        "product_name": "name",
        "brands": "brand",
        "categories": "category",
        "nutriscore_grade": "nutriscore_grade",
    }
    cols = [c for c in cols_map if c in df.columns]
    if not cols:
        return
    dim = df[cols].rename(columns=cols_map).drop_duplicates("code")
    sql = text(
        """
        INSERT INTO dim_product (code, name, brand, category, nutriscore_grade)
        VALUES (:code, :name, :brand, :category, :nutriscore_grade)
        ON CONFLICT (code) DO UPDATE SET
          name = EXCLUDED.name,
          brand = EXCLUDED.brand,
          category = EXCLUDED.category,
          nutriscore_grade = EXCLUDED.nutriscore_grade
        """
    )
    rows = dim.to_dict(orient="records")
    con.execute(sql, rows)


def ensure_nutrients(nutrient_names: list[str], con):
    existing = pd.read_sql("SELECT name, nutrient_id FROM dim_nutrient", con)
    existing_names = set(existing["name"]) if not existing.empty else set()
    to_add = [n for n in nutrient_names if n not in existing_names]
    if to_add:
        sql = text(
            """
            INSERT INTO dim_nutrient (name, unit)
            VALUES (:name, :unit)
            ON CONFLICT (name) DO NOTHING
            """
        )
        con.execute(sql, [{"name": n, "unit": "per_100g"} for n in to_add])
    return pd.read_sql("SELECT name, nutrient_id FROM dim_nutrient", con)


def pivot_nutrients(df: pd.DataFrame) -> pd.DataFrame:
    # supposer colonnes *_100g sont des nutriments
    nut_cols = [c for c in df.columns if c.endswith("_100g")]
    melted = df[["code"] + nut_cols].melt(
        id_vars="code", var_name="name", value_name="value_per_100g"
    )
    return melted


def main(in_path: str = "data/processed/products_harmonized.jsonl"):
    engine = create_engine(DB_URL, future=True)
    df = pd.read_json(in_path, lines=True)
    with engine.begin() as con:
        upsert_dim_product(df, con)
        m = pivot_nutrients(df)
        nutrient_index = ensure_nutrients(sorted(m["name"].unique()), con)
        m = m.merge(nutrient_index, on="name", how="left")
        facts = m[["code", "nutrient_id", "value_per_100g"]].dropna(
            subset=["value_per_100g"]
        )
        sql_facts = text(
            """
            INSERT INTO fact_product_nutrient (code, nutrient_id, value_per_100g)
            VALUES (:code, :nutrient_id, :value_per_100g)
            ON CONFLICT (code, nutrient_id) DO UPDATE SET
              value_per_100g = EXCLUDED.value_per_100g
            """
        )
        con.execute(sql_facts, facts.to_dict(orient="records"))
    print(f"Chargement terminé ({len(facts)} lignes dans fact_product_nutrient).")
    return True


if __name__ == "__main__":
    main()

