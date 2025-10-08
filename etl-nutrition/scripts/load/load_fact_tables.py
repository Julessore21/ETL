import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg://user:pass@localhost:5432/food")


def ensure_source(con, name: str = "OpenFoodFacts", version: str | None = None, url: str | None = None) -> int:
    sql = text(
        """
        INSERT INTO dim_source (name, version, url)
        VALUES (:name, :version, :url)
        ON CONFLICT (source_id) DO NOTHING
        RETURNING source_id
        """
    )
    # Try insert; if no returning (conflict path), fetch existing
    res = con.execute(sql, {"name": name, "version": version, "url": url}).first()
    if res and len(res) == 1:
        return int(res[0])
    # Fallback: select by name
    q = text("SELECT source_id FROM dim_source WHERE name = :name ORDER BY extracted_at DESC LIMIT 1")
    sid = con.execute(q, {"name": name}).scalar_one()
    return int(sid)


def upsert_dim_product(df: pd.DataFrame, con, source_id: int | None = None):
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
    if source_id is not None:
        dim["source_id"] = source_id
    sql = text(
        """
        INSERT INTO dim_product (code, name, brand, category, nutriscore_grade, source_id)
        VALUES (:code, :name, :brand, :category, :nutriscore_grade, :source_id)
        ON CONFLICT (code) DO UPDATE SET
          name = EXCLUDED.name,
          brand = EXCLUDED.brand,
          category = EXCLUDED.category,
          nutriscore_grade = EXCLUDED.nutriscore_grade,
          source_id = COALESCE(EXCLUDED.source_id, dim_product.source_id)
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
        src_id = ensure_source(con, name="OpenFoodFacts", url="https://world.openfoodfacts.org/")
        upsert_dim_product(df, con, source_id=src_id)
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
