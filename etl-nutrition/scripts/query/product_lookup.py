import os
import sys
from typing import Optional
from sqlalchemy import create_engine, text


def lookup(code: str, top_nutrients: int = 8) -> Optional[dict]:
    db_url = os.getenv("DB_URL", "postgresql+psycopg://user:pass@localhost:5432/food")
    engine = create_engine(db_url, future=True)
    with engine.connect() as con:
        p = con.execute(
            text(
                """
                SELECT code, name, brand, category, nutriscore_grade
                FROM dim_product WHERE code = :code
                """
            ),
            {"code": code},
        ).mappings().first()
        if not p:
            return None
        nutrients = con.execute(
            text(
                """
                SELECT n.name, f.value_per_100g
                FROM fact_product_nutrient f
                JOIN dim_nutrient n USING (nutrient_id)
                WHERE f.code = :code AND f.value_per_100g IS NOT NULL
                ORDER BY n.name ASC
                LIMIT :lim
                """
            ),
            {"code": code, "lim": top_nutrients},
        ).mappings().all()
        return {"product": dict(p), "nutrients": [dict(x) for x in nutrients]}


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.query.product_lookup <barcode>")
        return 2
    code = sys.argv[1]
    res = lookup(code)
    if not res:
        print(f"Code {code} introuvable dans la base.")
        return 1
    print(res)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

