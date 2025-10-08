from sqlalchemy import create_engine, text
from pathlib import Path
import os


def main(schema_path: str = "database/schema.sql") -> bool:
    db_url = os.getenv("DB_URL", "postgresql+psycopg://user:pass@localhost:5432/food")
    engine = create_engine(db_url, future=True)
    sql = Path(schema_path).read_text(encoding="utf-8")
    with engine.begin() as con:
        con.execute(text(sql))
    print("Base initialisée (schema.sql exécuté)")
    return True


if __name__ == "__main__":
    main()

