from pathlib import Path
import json
from datetime import datetime


TARGET_FILE = Path("data/processed/tmp_products.jsonl")


def _latest_off_dir() -> Path | None:
    root = Path("data/raw")
    if not root.exists():
        return None
    dated = [p for p in root.iterdir() if p.is_dir()]
    if not dated:
        return None
    latest = sorted(dated, key=lambda p: p.name)[-1]
    off = latest / "openfoodfacts"
    return off if off.exists() else None


def _flatten_product(p: dict) -> dict:
    out: dict = {}
    # Basic fields
    out["code"] = p.get("code")
    out["product_name"] = p.get("product_name")
    out["brands"] = p.get("brands")
    out["categories"] = p.get("categories")
    out["ingredients_text"] = p.get("ingredients_text")
    out["nutriscore_grade"] = p.get("nutriscore_grade")

    nutr = p.get("nutriments", {}) or {}
    # Energy: prefer kcal if available
    kcal = nutr.get("energy-kcal_100g")
    kj = nutr.get("energy-kj_100g")
    if kcal is not None:
        out["energy_100g"] = kcal
        out["energy_100g_unit"] = "kcal"
    elif kj is not None:
        out["energy_100g"] = kj
        out["energy_100g_unit"] = "kJ"

    # Macros (assume grams per 100g)
    for key in [
        ("fat_100g", "g"),
        ("saturated-fat_100g", "g"),
        ("carbohydrates_100g", "g"),
        ("sugars_100g", "g"),
        ("fiber_100g", "g"),
        ("proteins_100g", "g"),
        ("salt_100g", "g"),
        ("sodium_100g", "g"),  # OFF often in grams
    ]:
        col, unit = key
        val = nutr.get(col)
        if val is not None:
            out[col] = val
            out[f"{col}_unit"] = unit

    return out


def main() -> str:
    src_dir = _latest_off_dir()
    if not src_dir:
        raise FileNotFoundError("No OpenFoodFacts raw directory found under data/raw/*/openfoodfacts")

    # Collect all page files
    pages = sorted(src_dir.glob("off_p*.json"))
    TARGET_FILE.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with TARGET_FILE.open("w", encoding="utf-8") as out:
        for f in pages:
            try:
                products = json.loads(f.read_text(encoding="utf-8"))
            except Exception as e:
                # skip malformed page
                continue
            for p in products:
                flat = _flatten_product(p)
                if flat.get("code"):
                    out.write(json.dumps(flat, ensure_ascii=False) + "\n")
                    count += 1

    print(f"Consolidation OFF: {count} produits → {TARGET_FILE}")
    return str(TARGET_FILE)


if __name__ == "__main__":
    main()

