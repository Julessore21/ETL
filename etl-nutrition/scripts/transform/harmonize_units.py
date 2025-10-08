import pandas as pd, yaml, json, os
from pathlib import Path

def load_conversions(cfg_path: str = "configs/mappings.yaml"):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    conv = pd.DataFrame(cfg["unit_conversions"])
    targets = cfg["nutrient_targets"]
    return conv, targets

def convert_value(val, from_u, to_u, conv_df):
    if pd.isna(val) or from_u is None or to_u is None:
        return val
    if from_u == to_u:
        return val
    row = conv_df[(conv_df["from"] == from_u) & (conv_df["to"] == to_u)]
    if row.empty:
        return val
    factor = float(row["factor"].iloc[0])
    return val * factor

def main(in_path: str = "data/processed/tmp_products.jsonl", out_path: str = "data/processed/products_harmonized.jsonl"):
    conv, targets = load_conversions()
    df = pd.read_json(in_path, lines=True)

    # Exemple : standardiser quelques nutriments connus
    for col, target_unit in targets.items():
        unit_col = f"{col}_unit"
        if col in df.columns:
            if unit_col not in df.columns:
                # heuristique: si le col est énergie et unités en kJ → créer unit col
                df[unit_col] = target_unit
            df[col] = df.apply(lambda r: convert_value(r[col], r.get(unit_col), target_unit, conv), axis=1)
            df[unit_col] = target_unit

    # Normalisation des noms de colonnes
    df.columns = [c.strip().lower().replace("-", "_") for c in df.columns]

    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    df.to_json(out_path, orient="records", lines=True, force_ascii=False)
    print(f"Harmonisation terminée → {out_path} ({len(df)} lignes)")
    return out_path

if __name__ == "__main__":
    main()
