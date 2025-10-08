from pathlib import Path
import requests, json, time, os, yaml
from datetime import datetime


def _load_source_cfg():
    cfg_path = Path(__file__).resolve().parents[2] / "configs" / "sources.yaml"
    base = {
        "openfoodfacts": {
            "base_url": "https://world.openfoodfacts.org/cgi/search.pl",
            "page_size": 1000,
            "fields": [
                "code",
                "product_name",
                "brands",
                "nutriscore_grade",
                "nutriments",
                "ingredients_text",
                "categories",
            ],
        }
    }
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            file_cfg = yaml.safe_load(f) or {}
        base.update(file_cfg)
    return base["openfoodfacts"]


def main(page_size: int | None = None, max_pages: int | None = None) -> dict:
    cfg = _load_source_cfg()
    base_url = os.getenv("OFF_BASE_URL", cfg.get("base_url"))
    fields_list = cfg.get("fields", [])
    # Allow override via env as comma-separated
    fields_env = os.getenv("OFF_FIELDS")
    if fields_env:
        fields_list = [f.strip() for f in fields_env.split(",") if f.strip()]
    fields = ",".join(fields_list)
    page_size = int(os.getenv("OFF_PAGE_SIZE", page_size or cfg.get("page_size", 1000)))
    params = {
        "action": "process",
        "json": 1,
        "page_size": page_size,
        "fields": fields,
    }
    date_prefix = datetime.now().strftime("%Y%m%d")
    out_dir = Path(f"data/raw/{date_prefix}/openfoodfacts")
    out_dir.mkdir(parents=True, exist_ok=True)

    page = 1
    total = 0
    while True:
        if max_pages and page > max_pages:
            break
        r = requests.get(
            base_url,
            params={**params, "page": page},
            timeout=60,
            headers={"User-Agent": "etl-nutrition/1.0"},
        )
        r.raise_for_status()
        data = r.json()
        products = data.get("products", [])
        if not products:
            break
        (out_dir / f"off_p{page:04d}.json").write_text(
            json.dumps(products, ensure_ascii=False), encoding="utf-8"
        )
        total += len(products)
        page += 1
        time.sleep(0.4)  # courteous rate limit

    manifest = {
        "count": total,
        "pages": page - 1,
        "base_url": base_url,
        "fields": fields,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Extraction OFF terminée: {total} produits, {page-1} pages → {out_dir}")
    return manifest


if __name__ == "__main__":
    main()

