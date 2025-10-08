from pathlib import Path
import requests, json, time, os
from datetime import datetime

def main(page_size: int = 1000, max_pages: int | None = None) -> dict:
    base_url = os.getenv("OFF_BASE_URL", "https://world.openfoodfacts.org/cgi/search.pl")
    fields = os.getenv("OFF_FIELDS", "code,product_name,brands,nutriscore_grade,nutriments,ingredients_text,categories")
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
        r = requests.get(base_url, params={**params, "page": page}, timeout=60, headers={"User-Agent":"etl-nutrition/1.0"})
        r.raise_for_status()
        data = r.json()
        products = data.get("products", [])
        if not products:
            break
        (out_dir / f"off_p{page:04d}.json").write_text(json.dumps(products, ensure_ascii=False), encoding="utf-8")
        total += len(products)
        page += 1
        time.sleep(0.4)  # courteous rate limit

    manifest = {"count": total, "pages": page-1, "base_url": base_url, "fields": fields, "generated_at": datetime.utcnow().isoformat()+"Z"}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Extraction OFF terminée: {total} produits, {page-1} pages → {out_dir}")
    return manifest

if __name__ == "__main__":
    main()
