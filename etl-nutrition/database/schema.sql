-- Schéma relationnel minimal (étoile) pour nutrition & impacts
CREATE TABLE IF NOT EXISTS dim_source(
  source_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  version TEXT,
  url TEXT,
  extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product(
  code TEXT PRIMARY KEY,
  name TEXT,
  brand TEXT,
  category TEXT,
  nutriscore_grade TEXT,
  source_id INT REFERENCES dim_source(source_id),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_nutrient(
  nutrient_id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  unit TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_product_nutrient(
  code TEXT REFERENCES dim_product(code) ON UPDATE CASCADE ON DELETE CASCADE,
  nutrient_id INT REFERENCES dim_nutrient(nutrient_id) ON UPDATE CASCADE,
  value_per_100g DOUBLE PRECISION,
  PRIMARY KEY (code, nutrient_id)
);

CREATE TABLE IF NOT EXISTS fact_product_impact(
  code TEXT REFERENCES dim_product(code) ON UPDATE CASCADE ON DELETE CASCADE,
  indicator TEXT,
  value DOUBLE PRECISION,
  unit TEXT,
  PRIMARY KEY (code, indicator)
);

-- Index utiles
CREATE INDEX IF NOT EXISTS idx_dim_product_brand ON dim_product(brand);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);
CREATE INDEX IF NOT EXISTS idx_fact_product_nutrient_nutrient ON fact_product_nutrient(nutrient_id);
