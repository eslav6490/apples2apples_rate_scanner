CREATE TABLE IF NOT EXISTS offers (
  id BIGSERIAL PRIMARY KEY,
  snapshot_ts TIMESTAMP NOT NULL,
  supplier TEXT NOT NULL,
  price_dollars_per_kwh NUMERIC(10,5) NOT NULL,
  rate_type TEXT,
  term_months INTEGER,
  etf TEXT,
  etf_amount NUMERIC(10,2),
  monthly_fee TEXT,
  monthly_fee_amount NUMERIC(10,2),
  renewable TEXT,
  promo TEXT,
  intro_price TEXT,
  url TEXT,
  selection_type TEXT NOT NULL DEFAULT 'overall'
);

CREATE INDEX IF NOT EXISTS offers_snapshot_ts_idx ON offers (snapshot_ts);
CREATE INDEX IF NOT EXISTS offers_price_idx ON offers (price_dollars_per_kwh);
CREATE INDEX IF NOT EXISTS offers_selection_type_idx ON offers (selection_type);
