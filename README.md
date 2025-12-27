# apples2apples_rate_scanner

Scrapes the Ohio Apples to Apples page, filters to fixed-rate offers with $0 monthly fee and $0 early termination fee, and stores:
- the overall cheapest qualifying offer per run
- the cheapest qualifying offer per term length

Data can be written to PostgreSQL and/or CSV.

## Requirements
- Python 3.11+
- PostgreSQL (optional but recommended)

## Setup
```bash
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install requests beautifulsoup4 lxml psycopg2-binary
```

## Database
Create the table:
```bash
psql -d apples_db -f schema.sql
```

Set connection environment variables:
```bash
export PGHOST=127.0.0.1
export PGPORT=5432
export PGDATABASE=apples_db
export PGUSER=apples_app
export PGPASSWORD='your_password'
```

You can also use `APPLES_DB_DSN` instead of the individual `PG*` variables.

## Run
```bash
./venv/bin/python apples_v2.py
```

Disable CSV output (DB only):
```bash
./venv/bin/python apples_v2.py --no-csv
```

By default CSV is written to `apples_to_apples_snapshot_v2.csv`.

## Selection types
Rows are tagged as:
- `overall`: cheapest qualifying offer across all terms
- `term_best`: cheapest qualifying offer for each term length

## Cron example
```cron
PGHOST=127.0.0.1
PGPORT=5432
PGDATABASE=apples_db
PGUSER=apples_app
PGPASSWORD=...     # or use ~/.pgpass
5 2 * * * /path/to/venv/bin/python /path/to/apples_v2.py --insecure --no-csv >> /var/log/apples_v2/daily.log 2>&1
```

## PowerBI
Use the PostgreSQL connector. If you see an SSL certificate error, set SSL Mode to `Disable` in Advanced options.
