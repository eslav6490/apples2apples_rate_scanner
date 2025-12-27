#!/usr/bin/env python3
"""
Ohio Apples to Apples scraper — v2
Filters out ANY offer that is variable-rate OR has a non-zero monthly fee OR a non-zero early termination fee.
Finds the lowest qualifying electric rate at run time and appends a snapshot to CSV.

Usage examples:
  python apples_v2.py
  python apples_v2.py --insecure
  python apples_v2.py --csv "C:\\Users\\evanx\\Desktop\\rates_v2.csv"
"""

import sys
import os
import re
import csv
import time
import argparse
import json
import smtplib
import sqlite3
from email.message import EmailMessage
from datetime import datetime

import requests
from bs4 import BeautifulSoup
try:
    import psycopg2
except Exception:
    psycopg2 = None

try:
    import urllib3
except Exception:
    urllib3 = None

DEFAULT_URL = "https://energychoice.ohio.gov/ApplesToApplesComparision.aspx?Category=Electric&TerritoryId=4&RateCode=1"
DEFAULT_DBNAME = "apples_db"
DEFAULT_DBHOST = "localhost"
DEFAULT_DBPORT = "5432"

# ---------- HTTP ----------
def fetch_html(url: str, retries: int = 3, timeout: int = 25, verify: bool = True) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://energychoice.ohio.gov/",
    }
    last_err = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, verify=verify)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.5)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")

# ---------- parsing helpers ----------
def norm_header(txt: str) -> str:
    t = re.sub(r"\s+", " ", txt.strip().lower())
    mapping = {
        "click to compare": "compare",
        "compare": "compare",
        "supplier": "supplier",
        "company": "supplier",
        "$/kwh": "price",
        "price": "price",
        "rate type": "rate_type",
        "renew. content": "renewable",
        "renewable content": "renewable",
        "intro. price": "intro_price",
        "intro price": "intro_price",
        "term. length": "term",
        "term length": "term",
        "early term. fee": "etf",
        "early termination fee": "etf",
        "monthly fee": "monthly_fee",
        "promo. offers": "promo",
        "promo offers": "promo",
    }
    return mapping.get(t, t)

def parse_dollars_per_kwh(s: str):
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", s.replace(",", ""))
    return float(m.group(1)) if m else None

def parse_term_months(s: str):
    if not s:
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None

def parse_money_to_float(s: str):
    """Return a float dollar amount if clearly numeric, else None.
    Handles "$0", "$0.00", "0", "0.00", etc. If it contains any non-numeric besides symbols, try to parse first number.
    """
    if not s:
        return None
    s = s.strip()
    # common placeholders meaning zero or none
    if s in {"—", "-", "n/a", "na", "none", "no", "not applicable"}:
        return None  # unknown, treat as disqualifying later
    m = re.search(r"(-?\d+(?:\.\d+)?)", s.replace(",", ""))
    return float(m.group(1)) if m else None

def find_offers_table(soup: BeautifulSoup):
    # Prefer a table that contains both Supplier and $/kWh headers
    for tbl in soup.select("table"):
        headers = [norm_header(th.get_text(" ", strip=True)) for th in tbl.select("thead th")]
        if not headers:
            first = tbl.find("tr")
            if first:
                headers = [norm_header(x.get_text(" ", strip=True)) for x in first.find_all(["th", "td"])]
        if "price" in headers and "supplier" in headers:
            return tbl, headers
    return None, []

def extract_rows(html: str):
    soup = BeautifulSoup(html, "lxml")
    tbl, headers = find_offers_table(soup)
    if not tbl:
        raise RuntimeError("Could not locate the offers table on the page.")

    idx = {name: i for i, name in enumerate(headers)}
    trs = tbl.select("tbody tr") or tbl.find_all("tr")
    rows = []

    for tr in trs:
        tds = tr.find_all("td")
        if not tds:
            continue

        # pad for missing cells
        cells = tds + [None] * max(0, len(headers) - len(tds))

        # supplier
        supplier_cell = cells[idx.get("supplier", 0)]
        supplier = ""
        if supplier_cell:
            span = supplier_cell.find("span", class_="retail-title")
            supplier = span.get_text(" ", strip=True) if span else supplier_cell.get_text(" ", strip=True)

        # price
        price_cell = cells[idx.get("price", 0)]
        price = parse_dollars_per_kwh(price_cell.get_text(" ", strip=True) if price_cell else "")

        if not supplier or price is None:
            # probably a spacer or non-offer row
            continue

        rate_type = cells[idx["rate_type"]].get_text(" ", strip=True) if "rate_type" in idx and cells[idx["rate_type"]] else ""
        term = parse_term_months(cells[idx["term"]].get_text(" ", strip=True)) if "term" in idx and cells[idx["term"]] else None
        etf_str = cells[idx["etf"]].get_text(" ", strip=True) if "etf" in idx and cells[idx["etf"]] else ""
        monthly_fee_str = cells[idx["monthly_fee"]].get_text(" ", strip=True) if "monthly_fee" in idx and cells[idx["monthly_fee"]] else ""
        renewable = cells[idx["renewable"]].get_text(" ", strip=True) if "renewable" in idx and cells[idx["renewable"]] else ""
        promo = cells[idx["promo"]].get_text(" ", strip=True) if "promo" in idx and cells[idx["promo"]] else ""
        intro_price = cells[idx["intro_price"]].get_text(" ", strip=True) if "intro_price" in idx and cells[idx["intro_price"]] else ""

        etf_amt = parse_money_to_float(etf_str)
        monthly_fee_amt = parse_money_to_float(monthly_fee_str)

        rows.append({
            "supplier": supplier,
            "price_dollars_per_kwh": price,
            "rate_type": rate_type,
            "term_months": term,
            "etf": etf_str,
            "etf_amount": etf_amt,
            "monthly_fee": monthly_fee_str,
            "monthly_fee_amount": monthly_fee_amt,
            "renewable": renewable,
            "promo": promo,
            "intro_price": intro_price,
        })
    return rows

def qualifies_v2(row: dict) -> bool:
    """Return True if offer is Fixed (not Variable), Monthly Fee == 0, and Early Termination Fee == 0.
    If fee fields can't be parsed (None), treat as NOT qualifying to be safe.
    """
    rate = (row.get("rate_type") or "").lower()
    if rate.startswith("variable"):
        return False
    # Only accept explicit zeros
    etf_amt = row.get("etf_amount")
    monthly_amt = row.get("monthly_fee_amount")
    return (etf_amt == 0.0) and (monthly_amt == 0.0)

def choose_lowest(rows):
    filtered = [r for r in rows if r.get("price_dollars_per_kwh") is not None and qualifies_v2(r)]
    if not filtered:
        return None
    # sort by price asc, then by longer term desc
    return min(filtered, key=lambda r: (r["price_dollars_per_kwh"], -(r["term_months"] or -1)))

def choose_lowest_per_term(rows):
    filtered = [r for r in rows if r.get("price_dollars_per_kwh") is not None and qualifies_v2(r)]
    by_term = {}
    for row in filtered:
        term = row.get("term_months")
        if term is None:
            continue
        best = by_term.get(term)
        if best is None:
            by_term[term] = row
            continue
        if (row["price_dollars_per_kwh"], row.get("supplier", "")) < (
            best["price_dollars_per_kwh"],
            best.get("supplier", ""),
        ):
            by_term[term] = row
    return [by_term[term] for term in sorted(by_term)]

def _db_dsn_from_env() -> str | None:
    dsn = os.environ.get("APPLES_DB_DSN")
    if dsn:
        return dsn
    if not os.environ.get("PGUSER") or not os.environ.get("PGPASSWORD"):
        return None
    host = os.environ.get("PGHOST", DEFAULT_DBHOST)
    port = os.environ.get("PGPORT", DEFAULT_DBPORT)
    dbname = os.environ.get("PGDATABASE", DEFAULT_DBNAME)
    user = os.environ.get("PGUSER")
    password = os.environ.get("PGPASSWORD")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"

def write_snapshot_to_db(rows: list[dict], snapshot_ts: str, url: str) -> bool:
    if psycopg2 is None:
        print("Warning: psycopg2 not available; skipping DB insert.", file=sys.stderr)
        return False
    dsn = _db_dsn_from_env()
    if not dsn:
        return False
    sql = """
        INSERT INTO offers (
            snapshot_ts, supplier, price_dollars_per_kwh, rate_type, term_months,
            etf, etf_amount, monthly_fee, monthly_fee_amount, renewable, promo, intro_price, url,
            selection_type
        ) VALUES (
            %(snapshot_ts)s, %(supplier)s, %(price_dollars_per_kwh)s, %(rate_type)s, %(term_months)s,
            %(etf)s, %(etf_amount)s, %(monthly_fee)s, %(monthly_fee_amount)s, %(renewable)s, %(promo)s,
            %(intro_price)s, %(url)s, %(selection_type)s
        )
    """
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                for row in rows:
                    payload = row.copy()
                    payload["snapshot_ts"] = snapshot_ts
                    payload["url"] = url
                    cur.execute(sql, payload)
        return True
    except Exception as exc:
        print(f"Warning: could not write DB snapshot: {exc}", file=sys.stderr)
        return False

def _env_or_arg(val, env_key: str):
    return val if val is not None else os.environ.get(env_key)

def send_email_alert(subject: str, body: str, smtp_host: str, smtp_port: int, smtp_user: str | None,
                     smtp_pass: str | None, smtp_from: str, smtp_to: str, starttls: bool = True) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = smtp_to
    msg.set_content(body)
    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
        if starttls:
            server.starttls()
        if smtp_user and smtp_pass:
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)

def ensure_alerts_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                threshold REAL NOT NULL,
                email_to TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                triggered_at TEXT NOT NULL,
                price REAL NOT NULL,
                term_months INTEGER,
                supplier TEXT,
                message TEXT,
                FOREIGN KEY(alert_id) REFERENCES alerts(id)
            )
            """
        )
        conn.commit()

def load_alerts(db_path: str) -> list[dict]:
    if not db_path or not os.path.exists(db_path):
        return []
    ensure_alerts_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, threshold, email_to, active FROM alerts WHERE active = 1 ORDER BY id DESC"
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]

def record_alert_history(db_path: str, alert_id: int, triggered_at: str, price: float,
                         term_months: int | None, supplier: str, message: str) -> None:
    if not db_path:
        return
    ensure_alerts_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO alert_history (alert_id, triggered_at, price, term_months, supplier, message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (alert_id, triggered_at, price, term_months, supplier, message),
        )
        conn.commit()

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Find the lowest qualifying (Fixed, no ETF, no monthly fee) electric rate on Ohio Apples to Apples.")
    ap.add_argument("--url", default=DEFAULT_URL, help="Results URL to scrape.")
    ap.add_argument("--csv", default="apples_to_apples_snapshot_v2.csv", help="CSV to append the best snapshot.")
    ap.add_argument("--no-csv", action="store_true", help="Disable CSV output (use DB only).")
    ap.add_argument("--json", action="store_true", help="Print JSON for the best offer.")
    ap.add_argument("--top", type=int, default=5, help="Also print top N qualifying offers.")
    ap.add_argument(
        "--alert-below",
        type=float,
        default=None,
        help="Alert if the best price for any term falls below this $/kWh value.",
    )
    ap.add_argument("--smtp-host", default=None, help="SMTP host for alerts. Env: ALERT_SMTP_HOST")
    ap.add_argument("--smtp-port", type=int, default=None, help="SMTP port. Env: ALERT_SMTP_PORT (default 587)")
    ap.add_argument("--smtp-user", default=None, help="SMTP username. Env: ALERT_SMTP_USER")
    ap.add_argument("--smtp-pass", default=None, help="SMTP password. Env: ALERT_SMTP_PASS")
    ap.add_argument("--smtp-from", default=None, help="SMTP from address. Env: ALERT_SMTP_FROM")
    ap.add_argument("--smtp-to", default=None, help="SMTP to address. Env: ALERT_SMTP_TO")
    ap.add_argument("--alerts-db", default=None, help="SQLite alerts DB path. Env: ALERTS_DB")
    ap.add_argument("--smtp-starttls", action="store_true", help="Enable STARTTLS (default).")
    ap.add_argument("--no-smtp-starttls", action="store_true", help="Disable STARTTLS.")
    ap.add_argument("--insecure", action="store_true", help="Skip TLS certificate verification.")
    args = ap.parse_args()

    if args.insecure and urllib3:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    html = fetch_html(args.url, verify=not args.insecure)
    rows = extract_rows(html)
    best = choose_lowest(rows)
    best_per_term = choose_lowest_per_term(rows)

    if not rows or not best:
        print("No qualifying offers found (fixed rate with $0 monthly fee and $0 ETF).")
        sys.exit(2)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    p = best["price_dollars_per_kwh"]
    term = best["term_months"]
    tstr = f"{term} mo" if term else "n/a"
    print(f"[{ts}] Lowest qualifying rate: ${p:.4f}/kWh - {best['supplier']} | {best.get('rate_type','')} | term {tstr}")

    # Top N
    topN = sorted(
        (r for r in rows if r.get("price_dollars_per_kwh") is not None and qualifies_v2(r)),
        key=lambda r: (r["price_dollars_per_kwh"], -(r["term_months"] or -1))
    )[:args.top]
    for i, r in enumerate(topN, 1):
        t = f"{r['term_months']} mo" if r['term_months'] else "n/a"
        print(f"  {i}. ${r['price_dollars_per_kwh']:.4f}/kWh - {r['supplier']} | {r.get('rate_type','')} | {t} | ETF: {r.get('etf','')} | Monthly: {r.get('monthly_fee','')}")

    db_rows = []
    overall_row = best.copy()
    overall_row["selection_type"] = "overall"
    db_rows.append(overall_row)
    for row in best_per_term:
        term_row = row.copy()
        term_row["selection_type"] = "term_best"
        db_rows.append(term_row)
    db_written = write_snapshot_to_db(db_rows, ts, args.url)

    if args.alert_below is not None and best_per_term:
        # Alert when any term's best price drops below the threshold.
        tripped = [r for r in best_per_term if r["price_dollars_per_kwh"] < args.alert_below]
        if tripped:
            lines = []
            for r in sorted(tripped, key=lambda x: (x["price_dollars_per_kwh"], x.get("term_months") or 0)):
                t = f"{r['term_months']} mo" if r["term_months"] else "n/a"
                lines.append(
                    f"${r['price_dollars_per_kwh']:.4f}/kWh below ${args.alert_below:.4f} "
                    f"for term {t} - {r['supplier']}"
                )
            for line in lines:
                print(f"ALERT: {line}")

            smtp_host = _env_or_arg(args.smtp_host, "ALERT_SMTP_HOST")
            smtp_port = int(_env_or_arg(args.smtp_port, "ALERT_SMTP_PORT") or 587)
            smtp_user = _env_or_arg(args.smtp_user, "ALERT_SMTP_USER")
            smtp_pass = _env_or_arg(args.smtp_pass, "ALERT_SMTP_PASS")
            smtp_from = _env_or_arg(args.smtp_from, "ALERT_SMTP_FROM")
            smtp_to = _env_or_arg(args.smtp_to, "ALERT_SMTP_TO")
            starttls = True
            if args.no_smtp_starttls:
                starttls = False
            elif args.smtp_starttls:
                starttls = True

            if smtp_host and smtp_from and smtp_to:
                subject = "Apples to Apples rate alert"
                body = "Alert triggered:\n" + "\n".join(lines)
                try:
                    send_email_alert(
                        subject,
                        body,
                        smtp_host,
                        smtp_port,
                        smtp_user,
                        smtp_pass,
                        smtp_from,
                        smtp_to,
                        starttls=starttls,
                    )
                    print(f"Email alert sent to {smtp_to}.")
                except Exception as exc:
                    print(f"Warning: failed to send email alert: {exc}", file=sys.stderr)
            else:
                print("Warning: alert triggered but SMTP is not fully configured.", file=sys.stderr)

    alerts_db = _env_or_arg(args.alerts_db, "ALERTS_DB")
    if alerts_db:
        alerts = load_alerts(alerts_db)
        if alerts and best_per_term:
            smtp_host = _env_or_arg(args.smtp_host, "ALERT_SMTP_HOST")
            smtp_port = int(_env_or_arg(args.smtp_port, "ALERT_SMTP_PORT") or 587)
            smtp_user = _env_or_arg(args.smtp_user, "ALERT_SMTP_USER")
            smtp_pass = _env_or_arg(args.smtp_pass, "ALERT_SMTP_PASS")
            smtp_from = _env_or_arg(args.smtp_from, "ALERT_SMTP_FROM")
            starttls = True
            if args.no_smtp_starttls:
                starttls = False
            elif args.smtp_starttls:
                starttls = True

            for alert in alerts:
                threshold = alert["threshold"]
                tripped = [r for r in best_per_term if r["price_dollars_per_kwh"] < threshold]
                if not tripped:
                    continue
                lines = []
                for r in sorted(tripped, key=lambda x: (x["price_dollars_per_kwh"], x.get("term_months") or 0)):
                    t = f"{r['term_months']} mo" if r["term_months"] else "n/a"
                    msg = f"${r['price_dollars_per_kwh']:.4f}/kWh below ${threshold:.4f} for term {t} - {r['supplier']}"
                    lines.append(msg)
                    record_alert_history(
                        alerts_db,
                        alert["id"],
                        ts,
                        r["price_dollars_per_kwh"],
                        r["term_months"],
                        r.get("supplier", ""),
                        msg,
                    )
                for line in lines:
                    print(f"ALERT: {line}")

                if smtp_host and smtp_from and alert.get("email_to"):
                    subject = f"Apples to Apples rate alert: {alert['name']}"
                    body = "Alert triggered:\n" + "\n".join(lines)
                    try:
                        send_email_alert(
                            subject,
                            body,
                            smtp_host,
                            smtp_port,
                            smtp_user,
                            smtp_pass,
                            smtp_from,
                            alert["email_to"],
                            starttls=starttls,
                        )
                        print(f"Email alert sent to {alert['email_to']} for '{alert['name']}'.")
                    except Exception as exc:
                        print(f"Warning: failed to send email alert for '{alert['name']}': {exc}", file=sys.stderr)
                else:
                    print(
                        f"Warning: alert '{alert['name']}' triggered but SMTP is not fully configured.",
                        file=sys.stderr,
                    )

    # Append snapshot to CSV
    if not args.no_csv:
        fieldnames = [
            "timestamp","supplier","price_dollars_per_kwh","rate_type","term_months",
            "etf","etf_amount","monthly_fee","monthly_fee_amount","renewable","promo","intro_price","url"
        ]
        try:
            with open(args.csv, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                if f.tell() == 0:
                    w.writeheader()
                row = best.copy()
                row["timestamp"] = ts
                row["url"] = args.url
                w.writerow(row)
        except Exception as e:
            print(f"Warning: could not write CSV '{args.csv}': {e}", file=sys.stderr)
    elif not db_written:
        print("Warning: CSV disabled but DB insert did not run. Check DB env vars.", file=sys.stderr)

    if args.json:
        print(json.dumps(best, indent=2))

if __name__ == "__main__":
    main()
