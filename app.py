#!/usr/bin/env python3
import os
import sqlite3
import smtplib
from datetime import datetime
from email.message import EmailMessage

def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


load_dotenv()

from flask import Flask, render_template, request, redirect, url_for, flash

APP_SECRET = os.environ.get("FLASK_SECRET_KEY", "dev")
DB_PATH = os.environ.get("ALERTS_DB", "alerts.db")
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5000"))

app = Flask(__name__)
app.secret_key = APP_SECRET
app.config["ALERTS_DB"] = DB_PATH


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                threshold REAL NOT NULL,
                email_to TEXT NOT NULL,
                term_expr TEXT,
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
        cur.execute("PRAGMA table_info(alerts)")
        cols = {row[1] for row in cur.fetchall()}
        if "term_expr" not in cols:
            cur.execute("ALTER TABLE alerts ADD COLUMN term_expr TEXT")
        conn.commit()


init_db()


@app.get("/")
def index():
    with get_db() as conn:
        alerts = conn.execute(
            "SELECT id, name, threshold, email_to, term_expr, active, created_at FROM alerts ORDER BY id DESC"
        ).fetchall()
    return render_template("index.html", alerts=alerts)

def get_smtp_config() -> dict:
    host = os.environ.get("ALERT_SMTP_HOST")
    port = int(os.environ.get("ALERT_SMTP_PORT", "587"))
    user = os.environ.get("ALERT_SMTP_USER")
    password = os.environ.get("ALERT_SMTP_PASS")
    mail_from = os.environ.get("ALERT_SMTP_FROM")
    starttls_raw = os.environ.get("ALERT_SMTP_STARTTLS", "1").strip().lower()
    starttls = starttls_raw not in {"0", "false", "no"}
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "from": mail_from,
        "starttls": starttls,
    }

def send_email_alert(subject: str, body: str, smtp_cfg: dict, mail_to: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_cfg["from"]
    msg["To"] = mail_to
    msg.set_content(body)
    with smtplib.SMTP(smtp_cfg["host"], smtp_cfg["port"], timeout=20) as server:
        if smtp_cfg["starttls"]:
            server.starttls()
        if smtp_cfg["user"] and smtp_cfg["password"]:
            server.login(smtp_cfg["user"], smtp_cfg["password"])
        server.send_message(msg)

def parse_term_expression(expr: str) -> tuple[float | None, float | None]:
    expr = (expr or "").strip().lower()
    if not expr:
        return None, None

    cleaned = expr.replace("months", "").replace("month", "").replace("mos", "").replace("mo", "")
    cleaned = cleaned.replace("to", "-")
    cleaned = cleaned.replace(" ", "")

    if cleaned.startswith((">=", "<=", ">", "<")):
        op = cleaned[:2] if cleaned[:2] in {">=", "<="} else cleaned[:1]
        val_str = cleaned[len(op):]
        val = float(val_str)
        if op == ">=":
            return val, None
        if op == ">":
            return val + 1e-9, None
        if op == "<=":
            return None, val
        if op == "<":
            return None, val - 1e-9

    if cleaned.endswith("+"):
        val = float(cleaned[:-1])
        return val, None

    if "-" in cleaned:
        lo, hi = cleaned.split("-", 1)
        return float(lo), float(hi)

    if cleaned.startswith(("=", "==")):
        val = float(cleaned.lstrip("="))
        return val, val

    if "exact" in expr:
        val = float(cleaned.replace("exactly", "").replace("exact", ""))
        return val, val

    val = float(cleaned)
    return val, val


@app.post("/alerts")
def create_alert():
    name = (request.form.get("name") or "").strip()
    threshold_raw = (request.form.get("threshold") or "").strip()
    email_to = (request.form.get("email_to") or "").strip()
    term_expr = (request.form.get("term_expr") or "").strip()
    active = 1 if request.form.get("active") == "on" else 0

    if not name or not threshold_raw or not email_to:
        flash("Name, threshold, and email are required.", "error")
        return redirect(url_for("index"))

    try:
        threshold = float(threshold_raw)
    except ValueError:
        flash("Threshold must be a number.", "error")
        return redirect(url_for("index"))

    if term_expr:
        try:
            parse_term_expression(term_expr)
        except Exception:
            flash("Term qualifier is invalid. Examples: 12+, 12-24, 3, >=6.", "error")
            return redirect(url_for("index"))

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO alerts (name, threshold, email_to, term_expr, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, threshold, email_to, term_expr or None, active, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()

    flash("Alert created.", "success")
    return redirect(url_for("index"))

@app.post("/alerts/<int:alert_id>/test")
def test_alert(alert_id: int):
    smtp_cfg = get_smtp_config()
    if not smtp_cfg["host"] or not smtp_cfg["from"]:
        flash("SMTP is not configured. Set ALERT_SMTP_HOST and ALERT_SMTP_FROM.", "error")
        return redirect(url_for("index"))

    with get_db() as conn:
        alert = conn.execute(
            "SELECT id, name, threshold, email_to, term_expr FROM alerts WHERE id = ?",
            (alert_id,),
        ).fetchone()
    if not alert:
        flash("Alert not found.", "error")
        return redirect(url_for("index"))

    subject = f"Test alert: {alert['name']}"
    term_note = alert["term_expr"] or "Any term"
    body = (
        "This is a test alert from Apples to Apples.\n"
        f"Alert: {alert['name']}\n"
        f"Threshold: ${alert['threshold']:.4f}/kWh\n"
        f"Term qualifier: {term_note}\n"
    )
    try:
        send_email_alert(subject, body, smtp_cfg, alert["email_to"])
        flash(f"Test alert sent to {alert['email_to']}.", "success")
    except Exception as exc:
        flash(f"Failed to send test alert: {exc}", "error")
    return redirect(url_for("index"))


@app.post("/alerts/<int:alert_id>/toggle")
def toggle_alert(alert_id: int):
    with get_db() as conn:
        cur = conn.execute("SELECT active FROM alerts WHERE id = ?", (alert_id,))
        row = cur.fetchone()
        if not row:
            flash("Alert not found.", "error")
            return redirect(url_for("index"))
        new_val = 0 if row["active"] else 1
        conn.execute("UPDATE alerts SET active = ? WHERE id = ?", (new_val, alert_id))
        conn.commit()
    flash("Alert updated.", "success")
    return redirect(url_for("index"))


@app.post("/alerts/<int:alert_id>/delete")
def delete_alert(alert_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
        conn.execute("DELETE FROM alert_history WHERE alert_id = ?", (alert_id,))
        conn.commit()
    flash("Alert deleted.", "success")
    return redirect(url_for("index"))


@app.get("/history")
def history():
    alert_id = request.args.get("alert_id")
    params = []
    where = ""
    if alert_id:
        where = "WHERE h.alert_id = ?"
        params.append(alert_id)
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT h.id, h.alert_id, h.triggered_at, h.price, h.term_months, h.supplier, h.message,
                   a.name, a.email_to
            FROM alert_history h
            JOIN alerts a ON a.id = h.alert_id
            {where}
            ORDER BY h.triggered_at DESC
            LIMIT 200
            """,
            params,
        ).fetchall()
        alerts = conn.execute(
            "SELECT id, name FROM alerts ORDER BY name"
        ).fetchall()
    return render_template("history.html", rows=rows, alerts=alerts, selected_alert_id=alert_id)


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=True)
