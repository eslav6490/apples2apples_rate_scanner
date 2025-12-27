#!/usr/bin/env python3
import os
import sqlite3
from datetime import datetime

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


init_db()


@app.get("/")
def index():
    with get_db() as conn:
        alerts = conn.execute(
            "SELECT id, name, threshold, email_to, active, created_at FROM alerts ORDER BY id DESC"
        ).fetchall()
    return render_template("index.html", alerts=alerts)


@app.post("/alerts")
def create_alert():
    name = (request.form.get("name") or "").strip()
    threshold_raw = (request.form.get("threshold") or "").strip()
    email_to = (request.form.get("email_to") or "").strip()
    active = 1 if request.form.get("active") == "on" else 0

    if not name or not threshold_raw or not email_to:
        flash("Name, threshold, and email are required.", "error")
        return redirect(url_for("index"))

    try:
        threshold = float(threshold_raw)
    except ValueError:
        flash("Threshold must be a number.", "error")
        return redirect(url_for("index"))

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO alerts (name, threshold, email_to, active, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, threshold, email_to, active, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()

    flash("Alert created.", "success")
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
