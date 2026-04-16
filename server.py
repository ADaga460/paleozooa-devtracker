"""
Paleozooa Metrics Collector
Deploy on Render (or any Python host). Receives events from the live site,
stores them in SQLite, and serves aggregated stats via a JSON API.

Endpoints:
  POST /api/ingest       — receive an event from the frontend
  GET  /api/stats        — aggregated stats (JSON)
  GET  /api/stats/stream — SSE stream, pushes stats every 5s
  GET  /api/events       — raw recent events (last 200)
  GET  /api/health       — health check
  GET  /                 — minimal status page

Env vars:
  PORT           — port to listen on (default 8060)
  COLLECTOR_KEY  — optional auth key; if set, ingest requires ?key=<value>
  DB_PATH        — SQLite file path (default ./paleozooa_metrics.db)
"""

import os
import json
import time
import sqlite3
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta

PORT = int(os.environ.get("PORT", 8060))
COLLECTOR_KEY = os.environ.get("COLLECTOR_KEY", "")
DB_PATH = os.environ.get("DB_PATH", "./paleozooa_metrics.db")

# --- Database ---

_local = threading.local()

def get_db() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA synchronous=NORMAL")
    return _local.conn

def init_db():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT NOT NULL,
            data TEXT NOT NULL DEFAULT '{}',
            session_id TEXT,
            timestamp TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_event ON events(event)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON events(timestamp)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_session ON events(session_id)")
    db.commit()

def insert_event(event: str, data: dict, session_id: str | None, timestamp: str):
    db = get_db()
    db.execute(
        "INSERT INTO events (event, data, session_id, timestamp) VALUES (?, ?, ?, ?)",
        (event, json.dumps(data), session_id, timestamp),
    )
    db.commit()

def aggregate_stats() -> dict:
    db = get_db()
    now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")
    week_ago = (now - timedelta(days=7)).isoformat()

    total_events = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    # Game starts
    starts = db.execute("""
        SELECT json_extract(data, '$.mode') as mode,
               json_extract(data, '$.difficulty') as diff,
               COUNT(*) as c
        FROM events WHERE event='game_start'
        GROUP BY mode, diff
    """).fetchall()
    games_by_mode = {"daily": {}, "practice": {}}
    total_started = 0
    for mode, diff, c in starts:
        mode = mode or "practice"
        diff = diff or "normal"
        games_by_mode.setdefault(mode, {})[diff] = c
        total_started += c

    # Completions
    completions = db.execute("""
        SELECT json_extract(data, '$.mode') as mode,
               json_extract(data, '$.won') as won,
               json_extract(data, '$.guessCount') as gc,
               COUNT(*) as c
        FROM events WHERE event='game_complete'
        GROUP BY mode, won
    """).fetchall()
    total_completed = 0
    total_wins = 0
    total_guesses_completed = 0
    wins_by_mode = {"daily": 0, "practice": 0}
    completed_by_mode = {"daily": 0, "practice": 0}
    for mode, won, gc, c in completions:
        mode = mode or "practice"
        total_completed += c
        completed_by_mode[mode] = completed_by_mode.get(mode, 0) + c
        if won in (True, 1, "true", "True"):
            total_wins += c
            wins_by_mode[mode] = wins_by_mode.get(mode, 0) + c

    # Average guesses
    avg_row = db.execute("""
        SELECT AVG(CAST(json_extract(data, '$.guessCount') AS REAL))
        FROM events WHERE event='game_complete'
    """).fetchone()
    avg_guesses = round(avg_row[0] or 0, 2)

    win_rate = round((total_wins / total_completed * 100) if total_completed > 0 else 0, 2)

    # Hints
    hints = db.execute("""
        SELECT json_extract(data, '$.type') as t, COUNT(*) as c
        FROM events WHERE event='hint_used' GROUP BY t
    """).fetchall()
    hints_used = {"period": 0, "tree": 0}
    for t, c in hints:
        if t in hints_used:
            hints_used[t] = c
    hint_total = sum(hints_used.values())
    hint_rate = round((hint_total / total_completed * 100) if total_completed > 0 else 0, 2)

    # Top first guesses
    first_guesses = db.execute("""
        SELECT json_extract(data, '$.organismId') as oid, COUNT(*) as c
        FROM events WHERE event='guess' AND json_extract(data, '$.guessNumber')=1
        GROUP BY oid ORDER BY c DESC LIMIT 15
    """).fetchall()

    # Most guessed mystery organisms (from game_complete)
    top_mysteries = db.execute("""
        SELECT json_extract(data, '$.organismId') as oid, COUNT(*) as c
        FROM events WHERE event='game_complete'
        GROUP BY oid ORDER BY c DESC LIMIT 15
    """).fetchall()

    # Page views
    page_views = db.execute("""
        SELECT json_extract(data, '$.page') as p, COUNT(*) as c
        FROM events WHERE event='page_view' GROUP BY p ORDER BY c DESC
    """).fetchall()

    # Learn views (organism detail pages)
    learn_views = db.execute("""
        SELECT json_extract(data, '$.organismId') as oid, COUNT(*) as c
        FROM events WHERE event='learn_view'
        GROUP BY oid ORDER BY c DESC LIMIT 15
    """).fetchall()

    # Shares
    share_count = db.execute("SELECT COUNT(*) FROM events WHERE event='share'").fetchone()[0]

    # Difficulty changes
    diff_changes = db.execute("""
        SELECT json_extract(data, '$.to') as t, COUNT(*) as c
        FROM events WHERE event='difficulty_change' GROUP BY t
    """).fetchall()

    # Mode changes
    mode_changes = db.execute("""
        SELECT json_extract(data, '$.to') as t, COUNT(*) as c
        FROM events WHERE event='mode_change' GROUP BY t
    """).fetchall()

    # Sessions
    unique_sessions = db.execute(
        "SELECT COUNT(DISTINCT session_id) FROM events WHERE session_id IS NOT NULL"
    ).fetchone()[0]

    # Session durations from session_end events
    avg_session = db.execute("""
        SELECT AVG(CAST(json_extract(data, '$.durationMs') AS REAL))
        FROM events WHERE event='session_end'
    """).fetchone()
    avg_session_ms = round(avg_session[0] or 0)

    # Errors
    error_count = db.execute("SELECT COUNT(*) FROM events WHERE event='error'").fetchone()[0]
    recent_errors = db.execute("""
        SELECT json_extract(data, '$.error') as e, json_extract(data, '$.context') as ctx, timestamp
        FROM events WHERE event='error' ORDER BY id DESC LIMIT 10
    """).fetchall()

    # Events today
    events_today = db.execute(
        "SELECT COUNT(*) FROM events WHERE timestamp >= ?", (today,)
    ).fetchone()[0]

    # Events this week
    events_week = db.execute(
        "SELECT COUNT(*) FROM events WHERE timestamp >= ?", (week_ago,)
    ).fetchone()[0]

    # Hourly distribution (last 24h)
    hourly = db.execute("""
        SELECT strftime('%H', timestamp) as h, COUNT(*) as c
        FROM events WHERE timestamp >= datetime('now', '-24 hours')
        GROUP BY h ORDER BY h
    """).fetchall()

    # Device breakdown (rough: mobile vs desktop from screen width)
    devices = db.execute("""
        SELECT
            CASE
                WHEN CAST(json_extract(data, '$.screenWidth') AS INTEGER) < 768 THEN 'mobile'
                WHEN CAST(json_extract(data, '$.screenWidth') AS INTEGER) < 1024 THEN 'tablet'
                ELSE 'desktop'
            END as device,
            COUNT(DISTINCT session_id) as c
        FROM events
        WHERE session_id IS NOT NULL
          AND json_extract(data, '$.screenWidth') IS NOT NULL
          AND json_extract(data, '$.screenWidth') != 0
        GROUP BY device
    """).fetchall()

    # Tree node clicks
    node_clicks = db.execute("""
        SELECT json_extract(data, '$.nodeName') as n, COUNT(*) as c
        FROM events WHERE event='tree_node_click'
        GROUP BY n ORDER BY c DESC LIMIT 10
    """).fetchall()

    # Guess LCA depth distribution
    lca_dist = db.execute("""
        SELECT CAST(json_extract(data, '$.lcaDepth') AS INTEGER) as d, COUNT(*) as c
        FROM events WHERE event='guess'
        GROUP BY d ORDER BY d
    """).fetchall()

    return {
        "totalEvents": total_events,
        "eventsToday": events_today,
        "eventsThisWeek": events_week,
        "totalGamesStarted": total_started,
        "totalGamesCompleted": total_completed,
        "gamesByMode": games_by_mode,
        "winRate": win_rate,
        "averageGuessCount": avg_guesses,
        "winsPerMode": wins_by_mode,
        "completedPerMode": completed_by_mode,
        "hintsUsed": hints_used,
        "hintUsageRate": hint_rate,
        "mostCommonFirstGuesses": [{"organismId": o, "count": c} for o, c in first_guesses],
        "topMysteryOrganisms": [{"organismId": o, "count": c} for o, c in top_mysteries],
        "pageViews": {p: c for p, c in page_views},
        "topLearnViews": [{"organismId": o, "count": c} for o, c in learn_views],
        "shareCount": share_count,
        "difficultyChanges": {t: c for t, c in diff_changes},
        "modeChanges": {t: c for t, c in mode_changes},
        "uniqueSessions": unique_sessions,
        "avgSessionDurationMs": avg_session_ms,
        "errorCount": error_count,
        "recentErrors": [{"error": e, "context": ctx, "timestamp": ts} for e, ctx, ts in recent_errors],
        "hourlyDistribution": {h: c for h, c in hourly},
        "deviceBreakdown": {d: c for d, c in devices},
        "nodeClicks": [{"name": n, "count": c} for n, c in node_clicks],
        "lcaDepthDistribution": {str(d): c for d, c in lca_dist},
        "serverTime": datetime.utcnow().isoformat() + "Z",
    }


# --- HTTP Server ---

class CollectorHandler(BaseHTTPRequestHandler):

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/api/stats":
            self._json(aggregate_stats())

        elif path == "/api/stats/stream":
            # SSE endpoint
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self._cors()
            self.end_headers()
            try:
                while True:
                    stats = aggregate_stats()
                    msg = f"data: {json.dumps(stats)}\n\n"
                    self.wfile.write(msg.encode())
                    self.wfile.flush()
                    time.sleep(5)
            except (BrokenPipeError, ConnectionResetError):
                pass

        elif path == "/api/events":
            db = get_db()
            rows = db.execute(
                "SELECT event, data, session_id, timestamp FROM events ORDER BY id DESC LIMIT 200"
            ).fetchall()
            events = [
                {"event": e, "data": json.loads(d), "sessionId": s, "timestamp": t}
                for e, d, s, t in rows
            ]
            self._json(events)

        elif path == "/api/health":
            db = get_db()
            count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            self._json({"status": "ok", "events": count, "uptime": time.process_time()})

        elif path == "" or path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            db = get_db()
            count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            self.wfile.write(f"paleozooa collector | {count} events | ok\n".encode())

        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/api/ingest":
            # Auth check
            if COLLECTOR_KEY:
                qs = parse_qs(parsed.query)
                key = qs.get("key", [None])[0]
                if key != COLLECTOR_KEY:
                    self._json({"error": "unauthorized"}, 401)
                    return

            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                self._json({"error": "invalid json"}, 400)
                return

            event = payload.get("event")
            data = payload.get("data", {})
            if not event or not isinstance(event, str):
                self._json({"error": "missing event field"}, 400)
                return

            session_id = data.pop("sessionId", None) if isinstance(data, dict) else None
            timestamp = data.pop("timestamp", None) if isinstance(data, dict) else None
            if not timestamp:
                timestamp = datetime.utcnow().isoformat() + "Z"

            insert_event(event, data, session_id, timestamp)
            self._json({"ok": True})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, format, *args):
        # Compact logging
        print(f"  {args[0]}" if args else "")


def main():
    init_db()
    server = HTTPServer(("0.0.0.0", PORT), CollectorHandler)
    print(f"\n  paleozooa collector")
    print(f"  http://0.0.0.0:{PORT}")
    print(f"  db: {os.path.abspath(DB_PATH)}")
    if COLLECTOR_KEY:
        print(f"  auth: key required")
    print(f"  ready.\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
