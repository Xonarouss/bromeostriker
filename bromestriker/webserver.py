import os
import time
import sqlite3
import secrets
import hashlib
import threading
from typing import Optional, Dict, Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse, PlainTextResponse, JSONResponse

DB_DEFAULT_PATH = os.path.join(os.getcwd(), "data", "bromestriker.db")

AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
USERINFO_URL = "https://open.tiktokapis.com/v2/user/info/"


def _db_path() -> str:
    return (os.getenv("DB_PATH") or DB_DEFAULT_PATH).strip()


def _conn() -> sqlite3.Connection:
    # separate connection per request/thread
    return sqlite3.connect(_db_path())


def _init_tables() -> None:
    con = _conn()
    try:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tiktok_oauth (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                access_token TEXT,
                refresh_token TEXT,
                open_id TEXT,
                scope TEXT,
                token_type TEXT,
                expires_at INTEGER,
                refresh_expires_at INTEGER,
                updated_at INTEGER
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tiktok_state (
                state TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            );
            """
        )
        con.commit()
    finally:
        con.close()


def _save_state(state: str) -> None:
    con = _conn()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM tiktok_state")  # single-user simplest
        cur.execute("INSERT INTO tiktok_state(state, created_at) VALUES(?, ?)", (state, int(time.time())))
        con.commit()
    finally:
        con.close()


def _consume_state(state: str, max_age_sec: int = 600) -> bool:
    con = _conn()
    try:
        cur = con.cursor()
        row = cur.execute("SELECT state, created_at FROM tiktok_state WHERE state = ?", (state,)).fetchone()
        if not row:
            return False
        created_at = int(row[1])
        cur.execute("DELETE FROM tiktok_state")
        con.commit()
        return (time.time() - created_at) <= max_age_sec
    finally:
        con.close()


def _upsert_tokens(payload: Dict[str, Any]) -> None:
    now = int(time.time())
    expires_in = int(payload.get("expires_in") or 0)
    refresh_expires_in = int(payload.get("refresh_expires_in") or 0)

    con = _conn()
    try:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO tiktok_oauth
                (id, access_token, refresh_token, open_id, scope, token_type, expires_at, refresh_expires_at, updated_at)
            VALUES
                (1, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                access_token=excluded.access_token,
                refresh_token=excluded.refresh_token,
                open_id=excluded.open_id,
                scope=excluded.scope,
                token_type=excluded.token_type,
                expires_at=excluded.expires_at,
                refresh_expires_at=excluded.refresh_expires_at,
                updated_at=excluded.updated_at;
            """,
            (
                payload.get("access_token"),
                payload.get("refresh_token"),
                payload.get("open_id"),
                payload.get("scope"),
                payload.get("token_type"),
                now + expires_in if expires_in else None,
                now + refresh_expires_in if refresh_expires_in else None,
                now,
            ),
        )
        con.commit()
    finally:
        con.close()


def get_tiktok_tokens() -> Optional[Dict[str, Any]]:
    _init_tables()
    con = _conn()
    try:
        cur = con.cursor()
        row = cur.execute(
            "SELECT access_token, refresh_token, open_id, scope, token_type, expires_at, refresh_expires_at, updated_at FROM tiktok_oauth WHERE id=1"
        ).fetchone()
        if not row:
            return None
        return {
            "access_token": row[0],
            "refresh_token": row[1],
            "open_id": row[2],
            "scope": row[3],
            "token_type": row[4],
            "expires_at": row[5],
            "refresh_expires_at": row[6],
            "updated_at": row[7],
        }
    finally:
        con.close()


async def refresh_tiktok_access_token_if_needed() -> Optional[str]:
    """Returns a valid access token (refreshing if needed), or None."""
    _init_tables()
    tokens = get_tiktok_tokens()
    if not tokens:
        # fallback to env
        env_tok = (os.getenv("TIKTOK_ACCESS_TOKEN") or "").strip()
        return env_tok or None

    access_token = (tokens.get("access_token") or "").strip()
    refresh_token = (tokens.get("refresh_token") or "").strip()
    expires_at = int(tokens.get("expires_at") or 0)

    # still valid for >10 minutes
    if access_token and expires_at and (expires_at - int(time.time())) > 600:
        return access_token

    # refresh if possible
    client_key = (os.getenv("TIKTOK_CLIENT_KEY") or "").strip()
    client_secret = (os.getenv("TIKTOK_CLIENT_SECRET") or "").strip()
    if not (client_key and client_secret and refresh_token):
        return access_token or None

    data = {
        "client_key": client_key,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(TOKEN_URL, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        r.raise_for_status()
        payload = r.json()

    # TikTok may return a new refresh_token; store whatever comes back
    _upsert_tokens(payload)
    return (payload.get("access_token") or "").strip() or None


def create_app() -> FastAPI:
    app = FastAPI(title="BromeStriker OAuth")

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/tiktok/login")
    def tiktok_login():
        _init_tables()
        client_key = (os.getenv("TIKTOK_CLIENT_KEY") or "").strip()
        redirect_uri = (os.getenv("TIKTOK_REDIRECT_URI") or "").strip()
        scopes = (os.getenv("TIKTOK_SCOPES") or "user.info.basic,user.info.stats").strip()

        if not client_key or not redirect_uri:
            return JSONResponse(
                status_code=500,
                content={"error": "TIKTOK_CLIENT_KEY en TIKTOK_REDIRECT_URI zijn verplicht."},
            )

        state = secrets.token_urlsafe(24)
        _save_state(state)

        # TikTok docs: use URL encoded params
        params = {
            "client_key": client_key,
            "scope": scopes,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "state": state,
        }
        url = AUTH_URL + "?" + urlencode(params)
        return RedirectResponse(url=url, status_code=302)

    @app.get("/tiktok/callback")
    async def tiktok_callback(request: Request):
        _init_tables()
        code = request.query_params.get("code")
        state = request.query_params.get("state")
        error = request.query_params.get("error")
        error_description = request.query_params.get("error_description")

        if error:
            return HTMLResponse(f"<h2>TikTok OAuth error</h2><pre>{error}: {error_description}</pre>", status_code=400)

        if not code or not state:
            return HTMLResponse("<h2>Ongeldige callback</h2><p>code/state ontbreekt.</p>", status_code=400)

        if not _consume_state(state):
            return HTMLResponse("<h2>Ongeldige state</h2><p>Probeer opnieuw in te loggen.</p>", status_code=400)

        client_key = (os.getenv("TIKTOK_CLIENT_KEY") or "").strip()
        client_secret = (os.getenv("TIKTOK_CLIENT_SECRET") or "").strip()
        redirect_uri = (os.getenv("TIKTOK_REDIRECT_URI") or "").strip()

        if not (client_key and client_secret and redirect_uri):
            return HTMLResponse("<h2>Server misconfig</h2><p>Client key/secret/redirect ontbreken.</p>", status_code=500)

        data = {
            "client_key": client_key,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }

        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(TOKEN_URL, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
            # TikTok returns JSON error bodies too
            if r.status_code >= 400:
                try:
                    payload = r.json()
                except Exception:
                    payload = {"error": r.text}
                return HTMLResponse(f"<h2>Token exchange failed</h2><pre>{payload}</pre>", status_code=400)
            payload = r.json()

        _upsert_tokens(payload)

        return HTMLResponse(
            """
            <h2>✅ TikTok gekoppeld!</h2>
            <p>Je kunt dit tabblad sluiten. De Discord bot pakt vanaf nu automatisch je TikTok volgers.</p>
            """,
            status_code=200,
        )

    @app.get("/tiktok/status")
    async def tiktok_status():
        tok = get_tiktok_tokens()
        if not tok:
            return {"connected": False}
        return {
            "connected": True,
            "open_id": tok.get("open_id"),
            "scope": tok.get("scope"),
            "expires_at": tok.get("expires_at"),
            "refresh_expires_at": tok.get("refresh_expires_at"),
        }

    return app


def start_webserver_in_thread() -> None:
    """Starts FastAPI (uvicorn) in a daemon thread if enabled."""
    if (os.getenv("TIKTOK_OAUTH_ENABLED") or "1").strip() not in ("1", "true", "True", "yes", "YES"):
        return

    # If user hasn't configured TikTok, still start /health for convenience.
    port = int(os.getenv("WEB_PORT") or os.getenv("PORT") or "8080")
    host = (os.getenv("WEB_HOST") or "0.0.0.0").strip()

    import uvicorn

    app = create_app()

    def _run():
        uvicorn.run(app, host=host, port=port, log_level="info")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
