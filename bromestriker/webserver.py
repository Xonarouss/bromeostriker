import os
import time
import sqlite3
import secrets
import hashlib
import json
from typing import Optional, Dict, Any
from urllib.parse import urlencode
import threading

import httpx
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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


def create_app(bot=None) -> FastAPI:
    app = FastAPI(title="BromeoStriker Dashboard")

    # Serve dashboard static assets (favicons, logos, etc.)
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/favicon.ico")
    async def favicon_ico():
        p = os.path.join(static_dir, "favicon.ico")
        if os.path.exists(p):
            return FileResponse(p)
        return PlainTextResponse("", status_code=404)

    @app.get("/site.webmanifest")
    async def webmanifest():
        p = os.path.join(static_dir, "site.webmanifest")
        if os.path.exists(p):
            return FileResponse(p, media_type="application/manifest+json")
        return PlainTextResponse("", status_code=404)


    # -----------------------------
    # Discord Dashboard (OAuth2)
    # -----------------------------

    DISCORD_OAUTH_AUTHORIZE = "https://discord.com/oauth2/authorize"
    DISCORD_OAUTH_TOKEN = "https://discord.com/api/oauth2/token"
    DISCORD_API_ME = "https://discord.com/api/users/@me"

    def _public_base_url() -> str:
        return (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")

    def _discord_redirect_uri() -> str:
        base = _public_base_url()
        return f"{base}/auth/callback" if base else ""

    def _session_secret() -> str:
        return (os.getenv("SESSION_SECRET") or "").strip()

    def _bcrew_role_id() -> int:
        try:
            return int(os.getenv("B_CREW_ROLE_ID", "1027533834318774293") or "1027533834318774293")
        except Exception:
            return 1027533834318774293

    def _sign(value: str) -> str:
        sec = _session_secret()
        if not sec:
            return ""
        return hashlib.sha256((sec + ":" + value).encode("utf-8")).hexdigest()

    def _make_session(user_id: int) -> str:
        ts = str(int(time.time()))
        payload = f"{user_id}:{ts}"
        sig = _sign(payload)
        return f"{payload}:{sig}"

    def _parse_session(cookie_val: str, max_age_sec: int = 7*24*3600) -> int | None:
        try:
            user_id_s, ts_s, sig = cookie_val.split(":", 2)
            payload = f"{user_id_s}:{ts_s}"
            if not sig or sig != _sign(payload):
                return None
            ts = int(ts_s)
            if int(time.time()) - ts > max_age_sec:
                return None
            return int(user_id_s)
        except Exception:
            return None

    def _get_user_id_from_request(req: Request) -> int | None:
        raw = req.cookies.get("bs_session") or ""
        return _parse_session(raw)

    async def _require_allowed(req: Request) -> int:
        user_id = _get_user_id_from_request(req)
        if not user_id:
            raise PermissionError("not_logged_in")
        if bot is None:
            raise PermissionError("bot_not_ready")
        guild = bot.get_guild(getattr(bot, "guild_id", 0))
        if guild is None:
            # try fetch
            try:
                guild = await bot.fetch_guild(getattr(bot, "guild_id", 0))
            except Exception:
                guild = None
        if guild is None:
            raise PermissionError("guild_not_found")
        try:
            member = guild.get_member(user_id) or await guild.fetch_member(user_id)
        except Exception:
            member = None
        if member is None:
            raise PermissionError("not_in_guild")
        is_admin = bool(getattr(member.guild_permissions, "administrator", False))
        is_bcrew = member.get_role(_bcrew_role_id()) is not None
        if not (is_admin or is_bcrew):
            raise PermissionError("forbidden")
        return user_id

    def _error(status: int, msg: str):
        return JSONResponse(status_code=status, content={"error": msg})

    @app.get("/auth/login")
    async def discord_login():
        client_id = (os.getenv("DISCORD_CLIENT_ID") or "").strip()
        redirect_uri = _discord_redirect_uri()
        if not (client_id and redirect_uri):
            return _error(500, "DISCORD_CLIENT_ID en PUBLIC_BASE_URL zijn verplicht")
        state = secrets.token_urlsafe(24)
        # reuse state table but store with prefix
        _save_state("discord:" + state)
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": "identify guilds",
            "state": state,
        }
        return RedirectResponse(f"{DISCORD_OAUTH_AUTHORIZE}?{urlencode(params)}")

    @app.get("/auth/callback")
    async def discord_callback(code: str | None = None, state: str | None = None):
        if not code or not state:
            return _error(400, "Missing code/state")
        if not _consume_state("discord:" + state):
            return _error(400, "Invalid/expired state")

        client_id = (os.getenv("DISCORD_CLIENT_ID") or "").strip()
        client_secret = (os.getenv("DISCORD_CLIENT_SECRET") or "").strip()
        redirect_uri = _discord_redirect_uri()
        if not (client_id and client_secret and redirect_uri):
            return _error(500, "DISCORD_CLIENT_ID/SECRET en PUBLIC_BASE_URL zijn verplicht")

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(DISCORD_OAUTH_TOKEN, data=data, headers=headers)
            if r.status_code != 200:
                return _error(400, f"Token exchange failed: {r.status_code} {r.text}")
            tok = r.json()
            access = (tok.get("access_token") or "").strip()
            if not access:
                return _error(400, "No access token")
            me = await client.get(DISCORD_API_ME, headers={"Authorization": f"Bearer {access}"})
            if me.status_code != 200:
                return _error(400, f"/users/@me failed: {me.status_code} {me.text}")
            me_js = me.json()

        user_id = int(me_js.get("id"))
        session = _make_session(user_id)
        resp = RedirectResponse(url="/dashboard")
        # Secure cookies are only stored by browsers over HTTPS.
        # During initial setup you might access the dashboard over plain HTTP
        # (e.g., when TLS isn't ready yet). In that case, force secure=False
        # so login sessions actually persist.
        base_url = (os.getenv("PUBLIC_BASE_URL") or "").strip().lower()
        cookie_secure = base_url.startswith("https://")
        resp.set_cookie(
            "bs_session",
            session,
            httponly=True,
            secure=cookie_secure,
            samesite="lax",
            max_age=7*24*3600,
        )
        return resp

    @app.get("/logout")
    async def logout():
        resp = RedirectResponse(url="/dashboard")
        resp.delete_cookie("bs_session")
        return resp

    DASHBOARD_HTML = """<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>BromeoStriker Dashboard</title>
  <link rel="icon" href="/favicon.ico" sizes="any">
  <link rel="icon" type="image/png" href="/static/favicon-32x32.png" sizes="32x32">
  <link rel="icon" type="image/png" href="/static/favicon-16x16.png" sizes="16x16">
  <link rel="apple-touch-icon" href="/static/apple-touch-icon.png">
  <link rel="manifest" href="/site.webmanifest">
  <meta name="theme-color" content="#0b1220">
  <script src="https://unpkg.com/react@18/umd/react.development.js" crossorigin></script>
  <script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js" crossorigin></script>
  <script src="https://unpkg.com/babel-standalone@6/babel.min.js"></script>
  <style>
    :root{--bg:#0b1220;--panel:#0f172a;--panel2:#0b1220;--border:#1f2937;--text:#e5e7eb;--muted:#94a3b8;--accent:#16a34a;}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial; margin:0; background:radial-gradient(1200px 600px at 30% -10%, rgba(22,163,74,.18), transparent 60%), var(--bg); color:var(--text);} 
    .layout{display:flex;min-height:100vh}
    .sidebar{width:240px;position:sticky;top:0;height:100vh;border-right:1px solid var(--border);background:linear-gradient(180deg, rgba(15,23,42,.9), rgba(15,23,42,.65));padding:16px}
    .sidebrand{display:flex;align-items:center;gap:10px;margin-bottom:14px}
    .sidebrand img{width:34px;height:34px;border-radius:10px}
    .sidebrand .t{font-weight:900;letter-spacing:.2px}
    .nav{display:flex;flex-direction:column;gap:8px}
    .navitem{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:14px;border:1px solid transparent;background:transparent;cursor:pointer;color:var(--text)}
    .navitem:hover{background:rgba(148,163,184,.08);border-color:rgba(148,163,184,.12)}
    .navitem.active{background:rgba(22,163,74,.14);border-color:rgba(22,163,74,.25)}
    .main{flex:1;min-width:0}
    .top{display:flex;justify-content:space-between;align-items:center;padding:14px 18px;border-bottom:1px solid var(--border);background:rgba(11,18,32,.85);backdrop-filter:blur(10px);position:sticky;top:0;z-index:10}
    .brand{font-weight:900;letter-spacing:0.2px}
    .btn{background:#111827;border:1px solid #334155;color:var(--text);padding:8px 12px;border-radius:12px;cursor:pointer}
    .btn.primary{background:var(--accent);border-color:var(--accent);color:#04120a;font-weight:800}
    .btn.ghost{background:transparent;border-color:rgba(148,163,184,.18)}
    .wrap{max-width:1150px;margin:0 auto;padding:18px}
    .card{background:linear-gradient(180deg, rgba(15,23,42,.96), rgba(15,23,42,.78));border:1px solid var(--border);border-radius:18px;padding:16px;margin-bottom:12px;box-shadow:0 18px 45px rgba(0,0,0,.28)}
    .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}
    .col6{grid-column:span 6}
    .col12{grid-column:span 12}
    input,select,textarea{width:100%;padding:10px 12px;border-radius:14px;border:1px solid #334155;background:rgba(11,18,32,.8);color:var(--text)}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
    .muted{color:var(--muted)}
    .danger{border-color:#ef4444}
    .btn.danger{background:#ef4444;border-color:#ef4444;color:#0b1220}
    a{color:#22c55e}
    .seg{display:flex;gap:8px}
    .segBtn{padding:8px 10px;border-radius:999px;border:1px solid rgba(148,163,184,.2);background:transparent;cursor:pointer;color:var(--text)}
    .segBtn.on{background:rgba(22,163,74,.14);border-color:rgba(22,163,74,.25)}
    .stations{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}
    .station{display:flex;gap:10px;align-items:center;justify-content:space-between;padding:12px;border-radius:16px;border:1px solid rgba(148,163,184,.16);background:rgba(11,18,32,.35)}
    .stationL{display:flex;gap:10px;align-items:center;min-width:0}
    .stationLogo{width:34px;height:34px;border-radius:10px;object-fit:contain;background:rgba(148,163,184,.08);padding:6px}
    .stationName{font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .footer{margin-top:16px;padding:14px 0;color:var(--muted);font-size:13px;text-align:center}
    @media (max-width: 980px){.sidebar{display:none}.col6{grid-column:span 12}.wrap{padding:14px}}
  </style>
</head>
<body>
  <div id="root"></div>
  <script type="text/babel" data-presets="react">
    const {useEffect, useMemo, useState} = React;

    async function api(path, opts={}){
      const r = await fetch(path, {credentials:'include', headers:{'Content-Type':'application/json', ...(opts.headers||{})}, ...opts});
      const t = await r.text();
      let j=null;
      try{ j = t ? JSON.parse(t) : null; }catch(e){}
      if(!r.ok){ throw new Error((j && j.error) ? j.error : (t || r.statusText)); }
      return j;
    }

    function App(){
      const [me, setMe] = useState(null);
      const [tab, setTab] = useState('music');
      const [err, setErr] = useState('');
      const [nowNl, setNowNl] = useState('');

      const loadMe = async()=>{
        setErr('');
        try{ setMe(await api('/api/me')); }catch(e){ setMe(null); }
      };

      useEffect(()=>{ loadMe(); },[]);

      // IMPORTANT: Hooks must be called in the same order on every render.
      // So we always register this effect, and guard inside.
      useEffect(()=>{
        if(!me || (me && me.allowed === false)) return;
        const fmt = new Intl.DateTimeFormat('nl-NL', { timeZone:'Europe/Amsterdam', hour:'2-digit', minute:'2-digit', second:'2-digit', day:'2-digit', month:'2-digit', year:'numeric' });
        const tick = ()=>setNowNl(fmt.format(new Date()));
        tick();
        const t = setInterval(tick, 1000);
        return ()=>clearInterval(t);
      },[me]);

      if(!me){
        return (
          <div className='wrap'>
            <div className='card'>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                <div>
                  <div style={{fontSize:22,fontWeight:800}}>BromeoStriker Dashboard</div>
                  <div className='muted'>Login met Discord om verder te gaan.</div>
                </div>
                <a className='btn primary' href='/auth/login'>Login</a>
              </div>
            </div>
          </div>
        );
      }

      if(me && me.allowed === false){
        return (
          <div className='wrap'>
            <div className='card'>
              <div style={{fontSize:20,fontWeight:800}}>Geen toegang</div>
              <div className='muted'>Alleen B-Crew of Discord Admin mag dit dashboard gebruiken.</div>
              <div className='row' style={{marginTop:10}}>
                <a className='btn' href='/logout'>Logout</a>
              </div>
            </div>
          </div>
        );
      }

      const NAV = [
        {k:'music', label:'Muziek'},
        {k:'messages', label:'Berichten'},
        {k:'giveaways', label:'Giveaways'},
        {k:'strikes', label:'Strikes'},
        {k:'counters', label:'Counters'},
        {k:'warns', label:'Waarschuwingen'},
        {k:'mutes', label:'Mutes'},
        {k:'bans', label:'Bans'},
      ];

      return (
        <div className='layout'>
          <div className='sidebar'>
            <div className='sidebrand'>
              <img src='/static/logo.png' alt='logo'/>
              <div>
                <div className='t'>BromeoStriker</div>
                <div className='muted' style={{fontSize:12}}>Dashboard</div>
              </div>
            </div>
            <div className='nav'>
              {NAV.map(it=> (
                <div key={it.k} className={'navitem '+(tab===it.k?'active':'')} onClick={()=>setTab(it.k)}>
                  <div style={{fontWeight:800}}>{it.label}</div>
                </div>
              ))}
            </div>
          </div>
          <div className='main'>
            <div className='top'>
              <div className='row'>
                <div className='brand'>BromeoStriker Dashboard</div>
                <div className='muted' style={{marginLeft:10}}>{nowNl}</div>
              </div>
              <div className='row'>
                <div className='muted'>Ingelogd als {me.username}</div>
                <a className='btn' href='/logout'>Logout</a>
              </div>
            </div>
            <div className='wrap'>
              {err && <div className='card danger'>❌ {err}</div>}
              {tab==='music' && <Music setErr={setErr} />}
              {tab==='messages' && <Messages setErr={setErr} />}
              {tab==='giveaways' && <Giveaways setErr={setErr} />}
              {tab==='strikes' && <Strikes setErr={setErr} />}
              {tab==='counters' && <Counters setErr={setErr} />}
              {tab==='warns' && <Warns setErr={setErr} />}
              {tab==='mutes' && <Mutes setErr={setErr} />}
              {tab==='bans' && <Bans setErr={setErr} />}
              <div className='footer'>Made with ❤️ by <a href='https://xonarous.nl' target='_blank' rel='noreferrer'>Xonarous</a></div>
            </div>
          </div>
        </div>
      );
    }

    function Music({setErr}){
      const [st, setSt] = useState(null);
      const [url, setUrl] = useState('');
      const [mode, setMode] = useState('url');
      const [stations, setStations] = useState([]);
      const [voiceChannels, setVoiceChannels] = useState([]);
      const [voiceId, setVoiceId] = useState('');
      const load = async()=>{
        setErr('');
        try{ setSt(await api('/api/music/status')); }catch(e){ setErr(e.message); }
      };
      const loadVoice = async()=>{
        try{
          const v = await api('/api/voice_channels');
          setVoiceChannels(v.items||[]);
          if(!voiceId && (v.items||[]).length) setVoiceId(String(v.items[0].id));
        }catch(e){ /* ignore */ }
      };
      useEffect(()=>{ load(); loadVoice(); const t=setInterval(load, 4000); return ()=>clearInterval(t); },[]);

      const act = async(action, payload={})=>{
        setErr('');
        try{ await api('/api/music/action', {method:'POST', body: JSON.stringify({action, ...payload})}); await load(); }catch(e){ setErr(e.message); }
      };

      const loadStations = async()=>{
        setErr('');
        try{
          const r = await api('/api/radio/stations');
          setStations((r && r.stations) ? r.stations : []);
        }catch(e){ /* radio is optional */ }
      };
      useEffect(()=>{ loadStations(); },[]);

      return (
        <div className='grid'>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Now Playing</div>
            <div className='muted'>
              {(st && st.now && (typeof st.now === 'object')) ? (
                <a href={st.now.webpage_url || '#'} target='_blank' rel='noreferrer'>
                  {st.now.title || '—'}
                </a>
              ) : ((st && st.now) ? String(st.now) : '—')}
            </div>
            <div className='row' style={{marginTop:12}}>
              <select value={voiceId} onChange={e=>setVoiceId(e.target.value)} style={{flex:1}}>
                {voiceChannels.map(v=> <option key={v.id} value={v.id}>{v.name}</option>)}
              </select>
              {/* Discord Snowflakes must stay strings (JS Number loses precision) */}
              <button className='btn' onClick={()=>act('join', {channel_id: String(voiceId)})}>🔌 Join</button>
              <button className='btn danger' onClick={()=>act('disconnect')}>🛑 Disconnect</button>
            </div>
            <div className='row' style={{marginTop:12}}>
              <button className='btn' onClick={()=>act('pause_resume')}>⏯️</button>
              <button className='btn primary' onClick={()=>act('skip')}>⏭️ Skip</button>
              <button className='btn' onClick={()=>act('stop')}>⏹️ Stop</button>
              <button className='btn' onClick={()=>act('vol_down')}>🔉</button>
              <button className='btn' onClick={()=>act('vol_up')}>🔊</button>
            </div>
            <div style={{marginTop:14}}>
              <div className='row' style={{justifyContent:'space-between',marginBottom:10}}>
                <div className='seg'>
                  <button className={'segBtn '+(mode==='url'?'on':'')} onClick={()=>setMode('url')}>YouTube / URL</button>
                  <button className={'segBtn '+(mode==='radio'?'on':'')} onClick={()=>setMode('radio')}>Radio</button>
                </div>
              </div>
              {mode==='url' ? (
                <div className='row'>
                  <input placeholder='YouTube link, livestream of zoekterm…' value={url} onChange={e=>setUrl(e.target.value)} />
                  <button className='btn primary' onClick={()=>{ if(!url.trim()) return; act('play', {query:url}); }}>▶️ Play</button>
                  <button className='btn' onClick={()=>{ if(!url.trim()) return; act('add_playlist', {query:url}); }}>➕ Playlist</button>
                </div>
              ) : (
                <div className='stations'>
                  {stations.length ? stations.map(s=> (
                    <div className='station' key={s.id}>
                      <div className='stationL'>
                        {s.logo_url ? <img className='stationLogo' src={s.logo_url} alt={s.name}/> : <div className='stationLogo' />}
                        <div className='stationName' title={s.name}>{s.name}</div>
                      </div>
                      <button className='btn primary' onClick={()=>act('radio_play', {station_id: String(s.id)})}>Afspelen</button>
                    </div>
                  )) : <div className='muted'>Geen radiozenders ingesteld.</div>}
                </div>
              )}
            </div>
          </div>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Queue</div>
            <div className='muted' style={{whiteSpace:'pre-wrap'}}>
              {(() => {
                const q = (st && Array.isArray(st.queue)) ? st.queue : [];
                if (!q.length) return '—';
                return q.map((it, idx) => {
                  if (it && typeof it === 'object') {
                    return (
                      <div key={idx}>
                        <a href={it.webpage_url || '#'} target='_blank' rel='noreferrer'>{it.title || '—'}</a>
                      </div>
                    );
                  }
                  return <div key={idx}>{String(it)}</div>;
                });
              })()}
            </div>
            <div className='row' style={{marginTop:12}}>
              <button className='btn' onClick={()=>act('play_playlist')}>▶️ Play playlist</button>
              <button className='btn danger' onClick={()=>act('clear_playlist')}>🧹 Clear playlist</button>
            </div>
          </div>
        </div>
      );
    }

    function Messages({setErr}){
      const [channels, setChannels] = useState([]);
      const [channelId, setChannelId] = useState('');
      const [content, setContent] = useState('');
      const [embed, setEmbed] = useState({title:'', description:'', url:'', color:'#16a34a', thumbnail_url:'', image_url:'', footer:''});
      const [useEmbed, setUseEmbed] = useState(true);
      const [sent, setSent] = useState([]);
      const [editId, setEditId] = useState(null);
      const [editContent, setEditContent] = useState('');
      const [editEmbed, setEditEmbed] = useState({title:'', description:'', url:'', color:'#16a34a', thumbnail_url:'', image_url:'', footer:''});
      const [editUseEmbed, setEditUseEmbed] = useState(true);

      const load = async()=>{
        setErr('');
        try{
          const res = await Promise.all([api('/api/channels'), api('/api/messages/sent')]);
          const ch = res[0];
          const s = res[1];
          setChannels(ch.items||[]);
          setSent(s.items||[]);
          if(!channelId && (ch.items||[]).length) setChannelId(String(ch.items[0].id));
        }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); },[]);

      const send = async()=>{
        setErr('');
        try{
          await api('/api/messages/send', {
            method:'POST',
            body: JSON.stringify({
              channel_id: String(channelId),
              content,
              embed: useEmbed ? ({
                title: embed.title,
                description: embed.description,
                url: embed.url,
                color: (embed.color||'').replace('#',''),
                thumbnail_url: embed.thumbnail_url,
                image_url: embed.image_url,
                footer: embed.footer
              }) : null
            })
          });
          setContent('');
          await load();
        }catch(e){ setErr(e.message); }
      };

      const hasEmbed = useEmbed && Object.values(embed).some(v=>String(v||'').trim()!=='' && v!=='#16a34a');

      const startEdit = (it)=>{
        setEditId(it.id);
        setEditContent(it.content || '');
        let ej = null;
        try{ ej = it.embed_json ? JSON.parse(it.embed_json) : null; }catch(e){ ej = null; }
        if(ej){
          setEditUseEmbed(true);
          setEditEmbed({
            title: ej.title||'', description: ej.description||'', url: ej.url||'',
            color: ej.color ? ('#'+String(ej.color).replace('#','')) : '#16a34a',
            thumbnail_url: ej.thumbnail_url||'', image_url: ej.image_url||'', footer: ej.footer||''
          });
        }else{
          setEditUseEmbed(false);
          setEditEmbed({title:'', description:'', url:'', color:'#16a34a', thumbnail_url:'', image_url:'', footer:''});
        }
      };

      const saveEdit = async()=>{
        if(editId===null) return;
        setErr('');
        try{
          await api(`/api/messages/${editId}/update`, {
            method:'POST',
            body: JSON.stringify({
              content: editContent,
              embed: editUseEmbed ? ({
                title: editEmbed.title,
                description: editEmbed.description,
                url: editEmbed.url,
                color: (editEmbed.color||'').replace('#',''),
                thumbnail_url: editEmbed.thumbnail_url,
                image_url: editEmbed.image_url,
                footer: editEmbed.footer
              }) : null
            })
          });
          setEditId(null);
          await load();
        }catch(e){ setErr(e.message); }
      };

      const delEdit = async()=>{
        if(editId===null) return;
        setErr('');
        try{
          await api(`/api/messages/${editId}/delete`, {method:'POST'});
          setEditId(null);
          await load();
        }catch(e){ setErr(e.message); }
      };

      return (
        <div className='grid'>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Bericht versturen</div>
            <div className='row'>
              <select value={channelId} onChange={e=>setChannelId(e.target.value)}>
                {channels.map(c=> <option key={c.id} value={c.id}>#{c.name}</option>)}
              </select>
            </div>
            <div style={{marginTop:10}}>
              <textarea rows='4' placeholder='Message content (optioneel)' value={content} onChange={e=>setContent(e.target.value)}></textarea>
            </div>
            <div className='row' style={{marginTop:12, justifyContent:'space-between'}}>
              <div style={{fontWeight:800}}>Embed</div>
              <label className='muted' style={{display:'flex',alignItems:'center',gap:8}}>
                <input type='checkbox' checked={useEmbed} onChange={e=>setUseEmbed(e.target.checked)} />
                Gebruik embed
              </label>
            </div>
            <div className='row' style={{marginTop:8}}>
              <input placeholder='Title' value={embed.title} onChange={e=>setEmbed(s=>({...s,title:e.target.value}))}/>
              <input placeholder='URL' value={embed.url} onChange={e=>setEmbed(s=>({...s,url:e.target.value}))}/>
            </div>
            <div style={{marginTop:8}}>
              <textarea rows='4' placeholder='Description' value={embed.description} onChange={e=>setEmbed(s=>({...s,description:e.target.value}))}></textarea>
            </div>
            <div className='row' style={{marginTop:8}}>
              <input placeholder='Thumbnail URL' value={embed.thumbnail_url} onChange={e=>setEmbed(s=>({...s,thumbnail_url:e.target.value}))}/>
              <input placeholder='Image URL' value={embed.image_url} onChange={e=>setEmbed(s=>({...s,image_url:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:8}}>
              <input placeholder='Footer' value={embed.footer} onChange={e=>setEmbed(s=>({...s,footer:e.target.value}))}/>
              <input type='color' value={embed.color} onChange={e=>setEmbed(s=>({...s,color:e.target.value}))} style={{width:60,padding:0,height:42}}/>
            </div>
            <div className='row' style={{marginTop:12}}>
              <button className='btn primary' onClick={send}>📨 Send</button>
            </div>
            <div className='muted' style={{marginTop:8}}>Tip: embed preview rechts is een benadering; Discord kan net anders renderen.</div>
          </div>

          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Preview</div>
            <div className='card' style={{borderLeft:`4px solid ${embed.color||'#16a34a'}`, padding:12}}>
              {content && <div style={{marginBottom:10}}>{content}</div>}
              {hasEmbed ? (
                <div>
                  {embed.title && (
                    embed.url ? <a href={embed.url} target='_blank' rel='noreferrer' style={{fontWeight:800,fontSize:16}}>{embed.title}</a>
                              : <div style={{fontWeight:800,fontSize:16}}>{embed.title}</div>
                  )}
                  {embed.description && <div className='muted' style={{marginTop:6,whiteSpace:'pre-wrap'}}>{embed.description}</div>}
                  {embed.image_url && <img src={embed.image_url} alt='' style={{marginTop:10,maxWidth:'100%',borderRadius:12}} />}
                  {embed.footer && <div className='muted' style={{marginTop:10,fontSize:12}}>{embed.footer}</div>}
                </div>
              ) : (
                <div className='muted'>—</div>
              )}
            </div>
          </div>

          <div className='card col12'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Geposte berichten</div>
            {sent.length===0 && <div className='muted'>—</div>}
            {sent.map(it=> (
              <div key={it.id} className='row' style={{justifyContent:'space-between', padding:'8px 0', borderBottom:'1px solid #1f2937'}}>
                <div>
                  <div style={{fontWeight:800}}># {it.channel_id} • msg {it.message_id}</div>
                  <div className='muted' style={{whiteSpace:'pre-wrap'}}>{(it.content||'').slice(0,120)}{((it.content||'').length>120)?'…':''}</div>
                </div>
                <button className='btn' onClick={()=>startEdit(it)}>✏️ Edit</button>
              </div>
            ))}

            {editId!==null && (
              <div className='card' style={{marginTop:12, padding:12}}>
                <div style={{fontWeight:800, marginBottom:8}}>Bewerken (ID {editId})</div>
                <div style={{marginBottom:8}}>
                  <textarea rows='4' value={editContent} onChange={e=>setEditContent(e.target.value)}></textarea>
                </div>
                <div className='row' style={{justifyContent:'space-between', marginBottom:8}}>
                  <label className='muted' style={{display:'flex',alignItems:'center',gap:8}}>
                    <input type='checkbox' checked={editUseEmbed} onChange={e=>setEditUseEmbed(e.target.checked)} />
                    Embed
                  </label>
                  <div className='row'>
                    <button className='btn primary' onClick={saveEdit}>💾 Opslaan</button>
                    <button className='btn danger' onClick={delEdit}>🗑️ Verwijder</button>
                    <button className='btn' onClick={()=>setEditId(null)}>Sluiten</button>
                  </div>
                </div>
                {editUseEmbed && (
                  <div>
                    <div className='row'>
                      <input placeholder='Title' value={editEmbed.title} onChange={e=>setEditEmbed(s=>({...s,title:e.target.value}))}/>
                      <input placeholder='URL' value={editEmbed.url} onChange={e=>setEditEmbed(s=>({...s,url:e.target.value}))}/>
                    </div>
                    <div style={{marginTop:8}}>
                      <textarea rows='3' placeholder='Description' value={editEmbed.description} onChange={e=>setEditEmbed(s=>({...s,description:e.target.value}))}></textarea>
                    </div>
                    <div className='row' style={{marginTop:8}}>
                      <input placeholder='Thumbnail URL' value={editEmbed.thumbnail_url} onChange={e=>setEditEmbed(s=>({...s,thumbnail_url:e.target.value}))}/>
                      <input placeholder='Image URL' value={editEmbed.image_url} onChange={e=>setEditEmbed(s=>({...s,image_url:e.target.value}))}/>
                    </div>
                    <div className='row' style={{marginTop:8}}>
                      <input placeholder='Footer' value={editEmbed.footer} onChange={e=>setEditEmbed(s=>({...s,footer:e.target.value}))}/>
                      <input type='color' value={editEmbed.color} onChange={e=>setEditEmbed(s=>({...s,color:e.target.value}))} style={{width:60,padding:0,height:42}}/>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      );
    }

    function Strikes({setErr}){
      const [q, setQ] = useState('');
      const [items, setItems] = useState([]);
      const search = async()=>{
        setErr('');
        try{
          const res = await api(`/api/strikes/search?q=${encodeURIComponent(q)}`);
          setItems(res.items||[]);
        }catch(e){ setErr(e.message); }
      };
      const setStrike = async(uid, val)=>{
        setErr('');
        try{ await api('/api/strikes/set', {method:'POST', body: JSON.stringify({user_id: uid, strikes: Number(val)})}); await search(); }catch(e){ setErr(e.message); }
      };
      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Strikes zoeken</div>
          <div className='row'>
            <input placeholder='Zoek op naam of user id…' value={q} onChange={e=>setQ(e.target.value)}/>
            <button className='btn primary' onClick={search}>🔎 Search</button>
          </div>
          <div className='muted' style={{marginTop:8}}>Resultaten tonen wat er in de DB staat. Gebruik “set” om te corrigeren.</div>
          <div style={{marginTop:12}}>
            {items.length===0 ? <div className='muted'>—</div> : items.map(u=> (
              <div key={u.user_id} className='row' style={{justifyContent:'space-between', padding:'10px 0', borderBottom:'1px solid #1f2937'}}>
                <div>
                  <div style={{fontWeight:800}}>{u.user_tag || u.user_id}</div>
                  <div className='muted'>strikes: {u.strikes}</div>
                </div>
                <div className='row'>
                  <input type='number' style={{width:90}} defaultValue={u.strikes} onBlur={e=>setStrike(u.user_id, e.target.value)} />
                  <button className='btn danger' onClick={()=>setStrike(u.user_id, 0)}>Reset</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      );
    }

    function Counters({setErr}){
      const [items, setItems] = useState([]);
      const [manualDraft, setManualDraft] = useState({});

      const load = async()=>{
        setErr('');
        try{
          const r = await api('/api/counters');
          const its = r.items||[];
          setItems(its);
          // init drafts for inputs
          const d = {};
          its.forEach(it=>{ d[it.kind] = (it.manual===null||it.manual===undefined) ? '' : String(it.manual); });
          setManualDraft(d);
        }catch(e){ setErr(e.message); }
      };

      useEffect(()=>{ load(); },[]);

      const saveOne = async(kind)=>{
        setErr('');
        try{
          const v = (manualDraft[kind]||'').trim();
          if(v==='') return;
          await api('/api/counters/override', {method:'POST', body: JSON.stringify({kind, value: Number(v)})});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const clear = async(kind)=>{
        setErr('');
        try{ await api('/api/counters/clear', {method:'POST', body: JSON.stringify({kind})}); await load(); }catch(e){ setErr(e.message); }
      };

      const fetchNow = async()=>{
        setErr('');
        try{ await api('/api/counters/fetch', {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      const resetAll = async()=>{
        setErr('');
        try{ await api('/api/counters/reset', {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      const showVal = (v)=> (v===null || v===undefined) ? '—' : v;

      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Counters</div>
          <div className='muted' style={{marginBottom:10}}>
            Handmatige value wint altijd, behalve als de automatisch gefetchte value hoger is.
          </div>

          {items.map(it=> (
            <div key={it.kind} className='row' style={{justifyContent:'space-between', padding:'10px 0', borderBottom:'1px solid #1f2937'}}>
              <div>
                <div style={{fontWeight:800}}>{it.kind}</div>
                <div className='muted'>fetched: {showVal(it.fetched)} • manual: {showVal(it.manual)} • effective: {showVal(it.effective)}</div>
              </div>
              <div className='row'>
                <input
                  type='number'
                  style={{width:140}}
                  value={manualDraft[it.kind]===undefined ? '' : manualDraft[it.kind]}
                  placeholder='manual'
                  onChange={e=>setManualDraft(s=>({...s,[it.kind]:e.target.value}))}
                />
                <button className='btn primary' onClick={()=>saveOne(it.kind)}>Save</button>
                <button className='btn' onClick={()=>clear(it.kind)}>Clear</button>
              </div>
            </div>
          ))}

          <div className='row' style={{marginTop:12}}>
            <button className='btn' onClick={load}>↻ Refresh</button>
            <button className='btn primary' onClick={fetchNow}>⬇️ Fetch</button>
            <button className='btn danger' onClick={resetAll}>🧹 Reset overrides</button>
          </div>
        </div>
      );
    }

    function Giveaways({setErr}){
      const [list, setList] = useState([]);
      const [channels, setChannels] = useState([]);
      const [templates, setTemplates] = useState([]);

      // 'end' is reused for template apply + manual create.
      const [form, setForm] = useState({channel_id:'', prize:'', end:'30m', winners:1, max_participants:'', description:'', thumbnail_b64:null, thumbnail_name:null});
      const [tpl, setTpl] = useState({name:'', prize:'', description:'', winners:1, max_participants:'', thumbnail_b64:null, thumbnail_name:null});

      const load = async()=>{
        setErr('');
        try{
          const res = await Promise.all([api('/api/giveaways'), api('/api/channels'), api('/api/giveaways/templates')]);
          const a = res[0]; const b = res[1]; const t = res[2];
          setList(a.items||[]);
          setChannels(b.items||[]);
          setTemplates(t.items||[]);
          if(!form.channel_id && (b.items||[]).length) setForm(f=>({...f, channel_id: String(b.items[0].id)}));
        }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); },[]);

      const pickFile = (file, setter)=>{
        if(!file){ setter(f=>({...f, thumbnail_b64:null, thumbnail_name:null})); return; }
        const r = new FileReader();
        r.onload = ()=>{ setter(f=>({...f, thumbnail_b64: String(r.result), thumbnail_name: file.name})); };
        r.readAsDataURL(file);
      };

      const submit = async()=>{
        setErr('');
        try{
          const payload = {
            // Discord Snowflakes must stay strings (JS Number loses precision)
            channel_id: String(form.channel_id),
            prize: form.prize,
            description: form.description,
            winners: Number(form.winners||1),
            max_participants: (form.max_participants===''? null : form.max_participants),
            end_in: form.end,
            thumbnail_b64: form.thumbnail_b64,
            thumbnail_name: form.thumbnail_name
          };
          await api('/api/giveaways/create', {method:'POST', body: JSON.stringify(payload)});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const action = async(id, act)=>{
        setErr('');
        try{ await api(`/api/giveaways/${id}/${act}`, {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      const delGiveaway = async(id)=>{
        setErr('');
        try{ await api(`/api/giveaways/${id}/delete`, {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      const createTemplate = async()=>{
        setErr('');
        try{
          await api('/api/giveaways/templates/create', {method:'POST', body: JSON.stringify({
            name: tpl.name,
            prize: tpl.prize,
            description: tpl.description,
            winners: Number(tpl.winners||1),
            max_participants: tpl.max_participants,
            thumbnail_b64: tpl.thumbnail_b64,
            thumbnail_name: tpl.thumbnail_name
          })});
          setTpl({name:'', prize:'', description:'', winners:1, max_participants:'', thumbnail_b64:null, thumbnail_name:null});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const deleteTemplate = async(id)=>{
        setErr('');
        try{ await api(`/api/giveaways/templates/${id}/delete`, {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      const useTemplate = async(id)=>{
        setErr('');
        try{
          await api(`/api/giveaways/templates/${id}/use`, {method:'POST', body: JSON.stringify({
            // Discord Snowflakes must stay strings (JS Number loses precision)
            channel_id: String(form.channel_id),
            end_in: form.end,
            winners: Number(form.winners||1),
            max_participants: (form.max_participants===''? null : form.max_participants)
          })});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const renderTemplateCard = (t)=>{
        const isBuiltin = String(t.id).indexOf('builtin_')===0;
        return (
          <div key={t.id} className='card' style={{padding:12, margin:'10px 0'}}>
            <div className='row' style={{justifyContent:'space-between', alignItems:'flex-start'}}>
              <div>
                <div style={{fontWeight:800}}>{t.name}</div>
                <div className='muted'>{t.prize}</div>
                <div className='muted' style={{marginTop:4}}>winners: {t.winners} {t.max_participants ? `• max: ${t.max_participants}` : ''}</div>
                {t.thumbnail_name ? <div className='muted' style={{marginTop:4}}>icon: {t.thumbnail_name}</div> : null}
              </div>
              <div className='row'>
                <button className='btn primary' onClick={()=>useTemplate(t.id)}>Gebruik</button>
                {isBuiltin ? null : <button className='btn danger' onClick={()=>deleteTemplate(t.id)}>🗑️</button>}
              </div>
            </div>
          </div>
        );
      };

      return (
        <div className='grid'>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Templates</div>
            <div className='muted' style={{marginBottom:10}}>Gebruik de velden rechts (kanaal/duur/winners/max) om de template-use te overriden.</div>

            {templates.length===0 ? <div className='muted'>Geen templates.</div> : templates.map(renderTemplateCard)}

            <div style={{marginTop:16, fontWeight:800}}>Nieuwe template</div>
            <div className='row' style={{marginTop:8}}>
              <input placeholder='Naam' value={tpl.name} onChange={e=>setTpl(s=>({...s,name:e.target.value}))}/>
              <input placeholder='Prijs' value={tpl.prize} onChange={e=>setTpl(s=>({...s,prize:e.target.value}))}/>
            </div>
            <div style={{marginTop:8}}>
              <textarea rows='3' placeholder='Beschrijving' value={tpl.description} onChange={e=>setTpl(s=>({...s,description:e.target.value}))}></textarea>
            </div>
            <div className='row' style={{marginTop:8}}>
              <input placeholder='Winners' type='number' value={tpl.winners} onChange={e=>setTpl(s=>({...s,winners:e.target.value}))}/>
              <input placeholder='Max deelnemers (optioneel)' value={tpl.max_participants} onChange={e=>setTpl(s=>({...s,max_participants:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:8, alignItems:'center'}}>
              <input type='file' accept='image/*' onChange={e=>pickFile((e.target.files||[])[0], setTpl)} />
              <div className='muted'>{tpl.thumbnail_name ? tpl.thumbnail_name : 'Geen icon'}</div>
            </div>
            <div className='row' style={{marginTop:10}}>
              <button className='btn primary' onClick={createTemplate}>➕ Opslaan</button>
            </div>
          </div>

          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Nieuwe giveaway / template instellingen</div>
            <div className='row'>
              <select value={form.channel_id} onChange={e=>setForm(f=>({...f, channel_id:e.target.value}))}>
                {channels.map(c=> <option key={c.id} value={c.id}>#{c.name}</option>)}
              </select>
            </div>
            <div className='row' style={{marginTop:10}}>
              <input placeholder='Prijs (voor handmatige giveaway)' value={form.prize} onChange={e=>setForm(f=>({...f, prize:e.target.value}))}/>
              <input placeholder='Duur/Eind (30m, 2h, 1d, 19:00, 2026-01-12 19:00)' value={form.end} onChange={e=>setForm(f=>({...f, end:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:10}}>
              <input placeholder='Winners' type='number' value={form.winners} onChange={e=>setForm(f=>({...f, winners:e.target.value}))}/>
              <input placeholder='Max deelnemers (optioneel)' value={form.max_participants} onChange={e=>setForm(f=>({...f, max_participants:e.target.value}))}/>
            </div>
            <div style={{marginTop:10}}>
              <textarea placeholder='Beschrijving (voor handmatige giveaway)' rows='4' value={form.description} onChange={e=>setForm(f=>({...f, description:e.target.value}))}></textarea>
            </div>
            <div className='row' style={{marginTop:8, alignItems:'center'}}>
              <input type='file' accept='image/*' onChange={e=>pickFile((e.target.files||[])[0], setForm)} />
              <div className='muted'>{form.thumbnail_name ? form.thumbnail_name : 'Geen icon'}</div>
            </div>
            <div className='row' style={{marginTop:10}}>
              <button className='btn primary' onClick={submit}>✅ Maak handmatig</button>
            </div>

            <div style={{marginTop:16, fontWeight:800}}>Actieve/Recente giveaways</div>
            {list.length===0 && <div className='muted' style={{marginTop:8}}>Geen giveaways.</div>}
            {list.map(g=> (
              <div key={g.id} className='card' style={{padding:12, margin:'10px 0'}}>
                <div style={{fontWeight:800}}>{g.prize} {g.ended? '(ended)':''}</div>
                <div className='muted'>ID: {g.id} • entries: {g.entries} • end: {g.end_at_human}</div>
                <div className='row' style={{marginTop:8}}>
                  <button className='btn' onClick={()=>action(g.id,'reroll')}>🎲 Reroll</button>
                  <button className='btn danger' onClick={()=>action(g.id,'cancel')}>🛑 Cancel</button>
                  <button className='btn danger' onClick={()=>delGiveaway(g.id)}>🗑️ Delete</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      );
    }

    function Warns({setErr}){
      const [items, setItems] = useState([]);
      const load = async()=>{ setErr(''); try{ setItems((await api('/api/warns')).items||[]); }catch(e){ setErr(e.message); } };
      useEffect(()=>{ load(); },[]);
      const clear = async(uid)=>{ setErr(''); try{ await api('/api/warns/clear', {method:'POST', body: JSON.stringify({user_id: uid})}); await load(); }catch(e){ setErr(e.message);} };
      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Waarschuwingen</div>
          {items.length===0 && <div className='muted'>Geen warns in DB.</div>}
          {items.map(w=> (
            <div key={w.user_id} className='row' style={{justifyContent:'space-between', padding:'8px 0', borderBottom:'1px solid #1f2937'}}>
              <div>
                <div style={{fontWeight:800}}>{w.user_tag || w.user_id}</div>
                <div className='muted'>warns: {w.warns}</div>
              </div>
              <button className='btn danger' onClick={()=>clear(w.user_id)}>Reset</button>
            </div>
          ))}
        </div>
      );
    }

    function Mutes({setErr}){
      const [items, setItems] = useState([]);
      const load = async()=>{ setErr(''); try{ setItems((await api('/api/mutes')).items||[]); }catch(e){ setErr(e.message); } };
      useEffect(()=>{ load(); },[]);
      const unmute = async(uid)=>{ setErr(''); try{ await api('/api/mutes/unmute', {method:'POST', body: JSON.stringify({user_id: uid})}); await load(); }catch(e){ setErr(e.message);} };
      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Mutes</div>
          {items.length===0 && <div className='muted'>Geen actieve mutes in DB.</div>}
          {items.map(m=> (
            <div key={m.user_id} className='row' style={{justifyContent:'space-between', padding:'8px 0', borderBottom:'1px solid #1f2937'}}>
              <div>
                <div style={{fontWeight:800}}>{m.user_tag || m.user_id}</div>
                <div className='muted'>unmute: {m.unmute_at_human}</div>
              </div>
              <button className='btn primary' onClick={()=>unmute(m.user_id)}>Unmute</button>
            </div>
          ))}
        </div>
      );
    }

    function Bans({setErr}){
      const [items, setItems] = useState([]);
      const load = async()=>{ setErr(''); try{ setItems((await api('/api/bans')).items||[]); }catch(e){ setErr(e.message); } };
      useEffect(()=>{ load(); },[]);
      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Bans</div>
          {items.length===0 && <div className='muted'>Geen bans gevonden.</div>}
          {items.map(b=> (
            <div key={b.user_id} className='row' style={{justifyContent:'space-between', padding:'8px 0', borderBottom:'1px solid #1f2937'}}>
              <div>
                <div style={{fontWeight:800}}>{b.name || b.user_id}</div>
                {b.reason ? <div className='muted'>reason: {b.reason}</div> : <div className='muted'>—</div>}
              </div>
              <div className='muted'>ID: {b.user_id}</div>
            </div>
          ))}
          <div className='row' style={{marginTop:12}}>
            <button className='btn' onClick={load}>↻ Refresh</button>
          </div>
        </div>
      );
    }

    ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
  </script>
</body>
</html>"""

    @app.get("/dashboard")
    async def dashboard(req: Request):
        # If no session: show login screen (React handles it)
        return HTMLResponse(DASHBOARD_HTML)

    @app.get("/api/me")
    async def api_me(req: Request):
        uid = _get_user_id_from_request(req)
        if not uid or bot is None:
            return JSONResponse(content=None)
        guild = bot.get_guild(getattr(bot, "guild_id", 0))
        if guild is None:
            try:
                guild = await bot.fetch_guild(getattr(bot, "guild_id", 0))
            except Exception:
                guild = None
        allowed = False
        username = str(uid)
        if guild is not None:
            try:
                member = guild.get_member(uid) or await guild.fetch_member(uid)
                username = f"{member.name}#{member.discriminator}" if getattr(member, "discriminator", None) else member.name
                allowed = bool(getattr(member.guild_permissions, "administrator", False)) or (member.get_role(_bcrew_role_id()) is not None)
            except Exception:
                allowed = False
        return {"user_id": uid, "username": username, "allowed": allowed}

    @app.get("/api/channels")
    async def api_channels(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        guild = bot.get_guild(getattr(bot, "guild_id", 0))
        items = []
        if guild and bot and bot.user:
            # Ensure we have the bot member so we can check permissions
            me = guild.me
            if me is None:
                try:
                    me = guild.get_member(bot.user.id) or await guild.fetch_member(bot.user.id)
                except Exception:
                    me = None
            for ch in getattr(guild, 'text_channels', []):
                try:
                    if me is not None:
                        perms = ch.permissions_for(me)
                        if not (getattr(perms, 'view_channel', False) and getattr(perms, 'send_messages', False)):
                            continue
                    items.append({"id": str(ch.id), "name": ch.name})
                except Exception:
                    continue
        return {"items": items}

    @app.get("/api/voice_channels")
    async def api_voice_channels(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        guild = bot.get_guild(getattr(bot, "guild_id", 0))
        items = []
        if guild:
            for ch in guild.voice_channels:
                items.append({"id": str(ch.id), "name": ch.name})
        return {"items": items}

    @app.get("/api/bans")
    async def api_bans(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        guild = bot.get_guild(getattr(bot, "guild_id", 0))
        if not guild:
            return {"items": []}
        out = []
        try:
            async for entry in guild.bans(limit=200):
                u = entry.user
                out.append({"user_id": str(u.id), "name": str(u), "reason": entry.reason})
        except Exception:
            out = []
        return {"items": out}

    # --- Message sender (Mee6-style) ---
    @app.post("/api/messages/send")
    async def api_messages_send(req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        channel_id = int(body.get("channel_id") or 0)
        content = str(body.get("content") or "")
        embed = body.get("embed")
        if not channel_id:
            return _error(400, "channel_id missing")

        guild = bot.get_guild(getattr(bot, "guild_id", 0))
        ch = bot.get_channel(channel_id)
        if ch is None and guild is not None:
            try:
                ch = guild.get_channel(channel_id)
            except Exception:
                ch = None
        if ch is None:
            try:
                ch = await bot.fetch_channel(channel_id)
            except Exception:
                ch = None
        if ch is None:
            return _error(400, "Channel not found")

        # Permission check (avoid confusing 400s)
        try:
            if guild and bot and bot.user:
                me = guild.me
                if me is None:
                    me = guild.get_member(bot.user.id)
                if me is not None and hasattr(ch, 'permissions_for'):
                    perms = ch.permissions_for(me)
                    if not (getattr(perms, 'view_channel', False) and getattr(perms, 'send_messages', False)):
                        return _error(400, "Bot heeft geen rechten om in dit kanaal te sturen")
        except Exception:
            pass

        # Build embed if provided
        eobj = None
        if embed and isinstance(embed, dict):
            try:
                import discord
                color_hex = str(embed.get("color") or "").replace('#','').strip()
                color = int(color_hex, 16) if color_hex else 0x16A34A
                eobj = discord.Embed(
                    title=(embed.get("title") or None),
                    description=(embed.get("description") or None),
                    url=(embed.get("url") or None),
                    color=color,
                )
                if embed.get("thumbnail_url"):
                    eobj.set_thumbnail(url=str(embed.get("thumbnail_url")))
                if embed.get("image_url"):
                    eobj.set_image(url=str(embed.get("image_url")))
                if embed.get("footer"):
                    eobj.set_footer(text=str(embed.get("footer")))
            except Exception:
                eobj = None

        # Send
        try:
            msg = await ch.send(content=content or None, embed=eobj)
        except Exception as e:
            return _error(500, f"send_failed: {e}")

        # Save history
        gid = getattr(bot, "guild_id", 0)
        try:
            bot.db.add_sent_message(gid, int(channel_id), int(msg.id), content, (json.dumps(embed) if embed else None))
        except Exception:
            pass
        return {"ok": True, \"message_id\": str(msg.id)}

    @app.get("/api/messages/sent")
    async def api_messages_sent(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        rows = bot.db.list_sent_messages(gid, limit=75)
        items = []
        for r in rows:
            items.append(
                {
                    "id": int(r["id"]),
                    "channel_id": str(r["channel_id"]),
                    "message_id": str(r["message_id"]),
                    "content": r["content"],
                    "embed_json": r["embed_json"],
                    "created_at": int(r["created_at"]),
                    "updated_at": int(r["updated_at"]),
                }
            )
        return {"items": items}

    @app.post("/api/messages/{sent_id}/update")
    async def api_messages_update(sent_id: int, req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        gid = getattr(bot, "guild_id", 0)
        row = bot.db.get_sent_message(gid, int(sent_id))
        if not row:
            return _error(404, "not_found")

        channel_id = int(row["channel_id"])
        message_id = int(row["message_id"])
        content = (body.get("content") or "").rstrip() or None
        embed_in = body.get("embed")

        # build embed if provided
        discord_embed = None
        try:
            import discord
            if isinstance(embed_in, dict):
                title = (embed_in.get("title") or "").strip() or None
                description = (embed_in.get("description") or "").strip() or None
                url = (embed_in.get("url") or "").strip() or None
                color_hex = (embed_in.get("color") or "").strip().lstrip("#")
                color = None
                if color_hex:
                    try:
                        color = int(color_hex, 16)
                    except Exception:
                        color = None
                discord_embed = discord.Embed(title=title, description=description, url=url, color=color)
                thumb = (embed_in.get("thumbnail_url") or "").strip()
                if thumb:
                    discord_embed.set_thumbnail(url=thumb)
                img = (embed_in.get("image_url") or "").strip()
                if img:
                    discord_embed.set_image(url=img)
                footer = (embed_in.get("footer") or "").strip()
                if footer:
                    discord_embed.set_footer(text=footer)
                if not any([title, description, url, thumb, img, footer]):
                    discord_embed = None
                    embed_in = None
        except Exception:
            discord_embed = None
            embed_in = None

        ch = bot.get_channel(channel_id)
        if ch is None:
            try:
                ch = await bot.fetch_channel(channel_id)
            except Exception:
                ch = None
        if ch is None:
            return _error(400, "channel_not_found")
        try:
            msg = await ch.fetch_message(message_id)
        except Exception:
            msg = None
        if not msg:
            return _error(404, "message_not_found")

        try:
            await msg.edit(content=content, embed=discord_embed)
        except Exception as e:
            return _error(500, f"edit_failed:{e}")

        try:
            import json as _json
            embed_json = _json.dumps(embed_in) if embed_in else None
            bot.db.update_sent_message(gid, int(sent_id), content=content, embed_json=embed_json)
        except Exception:
            pass
        return {"ok": True}

    @app.post("/api/messages/{sent_id}/delete")
    async def api_messages_delete(sent_id: int, req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        row = bot.db.get_sent_message(gid, int(sent_id))
        if not row:
            return {"ok": True}
        channel_id = int(row["channel_id"])
        message_id = int(row["message_id"])
        ch = bot.get_channel(channel_id)
        if ch is None:
            try:
                ch = await bot.fetch_channel(channel_id)
            except Exception:
                ch = None
        if ch is not None:
            try:
                msg = await ch.fetch_message(message_id)
                await msg.delete()
            except Exception:
                pass
        try:
            bot.db.delete_sent_message(gid, int(sent_id))
        except Exception:
            pass
        return {"ok": True}

    # --- Counters overrides ---
    @app.get("/api/counters")
    async def api_counters(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cog = bot.get_cog('Counters') if bot else None
        if not cog:
            # still allow reading manual overrides from DB
            items = []
            for kind in ["members","twitch","instagram","tiktok"]:
                try:
                    manual = bot.db.get_counter_override(gid, kind)
                except Exception:
                    manual = None
                items.append({"kind": kind, "fetched": None, "manual": manual, "effective": manual})
            return {"items": items}
        return cog.dashboard_counters(gid)

    @app.post("/api/counters/override")
    async def api_counters_override(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        kind = str(body.get("kind") or "").strip().lower()
        value = body.get("value")
        if kind not in {"members","twitch","instagram","tiktok"}:
            return _error(400, "Invalid kind")
        try:
            value = int(value)
        except Exception:
            return _error(400, "Invalid value")
        gid = getattr(bot, "guild_id", 0)
        bot.db.set_counter_override(gid, kind, max(0, value))
        return {"ok": True}

    @app.post("/api/counters/clear")
    async def api_counters_clear(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        kind = str(body.get("kind") or "").strip().lower()
        if kind not in {"members","twitch","instagram","tiktok"}:
            return _error(400, "Invalid kind")
        gid = getattr(bot, "guild_id", 0)
        bot.db.clear_counter_override(gid, kind)
        return {"ok": True}


    @app.post("/api/counters/fetch")
    async def api_counters_fetch(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cog = bot.get_cog('Counters') if bot else None
        guild = bot.get_guild(gid) if bot else None
        if not cog or not guild:
            return _error(400, "Counters cog or guild not available")
        try:
            await cog._ensure_setup(guild)  # type: ignore[attr-defined]
            await cog._refresh_guild(guild)  # type: ignore[attr-defined]
        except Exception as e:
            return _error(500, f"fetch_failed: {e}")
        return cog.dashboard_counters(gid)

    @app.post("/api/counters/reset")
    async def api_counters_reset(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        for kind in ["members","twitch","instagram","tiktok"]:
            try:
                bot.db.clear_counter_override(gid, kind)
            except Exception:
                pass
        return {"ok": True}

    @app.get("/api/warns")
    async def api_warns(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cur = bot.db.conn.cursor()
        rows = cur.execute("SELECT user_id, warns, updated_at FROM warns WHERE guild_id=? AND warns>0 ORDER BY warns DESC", (gid,)).fetchall()
        items=[]
        guild = bot.get_guild(gid)
        for r in rows:
            uid=int(r["user_id"])
            tag=None
            if guild:
                m = guild.get_member(uid)
                if m:
                    tag = str(m)
            items.append({"user_id": uid, "warns": int(r["warns"]), "user_tag": tag})
        return {"items": items}

    # --- Strikes ---
    @app.get("/api/strikes/search")
    async def api_strikes_search(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))

        q = (req.query_params.get("q") or "").strip()
        gid = getattr(bot, "guild_id", 0)
        guild = bot.get_guild(gid) if bot else None

        def _match(member) -> bool:
            if not q:
                return False
            if q.isdigit():
                return int(q) == int(member.id)
            name = f"{member.name}#{member.discriminator}" if getattr(member, "discriminator", None) else member.name
            return q.lower() in name.lower()

        results = []
        if guild:
            # Prefer cached members. For very large guilds this may be partial, but it's fast.
            for m in guild.members:
                if _match(m):
                    results.append(m)
                if len(results) >= 25:
                    break
        # If query is digits, we can return even if not cached
        if q.isdigit() and guild:
            uid = int(q)
            if not any(int(m.id) == uid for m in results):
                try:
                    m = guild.get_member(uid) or await guild.fetch_member(uid)
                    results = [m]
                except Exception:
                    results = []

        items = []
        if bot:
            cur = bot.db.conn.cursor()
            for m in results:
                row = cur.execute("SELECT strikes FROM strikes WHERE guild_id=? AND user_id=?", (gid, int(m.id))).fetchone()
                strikes = int(row[0]) if row else 0
                items.append({\"user_id\": str(m.id), "user_tag": str(m), "strikes": strikes})
        return {"items": items}

    @app.post("/api/strikes/set")
    async def api_strikes_set(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        uid = int(body.get("user_id"))
        strikes = max(0, int(body.get("strikes") or 0))
        gid = getattr(bot, "guild_id", 0)
        bot.db.set_strikes(gid, uid, strikes)
        return {"ok": True}

    @app.post("/api/warns/clear")
    async def api_warns_clear(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        uid = int(body.get("user_id"))
        gid = getattr(bot, "guild_id", 0)
        bot.db.delete_warns(gid, uid)
        return {"ok": True}

    @app.get("/api/mutes")
    async def api_mutes(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cur = bot.db.conn.cursor()
        rows = cur.execute("SELECT user_id, unmute_at FROM mutes WHERE guild_id=? ORDER BY unmute_at ASC", (gid,)).fetchall()
        items=[]
        guild = bot.get_guild(gid)
        for r in rows:
            uid=int(r["user_id"])
            tag=None
            if guild:
                m=guild.get_member(uid)
                if m:
                    tag=str(m)
            items.append({"user_id": uid, "unmute_at": int(r["unmute_at"]), "unmute_at_human": time.strftime('%Y-%m-%d %H:%M', time.localtime(int(r['unmute_at']))), "user_tag": tag})
        return {"items": items}

    @app.post("/api/mutes/unmute")
    async def api_unmute(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        uid = int(body.get("user_id"))
        gid = getattr(bot, "guild_id", 0)
        guild = bot.get_guild(gid)
        if not guild:
            return _error(400, "Guild not cached")
        # Member might have left; still allow clearing DB record
        try:
            member = guild.get_member(uid) or await guild.fetch_member(uid)
        except Exception:
            member = None
        # pull roles_json from db
        cur = bot.db.conn.cursor()
        row = cur.execute("SELECT roles_json FROM mutes WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()
        roles_json = row[0] if row else "[]"
        try:
            if member is not None:
                await bot._restore_roles_after_mute(guild, member, roles_json)
        except Exception:
            pass
        try:
            bot.db.clear_mute(gid, uid)
        except Exception:
            pass
        return {"ok": True}

    # --- Music ---
    @app.get("/api/music/status")
    async def api_music_status(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cog = bot.get_cog('Music')
        if not cog:
            return {"now": None, "queue": []}
        return cog.dashboard_status(gid)

    @app.post("/api/music/action")
    async def api_music_action(req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        gid = getattr(bot, "guild_id", 0)
        cog = bot.get_cog('Music')
        if not cog:
            return _error(400, "Music cog not loaded")
        try:
            await cog.dashboard_action(gid, uid, body)
            return {"ok": True}
        except Exception as e:
            return _error(500, str(e))

    @app.get("/api/radio/stations")
    async def api_radio_stations(req: Request):
        """Expose radio stations to the dashboard.

        Env format supported:
          {"qmusic":"https://...mp3", ...}
        """
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))

        # Friendly names + logos (remote). You can override by setting RADIO_STATIONS_META_JSON.
        default_meta = {
            "qmusic": {"name": "Qmusic", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/Qmusic%20logo.svg"},
            "slam": {"name": "SLAM!", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/Slam%21_logo.png"},
            "lofi": {"name": "SomaFM – Groove Salad", "logo_url": "https://somafm.com/img3/somafm-logo.png"},
            "538": {"name": "Radio 538", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/Logo%20538%20Nederland.png"},
            "radio10": {"name": "Radio 10", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/Radio%2010%20logo%202019.svg"},
            "veronica": {"name": "Veronica", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/Radio_Veronica_logo.svg"},
            "sky_radio": {"name": "Sky Radio", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/Sky%20Radio%20logo%202019.svg"},
            "npo_radio1": {"name": "NPO Radio 1", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/NPO%20Radio%201%20logo%202014.svg"},
            "npo_radio2": {"name": "NPO Radio 2", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/NPO%20Radio%202%20logo.svg"},
            "npo_3fm": {"name": "NPO 3FM", "logo_url": "https://commons.wikimedia.org/wiki/Special:FilePath/NPO%203FM%20logo%202020.svg"},
        }

        meta_raw = (os.getenv("RADIO_STATIONS_META_JSON") or "").strip()
        meta = default_meta
        if meta_raw:
            try:
                parsed = json.loads(meta_raw)
                if isinstance(parsed, dict):
                    meta = {**default_meta, **parsed}
            except Exception:
                meta = default_meta

        cog = bot.get_cog('Music')
        stations = {}
        if cog and getattr(cog, "radio_stations", None):
            stations = dict(getattr(cog, "radio_stations"))
        else:
            # fallback: parse env directly
            raw = (os.getenv("RADIO_STATIONS_JSON") or "{}").strip()
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    stations = {str(k): str(v) for k, v in parsed.items()}
            except Exception:
                stations = {}

        items = []
        order = ["qmusic", "slam", "538", "radio10", "veronica", "sky_radio", "npo_radio1", "npo_radio2", "npo_3fm", "lofi"]
        keys = [k for k in order if k in stations] + [k for k in stations.keys() if k not in order]
        for k in keys:
            url = stations.get(k)
            if not url:
                continue
            m = meta.get(k, {}) if isinstance(meta, dict) else {}
            items.append({
                "id": str(k),
                "name": str(m.get("name") or k),
                "stream_url": str(url),
                "logo_url": str(m.get("logo_url") or ""),
            })
        return {"stations": items}



    # --- Playlist (default) ---
    @app.get("/api/playlist/tracks")
    async def api_playlist_tracks(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        pl_id = bot.db.get_or_create_playlist(gid, name="default", created_by=None)
        rows = bot.db.list_playlist_tracks(pl_id, limit=100)
        items = []
        for r in rows:
            items.append({"id": int(r["id"]), "title": r["title"], "webpage_url": r["webpage_url"] or r["url"], "added_at": int(r["added_at"])})
        return {"items": items}

    @app.post("/api/playlist/enqueue")
    async def api_playlist_enqueue(req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        track_id = int(body.get("track_id") or 0)
        gid = getattr(bot, "guild_id", 0)
        pl_id = bot.db.get_or_create_playlist(gid, name="default", created_by=None)
        # fetch track
        cur = bot.db.conn.cursor()
        row = cur.execute("SELECT title, url, webpage_url FROM playlist_tracks WHERE id=? AND playlist_id=?", (track_id, pl_id)).fetchone()
        if not row:
            return _error(404, "Track not found")
        cog = bot.get_cog('Music')
        if not cog:
            return _error(400, "Music cog not loaded")
        await cog.dashboard_action(gid, uid, {"action": "enqueue", "url": row["webpage_url"] or row["url"]})
        return {"ok": True}
    # --- Giveaways ---
    @app.get("/api/giveaways")
    async def api_giveaways(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cur = bot.db.conn.cursor()
        rows = cur.execute("SELECT id, prize, end_at, ended FROM giveaways WHERE guild_id=? ORDER BY id DESC LIMIT 20", (gid,)).fetchall()
        items=[]
        for r in rows:
            gidw=int(r['id'])
            entries = bot.db.giveaway_entry_count(gidw)
            items.append({"id": gidw, "prize": r['prize'], "end_at": int(r['end_at']), "end_at_human": time.strftime('%Y-%m-%d %H:%M', time.localtime(int(r['end_at']))), "ended": bool(int(r['ended'])), "entries": entries})
        return {"items": items}

    @app.post("/api/giveaways/create")
    async def api_giveaways_create(req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        cog = bot.get_cog('Giveaway')
        if not cog:
            return _error(400, "Giveaway cog not loaded")
        await cog.dashboard_create(guild_id=getattr(bot, 'guild_id', 0), actor_user_id=uid, **body)
        return {"ok": True}

    @app.post("/api/giveaways/{giveaway_id}/cancel")
    async def api_giveaways_cancel(giveaway_id: int, req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        cog = bot.get_cog('Giveaway')
        if not cog:
            return _error(400, "Giveaway cog not loaded")
        ok = await cog.dashboard_cancel(getattr(bot, 'guild_id', 0), giveaway_id, uid)
        if not ok:
            return _error(400, "Cancel failed")
        return {"ok": True}

    @app.post("/api/giveaways/{giveaway_id}/reroll")
    async def api_giveaways_reroll(giveaway_id: int, req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        cog = bot.get_cog('Giveaway')
        if not cog:
            return _error(400, "Giveaway cog not loaded")
        ok = await cog.dashboard_reroll(getattr(bot, 'guild_id', 0), giveaway_id, uid)
        if not ok:
            return _error(400, "Reroll failed")
        return {"ok": True}

    @app.post("/api/giveaways/{giveaway_id}/delete")
    async def api_giveaways_delete(giveaway_id: int, req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        row = bot.db.get_giveaway(int(giveaway_id))
        if row:
            # best effort delete message
            try:
                guild = bot.get_guild(getattr(bot, "guild_id", 0))
                channel = (guild.get_channel(int(row["channel_id"])) if guild else None) or await bot.fetch_channel(int(row["channel_id"]))
                if channel:
                    try:
                        msg = await channel.fetch_message(int(row["message_id"]))
                        await msg.delete()
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                bot.db.delete_giveaway(int(giveaway_id))
            except Exception:
                pass
        return {"ok": True}

    # --- Giveaway templates ---
    @app.get("/api/giveaways/templates")
    async def api_giveaway_templates(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        rows = bot.db.list_giveaway_templates(gid)
        items = []
        for r in rows:
            items.append(
                {
                    "id": int(r["id"]),
                    "name": r["name"],
                    "prize": r["prize"],
                    "description": r["description"],
                    "winners": int(r["winners_count"] or 1),
                    "max_participants": r["max_participants"],
                    "thumbnail_name": r["thumbnail_name"],
                    "thumbnail_b64": r["thumbnail_b64"],
                }
            )
        # Built-in default template (1000 vbucks)
        if not any(str(t.get("id")) == "builtin_1000" for t in items):
            items.append(
                {
                    "id": "builtin_1000",
                    "name": "1000 vbucks",
                    "prize": "1000 V-Bucks",
                    "description": "Win 1000 V-Bucks!",
                    "winners": 1,
                    "max_participants": None,
                    "thumbnail_name": "vbucks-1000.jpg",
                    "thumbnail_b64": None,
                }
            )
        return {"items": items}

    @app.post("/api/giveaways/templates/create")
    async def api_giveaway_templates_create(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        gid = getattr(bot, "guild_id", 0)
        tid = bot.db.create_giveaway_template(
            guild_id=gid,
            name=str(body.get("name") or "").strip() or "Template",
            prize=str(body.get("prize") or "").strip() or "Giveaway",
            description=(str(body.get("description")).strip() if body.get("description") is not None else None),
            winners_count=int(body.get("winners") or 1),
            max_participants=(int(body["max_participants"]) if body.get("max_participants") not in (None, "") else None),
            thumbnail_name=(str(body.get("thumbnail_name") or "").strip() or None),
            thumbnail_b64=(body.get("thumbnail_b64") or None),
        )
        return {"ok": True, "id": tid}

    @app.post("/api/giveaways/templates/{template_id}/delete")
    async def api_giveaway_templates_delete(template_id: str, req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        if str(template_id).startswith("builtin_"):
            return _error(400, "builtin_template")
        gid = getattr(bot, "guild_id", 0)
        try:
            bot.db.delete_giveaway_template(gid, int(template_id))
        except Exception:
            pass
        return {"ok": True}

    @app.post("/api/giveaways/templates/{template_id}/use")
    async def api_giveaway_templates_use(template_id: str, req: Request):
        try:
            uid = await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        channel_id = int(body.get("channel_id") or 0)
        if not channel_id:
            return _error(400, "channel_id missing")
        # resolve template
        tpl = None
        if str(template_id) == "builtin_1000":
            tpl = {
                "prize": "1000 V-Bucks",
                "description": "Win 1000 V-Bucks!",
                "winners": 1,
                "max_participants": None,
                "thumbnail_name": "vbucks-1000.jpg",
                "thumbnail_b64": None,
                "builtin_file": True,
            }
        else:
            row = bot.db.get_giveaway_template(getattr(bot, "guild_id", 0), int(template_id))
            if row:
                tpl = {
                    "prize": row["prize"],
                    "description": row["description"],
                    "winners": int(row["winners_count"] or 1),
                    "max_participants": row["max_participants"],
                    "thumbnail_name": row["thumbnail_name"],
                    "thumbnail_b64": row["thumbnail_b64"],
                    "builtin_file": False,
                }
        if not tpl:
            return _error(404, "template_not_found")
        cog = bot.get_cog("Giveaway")
        if not cog:
            return _error(400, "Giveaway cog not loaded")

        # builtin image file -> convert to data url so dashboard_create can attach it
        thumb_b64 = tpl.get("thumbnail_b64")
        thumb_name = tpl.get("thumbnail_name")
        if tpl.get("builtin_file") and thumb_name:
            try:
                import base64 as _b64
                import mimetypes as _mt
                p = os.path.join(os.path.dirname(__file__), "static", thumb_name)
                with open(p, "rb") as f:
                    blob = f.read()
                mime = _mt.guess_type(thumb_name)[0] or "image/jpeg"
                thumb_b64 = f"data:{mime};base64,{_b64.b64encode(blob).decode('utf-8')}"
            except Exception:
                thumb_b64 = None
        payload = {
            "channel_id": channel_id,
            "prize": tpl.get("prize"),
            "description": tpl.get("description"),
            "winners": int(tpl.get("winners") or 1),
            "max_participants": tpl.get("max_participants"),
            "thumbnail_b64": thumb_b64,
            "thumbnail_name": thumb_name,
        }
        # timing from request
        if body.get("end_at") is not None:
            payload["end_at"] = int(body.get("end_at"))
        if body.get("end_in") is not None:
            payload["end_in"] = str(body.get("end_in"))
        try:
            await cog.dashboard_create(guild_id=getattr(bot, "guild_id", 0), actor_user_id=uid, **payload)
        except Exception as e:
            return _error(500, str(e))
        return {"ok": True}

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
