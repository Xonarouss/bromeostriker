import os
import time
import sqlite3
import secrets
import hashlib
from typing import Optional, Dict, Any
from urllib.parse import urlencode
import threading

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


def create_app(bot=None) -> FastAPI:
    app = FastAPI(title="BromeStriker OAuth")


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
  <title>BromeStriker Dashboard</title>
  <script src="https://unpkg.com/react@18/umd/react.development.js" crossorigin></script>
  <script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js" crossorigin></script>
  <script src="https://unpkg.com/babel-standalone@6/babel.min.js"></script>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial; margin:0; background:#0b1220; color:#e5e7eb;}
    .top{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid #1f2937;background:#0b1220;position:sticky;top:0}
    .brand{font-weight:700;letter-spacing:0.3px}
    .btn{background:#111827;border:1px solid #334155;color:#e5e7eb;padding:8px 12px;border-radius:10px;cursor:pointer}
    .btn.primary{background:#16a34a;border-color:#16a34a;color:#04120a}
    .wrap{max-width:1100px;margin:0 auto;padding:18px}
    .tabs{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
    .tab{padding:8px 12px;border-radius:999px;border:1px solid #334155;background:#0f172a;cursor:pointer}
    .tab.active{background:#16a34a;border-color:#16a34a;color:#04120a}
    .card{background:#0f172a;border:1px solid #1f2937;border-radius:16px;padding:14px;margin-bottom:12px;box-shadow:0 10px 30px rgba(0,0,0,.25)}
    .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}
    .col6{grid-column:span 6}
    .col12{grid-column:span 12}
    input,select,textarea{width:100%;padding:10px 12px;border-radius:12px;border:1px solid #334155;background:#0b1220;color:#e5e7eb}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
    .muted{color:#94a3b8}
    .danger{border-color:#ef4444}
    .btn.danger{background:#ef4444;border-color:#ef4444;color:#0b1220}
    a{color:#22c55e}
    @media (max-width: 900px){.col6{grid-column:span 12}}
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

      const loadMe = async()=>{
        setErr('');
        try{ setMe(await api('/api/me')); }catch(e){ setMe(null); }
      };

      useEffect(()=>{ loadMe(); },[]);

      if(!me){
        return (
          <div className='wrap'>
            <div className='card'>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                <div>
                  <div style={{fontSize:22,fontWeight:800}}>BromeStriker Dashboard</div>
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

      return (
        <div>
          <div className='top'>
            <div className='brand'>BromeStriker Dashboard</div>
            <div className='row'>
              <div className='muted'>Ingelogd als {me.username}</div>
              <a className='btn' href='/logout'>Logout</a>
            </div>
          </div>
          <div className='wrap'>
            <div className='tabs'>
              {['music','giveaways','warns','mutes'].map(k=> (
                <div key={k} className={'tab '+(tab===k?'active':'')} onClick={()=>setTab(k)}>{k}</div>
              ))}
            </div>
            {err && <div className='card danger'>❌ {err}</div>}
            {tab==='music' && <Music setErr={setErr} />}
            {tab==='giveaways' && <Giveaways setErr={setErr} />}
            {tab==='warns' && <Warns setErr={setErr} />}
            {tab==='mutes' && <Mutes setErr={setErr} />}
          </div>
        </div>
      );
    }

    function Music({setErr}){
      const [st, setSt] = useState(null);
      const [url, setUrl] = useState('');
      const load = async()=>{
        setErr('');
        try{ setSt(await api('/api/music/status')); }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); const t=setInterval(load, 4000); return ()=>clearInterval(t); },[]);

      const act = async(action, payload={})=>{
        setErr('');
        try{ await api('/api/music/action', {method:'POST', body: JSON.stringify({action, ...payload})}); await load(); }catch(e){ setErr(e.message); }
      };

      return (
        <div className='grid'>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Now Playing</div>
            <div className='muted'>{st?.now || '—'}</div>
            <div className='row' style={{marginTop:12}}>
              <button className='btn' onClick={()=>act('pause_resume')}>⏯️</button>
              <button className='btn primary' onClick={()=>act('skip')}>⏭️ Skip</button>
              <button className='btn' onClick={()=>act('stop')}>⏹️ Stop</button>
              <button className='btn' onClick={()=>act('vol_down')}>🔉</button>
              <button className='btn' onClick={()=>act('vol_up')}>🔊</button>
            </div>
            <div className='row' style={{marginTop:14}}>
              <input placeholder='YouTube link of zoekterm…' value={url} onChange={e=>setUrl(e.target.value)} />
              <button className='btn primary' onClick={()=>act('play', {query:url})}>▶️ Play</button>
              <button className='btn' onClick={()=>act('add_playlist', {query:url})}>➕ Playlist</button>
            </div>
          </div>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Queue</div>
            <div className='muted' style={{whiteSpace:'pre-wrap'}}>{(st?.queue||[]).join('
') || '—'}</div>
            <div className='row' style={{marginTop:12}}>
              <button className='btn' onClick={()=>act('play_playlist')}>▶️ Play playlist</button>
              <button className='btn danger' onClick={()=>act('clear_playlist')}>🧹 Clear playlist</button>
            </div>
          </div>
        </div>
      );
    }

    function Giveaways({setErr}){
      const [list, setList] = useState([]);
      const [channels, setChannels] = useState([]);
      const [form, setForm] = useState({channel_id:'', prize:'', end:'30m', winners:1, max_participants:'', description:''});

      const load = async()=>{
        setErr('');
        try{
          const [a,b] = await Promise.all([api('/api/giveaways'), api('/api/channels')]);
          setList(a.items||[]); setChannels(b.items||[]);
          if(!form.channel_id && (b.items||[]).length) setForm(f=>({...f, channel_id: String(b.items[0].id)}));
        }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); },[]);

      const submit = async()=>{
        setErr('');
        try{
          await api('/api/giveaways/create', {method:'POST', body: JSON.stringify({...form, winners: Number(form.winners||1), channel_id: Number(form.channel_id)})});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const action = async(id, act)=>{
        setErr('');
        try{ await api(`/api/giveaways/${id}/${act}`, {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      return (
        <div className='grid'>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Nieuwe giveaway</div>
            <div className='row'>
              <select value={form.channel_id} onChange={e=>setForm(f=>({...f, channel_id:e.target.value}))}>
                {channels.map(c=> <option key={c.id} value={c.id}>#{c.name}</option>)}
              </select>
            </div>
            <div className='row' style={{marginTop:10}}>
              <input placeholder='Prijs' value={form.prize} onChange={e=>setForm(f=>({...f, prize:e.target.value}))}/>
              <input placeholder='Eind (30m, 2h, 1d, 19:00, 2026-01-12 19:00)' value={form.end} onChange={e=>setForm(f=>({...f, end:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:10}}>
              <input placeholder='Winners' type='number' value={form.winners} onChange={e=>setForm(f=>({...f, winners:e.target.value}))}/>
              <input placeholder='Max deelnemers (optioneel)' value={form.max_participants} onChange={e=>setForm(f=>({...f, max_participants:e.target.value}))}/>
            </div>
            <div style={{marginTop:10}}>
              <textarea placeholder='Beschrijving' rows='4' value={form.description} onChange={e=>setForm(f=>({...f, description:e.target.value}))}></textarea>
            </div>
            <div className='row' style={{marginTop:10}}>
              <button className='btn primary' onClick={submit}>✅ Maak</button>
            </div>
          </div>

          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Actieve/Recente giveaways</div>
            {list.length===0 && <div className='muted'>Geen giveaways.</div>}
            {list.map(g=> (
              <div key={g.id} className='card' style={{padding:12, margin:'10px 0'}}>
                <div style={{fontWeight:800}}>{g.prize} {g.ended? '(ended)':''}</div>
                <div className='muted'>ID: {g.id} • entries: {g.entries} • end: {g.end_at_human}</div>
                <div className='row' style={{marginTop:8}}>
                  <button className='btn' onClick={()=>action(g.id,'reroll')}>🎲 Reroll</button>
                  <button className='btn danger' onClick={()=>action(g.id,'cancel')}>🛑 Cancel</button>
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
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Warns</div>
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
        if guild:
            for ch in guild.text_channels:
                items.append({"id": ch.id, "name": ch.name})
        return {"items": items}

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
        member = guild.get_member(uid) or await guild.fetch_member(uid)
        # pull roles_json from db
        cur = bot.db.conn.cursor()
        row = cur.execute("SELECT roles_json FROM mutes WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()
        roles_json = row[0] if row else "[]"
        await bot._restore_roles_after_mute(guild, member, roles_json)
        bot.db.clear_mute(gid, uid)
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
        await cog.dashboard_action(gid, uid, body)
        return {"ok": True}



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
