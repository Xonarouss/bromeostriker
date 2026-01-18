import os
import time
import sqlite3
import secrets
import hashlib
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

    const TAB_LABELS = {
      music: 'Muziek',
      messages: 'Berichten',
      giveaways: 'Giveaways',
      strikes: 'Strikes',
      counters: 'Counters',
      warns: 'Waarschuwingen',
      mutes: 'Mutes',
      bans: 'Bans'
    };


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
      const [timeNL, setTimeNL] = useState('');

      const loadMe = async()=>{
        setErr('');
        try{ setMe(await api('/api/me')); }catch(e){ setMe(null); }
      };

      useEffect(()=>{ loadMe(); },[]);

      useEffect(()=>{
        const fmt = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', weekday:'short', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit' });
        const tick = ()=> setTimeNL(fmt.format(new Date()));
        tick();
        const id = setInterval(tick, 1000);
        return ()=>clearInterval(id);
      },[]);

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
                <a className='btn' href='/logout'>Uitloggen</a>
              </div>
            </div>
          </div>
        );
      }

      return (
        <div>
          <div className='top'>
            <div className='row' style={{gap:14}}>
              <div className='muted' style={{minWidth:170}}>{timeNL}</div>
              <div className='brand'>BromeoStriker Dashboard</div>
            </div>
            <div className='row'>
              <div className='muted'>Ingelogd als {me.username}</div>
              <a className='btn' href='/logout'>Uitloggen</a>
            </div>
          </div>
          <div className='wrap'>
            <div className='tabs'>
              {(() => {
  const L={music:'Muziek',messages:'Berichten',giveaways:'Giveaways',strikes:'Strikes',counters:'Tellers',warns:'Waarschuwingen',mutes:'Mutes',bans:'Bans'};
  return ['music','messages','giveaways','strikes','counters','warns','mutes','bans'].map(k=> (
    <div key={k} className={'tab '+(tab===k?'active':'')} onClick={()=>setTab(k)}>{L[k]||k}</div>
  ));
})()}
            </div>
            {err && <div className='card danger'>❌ {err}</div>}
            {tab==='music' && <Music setErr={setErr} />}
            {tab==='messages' && <Berichten setErr={setErr} />}
            {tab==='giveaways' && <Giveaways setErr={setErr} />}
            {tab==='strikes' && <Strikes setErr={setErr} />}
            {tab==='counters' && <Counters setErr={setErr} />}
            {tab==='warns' && <Warns setErr={setErr} />}
            {tab==='mutes' && <Mutes setErr={setErr} />}
            {tab==='bans' && <Bans setErr={setErr} />}
          </div>
            <div style={{marginTop:18, paddingTop:14, borderTop:'1px solid #1f2937', textAlign:'center'}} className='muted'>
              Made with ❤️ by <a href='https://xonarous.nl' target='_blank' rel='noreferrer'>Xonarous</a>
            </div>
        </div>
      );
    }

    

    function Bans({setErr}){
      const [items, setItems] = useState([]);
      const load = async()=>{ setErr(''); try{ setItems((await api('/api/bans')).items||[]); }catch(e){ setErr(e.message); } };
      useEffect(()=>{ load(); },[]);
      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Bans</div>
          <div className='muted' style={{marginBottom:10}}>Overzicht van recent opgehaalde bans (max 200).</div>
          <div className='row' style={{justifyContent:'space-between', marginBottom:10}}>
            <button className='btn' onClick={load}>↻ Vernieuwen</button>
          </div>
          <div style={{display:'flex',flexDirection:'column',gap:8}}>
            {(items||[]).length===0 ? <div className='muted'>Geen bans gevonden.</div> : (items||[]).map((b,i)=>(
              <div key={i} className='card' style={{margin:0, padding:12, background:'#0b1220'}}>
                <div style={{fontWeight:800}}>{b.user_tag || b.user_id}</div>
                <div className='muted'>Reden: {b.reason || '—'}</div>
              </div>
            ))}
          </div>
        </div>
      );
    }

    function Music({setErr}){
      const [st, setSt] = useState(null);
      const [url, setUrl] = useState('');
      const [voiceChannels, setSpraakkanaalChannels] = useState([]);
      const [voiceId, setSpraakkanaalId] = useState('');
      const load = async()=>{
        setErr('');
        try{ setSt(await api('/api/music/status')); }catch(e){ setErr(e.message); }
      };
      const loadSpraakkanaal = async()=>{
        try{
          const r = await api('/api/voice_channels');
          setSpraakkanaalChannels(r.items||[]);
          if(!voiceId && (r.items||[]).length) setSpraakkanaalId(String(r.items[0].id));
        }catch(e){}
      };
      useEffect(()=>{ load(); loadSpraakkanaal(); const t=setInterval(load, 4000); return ()=>clearInterval(t); },[]);

      const act = async(action, payload={})=>{
        setErr('');
        try{ await api('/api/music/action', {method:'POST', body: JSON.stringify({action, ...payload})}); await load(); }catch(e){ setErr(e.message); }
      };

      return (
        <div className='grid'>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Nu bezig</div>
            <div className='muted'>
              {(st && st.now && (typeof st.now === 'object')) ? (
                <a href={st.now.webpage_url || '#'} target='_blank' rel='noreferrer'>
                  {st.now.title || '—'}
                </a>
              ) : ((st && st.now) ? String(st.now) : '—')}
            </div>
            <div className='row' style={{marginTop:12}}>
              <button className='btn' onClick={()=>act('pause_resume')}>⏯️</button>
              <button className='btn primary' onClick={()=>act('skip')}>⏭️ Overslaan</button>
              <button className='btn' onClick={()=>act('stop')}>⏹️ Stop</button>
              <button className='btn' onClick={()=>act('vol_down')}>🔉</button>
              <button className='btn' onClick={()=>act('vol_up')}>🔊</button>
            </div>
            <div className='row' style={{marginTop:14}}>
              <input placeholder='YouTube-link of zoekterm…' value={url} onChange={e=>setUrl(e.target.value)} />
              <button className='btn primary' onClick={()=>act('play', {query:url})}>▶️ Afspelen</button>
              <button className='btn' onClick={()=>act('add_playlist', {query:url})}>➕ Afspeellijst</button>
            </div>

            <div style={{marginTop:14, fontWeight:800}}>Spraakkanaal</div>
            <div className='row' style={{marginTop:8}}>
              <select value={voiceId} onChange={e=>setSpraakkanaalId(e.target.value)}>
                {voiceChannels.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)
                }
              </select>
              <button className='btn' onClick={()=>act('join', {channel_id: String(voiceId)})}>Verbinden</button>
              <button className='btn danger' onClick={()=>act('disconnect')}>Loskoppelen</button>
              <button className='btn' onClick={loadSpraakkanaal}>↻</button>
            </div>
          </div>
          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:6}}>Wachtrij</div>
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
              <button className='btn' onClick={()=>act('play_playlist')}>▶️ Afspelen playlist</button>
              <button className='btn danger' onClick={()=>act('clear_playlist')}>🧹 Clear playlist</button>
            </div>
          </div>
        </div>
      );
    }

    function Berichten({setErr}){
      const [channels, setChannels] = useState([]);
      const [channelId, setChannelId] = useState('');
      const [content, setContent] = useState('');
      const [embed, setEmbed] = useState({title:'', description:'', url:'', color:'#16a34a', thumbnail_url:'', image_url:'', footer:''});

      const load = async()=>{
        setErr('');
        try{
          const ch = await api('/api/channels');
          setChannels(ch.items||[]);
          if(!channelId && (ch.items||[]).length) setChannelId(String(ch.items[0].id));
        }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); },[]);

      const send = async()=>{
        setErr('');
        try{
          // IMPORTANT: Discord snowflake IDs exceed JS safe integer range.
          // Send as string to avoid precision loss ("Channel not found" errors).
          await api('/api/messages/send', {
            method:'POST',
            body: JSON.stringify({
              channel_id: String(channelId),
              content,
              embed: {
                ...embed,
                color: (embed.color||'').replace('#','')
              }
            })
          });
          setContent('');
        }catch(e){ setErr(e.message); }
      };

      const hasEmbed = Object.values(embed).some(v=>String(v||'').trim()!=='' && v!=='#16a34a');

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
            <div style={{marginTop:12,fontWeight:800}}>Embed (optioneel)</div>
            <div className='row' style={{marginTop:8}}>
              <input placeholder='Title' value={embed.title} onChange={e=>setEmbed(s=>({...s,title:e.target.value}))}/>
              <input placeholder='URL' value={embed.url} onChange={e=>setEmbed(s=>({...s,url:e.target.value}))}/>
            </div>
            <div style={{marginTop:8}}>
              <textarea rows='4' placeholder='Beschrijving' value={embed.description} onChange={e=>setEmbed(s=>({...s,description:e.target.value}))}></textarea>
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
              <button className='btn primary' onClick={send}>📨 Versturen</button>
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
                  <button className='btn danger' onClick={()=>setStrike(u.user_id, 0)}>Resetten</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      );
    }

    function Counters({setErr}){
      const [items, setItems] = useState([]);
      const [draft, setDraft] = useState({});

      const load = async()=>{
        setErr('');
        try{
          const r = await api('/api/counters');
          const its = r.items || [];
          setItems(its);
          // seed drafts
          const d = {};
          its.forEach(it=>{ d[it.kind] = (it.manual === null || it.manual === undefined) ? '' : String(it.manual); });
          setDraft(d);
        }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); },[]);

      const save = async(kind)=>{
        setErr('');
        try{
          const v = (draft[kind] || '').toString().trim();
          if(v==='') return;
          await api('/api/counters/override', {method:'POST', body: JSON.stringify({kind, value: v})});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const reset = async(kind)=>{
        setErr('');
        try{ await api('/api/counters/clear', {method:'POST', body: JSON.stringify({kind})}); await load(); }catch(e){ setErr(e.message); }
      };

      const fetchNow = async()=>{
        setErr('');
        try{ await api('/api/counters/fetch', {method:'POST'}); await load(); }catch(e){ setErr(e.message); }
      };

      return (
        <div className='card'>
          <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Tellers</div>
          <div className='muted' style={{marginBottom:10}}>
            Handmatige value wint altijd, behalve als de automatisch gefetchte value hoger is.
          </div>

          {items.map(it=> (
            <div key={it.kind} style={{padding:'10px 0', borderBottom:'1px solid #1f2937'}}>
              <div className='row' style={{justifyContent:'space-between'}}>
                <div>
                  <div style={{fontWeight:800}}>{it.kind}</div>
                  <div className='muted'>
                    opgehaald: {(it.fetched === null || it.fetched === undefined) ? '—' : it.fetched}
                    {' • '}handmatig: {(it.manual === null || it.manual === undefined) ? '—' : it.manual}
                    {' • '}effectief: {(it.effective === null || it.effective === undefined) ? '—' : it.effective}
                  </div>
                </div>
                <div className='row'>
                  <input
                    type='number'
                    style={{width:140}}
                    value={(draft[it.kind] === undefined) ? '' : draft[it.kind]}
                    placeholder='handmatig'
                    onChange={e=>setDraft(d=>({...d,[it.kind]:e.target.value}))}
                  />
                  <button className='btn primary' onClick={()=>save(it.kind)}>Opslaan</button>
                  <button className='btn' onClick={()=>reset(it.kind)}>Resetten</button>
                </div>
              </div>
            </div>
          ))}

          <div className='row' style={{marginTop:12}}>
            <button className='btn' onClick={load}>↻ Vernieuwen</button>
            <button className='btn' onClick={fetchNow}>🌐 Nu ophalen</button>
          </div>
        </div>
      );
    }

    function Giveaways({setErr}){
      const [list, setList] = useState([]);
      const [channels, setChannels] = useState([]);
      const [form, setForm] = useState({channel_id:'', prize:'', end_in:'30m', winners:1, max_participants:'', description:'', thumbnail_b64:'', thumbnail_name:''});
      const [tpl, setTpl] = useState({channel_id:'', end_dt:'', winners:1});

      const fileToB64 = (file)=> new Promise((resolve, reject)=>{
        const r = new FileReader();
        r.onload = ()=>{
          const s = String(r.result||'');
          const idx = s.indexOf('base64,');
          resolve(idx>=0 ? s.slice(idx+7) : s);
        };
        r.onerror = ()=>reject(new Error('file read failed'));
        r.readAsDataURL(file);
      });

      const load = async()=>{
        setErr('');
        try{
          const [a,b] = await Promise.all([api('/api/giveaways'), api('/api/channels')]);
          setList(a.items||[]); setChannels(b.items||[]);
          if((b.items||[]).length){
            const first = String(b.items[0].id);
            if(!form.channel_id) setForm(f=>({...f, channel_id: first}));
            if(!tpl.channel_id) setTpl(t=>({...t, channel_id: first}));
          }
        }catch(e){ setErr(e.message); }
      };
      useEffect(()=>{ load(); },[]);

      const submit = async()=>{
        setErr('');
        try{
          const payload = {
            channel_id: String(form.channel_id),
            prize: form.prize,
            end_in: form.end_in,
            winners: Number(form.winners||1),
            description: form.description,
            max_participants: (String(form.max_participants||'').trim()==='') ? null : Number(form.max_participants),
            thumbnail_b64: form.thumbnail_b64 || null,
            thumbnail_name: form.thumbnail_name || null,
          };
          await api('/api/giveaways/create', {method:'POST', body: JSON.stringify(payload)});
          await load();
        }catch(e){ setErr(e.message); }
      };

      const createTpl = async()=>{
        setErr('');
        try{
          if(!tpl.end_dt) throw new Error('Vul een einddatum in');
          const end_at = Math.floor(new Date(tpl.end_dt).getTime() / 1000);
          if(!end_at) throw new Error('Ongeldige einddatum');
          // Fetch the template icon from our own dashboard static folder and send as attachment.
          const res = await fetch('/static/vbucks-1000.jpg');
          const blob = await res.blob();
          const b64 = await fileToB64(blob);
          const payload = {
            channel_id: String(tpl.channel_id),
            prize: '1000 V-Bucks',
            description: '🎁 1000 V-Bucks giveaway! Klik op **Deelnemen** om mee te doen.',
            winners: Number(tpl.winners||1),
            end_at,
            thumbnail_b64: b64,
            thumbnail_name: 'vbucks-1000.jpg',
          };
          await api('/api/giveaways/create', {method:'POST', body: JSON.stringify(payload)});
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
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Sjabloon</div>
            <div className='row' style={{alignItems:'center', gap:12}}>
              <img src='/static/vbucks-1000.jpg' alt='1000 V-Bucks' style={{width:72,height:72,borderRadius:12,objectFit:'cover',border:'1px solid #1f2937'}}/>
              <div>
                <div style={{fontWeight:900}}>1000 V-Bucks Giveaway</div>
                <div className='muted'>Vul alleen einddatum + aantal winnaars in.</div>
              </div>
            </div>
            <div className='row' style={{marginTop:10}}>
              <select value={tpl.channel_id} onChange={e=>setTpl(t=>({...t, channel_id:e.target.value}))}>
                {channels.map(c=> <option key={c.id} value={c.id}>#{c.name}</option>)}
              </select>
              <input type='datetime-local' value={tpl.end_dt} onChange={e=>setTpl(t=>({...t, end_dt:e.target.value}))}/>
              <input placeholder='Winnaars' type='number' value={tpl.winners} onChange={e=>setTpl(t=>({...t, winners:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:10}}>
              <button className='btn primary' onClick={createTpl}>🎁 Create template giveaway</button>
            </div>
          </div>

          <div className='card col6'>
            <div style={{fontSize:18,fontWeight:800, marginBottom:10}}>Nieuwe giveaway</div>
            <div className='row'>
              <select value={form.channel_id} onChange={e=>setForm(f=>({...f, channel_id:e.target.value}))}>
                {channels.map(c=> <option key={c.id} value={c.id}>#{c.name}</option>)}
              </select>
            </div>
            <div className='row' style={{marginTop:10}}>
              <input placeholder='Prijs' value={form.prize} onChange={e=>setForm(f=>({...f, prize:e.target.value}))}/>
              <input placeholder='Eind (30m, 2h, 1d, 19:00, 2026-01-12 19:00)' value={form.end_in} onChange={e=>setForm(f=>({...f, end_in:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:10}}>
              <input placeholder='Winnaars' type='number' value={form.winners} onChange={e=>setForm(f=>({...f, winners:e.target.value}))}/>
              <input placeholder='Max deelnemers (optioneel)' value={form.max_participants} onChange={e=>setForm(f=>({...f, max_participants:e.target.value}))}/>
            </div>
            <div className='row' style={{marginTop:10, alignItems:'center'}}>
              <input type='file' accept='image/*' onChange={async(e)=>{
                const file = e.target.files && e.target.files[0];
                if(!file) return;
                try{
                  const b64 = await fileToB64(file);
                  setForm(f=>({...f, thumbnail_b64: b64, thumbnail_name: file.name}));
                }catch(err){ setErr('Upload mislukt'); }
              }}/>
              <button className='btn' onClick={()=>setForm(f=>({...f, thumbnail_b64:'', thumbnail_name:''}))}>🗑️ Clear image</button>
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
              <button className='btn danger' onClick={()=>clear(w.user_id)}>Resetten</button>
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
                try:
            member = guild.get_member(uid) or await guild.fetch_member(uid)
        except Exception:
            return _error(400, 'Gebruiker niet gevonden in server')
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
                # Discord IDs are snowflakes (often larger than JS safe integers).
                # Return them as strings so the dashboard doesn't lose precision.
                items.append({"id": str(ch.id), "name": ch.name})
        return {"items": items}

    @app.get("/api/voice_channels")
    async def api_voice_channels(req: Request):
        """List joinable voice/stage channels for the dashboard."""
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        guild = bot.get_guild(gid) if bot else None
        if not guild:
            return {"items": []}

        items = []
        # Be robust across discord.py / pycord forks.
        ChannelType = getattr(discord, "ChannelType", None)
        voice_type = getattr(ChannelType, "voice", None) if ChannelType else None
        stage_type = getattr(ChannelType, "stage_voice", None) if ChannelType else None

        for ch in getattr(guild, "channels", []):
            ctype = getattr(ch, "type", None)
            is_voice = False
            if voice_type is not None and ctype == voice_type:
                is_voice = True
            elif stage_type is not None and ctype == stage_type:
                is_voice = True
            else:
                # Fallback heuristics
                name = str(ctype) if ctype is not None else ""
                if "voice" in name:
                    is_voice = True
                # Voice channels typically have bitrate + user_limit
                if hasattr(ch, "bitrate") and hasattr(ch, "user_limit"):
                    is_voice = True
            if is_voice:
                items.append({"id": str(getattr(ch, "id", "")), "name": getattr(ch, "name", "")})

        items = [it for it in items if it.get("id")]
        items.sort(key=lambda x: x.get("name") or "")
        return {"items": items}

    @app.get("/api/bans")
    async def api_bans(req: Request):
        """List bans for the configured guild."""
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        guild = bot.get_guild(gid) if bot else None
        if not guild:
            return {"items": []}
        items = []
        try:
            # Limit to avoid huge responses; increase if needed.
            async for entry in guild.bans(limit=200):
                u = getattr(entry, "user", None)
                items.append({
                    "user_id": str(getattr(u, "id", "")),
                    "user_tag": str(u) if u else "",
                    "reason": getattr(entry, "reason", None),
                })
        except Exception as e:
            return _error(500, f"Bans ophalen mislukt: {e}")
        return {"items": items}

    # --- Message sender (Mee6-style) ---
    @app.post("/api/messages/send")
    async def api_messages_send(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        body = await req.json()
        # Channel IDs are Discord snowflakes; the frontend sends them as strings
        # to avoid JS precision loss.
        try:
            channel_id = int(str(body.get("channel_id") or "0"))
        except Exception:
            channel_id = 0
        content = (body.get("content") or "").rstrip()
        embed_in = body.get("embed") or None
        if not channel_id:
            return _error(400, "channel_id missing")
        if not bot:
            return _error(400, "Bot not ready")

        ch = bot.get_channel(channel_id)
        if ch is None:
            try:
                ch = await bot.fetch_channel(channel_id)
            except Exception:
                ch = None
        if ch is None or not hasattr(ch, "send"):
            return _error(400, "Channel not found or not sendable")

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
                # If embed has no meaningful content, drop it.
                if not any([title, description, url, thumb, img, footer]):
                    discord_embed = None
        except Exception:
            discord_embed = None

        if not content and discord_embed is None:
            return _error(400, "Empty message")

        await ch.send(content=content or None, embed=discord_embed)
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
        # Apply immediately
        try:
            cog = bot.get_cog('Counters') if bot else None
            if cog:
                import asyncio
                asyncio.create_task(cog.dashboard_fetch(gid))
        except Exception:
            pass
        # Return updated list
        try:
            cog = bot.get_cog('Counters') if bot else None
            if cog:
                return cog.dashboard_counters(gid)
        except Exception:
            pass
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
        # Apply immediately
        try:
            cog = bot.get_cog('Counters') if bot else None
            if cog:
                import asyncio
                asyncio.create_task(cog.dashboard_fetch(gid))
        except Exception:
            pass
        try:
            cog = bot.get_cog('Counters') if bot else None
            if cog:
                return cog.dashboard_counters(gid)
        except Exception:
            pass
        return {"ok": True}

    @app.post("/api/counters/fetch")
    async def api_counters_fetch(req: Request):
        """Force-refresh counters right now (useful when overrides are changed)."""
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        cog = bot.get_cog('Counters') if bot else None
        if not cog:
            return _error(400, "Counters cog not loaded")
        try:
            await cog.dashboard_fetch(gid)
        except Exception as e:
            return _error(400, str(e))
        return cog.dashboard_counters(gid)

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
                items.append({"user_id": int(m.id), "user_tag": str(m), "strikes": strikes})
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
        try:
            uid = int(str(body.get("user_id") or "0"))
        except Exception:
            uid = 0
        gid = getattr(bot, "guild_id", 0)
        guild = bot.get_guild(gid)
        if not uid:
            return _error(400, "user_id ontbreekt")
        if not guild:
            return _error(400, "Guild niet in cache")

        # Fetch member safely
        member = guild.get_member(uid)
        if member is None:
            try:
                member = await guild.fetch_member(uid)
            except Exception:
                return _error(400, "Gebruiker niet gevonden in de server")

        try:
            cur = bot.db.conn.cursor()
            row = cur.execute("SELECT roles_json FROM mutes WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()
            roles_json = (row[0] if row else "[]")

            await bot._restore_roles_after_mute(guild, member, roles_json)
            bot.db.clear_mute(gid, uid)
            return {"ok": True}
        except Exception as e:
            return _error(500, f"Unmute mislukt: {e}")

    @app.get("/api/bans")
    async def api_bans(req: Request):
        try:
            await _require_allowed(req)
        except PermissionError as e:
            return _error(401, str(e))
        gid = getattr(bot, "guild_id", 0)
        guild = bot.get_guild(gid) if bot else None
        if not guild:
            return {"items": []}
        items = []
        try:
            # discord.py returns BanEntry objects
            async for entry in guild.bans(limit=200):
                user = getattr(entry, "user", None)
                reason = getattr(entry, "reason", None)
                if user:
                    items.append({"user_id": int(user.id), "user_tag": str(user), "reason": reason})
        except Exception as e:
            return _error(500, f"Kon bans niet ophalen: {e}")
        return {"items": items}

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



    # --- Afspelenlist (default) ---
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
        # Normalize payload from the inline dashboard.
        # - channel_id may arrive as string (Discord snowflake) -> cast safely
        # - dashboard UI may send `end` instead of `end_in`
        try:
            if "channel_id" in body:
                body["channel_id"] = int(str(body.get("channel_id") or "0"))
        except Exception:
            return _error(400, "Invalid channel_id")

        if (not body.get("end_at")) and (not body.get("end_in")) and body.get("end"):
            body["end_in"] = body.get("end")
        if "end" in body:
            body.pop("end", None)

        # max_participants optional int
        if "max_participants" in body:
            mp = body.get("max_participants")
            if mp in ("", None):
                body["max_participants"] = None
            else:
                try:
                    body["max_participants"] = int(mp)
                except Exception:
                    return _error(400, "Invalid max_participants")

        if "winners" in body:
            try:
                body["winners"] = int(body.get("winners") or 1)
            except Exception:
                body["winners"] = 1
        cog = bot.get_cog('Giveaway')
        if not cog:
            return _error(400, "Giveaway cog not loaded")
        try:
            await cog.dashboard_create(guild_id=getattr(bot, 'guild_id', 0), actor_user_id=uid, **body)
            return {"ok": True}
        except Exception as e:
            # Surface a friendly error to the dashboard (instead of generic 500)
            return _error(400, str(e))

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
