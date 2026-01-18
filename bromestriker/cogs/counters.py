import os
import time
import asyncio
import aiohttp
import logging
log = logging.getLogger('bromestriker.counters')
log.warning('✅ Counters loaded: SCRAPING-ONLY build (IG/TikTok)')
import re
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any

import discord
from discord import app_commands
from discord.ext import commands

def _fmt_nl(n: Optional[int]) -> str:
    if n is None:
        return "—"
    # 12,345 -> 12.345
    return f"{int(n):,}".replace(",", ".")


def _fmt_social(n: Optional[int]) -> str:
    """Format social counters.

    - < 10.000: exact with NL thousands separator (.)
    - >= 10.000: compact 'K' with 1 decimal, **floored** (never rounds up)
      so 13389 -> 13.3K (not 13.4K)
    """
    if n is None:
        return "—"
    n = int(n)
    if n >= 10_000:
        # floor to 1 decimal in thousands
        k10 = n // 100  # 13389 -> 133 (== 13.3K)
        whole = k10 // 10
        dec = k10 % 10
        return f"{whole}.{dec}K"
    return f"{n:,}".replace(",", ".")


def _clamp_channel_name(name: str) -> str:
    # Discord channel name limit is 100 chars.
    name = (name or "").strip()
    return name[:100]


async def _fetch_number_from_url(session, url: str, json_key: str = "count") -> Optional[int]:
    """
    Supported payloads:
      - plain text: 12345
      - JSON: {"count": 12345} (configurable key)
      - JSON: {"data": {"count": 12345}} not supported by default; use your own endpoint.
    """
    if not url:
        return None
    try:
        async with session.get(url, timeout=15) as resp:
            text_ct = (resp.headers.get("content-type") or "").lower()
            body = await resp.text()
            if resp.status >= 400:
                return None
            # JSON?
            if "application/json" in text_ct or body.strip().startswith("{"):
                try:
                    data = await resp.json()
                except Exception:
                    # invalid json
                    return None
                val = data.get(json_key)
                if isinstance(val, (int, float)):
                    return int(val)
                if isinstance(val, str) and val.strip().isdigit():
                    return int(val.strip())
                return None

            # plain number
            digits = "".join(ch for ch in body if ch.isdigit())
            return int(digits) if digits else None
    except Exception:
        return None


class Counters(commands.Cog):
    """Auto-updating counter channels (Members + Twitch/Instagram/TikTok)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: Optional[asyncio.Task] = None
        self._twitch_token: Optional[str] = None
        self._twitch_token_exp: int = 0

        # cache last known good counts per guild/kind to prevent flapping to '—' on intermittent scrape failures
        # key: (guild_id, kind) -> last int
        self._last_counter_values: Dict[tuple[int, str], int] = {}

        # Last fetched (raw) values for dashboard visibility.
        # key: (guild_id, kind) -> last fetched int (before overrides/stability)
        self._last_fetched_values: Dict[tuple[int, str], int] = {}

        # config
        self.category_name = (os.getenv("COUNTER_CATEGORY_NAME") or "📊 Counters").strip()

        # default templates (match the common "counter voice channels" look)
        self.tpl_members = (os.getenv("COUNTER_MEMBERS_TEMPLATE") or "👥 Leden: {count}").strip()
        self.tpl_twitch = (os.getenv("COUNTER_TWITCH_TEMPLATE") or "📺 Twitch: {count}").strip()
        self.tpl_instagram = (os.getenv("COUNTER_INSTAGRAM_TEMPLATE") or "📸 Instagram: {count}").strip()
        self.tpl_tiktok = (os.getenv("COUNTER_TIKTOK_TEMPLATE") or "🎵 TikTok: {count}").strip()

        self.update_seconds = int(os.getenv("COUNTER_UPDATE_SECONDS") or "300")
        self.enabled = (os.getenv("COUNTERS_ENABLED") or "1").strip() not in {"0", "false", "no"}

        # Official API tokens (optional):
        # Instagram Graph API requires an IG User id + access token with proper permissions (business/creator).

        # TikTok API v2 Get User Info requires a user access token with user.info.stats scope.

        # Scraping mode (no OAuth / no business verification). If username is set, we scrape public profile pages.
        # Note: scraping can break if platforms change their HTML/JSON, but it's how most counter bots work.
        self.instagram_username = (os.getenv("INSTAGRAM_USERNAME") or "").strip().lstrip("@")
        self.instagram_sessionid = (os.getenv("INSTAGRAM_SESSIONID") or "").strip()
        # Optional seed value to avoid starting at "—" when Instagram scraping is temporarily blocked
        self.instagram_static: Optional[int] = None
        _ig_static = (os.getenv("COUNTER_INSTAGRAM_STATIC") or "").strip()
        if _ig_static.isdigit():
            self.instagram_static = int(_ig_static)

        # Instagram rate-limit backoff (epoch seconds). When hit with HTTP 429 we pause scraping.
        self._ig_backoff_until: int = 0
        self.tiktok_username = (os.getenv("TIKTOK_USERNAME") or "").strip().lstrip("@")


    # ---------- Instagram / TikTok ----------
    def _parse_compact_number(self, s: str) -> Optional[int]:
        """Parse numbers like '12,345', '12.345', '1.2k', '3,4M'."""
        if not s:
            return None
        s = s.strip().lower()
        # keep digits, separators, and suffix
        m = re.match(r"^([0-9][0-9\.,]*)\s*([km]?)$", s)
        if not m:
            return None
        num, suf = m.group(1), m.group(2)
        # normalize separators: if both ',' and '.' exist, assume '.' thousands and ',' decimal (EU) or vice versa.
        # We'll just remove thousands separators and handle one decimal separator.
        if "," in num and "." in num:
            # assume last separator is decimal
            last_comma = num.rfind(",")
            last_dot = num.rfind(".")
            dec_pos = max(last_comma, last_dot)
            int_part = re.sub(r"[\.,]", "", num[:dec_pos])
            dec_part = re.sub(r"[\.,]", "", num[dec_pos+1:])
            num_norm = f"{int_part}.{dec_part}" if dec_part else int_part
        else:
            # if only one type of separator, treat '.' or ',' as decimal when suffix exists, else as thousands
            if suf and ("," in num or "." in num):
                num_norm = num.replace(",", ".")
            else:
                num_norm = num.replace(".", "").replace(",", "")
        try:
            val = float(num_norm)
        except Exception:
            return None
        mult = 1
        if suf == "k":
            mult = 1000
        elif suf == "m":
            mult = 1000000
        return int(val * mult)



    def _stable_count(self, guild_id: int, kind: str, new_value: Optional[int]) -> Optional[int]:
        """Keep last known good value when scraping fails.

        - If new_value is None/invalid -> return cached value (if any)
        - If new_value is an int -> store & return it (can go up or down)
        """
        key = (int(guild_id), str(kind))
        last = self._last_counter_values.get(key)

        if new_value is None:
            return last

        try:
            nv = int(new_value)
        except Exception:
            return last

        self._last_counter_values[key] = nv
        return nv
    
    async def _get_instagram_followers(self) -> Optional[int]:
        """Instagram follower count via web scraping (no API)."""
        if not self.instagram_username:
            return None


        # Backoff when Instagram rate-limits us (HTTP 429)
        now = int(time.time())
        if now < getattr(self, '_ig_backoff_until', 0):
            return None

        url = f"https://www.instagram.com/{self.instagram_username}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        try:
            # 1) Fetch profile HTML
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(url, timeout=25, allow_redirects=True) as resp:
                        if resp.status == 429:
                            log.warning("Instagram scrape HTTP 429 (rate limited) - backing off for 2 hours")
                            self._ig_backoff_until = int(time.time()) + 2 * 60 * 60
                            return None
                        if resp.status >= 400:
                            log.warning("Instagram scrape HTTP %s", resp.status)
                            return None
                        html = await resp.text()
            except Exception as e:
                log.exception("Instagram scrape failed (HTML fetch): %s", e)
                return None

            html_l = html.lower()

            # Detect login/challenge wall
            login_wall = ("accounts/login" in html_l) or ("challenge" in html_l) or (
                ("login" in html_l) and ("password" in html_l)
            )

            # If login wall -> go straight to JSON fallbacks (HTML patterns usually missing)
            if login_wall:
                log.warning("Instagram scrape: login wall detected on profile HTML; trying JSON fallbacks")

                # 1) Try __a=1 JSON endpoint
                try:
                    a1_url = f"https://www.instagram.com/{self.instagram_username}/?__a=1&__d=dis"
                    a1_headers = dict(headers)
                    a1_headers.update({
                        "Accept": "application/json,text/plain,*/*",
                        "X-IG-App-ID": "936619743392459",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": url,
                    })

                    async with aiohttp.ClientSession(headers=a1_headers) as a1s:
                        async with a1s.get(a1_url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                            if r.status < 400:
                                j = await r.json(content_type=None)

                                user = None
                                if isinstance(j, dict):
                                    user = (
                                        (j.get("graphql") or {}).get("user")
                                        or (j.get("data") or {}).get("user")
                                        or j.get("user")
                                    )

                                if isinstance(user, dict):
                                    eb = (user.get("edge_followed_by") or {}).get("count")
                                    if isinstance(eb, int):
                                        return eb
                                    if isinstance(user.get("followers_count"), int):
                                        return user.get("followers_count")
                                    if isinstance(user.get("follower_count"), int):
                                        return user.get("follower_count")
                except Exception:
                    pass

                # 2) Try web_profile_info endpoint (optionally with sessionid cookie)
                try:
                    api_url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={self.instagram_username}"
                    api_headers = dict(headers)
                    api_headers.update({
                        "Accept": "application/json,text/plain,*/*",
                        "X-IG-App-ID": "936619743392459",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": url,
                    })
                    if self.instagram_sessionid:
                        api_headers["Cookie"] = f"sessionid={self.instagram_sessionid};"

                    async with aiohttp.ClientSession(headers=api_headers) as apis:
                        async with apis.get(api_url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                            if r.status < 400:
                                j = await r.json(content_type=None)
                                data = j.get("data") if isinstance(j, dict) else None
                                user = data.get("user") if isinstance(data, dict) else None

                                if isinstance(user, dict):
                                    eb = (user.get("edge_followed_by") or {}).get("count")
                                    if isinstance(eb, int):
                                        return eb
                                    if isinstance(user.get("followers_count"), int):
                                        return user.get("followers_count")
                                    if isinstance(user.get("follower_count"), int):
                                        return user.get("follower_count")
                except Exception:
                    pass

                # Still login-walled
                return None

            # ---- Not login-walled: try HTML patterns ----

            # Common embedded JSON key (legacy)
            m = re.search(r'"edge_followed_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)\s*\}', html)
            if m:
                return int(m.group(1))

            # Newer keys sometimes contain followers_count / follower_count
            m = re.search(r'"followers_count"\s*:\s*(\d+)', html)
            if m:
                return int(m.group(1))

            m = re.search(r'"follower_count"\s*:\s*(\d+)', html)
            if m:
                return int(m.group(1))

            # Fallback: og:description contains e.g. "123K Followers"
            m = re.search(r'property="og:description"\s+content="([^"]+)"', html)
            if m:
                desc = m.group(1)
                m2 = re.search(r'([0-9][0-9\.,\s]*\s*[KkMm]?)\s*(?:Followers|Volgers)', desc, re.IGNORECASE)
                if m2:
                    return self._parse_compact_number(m2.group(1))

            # If HTML patterns fail, try web_profile_info anyway (sometimes HTML changes)
            try:
                api_url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={self.instagram_username}"
                api_headers = dict(headers)
                api_headers.update({
                    "Accept": "application/json,text/plain,*/*",
                    "X-IG-App-ID": "936619743392459",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": url,
                })
                if self.instagram_sessionid:
                    api_headers["Cookie"] = f"sessionid={self.instagram_sessionid};"

                async with aiohttp.ClientSession(headers=api_headers) as s2:
                    async with s2.get(api_url, timeout=25, allow_redirects=True) as r2:
                        if r2.status < 400:
                            data = await r2.json(content_type=None)
                            user = (((data or {}).get("data") or {}).get("user") or {})
                            eb = ((user.get("edge_followed_by") or {}).get("count"))
                            if isinstance(eb, int):
                                return eb
                            if isinstance(user.get("followers_count"), int):
                                return user.get("followers_count")
                            if isinstance(user.get("follower_count"), int):
                                return user.get("follower_count")
            except Exception:
                pass

            log.warning("Instagram scrape: pattern not found")
            return None

        except Exception as e:
            log.exception("Instagram scrape failed: %s", e)
            return None


    async def _get_tiktok_followers(self) -> Optional[int]:
        """TikTok follower count via web scraping (no API)."""
        if not self.tiktok_username:
            return None

        url = f"https://www.tiktok.com/@{self.tiktok_username}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=25, allow_redirects=True) as resp:
                    if resp.status >= 400:
                        log.warning("TikTok scrape HTTP %s", resp.status)
                        return None
                    html = await resp.text()

            # Fast path: JSON often contains followerCount
            m = re.search(r'"followerCount"\s*:\s*(\d+)', html)
            if m:
                return int(m.group(1))

            # SIGI_STATE contains full JSON
            m = re.search(r'<script id="SIGI_STATE" type="application/json">(.*?)</script>', html, re.S)
            if m:
                try:
                    data = json.loads(m.group(1))
                    # Try common path: UserModule -> users -> <uniqueId> -> stats -> followerCount
                    users = (((data or {}).get("UserModule") or {}).get("users") or {})
                    user = users.get(self.tiktok_username) or users.get(self.tiktok_username.lower())
                    if user:
                        stats = user.get("stats") or {}
                        fc = stats.get("followerCount")
                        if isinstance(fc, int):
                            return fc
                except Exception:
                    pass

            # Another blob sometimes exists
            m = re.search(r'__UNIVERSAL_DATA_FOR_REHYDRATION__"\s*:\s*(\{.*?\})\s*,\s*"__DEFAULT_SCOPE__', html, re.S)
            if m:
                try:
                    data = json.loads(m.group(1))
                    # Try to find follower count anywhere in this blob
                    s = json.dumps(data)
                    m2 = re.search(r'"followerCount"\s*:\s*(\d+)', s)
                    if m2:
                        return int(m2.group(1))
                except Exception:
                    pass

            log.warning("TikTok scrape: pattern not found")
            return None

        except Exception as e:
            log.exception("TikTok scrape failed: %s", e)
            return None


    async def cog_load(self) -> None:
        # Start loop only once bot is ready-ish
        if self.enabled and self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def cog_unload(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    # -------------------------
    # Slash commands
    # -------------------------
    @app_commands.command(name="counter", description="Maak de counter kanalen aan en zet auto-update aan")
    async def counter(self, interaction: discord.Interaction):
        """Maak de counter kanalen aan en zet auto-update aan."""
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild:
            return await interaction.followup.send("Dit werkt alleen in een server.", ephemeral=True)

        await self._ensure_setup(interaction.guild)
        await self._refresh_guild(interaction.guild)
        await interaction.followup.send(
            "✅ Counters staan aan. Ik update ze automatisch.\n"
            "Gebruik **/counterrefresh** als je direct wil bijwerken.",
            ephemeral=True,
        )

    @app_commands.command(name="counterrefresh", description="Refresh de counters nu")
    async def counter_refresh(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild:
            return await interaction.followup.send("Dit werkt alleen in een server.", ephemeral=True)

        try:
            await self._ensure_setup(interaction.guild)
            await self._refresh_guild(interaction.guild)
        except Exception as e:
            log.exception("Counter refresh failed")
            return await interaction.followup.send(f"❌ Counter refresh faalde: `{type(e).__name__}: {e}`", ephemeral=True)

        await interaction.followup.send("✅ Counters bijgewerkt.", ephemeral=True)


    # -------------------------
    # internals
    # -------------------------
    async def _ensure_setup(self, guild: discord.Guild) -> None:
        """Ensure channels exist; store IDs in DB."""
        # No category requested: create channels at guild root (no category).
        category = None

        # Helper to find/create voice channels
        async def ensure_voice(kind: str, initial_name: str) -> Optional[discord.VoiceChannel]:
            # Try DB first
            row = None
            try:
                rows = self.bot.db.get_counters(guild.id)  # type: ignore[attr-defined]
                for r in rows:
                    if r["kind"] == kind:
                        row = r
                        break
            except Exception:
                rows = []

            ch = None
            if row:
                ch = guild.get_channel(int(row["channel_id"]))

            if ch is None:
                # Find by name prefix fallback
                for vc in guild.voice_channels:
                    if vc.name.startswith(initial_name.split(":")[0]):
                        ch = vc
                        break

            if ch is None:
                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(connect=False, speak=False),
                }
                try:
                    ch = await guild.create_voice_channel(
                        name=_clamp_channel_name(initial_name),
                        # category=None (no counters category)
                        overwrites=overwrites,
                        reason="Counters: channel aanmaken",
                    )
                except Exception as e:
                    log.exception('Failed to create counter channel %s', kind)
                    raise
                # ensure not joinable
                try:
                    ow = ch.overwrites_for(guild.default_role)
                    if ow.connect is not False:
                        ow.connect = False
                        await ch.set_permissions(guild.default_role, overwrite=ow, reason="Counters: no connect")
                except Exception:
                    pass

            try:
                self.bot.db.upsert_counter(guild.id, kind, ch.id, category.id if category else None)  # type: ignore[attr-defined]
            except Exception:
                pass
            return ch

        # Create the 4 counters
        await ensure_voice("members", self.tpl_members.format(count=_fmt_nl(0)))
        await ensure_voice("twitch", self.tpl_twitch.format(count=_fmt_nl(0)))
        await ensure_voice("instagram", self.tpl_instagram.format(count=_fmt_nl(0)))
        await ensure_voice("tiktok", self.tpl_tiktok.format(count=_fmt_nl(0)))

    async def _loop(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                # We only support 1 guild in this bot (your env has GUILD_ID) but this still works generally.
                for guild in self.bot.guilds:
                    try:
                        await self._ensure_setup(guild)
                        await self._refresh_guild(guild)
                    except Exception:
                        continue
            except asyncio.CancelledError:
                return
            except Exception:
                pass

            await asyncio.sleep(max(60, self.update_seconds))

    async def _refresh_guild(self, guild: discord.Guild) -> None:
        # Pull stored channel ids
        try:
            rows = self.bot.db.get_counters(guild.id)  # type: ignore[attr-defined]
        except Exception:
            rows = []

        ch_by_kind: Dict[str, discord.abc.GuildChannel] = {}
        for r in rows:
            ch = guild.get_channel(int(r["channel_id"]))
            if ch:
                ch_by_kind[str(r["kind"])]= ch

        members = int(guild.member_count or 0)
        twitch = await self._get_twitch_followers()
        instagram_raw = await self._get_instagram_followers()
        tiktok_raw = await self._get_tiktok_followers()

        # Store last fetched values (before stability/overrides)
        self._last_fetched_values[(guild.id, "members")] = members
        if twitch is not None:
            self._last_fetched_values[(guild.id, "twitch")] = int(twitch)
        if instagram_raw is not None:
            self._last_fetched_values[(guild.id, "instagram")] = int(instagram_raw)
        if tiktok_raw is not None:
            self._last_fetched_values[(guild.id, "tiktok")] = int(tiktok_raw)

        # If Instagram scraping fails before we ever got a good value, seed from COUNTER_INSTAGRAM_STATIC
        if instagram_raw is None and getattr(self, "instagram_static", None) is not None:
            key = (int(guild.id), "instagram")
            if key not in self._last_counter_values:
                instagram_raw = int(self.instagram_static)

        # Stabilize social counters to prevent '—' flapping when scraping intermittently fails
        instagram = self._stable_count(guild.id, "instagram", instagram_raw)
        tiktok = self._stable_count(guild.id, "tiktok", tiktok_raw)

        def _resolve(kind: str, fetched: Optional[int]) -> Optional[int]:
            """Manual override wins unless fetched is higher."""
            try:
                manual = self.bot.db.get_counter_override(guild.id, kind)  # type: ignore[attr-defined]
            except Exception:
                manual = None
            if manual is None:
                return fetched
            if fetched is None:
                return int(manual)
            return int(fetched) if int(fetched) > int(manual) else int(manual)

        members_eff = _resolve("members", members)
        twitch_eff = _resolve("twitch", int(twitch) if twitch is not None else None)
        instagram_eff = _resolve("instagram", instagram)
        tiktok_eff = _resolve("tiktok", tiktok)

        await self._maybe_rename(ch_by_kind.get("members"), self.tpl_members, members_eff, fmt=_fmt_nl)
        await self._maybe_rename(ch_by_kind.get("twitch"), self.tpl_twitch, twitch_eff, fmt=_fmt_nl)
        # Social counters: compact K-format at >= 10k
        await self._maybe_rename(ch_by_kind.get("instagram"), self.tpl_instagram, instagram_eff, fmt=_fmt_social)
        await self._maybe_rename(ch_by_kind.get("tiktok"), self.tpl_tiktok, tiktok_eff, fmt=_fmt_social)

    # -------------------------
    # Dashboard helpers
    # -------------------------
    def dashboard_counters(self, guild_id: int) -> dict:
        """Return counters with manual overrides + last fetched + effective."""
        kinds = ["members", "twitch", "instagram", "tiktok"]
        out = []
        for kind in kinds:
            fetched = self._last_fetched_values.get((int(guild_id), kind))
            try:
                manual = self.bot.db.get_counter_override(int(guild_id), kind)  # type: ignore[attr-defined]
            except Exception:
                manual = None
            effective = None
            if manual is None:
                effective = fetched
            else:
                if fetched is None:
                    effective = int(manual)
                else:
                    effective = int(fetched) if int(fetched) > int(manual) else int(manual)
            out.append({"kind": kind, "fetched": fetched, "manual": manual, "effective": effective})
        return {"items": out}

    async def dashboard_fetch(self, guild_id: int) -> None:
        """Force an immediate refresh for one guild (used by the dashboard)."""
        g = self.bot.get_guild(int(guild_id))
        if not g:
            try:
                g = await self.bot.fetch_guild(int(guild_id))
            except Exception:
                g = None
        if not g:
            raise ValueError('Guild not found')
        await self._ensure_setup(g)
        await self._refresh_guild(g)

    async def _maybe_rename(self, ch: Optional[discord.abc.GuildChannel], template: str, count: Optional[int], fmt=_fmt_nl) -> None:
        if ch is None:
            return
        if not isinstance(ch, (discord.VoiceChannel, discord.TextChannel, discord.StageChannel)):
            return
        new_name = _clamp_channel_name(template.format(count=fmt(count)))
        try:
            if ch.name != new_name:
                await ch.edit(name=new_name, reason="Counters: update")
        except Exception as e:
            log.debug('Rename failed for %s: %s', getattr(ch,'id',None), e)

    # ---------- Twitch ----------
    async def _get_twitch_followers(self) -> Optional[int]:
        """Prefer official Twitch Helix, otherwise use COUNTER_TWITCH_URL / static."""
        # 1) static override
        static = (os.getenv("COUNTER_TWITCH_STATIC") or "").strip()
        if static.isdigit():
            return int(static)

        # 2) official Helix
        client_id = (os.getenv("TWITCH_CLIENT_ID") or "").strip()
        client_secret = (os.getenv("TWITCH_CLIENT_SECRET") or "").strip()
        broadcaster_id = (os.getenv("TWITCH_BROADCASTER_ID") or os.getenv("TWITCH_USER_ID") or "").strip()
        if client_id and client_secret and broadcaster_id:
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    token = await self._get_twitch_app_token(session, client_id, client_secret)
                    if not token:
                        return None
                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Client-Id": client_id,
                    }
                    url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={broadcaster_id}"
                    async with session.get(url, headers=headers, timeout=15) as resp:
                        if resp.status >= 400:
                            return None
                        data = await resp.json()
                        total = data.get("total")
                        if isinstance(total, (int, float)):
                            return int(total)
                        return None
            except Exception:
                return None

        # 3) custom URL endpoint
        url = (os.getenv("COUNTER_TWITCH_URL") or "").strip()
        key = (os.getenv("COUNTER_TWITCH_JSON_KEY") or "count").strip()
        if url:
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    return await _fetch_number_from_url(session, url, key)
            except Exception:
                return None

        return None

    async def _get_twitch_app_token(self, session, client_id: str, client_secret: str) -> Optional[str]:
        now = int(time.time())
        if self._twitch_token and now < self._twitch_token_exp - 30:
            return self._twitch_token
        try:
            url = "https://id.twitch.tv/oauth2/token"
            payload = {
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            }
            async with session.post(url, data=payload, timeout=15) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json()
                token = data.get("access_token")
                exp = data.get("expires_in")
                if not token:
                    return None
                self._twitch_token = str(token)
                self._twitch_token_exp = now + int(exp or 3600)
                return self._twitch_token
        except Exception:
            return None

    # ---------- Instagram / TikTok -----
