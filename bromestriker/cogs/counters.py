import os
import time
import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Optional, Dict, Any

import discord
from discord import app_commands
from discord.ext import commands

from ..webserver import refresh_tiktok_access_token_if_needed

def _fmt_nl(n: Optional[int]) -> str:
    if n is None:
        return "—"
    # 12,345 -> 12.345
    return f"{int(n):,}".replace(",", ".")


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
        self.ig_user_id = (os.getenv("IG_USER_ID") or "").strip()
        self.ig_access_token = (os.getenv("IG_ACCESS_TOKEN") or "").strip()
        self.ig_graph_version = (os.getenv("IG_GRAPH_VERSION") or "v20.0").strip()

        # TikTok API v2 Get User Info requires a user access token with user.info.stats scope.
        self.tiktok_access_token = (os.getenv("TIKTOK_ACCESS_TOKEN") or "").strip()

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
        await self._ensure_setup(interaction.guild)
        await self._refresh_guild(interaction.guild)
        await interaction.followup.send("✅ Counters refreshed.", ephemeral=True)

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
                except Exception:
                    return None
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

        members = guild.member_count
        twitch = await self._get_twitch_followers()
        instagram = await self._get_instagram_followers()
        tiktok = await self._get_tiktok_followers()

        await self._maybe_rename(ch_by_kind.get("members"), self.tpl_members, members)
        await self._maybe_rename(ch_by_kind.get("twitch"), self.tpl_twitch, twitch)
        await self._maybe_rename(ch_by_kind.get("instagram"), self.tpl_instagram, instagram)
        await self._maybe_rename(ch_by_kind.get("tiktok"), self.tpl_tiktok, tiktok)

    async def _maybe_rename(self, ch: Optional[discord.abc.GuildChannel], template: str, count: Optional[int]) -> None:
        if ch is None:
            return
        if not isinstance(ch, (discord.VoiceChannel, discord.TextChannel, discord.StageChannel)):
            return
        new_name = _clamp_channel_name(template.format(count=_fmt_nl(count)))
        try:
            if ch.name != new_name:
                await ch.edit(name=new_name, reason="Counters: update")
        except Exception:
            pass

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

    # ---------- Instagram / TikTok ----------
    async def _get_instagram_followers(self) -> Optional[int]:
        # Official: Instagram Graph API (IG User followers_count).
        if not self.ig_user_id or not self.ig_access_token:
            return None
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://graph.facebook.com/{self.ig_graph_version}/{self.ig_user_id}"
                params = {"fields": "followers_count", "access_token": self.ig_access_token}
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status >= 400:
                        return None
                    data = await resp.json()
                    val = data.get("followers_count")
                    return int(val) if isinstance(val, (int, float, str)) and str(val).isdigit() else None
        except Exception:
            return None

    async def _get_tiktok_followers(self) -> Optional[int]:
    # Official: TikTok API v2 Get User Info (follower_count).
    token = await refresh_tiktok_access_token_if_needed()
    if not token:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            url = "https://open.tiktokapis.com/v2/user/info/"
            params = {"fields": "follower_count"}
            headers = {"Authorization": f"Bearer {token}"}
            async with session.get(url, params=params, headers=headers, timeout=15) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json()
                user = (data.get("data") or {}).get("user") or {}
                val = user.get("follower_count")
                # follower_count can be int; be defensive
                try:
                    return int(val)
                except Exception:
                    return None
    except Exception:
        return None
