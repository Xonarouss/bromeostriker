import asyncio
import os
import shutil
from dataclasses import dataclass
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import commands

import yt_dlp

BRAND_GREEN = discord.Colour.from_rgb(46, 204, 113)

# Optional: path to a browser-exported cookies.txt (Netscape format).
# Set this as an environment variable (Coolify / Docker env):
#   YTDLP_COOKIES=/app/data/cookies.txt
# Note: we intentionally read this at *runtime* (not only import time) so changes
# can take effect after a restart/redeploy.
BASE_YTDL_OPTS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "extract_flat": False,

    # Prefer IPv4 on some VPS networks (also helps with some CDNs)
    "source_address": "0.0.0.0",

    # More resilient defaults
    "nocheckcertificate": True,
    "retries": 3,
    "fragment_retries": 3,
    "socket_timeout": 20,
}

FFMPEG_BEFORE_OPTS = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
FFMPEG_OPTS = "-vn"


def find_ffmpeg_exe() -> str:
    # 1) env override
    env = os.getenv("FFMPEG_PATH")
    if env and os.path.exists(env):
        return env

    # 2) local folder drop-in (./ffmpeg.exe or ./bin/ffmpeg.exe)
    for p in ("ffmpeg.exe", "ffmpeg", os.path.join("bin", "ffmpeg.exe"), os.path.join("bin", "ffmpeg")):
        if os.path.exists(p):
            return p

    # 3) PATH
    p = shutil.which("ffmpeg")
    return p or "ffmpeg"


@dataclass
class Track:
    title: str
    url: str
    webpage_url: str
    duration: Optional[int] = None
    requester_id: Optional[int] = None


class GuildPlayer:
    def __init__(self):
        self.queue: asyncio.Wachtrij[Track] = asyncio.Wachtrij()
        self.current: Optional[Track] = None
        self.volume: float = 0.5
        self.loop: bool = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()


class Music(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.players: dict[int, GuildPlayer] = {}
        self.ffmpeg_path = find_ffmpeg_exe()

    # --------- helpers ----------
    # --------- permissies ----------
    MUSIC_ROLE_ID = 1021765413056565328  # B-FAM

    def _is_admin(self, member: discord.Member) -> bool:
        # admin/owner bypass
        try:
            if member.guild_permissions.administrator:
                return True
        except Exception:
            pass
        return False

    def _has_music_role(self, member: discord.Member) -> bool:
        return any(r.id == self.MUSIC_ROLE_ID for r in getattr(member, "roles", []))

    async def _ensure_bfam(self, interaction: discord.Interaction) -> bool:
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Dit commando kan alleen in een server gebruikt worden.", ephemeral=True)
            return False
        if self._is_admin(member) or self._has_music_role(member):
            return True
        await interaction.response.send_message("❌ Je hebt geen toegang tot de muziek-commands. Je moet de **B-FAM** rol hebben.", ephemeral=True)
        return False

    def _get_player(self, guild_id: int) -> GuildPlayer:
        if guild_id not in self.players:
            self.players[guild_id] = GuildPlayer()
        return self.players[guild_id]

    async def _join(self, interaction: discord.Interaction) -> Optional[discord.VoiceClient]:
        if not interaction.guild:
            return None
        member = interaction.user
        if not isinstance(member, discord.Member) or not member.voice or not member.voice.channel:
            return None
        vc = interaction.guild.voice_client
        if vc and vc.is_connected():
            if vc.channel != member.voice.channel:
                await vc.move_to(member.voice.channel)
            return vc

        # Connect once; if Discord voice handshake fails, try one more time after a short delay.
        try:
            return await member.voice.channel.connect(reconnect=False)
        except Exception:
            await asyncio.sleep(1.5)
            try:
                return await member.voice.channel.connect(reconnect=False)
            except Exception:
                return None

    def _same_vc_or_admin(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        vc = interaction.guild.voice_client
        if not vc or not vc.channel:
            return True  # allow starting
        member = interaction.user
        if not isinstance(member, discord.Member):
            return False
        if member.guild_permissions.administrator:
            return True
        return member.voice and member.voice.channel and member.voice.channel.id == vc.channel.id

    async def _ytdl_extract(self, query: str) -> Track:
        loop = asyncio.get_running_loop()

        raw = (query or "").strip()
        use_sc = False
        if raw.lower().startswith("sc:"):
            use_sc = True
            raw = raw[3:].strip()

        # If the user did not provide a direct URL, force a single-result search.
        # This avoids edge-cases where yt-dlp returns a semi-flat entry that needs a 2nd extract step.
        if raw.startswith("http://") or raw.startswith("https://"):
            q_run = raw
        else:
            q_run = f"{'scsearch1' if use_sc else 'ytsearch1'}:{raw}"

        def run():
            # Build options per-call so env changes take effect after a restart/redeploy
            opts = dict(BASE_YTDL_OPTS)

            cookiefile = os.getenv("YTDLP_COOKIES")
            if cookiefile:
                opts["cookiefile"] = cookiefile

            # IMPORTANT: YouTube bot-check mitigations
            # Force more reliable player clients. This often helps when web requests get bot-checked.
            opts["extractor_args"] = {
                "youtube": {
                    "player_client": ["web", "tv"],
                }
            }


            # EJS / JS challenge solver (required for YouTube in newer yt-dlp)
            # Needs a JS runtime in the container (recommended: Deno) AND either yt-dlp[default] (yt-dlp-ejs)
            # or remote EJS components enabled.
            opts["js_runtimes"] = {"deno": {}}
            opts["remote_components"] = ["ejs:github"]

            # Better format fallback (some contexts expose only certain streams)
            opts["format"] = "bestaudio/best"
            opts["format_sort"] = ["acodec:opus", "abr", "asr", "ext"]

            # Debug line so you can SEE it in Coolify logs
            print(f"[music] yt-dlp cookiefile={cookiefile} exists={bool(cookiefile and os.path.exists(cookiefile))} q={q_run}")

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(q_run, download=False)

                # Searches return a playlist-like dict with entries
                if isinstance(info, dict) and "entries" in info:
                    entries = [e for e in (info.get("entries") or []) if e]
                    if not entries:
                        raise RuntimeFout("No results.")
                    info = entries[0]

                # Some providers can return a URL-type entry that needs a 2nd pass
                if isinstance(info, dict) and info.get("_type") in ("url", "url_transparent"):
                    u = info.get("url") or info.get("webpage_url")
                    if u:
                        info = ydl.extract_info(u, download=False)

                # SoundCloud sometimes yields a "soundcloud:tracks:ID" URL which needs resolving
                if isinstance(info, dict):
                    u = info.get("url")
                    if isinstance(u, str) and u.startswith("soundcloud:"):
                        info = ydl.extract_info(u, download=False)

                return info

        info = await loop.run_in_executor(None, run)

        title = (info.get("title") if isinstance(info, dict) else None) or "Unknown title"
        stream_url = info.get("url") if isinstance(info, dict) else None
        webpage = (info.get("webpage_url") or info.get("original_url")) if isinstance(info, dict) else None
        webpage = webpage or raw
        duration = info.get("duration") if isinstance(info, dict) else None

        if not stream_url:
            raise RuntimeFout("Could not get audio stream.")

        return Track(title=title, url=stream_url, webpage_url=webpage, duration=duration)

    def _format_duration(self, seconds: Optional[int]) -> str:
        if not seconds:
            return "?"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

    def _embed(self, title: str, desc: str) -> discord.Embed:
        e = discord.Embed(title=title, description=desc, colour=BRAND_GREEN)
        e.set_footer(text="XonarousLIVE • Music")
        return e

    async def _start_player_task(self, guild: discord.Guild, text_channel: discord.abc.Messageable):
        player = self._get_player(guild.id)
        async with player._lock:
            if player._task and not player._task.done():
                return
            player._task = asyncio.create_task(self._player_loop(guild, text_channel))

    async def _player_loop(self, guild: discord.Guild, text_channel: discord.abc.Messageable):
        player = self._get_player(guild.id)
        while True:
            try:
                track: Track = await asyncio.wait_for(player.queue.get(), timeout=180)
            except asyncio.TimeoutFout:
                # idle 3 min with no queue activity -> disconnect (only if not playing/paused)
                vc = guild.voice_client
                if vc and vc.is_connected() and (not vc.is_playing()) and (not vc.is_paused()):
                    try:
                        await text_channel.send(
                            embed=self._embed("👋 Leaving voice", "I left the voice channel due to **3 minutes of inactivity**.")
                        )
                    except Exception:
                        pass
                    try:
                        await vc.disconnect()
                    except Exception:
                        pass
                return

            if player.loop and player.current:
                # if loop is on, re-use current instead of consuming queue
                track = player.current

            player.current = track
            vc = guild.voice_client
            if not vc or not vc.is_connected():
                # can't play if disconnected
                continue

            source = discord.FFmpegPCMAudio(
                track.url,
                executable=self.ffmpeg_path,
                before_options=FFMPEG_BEFORE_OPTS,
                options=FFMPEG_OPTS,
            )
            audio = discord.PCMVolumeTransformer(source, volume=player.volume)

            done = asyncio.Event()

            def after(_err):
                self.bot.loop.call_soon_threadsafe(done.set)

            try:
                vc.play(audio, after=after)
            except Exception:
                continue

            try:
                await text_channel.send(
                    embed=self._embed(
                        "🎶 Nu aan het afspelen",
                        f"[{track.title}]({track.webpage_url})  •  `{self._format_duration(track.duration)}`",
                    )
                )
            except Exception:
                pass

            await done.wait()

            # If loop is off, advance naturally
            if not player.loop:
                player.current = None

    # --------- slash commands ----------
    music = app_commands.Group(name="muziek", description="Muziek-commands (alleen B-FAM / admins).")

    @music.command(name="speel", description="Play a song/URL (joins your voice channel).")
    @app_commands.describe(query="Search query or URL. Tip: prefix with 'sc:' for SoundCloud search.")
    async def play(self, interaction: discord.Interaction, query: str):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Join the same voice channel as the bot to control music.", ephemeral=True)

        # Defer immediately to avoid Discord timeouts
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

        # Extract first so we don't join/leave if the source blocks the request
        try:
            track = await self._ytdl_extract(query)
            track.requester_id = interaction.user.id
        except Exception as e:
            msg = str(e)
            if "Sign in to confirm you" in msg:
                msg = (
                    "YouTube blocked the request (bot-check). "
                    "Make sure YTDLP_COOKIES points to a browser-exported cookies.txt file. "
                    "Then restart/redeploy the bot."
                )
            return await interaction.followup.send(f"Couldn’t load that track. ({msg})", ephemeral=True)

        # Now join voice
        try:
            vc = await self._join(interaction)
        except Exception as e:
            return await interaction.followup.send(f"Voice connect failed: {e}", ephemeral=True)

        if not vc:
            return await interaction.followup.send("Ga eerst in een spraakkanaal zitten.", ephemeral=True)

        player = self._get_player(interaction.guild.id)
        await player.queue.put(track)
        await self._start_player_task(interaction.guild, interaction.channel)

        await interaction.followup.send(embed=self._embed("✅ Toegevoegd aan wachtrij", f"[{track.title}]({track.webpage_url})"), ephemeral=True)

    @music.command(name="pauze", description="Pauzeer afspelen.")
    async def pause(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)
        vc = interaction.guild.voice_client if interaction.guild else None
        if not vc or not vc.is_playing():
            return await interaction.response.send_message("Er speelt nu niks.", ephemeral=True)
        vc.pause()
        await interaction.response.send_message("⏸️ Gepauzeerd.", ephemeral=True)

    @music.command(name="hervat", description="Hervat afspelen.")
    async def resume(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)
        vc = interaction.guild.voice_client if interaction.guild else None
        if not vc or not vc.is_paused():
            return await interaction.response.send_message("Nothing is paused.", ephemeral=True)
        vc.resume()
        await interaction.response.send_message("▶️ Hervat.", ephemeral=True)

    @music.command(name="volgende", description="Sla de huidige track over.")
    async def skip(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)
        vc = interaction.guild.voice_client if interaction.guild else None
        if not vc or not vc.is_playing():
            return await interaction.response.send_message("Er speelt nu niks.", ephemeral=True)
        vc.stop()
        await interaction.response.send_message("⏭️ Overgeslagen.", ephemeral=True)

    @music.command(name="stop", description="Stop playback and clear the queue.")
    async def stop(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        player = self._get_player(interaction.guild.id)
        # Drain queue
        try:
            while True:
                player.queue.get_nowait()
        except asyncio.WachtrijEmpty:
            pass
        player.current = None
        vc = interaction.guild.voice_client if interaction.guild else None
        if vc:
            vc.stop()
        await interaction.response.send_message("⏹️ Stopped and cleared queue.", ephemeral=True)

    @music.command(name="wachtrij", description="Show the queue.")
    async def queue_cmd(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        items: List[Track] = list(player.queue._queue)  # ok for display
        if not player.current and not items:
            return await interaction.response.send_message("Wachtrij is empty.", ephemeral=True)

        lines = []
        if player.current:
            lines.append(f"**Now:** [{player.current.title}]({player.current.webpage_url})")
        for i, t in enumerate(items[:10], start=1):
            lines.append(f"{i}. [{t.title}]({t.webpage_url})")
        if len(items) > 10:
            lines.append(f"…and {len(items)-10} more")

        await interaction.response.send_message(embed=self._embed("📜 Wachtrij", "\n".join(lines)), ephemeral=True)

    @music.command(name="nowplaying", description="Show the current track.")
    async def nowplaying(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        if not player.current:
            return await interaction.response.send_message("Er speelt nu niks.", ephemeral=True)
        t = player.current
        await interaction.response.send_message(
            embed=self._embed("🎶 Nu aan het afspelen", f"[{t.title}]({t.webpage_url})  •  `{self._format_duration(t.duration)}`"),
            ephemeral=True,
        )

    @music.command(name="volume", description="Set volume (0-100).")
    async def volume(self, interaction: discord.Interaction, percent: app_commands.Range[int, 0, 100]):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        player.volume = max(0.0, min(1.0, percent / 100.0))
        await interaction.response.send_message(f"🔊 Volume ingesteld op {percent}%.", ephemeral=True)

    @music.command(name="herhaal", description="Toggle loop for the current track.")
    async def loop(self, interaction: discord.Interaction, enabled: bool):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        player.loop = bool(enabled)
        await interaction.response.send_message(f"🔁 Herhalen staat nu {'ON' if player.loop else 'OFF'}.", ephemeral=True)

    @music.command(name="weg", description="Verbreek verbinding met voice.")
    async def disconnect(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)
        vc = interaction.guild.voice_client if interaction.guild else None
        if not vc or not vc.is_connected():
            return await interaction.response.send_message("Ik ben niet verbonden.", ephemeral=True)
        await vc.disconnect()
        await interaction.response.send_message("👋 Losgekoppeld.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Music(bot))
