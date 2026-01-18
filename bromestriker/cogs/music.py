import asyncio
import os
import shutil
import time
import json
from dataclasses import dataclass
from typing import Optional, List, Dict

import discord
from discord import app_commands
from discord.ext import commands

import yt_dlp

BRAND_GREEN = discord.Colour.from_rgb(46, 204, 113)

# ---------------------------
# yt-dlp / ffmpeg
# ---------------------------

BASE_YTDL_OPTS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "extract_flat": False,
    "source_address": "0.0.0.0",  # prefer IPv4
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

    # 2) local folder drop-in (./ffmpeg or ./bin/ffmpeg)
    for p in ("ffmpeg.exe", "ffmpeg", os.path.join("bin", "ffmpeg.exe"), os.path.join("bin", "ffmpeg")):
        if os.path.exists(p):
            return p

    # 3) PATH
    p = shutil.which("ffmpeg")
    return p or "ffmpeg"


def _load_radio_stations() -> Dict[str, str]:
    """
    Stations from env RADIO_STATIONS_JSON:
      {"groovesalad":"https://ice1.somafm.com/groovesalad-128-mp3", ...}

    Coolify (and some Docker UIs) sometimes store JSON with escaped quotes, e.g.
      {\"qmusic\":\"https://...\"}
    We handle both normal JSON and escaped-JSON safely.

    If not provided / invalid, we ship a small default list (public Icecast).
    """
    raw = (os.getenv("RADIO_STATIONS_JSON", "") or "").strip()
    if raw:
        # 1) If the whole JSON was stored with escaped quotes, unescape it.
        #    Example: {\"a\":\"b\"}  -> {"a":"b"}
        if raw.startswith("{\\") or '\\"' in raw:
            raw = raw.replace('\\"', '"')

        # 2) Some UIs wrap the value in quotes (a JSON string that contains JSON).
        #    We'll try parsing up to 2 times.
        for _ in range(2):
            try:
                data = json.loads(raw)
            except Exception:
                data = None

            if isinstance(data, str):
                raw = data.strip()
                continue

            if isinstance(data, dict):
                out: Dict[str, str] = {}
                for k, v in data.items():
                    if isinstance(k, str) and isinstance(v, str) and v.startswith(("http://", "https://")):
                        out[k.strip().lower()] = v.strip()
                if out:
                    return out
            break

    return {
        "groovesalad": "https://ice1.somafm.com/groovesalad-128-mp3",
        "dronezone": "https://ice1.somafm.com/dronezone-128-mp3",
        "defcon": "https://ice1.somafm.com/defcon-128-mp3",
    }



@dataclass
class Track:
    title: str
    url: str
    webpage_url: str
    duration: Optional[int] = None
    requester_id: Optional[int] = None
    is_radio: bool = False
    radio_name: Optional[str] = None


class GuildPlayer:
    def __init__(self):
        self.queue: asyncio.Queue[Track] = asyncio.Queue()
        self.current: Optional[Track] = None

        self.volume: float = 0.5
        self.loop: bool = False
        self.autoplay: bool = False

        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        # now playing + progress
        self.now_msg: Optional[discord.Message] = None
        self.started_at: Optional[float] = None
        self.paused_at: Optional[float] = None
        self.paused_total: float = 0.0
        self.progress_task: Optional[asyncio.Task] = None

        # live volume updates
        self.current_audio: Optional[discord.PCMVolumeTransformer] = None

        # where to post embeds (always the most recent channel a music command was used in)
        self.text_channel_id: Optional[int] = None

        # activity for idle-disconnect logic
        self.last_activity: float = time.monotonic()


class PlayerControls(discord.ui.View):
    def __init__(self, cog: "Music", guild_id: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not await self.cog._ensure_bfam(interaction):
            return False
        if not self.cog._same_vc_or_admin(interaction):
            try:
                await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)
            except Exception:
                pass
            return False
        return True

    @discord.ui.button(label="⏯️", style=discord.ButtonStyle.secondary)
    async def pause_resume(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._guard(interaction):
            return
        self.cog._touch(self.guild_id, channel_id=getattr(interaction.channel, "id", None))
        vc = interaction.guild.voice_client if interaction.guild else None
        player = self.cog._get_player(self.guild_id)

        if vc and vc.is_playing():
            vc.pause()
            player.paused_at = time.monotonic()
            await interaction.response.send_message("⏸️ Gepauzeerd.", ephemeral=True)
            return
        if vc and vc.is_paused():
            vc.resume()
            if player.paused_at:
                player.paused_total += max(0.0, time.monotonic() - player.paused_at)
            player.paused_at = None
            await interaction.response.send_message("▶️ Hervat.", ephemeral=True)
            return

        await interaction.response.send_message("Er speelt nu niks.", ephemeral=True)

    @discord.ui.button(label="⏭️", style=discord.ButtonStyle.primary)
    async def skip(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._guard(interaction):
            return
        self.cog._touch(self.guild_id, channel_id=getattr(interaction.channel, "id", None))
        vc = interaction.guild.voice_client if interaction.guild else None
        if vc and (vc.is_playing() or vc.is_paused()):
            vc.stop()
        await interaction.response.send_message("⏭️ Overgeslagen.", ephemeral=True)

    @discord.ui.button(label="🔉", style=discord.ButtonStyle.secondary)
    async def vol_down(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._guard(interaction):
            return
        self.cog._touch(self.guild_id, channel_id=getattr(interaction.channel, "id", None))
        player = self.cog._get_player(self.guild_id)
        player.volume = max(0.0, player.volume - 0.1)
        if player.current_audio:
            player.current_audio.volume = player.volume
        await interaction.response.send_message(f"🔉 Volume: {int(player.volume * 100)}%", ephemeral=True)

    @discord.ui.button(label="🔊", style=discord.ButtonStyle.secondary)
    async def vol_up(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._guard(interaction):
            return
        self.cog._touch(self.guild_id, channel_id=getattr(interaction.channel, "id", None))
        player = self.cog._get_player(self.guild_id)
        player.volume = min(1.0, player.volume + 0.1)
        if player.current_audio:
            player.current_audio.volume = player.volume
        await interaction.response.send_message(f"🔊 Volume: {int(player.volume * 100)}%", ephemeral=True)

    @discord.ui.button(label="⏹️", style=discord.ButtonStyle.danger)
    async def stop(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._guard(interaction):
            return
        self.cog._touch(self.guild_id, channel_id=getattr(interaction.channel, "id", None))

        player = self.cog._get_player(self.guild_id)
        try:
            while True:
                player.queue.get_nowait()
        except Exception:
            pass
        player.current = None
        player.current_audio = None

        vc = interaction.guild.voice_client if interaction.guild else None
        if vc:
            vc.stop()

        await interaction.response.send_message("⏹️ Gestopt en wachtrij geleegd.", ephemeral=True)


class Music(commands.Cog):
    MUSIC_ROLE_ID = 1021765413056565328  # B-FAM

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.players: Dict[int, GuildPlayer] = {}
        self.ffmpeg_path = find_ffmpeg_exe()
        self.radio_stations = _load_radio_stations()

    # --------- permissions ----------
    def _is_admin(self, member: discord.Member) -> bool:
        try:
            return member.guild_permissions.administrator
        except Exception:
            return False

    def _has_music_role(self, member: discord.Member) -> bool:
        return any(r.id == self.MUSIC_ROLE_ID for r in getattr(member, "roles", []))

    async def _ensure_bfam(self, interaction: discord.Interaction) -> bool:
        member = interaction.user
        if not isinstance(member, discord.Member):
            try:
                await interaction.response.send_message("❌ Dit commando kan alleen in een server gebruikt worden.", ephemeral=True)
            except Exception:
                pass
            return False
        if self._is_admin(member) or self._has_music_role(member):
            return True
        try:
            await interaction.response.send_message("❌ Je hebt geen toegang tot de muziek-commands. Je moet de **B-FAM** rol hebben.", ephemeral=True)
        except Exception:
            pass
        return False

    def _same_vc_or_admin(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        vc = interaction.guild.voice_client
        if not vc or not vc.channel:
            return True
        member = interaction.user
        if not isinstance(member, discord.Member):
            return False
        if member.guild_permissions.administrator:
            return True
        return member.voice and member.voice.channel and member.voice.channel.id == vc.channel.id

    # --------- helpers ----------
    def _get_player(self, guild_id: int) -> GuildPlayer:
        if guild_id not in self.players:
            self.players[guild_id] = GuildPlayer()
        return self.players[guild_id]

    def _touch(self, guild_id: int, *, channel_id: Optional[int] = None) -> None:
        """Marks activity + remembers the latest text channel for embeds."""
        player = self._get_player(guild_id)
        player.last_activity = time.monotonic()
        if channel_id:
            player.text_channel_id = channel_id

    def _resolve_text_channel(self, guild: discord.Guild) -> Optional[discord.abc.Messageable]:
        show_in = self._get_player(guild.id).text_channel_id
        if show_in:
            ch = guild.get_channel(show_in)
            if isinstance(ch, discord.abc.Messageable):
                return ch
        # fallback: try system channel
        if guild.system_channel and isinstance(guild.system_channel, discord.abc.Messageable):
            return guild.system_channel
        return None

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

        try:
            return await member.voice.channel.connect(reconnect=False)
        except Exception:
            await asyncio.sleep(1.5)
            try:
                return await member.voice.channel.connect(reconnect=False)
            except Exception:
                return None

    def _format_duration(self, seconds: Optional[int]) -> str:
        if seconds is None:
            return "?"
        seconds = max(0, int(seconds))
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

    def _embed(self, title: str, desc: str) -> discord.Embed:
        e = discord.Embed(title=title, description=desc, colour=BRAND_GREEN)
        e.set_footer(text="BromeoLIVE • Muziekspeler")
        return e

    def _controls_view(self, guild_id: int) -> discord.ui.View:
        return PlayerControls(self, guild_id)

    async def _update_nowplaying_message(self, guild_id: int) -> None:
        player = self._get_player(guild_id)
        if not player.now_msg or not player.current:
            return

        t = player.current
        desc_lines: List[str] = [f"[{t.title}]({t.webpage_url})"]

        if t.is_radio and t.radio_name:
            desc_lines.append(f"📻 Radio: **{t.radio_name}**")
        elif t.duration:
            if player.started_at:
                elapsed = time.monotonic() - player.started_at
                if player.paused_at:
                    elapsed = player.paused_at - player.started_at
                elapsed -= player.paused_total
                remaining = max(0, int(t.duration - elapsed))
                desc_lines.append(f"⏳ Nog: `{self._format_duration(remaining)}`  •  Totaal: `{self._format_duration(t.duration)}`")
            else:
                desc_lines.append(f"`{self._format_duration(t.duration)}`")

        try:
            await player.now_msg.edit(
                embed=self._embed("🎶 Nu aan het afspelen", "\n".join(desc_lines)),
                view=self._controls_view(guild_id),
            )
        except Exception:
            pass

    async def _progress_updater(self, guild_id: int) -> None:
        player = self._get_player(guild_id)
        while True:
            await asyncio.sleep(15)
            if not player.current or not player.now_msg:
                return
            if player.current.is_radio:
                await self._update_nowplaying_message(guild_id)
                continue
            if not player.current.duration:
                continue
            await self._update_nowplaying_message(guild_id)

    async def _ytdl_extract(self, query: str) -> Track:
        loop = asyncio.get_running_loop()
        raw = (query or "").strip()
        lower = raw.lower()

        # allow direct ytsearch/scsearch prefixes (used by autoplay)
        if lower.startswith(("ytsearch", "scsearch")):
            q_run = raw
        else:
            use_sc = False
            if lower.startswith("sc:"):
                use_sc = True
                raw = raw[3:].strip()

            if raw.startswith("http://") or raw.startswith("https://"):
                q_run = raw
            else:
                q_run = f"{'scsearch1' if use_sc else 'ytsearch1'}:{raw}"

        def run():
            opts = dict(BASE_YTDL_OPTS)

            cookiefile = (
                os.getenv("YTDLP_COOKIES")
                or os.getenv("YTDLP_COOKIES_PATH")
                or "/app/data/cookies.txt"
            )
            if cookiefile:
                opts["cookiefile"] = cookiefile

            # YouTube: android client is most stable on VPS
            opts["extractor_args"] = {"youtube": {"player_client": ["android", "web"], "skip": ["dash", "hls"]}}
            opts["format"] = "bestaudio[acodec^=opus]/bestaudio[ext=m4a]/bestaudio/best"
            opts["format_sort"] = ["acodec:opus", "abr", "asr", "ext"]

            print(f"[music] yt-dlp cookiefile={cookiefile} exists={bool(cookiefile and os.path.exists(cookiefile))} q={q_run}")

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(q_run, download=False)

                if isinstance(info, dict) and "entries" in info:
                    entries = [e for e in (info.get("entries") or []) if e]
                    if not entries:
                        raise RuntimeError("No results.")
                    info = entries[0]

                if isinstance(info, dict) and info.get("_type") in ("url", "url_transparent"):
                    u = info.get("url") or info.get("webpage_url")
                    if u:
                        info = ydl.extract_info(u, download=False)

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
            raise RuntimeError("Could not get audio stream.")

        return Track(title=title, url=stream_url, webpage_url=webpage, duration=duration)

    async def _start_player_task(self, guild: discord.Guild):
        player = self._get_player(guild.id)
        async with player._lock:
            if player._task and not player._task.done():
                return
            player._task = asyncio.create_task(self._player_loop(guild))

    async def _player_loop(self, guild: discord.Guild):
        player = self._get_player(guild.id)

        async def safe_send(embed: discord.Embed | None = None, content: str | None = None, view: discord.ui.View | None = None):
            ch = self._resolve_text_channel(guild)
            if not ch:
                return None
            try:
                return await ch.send(content=content, embed=embed, view=view)
            except Exception:
                return None

        while True:
            try:
                track: Track = await asyncio.wait_for(player.queue.get(), timeout=300)  # 5 min
            except asyncio.TimeoutError:
                vc = guild.voice_client
                # Don't auto-disconnect if we're currently playing (e.g. radio stream)
                if vc and vc.is_connected() and (not vc.is_playing()) and (not vc.is_paused()):
                    # Only disconnect if we've truly been idle for 5 min (no playback + no commands)
                    idle_for = time.monotonic() - player.last_activity
                    if idle_for >= 300:
                        await safe_send(embed=self._embed("👋 Leaving voice", "Ik ben weggegaan wegens **5 minuten inactiviteit**."))
                        try:
                            await vc.disconnect()
                        except Exception:
                            pass
                return

            if player.loop and player.current:
                track = player.current

            player.current = track
            self._touch(guild.id)  # playback activity
            player.started_at = time.monotonic()
            player.paused_at = None
            player.paused_total = 0.0

            vc = guild.voice_client
            if not vc or not vc.is_connected():
                continue

            source = discord.FFmpegPCMAudio(
                track.url,
                executable=self.ffmpeg_path,
                before_options=FFMPEG_BEFORE_OPTS,
                options=FFMPEG_OPTS,
            )
            audio = discord.PCMVolumeTransformer(source, volume=player.volume)
            player.current_audio = audio

            done = asyncio.Event()

            def after(_err):
                try:
                    self.bot.loop.call_soon_threadsafe(done.set)
                except Exception:
                    pass

            try:
                vc.play(audio, after=after)
            except Exception:
                continue

            # Now playing message + controls
            try:
                player.now_msg = await safe_send(
                    embed=self._embed("🎶 Nu aan het afspelen", f"[{track.title}]({track.webpage_url})"),
                    view=self._controls_view(guild.id),
                )
            except Exception:
                player.now_msg = None

            if player.progress_task and not player.progress_task.done():
                player.progress_task.cancel()
            player.progress_task = asyncio.create_task(self._progress_updater(guild.id))

            await self._update_nowplaying_message(guild.id)

            await done.wait()

            player.current_audio = None

            if not player.loop:
                player.current = None

            # Autoplay: if enabled and queue empty, add 1 related track
            if player.autoplay and player.queue.empty():
                try:
                    seed = track.title if track else "lofi mix"
                    auto_track = await self._ytdl_extract(f"ytsearch1:{seed} mix")
                    await player.queue.put(auto_track)
                    try:
                        await safe_send(embed=self._embed("📻 Autoplay", f"Toegevoegd: [{auto_track.title}]({auto_track.webpage_url})"))
                    except Exception:
                        pass
                except Exception:
                    pass

            # Radio streams can sometimes end / drop. If the last track was radio and nothing else is queued,
            # automatically restart the same station instead of triggering idle disconnect.
            if track.is_radio and player.queue.empty() and not player.loop:
                try:
                    await player.queue.put(track)
                    self._touch(guild.id)
                except Exception:
                    pass

    # --------------------
    # Slash commands
    # --------------------
    music = app_commands.Group(name="muziek", description="Muziek-commands (alleen B-FAM / admins).")
    radio = app_commands.Group(name="radio", parent=music, description="Icecast radio (directe streams).")

    @music.command(name="autoplay", description="Zet autoplay aan/uit als de wachtrij leeg is.")
    async def autoplay_cmd(self, interaction: discord.Interaction, enabled: bool):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        player.autoplay = bool(enabled)
        await interaction.response.send_message(f"📻 Autoplay staat nu {'ON' if player.autoplay else 'OFF'}.", ephemeral=True)

    @radio.command(name="lijst", description="Toon beschikbare radio stations.")
    async def radio_list(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        names = sorted(self.radio_stations.keys())
        if not names:
            return await interaction.response.send_message("Geen stations geconfigureerd.", ephemeral=True)
        await interaction.response.send_message(
            embed=self._embed("📻 Radio stations", "\n".join([f"• `{n}`" for n in names])),
            ephemeral=True,
        )

    @radio.command(name="speel", description="Speel een Icecast station (directe stream).")
    @app_commands.describe(station="Naam uit /muziek radio lijst")
    async def radio_play(self, interaction: discord.Interaction, station: str):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        self._touch(interaction.guild.id, channel_id=getattr(interaction.channel, "id", None))

        st = (station or "").strip().lower()
        url = self.radio_stations.get(st)
        if not url:
            return await interaction.followup.send("Onbekend station. Gebruik `/muziek radio lijst`.", ephemeral=True)

        vc = await self._join(interaction)
        if not vc:
            return await interaction.followup.send("Ga eerst in een spraakkanaal zitten.", ephemeral=True)

        player = self._get_player(interaction.guild.id)

        # stop current playback + clear queue
        try:
            while True:
                player.queue.get_nowait()
        except Exception:
            pass
        player.loop = False
        player.autoplay = False

        try:
            vc.stop()
        except Exception:
            pass

        track = Track(
            title=f"Radio: {st}",
            url=url,
            webpage_url=url,
            duration=None,
            requester_id=interaction.user.id,
            is_radio=True,
            radio_name=st,
        )

        await player.queue.put(track)
        await self._start_player_task(interaction.guild)

        await interaction.followup.send(f"📻 Speelt nu **{st}**.", ephemeral=True)


    @radio.command(name="stop", description="Stop radio playback.")
    async def radio_stop(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        player = self._get_player(interaction.guild.id)
        vc = interaction.guild.voice_client if interaction.guild else None
        if vc:
            vc.stop()
        player.current = None
        player.current_audio = None
        await interaction.response.send_message("⏹️ Radio gestopt.", ephemeral=True)

    @music.command(name="speel", description="Play a song/URL (joins your voice channel).")
    @app_commands.describe(query="Zoekterm of URL. Tip: prefix met 'sc:' voor SoundCloud search.")
    async def play(self, interaction: discord.Interaction, query: str):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        self._touch(interaction.guild.id, channel_id=getattr(interaction.channel, "id", None))

        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

        try:
            track = await self._ytdl_extract(query)
            track.requester_id = interaction.user.id
        except Exception as e:
            msg = str(e)
            if "Sign in to confirm you" in msg:
                msg = (
                    "YouTube blocked the request (bot-check). "
                    "Check cookies are loaded (see logs line: [music] yt-dlp cookiefile=... exists=True). "
                    "If exists=True and it still blocks, your cookies may be expired or YouTube flags VPS IP."
                )
            return await interaction.followup.send(f"Couldn’t load that track. ({msg})", ephemeral=True)

        vc = await self._join(interaction)
        if not vc:
            return await interaction.followup.send("Ga eerst in een spraakkanaal zitten.", ephemeral=True)

        player = self._get_player(interaction.guild.id)
        await player.queue.put(track)
        await self._start_player_task(interaction.guild)

        await interaction.followup.send(
            embed=self._embed("✅ Toegevoegd aan wachtrij", f"[{track.title}]({track.webpage_url})"),
            ephemeral=True,
        )

    @music.command(name="pauze", description="Pauzeer afspelen.")
    async def pause(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        vc = interaction.guild.voice_client if interaction.guild else None
        player = self._get_player(interaction.guild.id)

        if not vc or not vc.is_playing():
            return await interaction.response.send_message("Er speelt nu niks.", ephemeral=True)
        vc.pause()
        player.paused_at = time.monotonic()
        await interaction.response.send_message("⏸️ Gepauzeerd.", ephemeral=True)

    @music.command(name="hervat", description="Hervat afspelen.")
    async def resume(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        vc = interaction.guild.voice_client if interaction.guild else None
        player = self._get_player(interaction.guild.id)

        if not vc or not vc.is_paused():
            return await interaction.response.send_message("Er is niks gepauzeerd.", ephemeral=True)
        vc.resume()
        if player.paused_at:
            player.paused_total += max(0.0, time.monotonic() - player.paused_at)
        player.paused_at = None
        await interaction.response.send_message("▶️ Hervat.", ephemeral=True)

    @music.command(name="volgende", description="Sla de huidige track over.")
    async def skip(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        if not self._same_vc_or_admin(interaction):
            return await interaction.response.send_message("Ga in hetzelfde spraakkanaal als de bot.", ephemeral=True)

        vc = interaction.guild.voice_client if interaction.guild else None
        if not vc or not (vc.is_playing() or vc.is_paused()):
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
        try:
            while True:
                player.queue.get_nowait()
        except Exception:
            pass
        player.current = None
        player.current_audio = None

        vc = interaction.guild.voice_client if interaction.guild else None
        if vc:
            vc.stop()

        await interaction.response.send_message("⏹️ Gestopt en wachtrij geleegd.", ephemeral=True)

    @music.command(name="wachtrij", description="Toon de wachtrij.")
    async def queue_cmd(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return

        player = self._get_player(interaction.guild.id)
        items: List[Track] = list(player.queue._queue)  # ok for display

        if not player.current and not items:
            return await interaction.response.send_message("Wachtrij is leeg.", ephemeral=True)

        lines: List[str] = []
        if player.current:
            lines.append(f"**Now:** [{player.current.title}]({player.current.webpage_url})")
        for i, t in enumerate(items[:10], start=1):
            lines.append(f"{i}. [{t.title}]({t.webpage_url})")
        if len(items) > 10:
            lines.append(f"…en nog {len(items)-10} meer")

        await interaction.response.send_message(embed=self._embed("📜 Wachtrij", "\n".join(lines)), ephemeral=True)

    @music.command(name="nowplaying", description="Toon de huidige track (update embed).")
    async def nowplaying(self, interaction: discord.Interaction):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        if not player.current:
            return await interaction.response.send_message("Er speelt nu niks.", ephemeral=True)
        await self._update_nowplaying_message(interaction.guild.id)
        await interaction.response.send_message("✅ Now playing geüpdatet.", ephemeral=True)

    @music.command(name="volume", description="Set volume (0-100).")
    async def volume(self, interaction: discord.Interaction, percent: app_commands.Range[int, 0, 100]):
        if not await self._ensure_bfam(interaction):
            return
        player = self._get_player(interaction.guild.id)
        player.volume = max(0.0, min(1.0, percent / 100.0))
        if player.current_audio:
            player.current_audio.volume = player.volume
        await interaction.response.send_message(f"🔊 Volume ingesteld op {percent}%.", ephemeral=True)

    @music.command(name="herhaal", description="Herhaal huidige track aan/uit.")
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


    # ---------------------
    # Dashboard helpers
    # ---------------------
    def dashboard_status(self, guild_id: int) -> dict:
        player = self._get_player(guild_id)
        now = None
        if player.current:
            now = {
                "title": player.current.title,
                "webpage_url": player.current.webpage_url,
                "volume": int(player.volume * 100),
            }
        # Snapshot queue (asyncio.Queue is not easily iterable; use _queue)
        q = []
        try:
            for t in list(player.queue._queue)[:15]:  # type: ignore[attr-defined]
                q.append({"title": t.title, "webpage_url": t.webpage_url})
        except Exception:
            q = []
        vc = None
        try:
            g = self.bot.get_guild(guild_id)
            vc = g.voice_client if g else None
        except Exception:
            vc = None
        state = "idle"
        if vc:
            if vc.is_paused():
                state = "paused"
            elif vc.is_playing():
                state = "playing"
            else:
                state = "connected"
        return {"state": state, "now": now, "queue": q}

    async def dashboard_action(self, guild_id: int, actor_user_id: int, payload: dict) -> None:
        action = (payload.get("action") or "").strip().lower()

        # Dashboard frontends historically used different action names.
        # Keep backwards compatibility so the UI can be updated independently.
        aliases = {
            "pause_resume": "toggle",
            "play": "enqueue",
            "add_playlist": "playlist_add",
        }
        action = aliases.get(action, action)

        # Accept both `url` and `query` payload keys.
        url = (payload.get("url") or payload.get("query") or "").strip()
        # Optional voice channel id for join/move.
        channel_id = None
        try:
            if payload.get("channel_id") is not None:
                channel_id = int(str(payload.get("channel_id")))
        except Exception:
            channel_id = None
        g = self.bot.get_guild(guild_id)
        if not g:
            return
        vc = g.voice_client
        player = self._get_player(guild_id)
        self._touch(guild_id)

        if action in {"pause", "resume", "toggle"}:
            if vc and vc.is_playing():
                vc.pause();
                player.paused_at = time.monotonic()
            elif vc and vc.is_paused():
                vc.resume();
                if player.paused_at:
                    player.paused_total += max(0.0, time.monotonic() - player.paused_at)
                player.paused_at = None
            return

        if action == "skip":
            if vc and (vc.is_playing() or vc.is_paused()):
                vc.stop()
            return

        if action == "disconnect":
            if vc:
                try:
                    await vc.disconnect(force=True)
                except Exception:
                    pass
            return

        if action == "join":
            if not channel_id:
                return
            ch = g.get_channel(channel_id)
            if ch is None:
                try:
                    ch = await self.bot.fetch_channel(channel_id)
                except Exception:
                    ch = None
            if not isinstance(ch, (discord.VoiceChannel, discord.StageChannel)):
                return
            try:
                if vc is None:
                    await ch.connect()
                else:
                    await vc.move_to(ch)
            except Exception:
                pass
            return

        if action == "stop":
            try:
                while True:
                    player.queue.get_nowait()
            except Exception:
                pass
            player.current = None
            player.current_audio = None
            if vc:
                vc.stop()
            return

        if action == "vol_up":
            player.volume = min(1.0, player.volume + 0.1)
            if player.current_audio:
                player.current_audio.volume = player.volume
            return

        if action == "vol_down":
            player.volume = max(0.0, player.volume - 0.1)
            if player.current_audio:
                player.current_audio.volume = player.volume
            return

        if action == "enqueue" and url:
            # Enqueue a URL like /music speel does
            # Use a fake interaction-less flow by extracting info and pushing to queue
            track = await self._extract_track(url, requester_id=actor_user_id)
            await player.queue.put(track)
            if player._task is None or player._task.done():
                player._task = asyncio.create_task(self._player_loop(guild_id))
            return

        if action == "playlist_add":
            # Add current or a provided URL to the default playlist
            pl_id = self.bot.db.get_or_create_playlist(guild_id, name="default", created_by=actor_user_id)
            if player.current:
                self.bot.db.add_playlist_track(pl_id, player.current.title, player.current.url, player.current.webpage_url, added_by=actor_user_id)
                return
            if url:
                track = await self._extract_track(url, requester_id=actor_user_id)
                self.bot.db.add_playlist_track(pl_id, track.title, track.url, track.webpage_url, added_by=actor_user_id)
            return

        if action == "play_playlist":
            pl_id = self.bot.db.get_or_create_playlist(guild_id, name="default", created_by=actor_user_id)
            rows = self.bot.db.list_playlist_tracks(pl_id, limit=200)
            # rows are ordered DESC (newest first) -> enqueue reversed so it plays oldest first
            for r in reversed(rows):
                try:
                    track = await self._extract_track(str(r["url"]), requester_id=actor_user_id)
                    await player.queue.put(track)
                except Exception:
                    continue
            if rows and (player._task is None or player._task.done()):
                player._task = asyncio.create_task(self._player_loop(guild_id))
            return

        if action == "clear_playlist":
            pl_id = self.bot.db.get_or_create_playlist(guild_id, name="default", created_by=actor_user_id)
            self.bot.db.clear_playlist_tracks(pl_id)
            return

    async def _extract_track(self, query: str, requester_id: int | None = None) -> Track:
        # Small helper for dashboard enqueue/playlist
        # In Py3.11+, get_event_loop() can fail depending on context;
        # within a coroutine we always want the running loop.
        loop = asyncio.get_running_loop()
        ytdl = yt_dlp.YoutubeDL({**BASE_YTDL_OPTS, "ffmpeg_location": self.ffmpeg_path})
        info = await loop.run_in_executor(None, lambda: ytdl.extract_info(query, download=False))
        if "entries" in info and isinstance(info["entries"], list) and info["entries"]:
            info = info["entries"][0]
        url = info.get("url") or query
        title = info.get("title") or query
        webpage_url = info.get("webpage_url") or query
        return Track(title=title, url=url, webpage_url=webpage_url, duration=info.get("duration"), requester_id=requester_id)


async def setup(bot: commands.Bot):
    await bot.add_cog(Music(bot))
