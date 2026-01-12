import asyncio
import datetime as dt
import random
import re
import time
from dataclasses import dataclass
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands


BRAND_GREEN = discord.Colour.from_rgb(46, 204, 113)

# Requirements from Chris
MIN_LEVEL_ROLE_ID = 1040704476962631811  # level 5
WINNER_ROLE_ID = 1047424554529730600     # Giveaway winnaar


def _is_admin(member: discord.Member) -> bool:
    try:
        return member.guild_permissions.administrator
    except Exception:
        return False


def _parse_endtime(s: str) -> int:
    """Parse a human-ish duration/date string into a unix timestamp (seconds).

    Supported:
      - 30m, 2h, 1d
      - YYYY-MM-DD HH:MM (24h)
      - HH:MM (today)
    """
    raw = (s or "").strip()
    if not raw:
        raise ValueError("Geen eindtijd opgegeven")

    now = dt.datetime.now()

    m = re.fullmatch(r"(\d+)\s*([mhd])", raw.lower())
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        delta = dt.timedelta(minutes=n) if unit == "m" else dt.timedelta(hours=n) if unit == "h" else dt.timedelta(days=n)
        return int((now + delta).timestamp())

    # YYYY-MM-DD HH:MM
    try:
        when = dt.datetime.strptime(raw, "%Y-%m-%d %H:%M")
        return int(when.timestamp())
    except Exception:
        pass

    # HH:MM (today)
    try:
        when_t = dt.datetime.strptime(raw, "%H:%M").time()
        when = dt.datetime.combine(now.date(), when_t)
        # if already passed today, schedule tomorrow
        if when <= now:
            when = when + dt.timedelta(days=1)
        return int(when.timestamp())
    except Exception:
        pass

    raise ValueError("Onbekend tijd-formaat. Gebruik bijv: 30m, 2h, 1d, 19:00 of 2026-01-12 19:00")


@dataclass
class GiveawayState:
    giveaway_id: int
    guild_id: int
    channel_id: int
    message_id: int
    prize: str
    description: str
    end_at: int
    max_participants: Optional[int]
    thumbnail_name: Optional[str]


class ParticipateView(discord.ui.View):
    def __init__(self, cog: "Giveaway", state: GiveawayState):
        super().__init__(timeout=None)
        self.cog = cog
        self.state = state

        # Persistent button needs a stable custom_id
        self.btn = discord.ui.Button(
            label="Participate",
            style=discord.ButtonStyle.primary,
            emoji="🎉",
            custom_id=f"giveaway_participate:{state.giveaway_id}",
        )
        self.btn.callback = self._on_click
        self.add_item(self.btn)

    async def _on_click(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Dit werkt alleen in een server.", ephemeral=True)

        member: discord.Member = interaction.user

        # Minimum role requirement
        if not any(r.id == MIN_LEVEL_ROLE_ID for r in member.roles) and not _is_admin(member):
            return await interaction.response.send_message(
                "Sorry, je kunt nog niet meedoen omdat je nog niet hoger bent dan **level 5**.\n"
                "Begin met chatten om je level omhoog te krijgen!",
                ephemeral=True,
            )

        # Cap check
        if self.state.max_participants is not None:
            current = self.cog.bot.db.giveaway_entry_count(self.state.giveaway_id)
            if current >= self.state.max_participants:
                return await interaction.response.send_message("Deze giveaway zit vol.", ephemeral=True)

        added = self.cog.bot.db.add_giveaway_entry(self.state.giveaway_id, member.id)
        if not added:
            return await interaction.response.send_message("Je doet al mee ✅", ephemeral=True)

        # Update message participant count
        try:
            msg = await interaction.channel.fetch_message(self.state.message_id)
        except Exception:
            msg = None

        count = self.cog.bot.db.giveaway_entry_count(self.state.giveaway_id)
        # Update button label with count
        try:
            self.btn.label = f"Participate ({count})"
        except Exception:
            pass

        if msg:
            try:
                await msg.edit(embed=self.cog._giveaway_embed(self.state, count=count), view=self)
            except Exception:
                pass

        await interaction.response.send_message("Je doet mee! 🎉", ephemeral=True)


class Giveaway(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._end_task: Optional[asyncio.Task] = None
        self._views_started = False

    async def cog_load(self) -> None:
        # Start watcher loop
        if not self._end_task:
            self._end_task = asyncio.create_task(self._giveaway_watcher())

    async def cog_unload(self) -> None:
        if self._end_task and not self._end_task.done():
            self._end_task.cancel()

    async def _ensure_persistent_views(self):
        """Re-register persistent views after restart so existing giveaway buttons keep working."""
        if self._views_started:
            return
        self._views_started = True
        try:
            rows = self.bot.db.get_active_giveaways()
        except Exception:
            rows = []
        for r in rows:
            st = self._row_to_state(r)
            self.bot.add_view(ParticipateView(self, st))

    def _row_to_state(self, r) -> GiveawayState:
        return GiveawayState(
            giveaway_id=int(r["id"]),
            guild_id=int(r["guild_id"]),
            channel_id=int(r["channel_id"]),
            message_id=int(r["message_id"]),
            prize=str(r["prize"]),
            description=str(r["description"] or ""),
            end_at=int(r["end_at"]),
            max_participants=(int(r["max_participants"]) if r["max_participants"] is not None else None),
            thumbnail_name=(str(r["thumbnail_name"]) if r["thumbnail_name"] else None),
        )

    def _giveaway_embed(self, st: GiveawayState, *, count: int) -> discord.Embed:
        end_dt = dt.datetime.fromtimestamp(st.end_at)
        e = discord.Embed(
            title=f"{st.prize}",
            description=(st.description or ""),
            colour=BRAND_GREEN,
        )
        e.add_field(name="Eindigt", value=f"<t:{st.end_at}:F>\n(<t:{st.end_at}:R>)", inline=False)
        if st.max_participants:
            e.add_field(name="Deelnemers", value=f"{count}/{st.max_participants}", inline=True)
        else:
            e.add_field(name="Deelnemers", value=str(count), inline=True)
        e.set_footer(text="BromeoLIVE • Giveaway")
        if st.thumbnail_name:
            e.set_thumbnail(url=f"attachment://{st.thumbnail_name}")
        return e

    def _results_embed(self, st: GiveawayState, *, winner: Optional[discord.Member], count: int) -> discord.Embed:
        title = f"{st.prize} [RESULTS]"
        desc = "The winner of this giveaway is tagged above!\nCongratulations 🎉" if winner else "Geen deelnemers 😢"
        e = discord.Embed(title=title, description=desc, colour=BRAND_GREEN)
        e.add_field(name="Prize", value=st.prize, inline=True)
        e.add_field(name="Participants", value=str(count), inline=True)
        e.set_footer(text="BromeoLIVE • Giveaway")
        if st.thumbnail_name:
            e.set_thumbnail(url=f"attachment://{st.thumbnail_name}")
        return e

    def _winner_dm_embed(self, st: GiveawayState) -> discord.Embed:
        e = discord.Embed(
            title="Gefeliciteerd! 🎉",
            description=f"Je hebt **{st.prize}** gewonnen!",
            colour=BRAND_GREEN,
        )
        e.set_footer(text="BromeoLIVE • Giveaway")
        return e

    async def _giveaway_watcher(self) -> None:
        # Wait for bot to be ready
        await self.bot.wait_until_ready()
        await self._ensure_persistent_views()

        while not self.bot.is_closed():
            try:
                now_ts = int(time.time())
                due = self.bot.db.get_active_giveaways(now_ts)
                for r in due:
                    try:
                        await self._finish_giveaway(self._row_to_state(r))
                    except Exception:
                        pass
            except Exception:
                pass
            await asyncio.sleep(20)

    async def _finish_giveaway(self, st: GiveawayState) -> None:
        # Double-check not ended
        row = self.bot.db.get_giveaway(st.giveaway_id)
        if not row or int(row["ended"]) == 1:
            return

        guild = self.bot.get_guild(st.guild_id)
        if not guild:
            self.bot.db.end_giveaway(st.giveaway_id, None)
            return

        channel = guild.get_channel(st.channel_id)
        if not isinstance(channel, discord.abc.Messageable):
            self.bot.db.end_giveaway(st.giveaway_id, None)
            return

        entries = self.bot.db.get_giveaway_entries(st.giveaway_id)
        count = len(entries)
        winner_member: Optional[discord.Member] = None

        if entries:
            winner_id = random.choice(entries)
            winner_member = guild.get_member(winner_id) or await guild.fetch_member(winner_id)
            self.bot.db.end_giveaway(st.giveaway_id, winner_id)
        else:
            self.bot.db.end_giveaway(st.giveaway_id, None)

        # Disable button on original message
        try:
            msg = await channel.fetch_message(st.message_id)
        except Exception:
            msg = None
        if msg:
            try:
                v = ParticipateView(self, st)
                for child in v.children:
                    if isinstance(child, discord.ui.Button):
                        child.disabled = True
                await msg.edit(view=v)
            except Exception:
                pass

        # Announce result
        tag_line = winner_member.mention if winner_member else ""
        files = []
        # Reuse the original attachment (so the results embed can keep the same thumbnail)
        if msg and msg.attachments:
            try:
                f = await msg.attachments[0].to_file()
                files = [f]
                st.thumbnail_name = f.filename
            except Exception:
                files = []
        await channel.send(
            content=tag_line,
            embed=self._results_embed(st, winner=winner_member, count=count),
            files=files,
        )

        # DM winner + assign role
        if winner_member:
            try:
                await winner_member.send(embed=self._winner_dm_embed(st))
            except Exception:
                pass
            try:
                role = guild.get_role(WINNER_ROLE_ID)
                if role:
                    await winner_member.add_roles(role, reason="Giveaway winnaar")
            except Exception:
                pass

    giveaway = app_commands.Group(name="giveaway", description="Giveaway commands (admins only)")

    @giveaway.command(name="maak", description="Maak een giveaway (admins only)")
    @app_commands.describe(
        prijs="Bijv: 1000 V-Bucks",
        eindtijd="Bijv: 30m, 2h, 1d, 19:00 of 2026-01-12 19:00",
        kanaal="Kanaal waar de giveaway gepost wordt",
        deelnemers="Max deelnemers (optioneel)",
        beschrijving="Extra tekst/omschrijving",
        afbeelding="Optionele afbeelding (thumbnail)"
    )
    async def giveaway_maak(
        self,
        interaction: discord.Interaction,
        prijs: str,
        winners: int = 1,
        eindtijd: str,
        kanaal: discord.TextChannel,
        deelnemers: Optional[int] = None,
        beschrijving: Optional[str] = None,
        afbeelding: Optional[discord.Attachment] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Dit kan alleen in een server.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("Alleen admins kunnen een giveaway aanmaken.", ephemeral=True)

        try:
            end_at = _parse_endtime(eindtijd)
        except Exception as e:
            return await interaction.response.send_message(str(e), ephemeral=True)

        if deelnemers is not None and deelnemers <= 0:
            deelnemers = None

        await interaction.response.defer(ephemeral=True)

        # Post embed
        thumb_name = None
        file_obj = None
        if afbeelding:
            try:
                file_obj = await afbeelding.to_file()
                thumb_name = file_obj.filename
            except Exception:
                file_obj = None
                thumb_name = None

        # Create temp state (message id known after send)
        tmp_state = GiveawayState(
            giveaway_id=0,
            guild_id=interaction.guild.id,
            channel_id=kanaal.id,
            message_id=0,
            prize=prijs,
            description=(beschrijving or ""),
            end_at=end_at,
            max_participants=deelnemers,
            thumbnail_name=thumb_name,
        )

        view = ParticipateView(self, tmp_state)
        # initial count is 0
        msg = await kanaal.send(embed=self._giveaway_embed(tmp_state, count=0), view=view, file=file_obj)

        # Persist to DB
        giveaway_id = self.bot.db.create_giveaway(
            guild_id=interaction.guild.id,
            channel_id=kanaal.id,
            message_id=msg.id,
            prize=prijs,
            description=(beschrijving or ""),
            max_participants=deelnemers,
            end_at=end_at,
            created_by=interaction.user.id,
            thumbnail_name=thumb_name,
        )

        # Update state + view to be persistent
        st = GiveawayState(
            giveaway_id=giveaway_id,
            guild_id=interaction.guild.id,
            channel_id=kanaal.id,
            message_id=msg.id,
            prize=prijs,
            description=(beschrijving or ""),
            end_at=end_at,
            max_participants=deelnemers,
            thumbnail_name=thumb_name,
        )
        self.bot.add_view(ParticipateView(self, st))

        # Update message with the real state (button label)
        try:
            v2 = ParticipateView(self, st)
            for child in v2.children:
                if isinstance(child, discord.ui.Button):
                    child.label = "Participate (0)"
            await msg.edit(embed=self._giveaway_embed(st, count=0), view=v2)
        except Exception:
            pass

        await interaction.followup.send(f"✅ Giveaway aangemaakt in {kanaal.mention}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Giveaway(bot))
