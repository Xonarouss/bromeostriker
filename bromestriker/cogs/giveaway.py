import asyncio
import datetime as dt
import random
import json
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


def _can_manage_giveaway(member: discord.Member) -> bool:
    """Admins OR B-Crew can cancel/reroll giveaways."""
    try:
        return bool(member.guild_permissions.administrator) or (member.get_role(1027533834318774293) is not None)
    except Exception:
        return False



def _can_create_giveaway(member: discord.Member) -> bool:
    """Admins OR B-Crew can create giveaways."""
    try:
        return bool(member.guild_permissions.administrator) or (member.get_role(1027533834318774293) is not None)
    except Exception:
        return False




def _row_get(row, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


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
    winners_count: int = 1
    winner_ids_json: Optional[str] = None
class ParticipateView(discord.ui.View):
    def __init__(self, cog: "Giveaway", state: GiveawayState, *, ended: bool = False):
        super().__init__(timeout=None)
        self.cog = cog
        self.state = state
        self.ended = ended

        # Persistent buttons need stable custom_id
        self.participate_btn = discord.ui.Button(
            label="Deelnemen",
            style=discord.ButtonStyle.primary,
            emoji="🎉",
            custom_id=f"giveaway_participate:{state.giveaway_id}",
            disabled=ended,
        )
        self.participate_btn.callback = self._on_click
        self.add_item(self.participate_btn)

        self.leave_btn = discord.ui.Button(
            label="Verlaten",
            style=discord.ButtonStyle.secondary,
            emoji="🚪",
            custom_id=f"giveaway_leave:{state.giveaway_id}",
            disabled=ended,
        )
        self.leave_btn.callback = self._on_leave
        self.add_item(self.leave_btn)

        self.cancel_btn = discord.ui.Button(
            label="Annuleren",
            style=discord.ButtonStyle.danger,
            emoji="🛑",
            custom_id=f"giveaway_cancel:{state.giveaway_id}",
            disabled=ended,
        )
        self.cancel_btn.callback = self._on_cancel
        self.add_item(self.cancel_btn)

        self.reroll_btn = discord.ui.Button(
            label="Opnieuw trekken",
            style=discord.ButtonStyle.secondary,
            emoji="🔁",
            custom_id=f"giveaway_reroll:{state.giveaway_id}",
            disabled=not ended,
        )
        self.reroll_btn.callback = self._on_reroll
        self.add_item(self.reroll_btn)

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
            return await interaction.response.send_message("Je doet al mee ✅ (druk op **Verlaten** als je eruit wil)", ephemeral=True)

        # Update message participant count
        count = self.cog.bot.db.giveaway_entry_count(self.state.giveaway_id)
        try:
            self.participate_btn.label = "Deelnemen"
        except Exception:
            pass

        # Edit the message that contains this button (most reliable)
        try:
            await interaction.response.edit_message(
                embed=self.cog._giveaway_embed(self.state, count=count),
                view=self,
            )
        except Exception:
            # Fallback: try a followup edit
            try:
                msg = interaction.message
                if msg:
                    await msg.edit(embed=self.cog._giveaway_embed(self.state, count=count), view=self)
            except Exception as e:
                print('Giveaway watcher error:', repr(e))
            try:
                # Ensure we at least acknowledge
                if not interaction.response.is_done():
                    await interaction.response.send_message("Je doet mee! 🎉", ephemeral=True)
                else:
                    await interaction.followup.send("Je doet mee! 🎉", ephemeral=True)
            except Exception as e:
                print('Giveaway watcher error:', repr(e))
        else:
            # Confirmation as ephemeral followup
            try:
                await interaction.followup.send("Je doet mee! 🎉", ephemeral=True)
            except Exception as e:
                print('Giveaway watcher error:', repr(e))
    async def _on_leave(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Dit werkt alleen in een server.", ephemeral=True)

        member: discord.Member = interaction.user

        removed = self.cog.bot.db.remove_giveaway_entry(self.state.giveaway_id, member.id)
        if not removed:
            return await interaction.response.send_message("Je deed niet mee aan deze giveaway.", ephemeral=True)

        count = self.cog.bot.db.giveaway_entry_count(self.state.giveaway_id)
        try:
            self.participate_btn.label = "Deelnemen"
        except Exception:
            pass

        try:
            await interaction.response.edit_message(
                embed=self.cog._giveaway_embed(self.state, count=count),
                view=self,
            )
        except Exception:
            try:
                msg = interaction.message
                if msg:
                    await msg.edit(embed=self.cog._giveaway_embed(self.state, count=count), view=self)
            except Exception:
                pass
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Je bent uit de giveaway gestapt. 🚪", ephemeral=True)
                else:
                    await interaction.followup.send("Je bent uit de giveaway gestapt. 🚪", ephemeral=True)
            except Exception:
                pass
        else:
            try:
                await interaction.followup.send("Je bent uit de giveaway gestapt. 🚪", ephemeral=True)
            except Exception:
                pass





    async def _on_cancel(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Dit werkt alleen in een server.", ephemeral=True)
        if not _can_manage_giveaway(interaction.user):
            return await interaction.response.send_message("Alleen admins of B-Crew kunnen een giveaway cancellen.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        responded = False
        ok = await self.cog._cancel_giveaway(self.state, interaction=interaction)
        if ok:
            await interaction.followup.send("Giveaway gecanceld.", ephemeral=True)
        else:
            await interaction.followup.send("Kon giveaway niet cancellen (misschien al afgelopen).", ephemeral=True)

    async def _on_reroll(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Dit werkt alleen in een server.", ephemeral=True)
        if not _can_manage_giveaway(interaction.user):
            return await interaction.response.send_message("Alleen admins of B-Crew kunnen rerollen.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        responded = False
        ok = await self.cog._reroll_giveaway(self.state, interaction=interaction)
        if ok:
            await interaction.followup.send("Reroll uitgevoerd.", ephemeral=True)
        else:
            await interaction.followup.send("Kon niet rerollen (geen deelnemers of giveaway niet afgelopen).", ephemeral=True)


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
            self.bot.add_view(ParticipateView(self, st, ended=bool(int(r['ended']))))

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
            winners_count=(int(r["winners_count"]) if ("winners_count" in r.keys() and r["winners_count"] is not None) else 1),
            winner_ids_json=(str(r["winner_ids"]) if ("winner_ids" in r.keys() and r["winner_ids"]) else None),
        )

    def _giveaway_embed(self, st: GiveawayState, *, count: int) -> discord.Embed:
        # Use Discord's native timestamp formatting so it auto-localizes per user.
        # :F = full date/time, :R = relative time (e.g. "in 2 hours").
        end_value = f"<t:{st.end_at}:F>\n(<t:{st.end_at}:R>)"
        e = discord.Embed(
            title=f"{st.prize}",
            description=(st.description or ""),
            colour=BRAND_GREEN,
        )
        e.add_field(name="Eindigt", value=end_value)
        if st.max_participants:
            e.add_field(name="Deelnemers", value=f"{count}/{st.max_participants}", inline=True)
        else:
            e.add_field(name="Deelnemers", value=str(count), inline=True)
        e.set_footer(text="BromeoLIVE • Giveaway")
        if st.thumbnail_name:
            e.set_thumbnail(url=f"attachment://{st.thumbnail_name}")
        return e

    def _results_embed(self, st: GiveawayState, *, winners: list[discord.Member], count: int) -> discord.Embed:
        title = f"{st.prize} [RESULTATEN]"
        if winners:
            desc = "De winnaar(s) van deze giveaway is/zijn hierboven getagd!\nGefeliciteerd 🎉" if len(winners) == 1 else "The winners of this giveaway are tagged above!\nGefeliciteerd 🎉"
        else:
            desc = "Geen deelnemers 😢"
        e = discord.Embed(title=title, description=desc, colour=BRAND_GREEN)
        e.add_field(name="Prijs", value=st.prize, inline=True)
        e.add_field(name="Deelnemers", value=str(count), inline=True)
        if winners:
            e.add_field(name="Winners", value=str(len(winners)), inline=True)
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
                    except Exception as e:
                        print('Giveaway finish error:', repr(e))
            except Exception as e:
                print('Giveaway watcher error:', repr(e))
            await asyncio.sleep(20)

    async def _finish_giveaway(self, st: GiveawayState) -> None:
        # Double-check not ended
        row = self.bot.db.get_giveaway(st.giveaway_id)
        if not row or int(row["ended"]) == 1:
            return

        guild = self.bot.get_guild(st.guild_id)
        if not guild:
            self.bot.db.end_giveaway(st.giveaway_id, winner_ids=None)
            return

        channel = guild.get_channel(st.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(st.channel_id)
            except Exception:
                channel = None
        if not isinstance(channel, discord.abc.Messageable):
            self.bot.db.end_giveaway(st.giveaway_id, winner_ids=None)
            return

        entries = self.bot.db.get_giveaway_entries(st.giveaway_id)
        count = len(entries)
        winner_ids: list[int] = []
        winner_members: list[discord.Member] = []

        if entries:
            k = min(max(1, int(getattr(st, 'winners_count', 1) or 1)), len(entries))
            winner_ids = random.sample(entries, k=k)
            for uid in winner_ids:
                try:
                    m = guild.get_member(uid) or await guild.fetch_member(uid)
                    if isinstance(m, discord.Member):
                        winner_members.append(m)
                except Exception:
                    pass
            self.bot.db.end_giveaway(st.giveaway_id, winner_ids=winner_ids)
        else:
            self.bot.db.end_giveaway(st.giveaway_id, winner_ids=None)

        # Disable button on original message
        try:
            msg = await channel.fetch_message(st.message_id)
        except Exception:
            msg = None
        if msg:
            try:
                v = ParticipateView(self, st, ended=True)
                try:
                    v.participate_btn.label = "Deelnemen"
                except Exception:
                    pass
                await msg.edit(embed=self._giveaway_embed(st, count=count), view=v)
            except Exception as e:
                print('Giveaway watcher error:', repr(e))


        # Announce result
        tag_line = " ".join(m.mention for m in winner_members) if winner_members else ""
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
            embed=self._results_embed(st, winners=winner_members, count=count),
            files=files,
        )

        # DM winner + assign role
        # DM winners + assign role
        if winner_members:
            for m in winner_members:
                try:
                    await m.send(embed=self._winner_dm_embed(st))
                except Exception:
                    pass
                try:
                    role = guild.get_role(WINNER_ROLE_ID)
                    if role:
                        await m.add_roles(role, reason="Giveaway winnaar")
                except Exception:
                    pass


    async def _cancel_giveaway(self, st: GiveawayState, *, interaction: discord.Interaction) -> bool:
        row = self.bot.db.get_giveaway(st.giveaway_id)
        if not row or int(row["ended"]) == 1:
            return False

        guild = interaction.guild
        if not guild:
            return False
        channel = guild.get_channel(st.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(st.channel_id)
            except Exception:
                channel = None
        if not isinstance(channel, discord.abc.Messageable):
            return False

        # End without winners
        self.bot.db.end_giveaway(st.giveaway_id, winner_ids=None)

        # Edit original message: disable participate/cancel, keep reroll disabled too
        try:
            msg = await channel.fetch_message(st.message_id)
        except Exception:
            msg = None
        if msg:
            try:
                count = self.bot.db.giveaway_entry_count(st.giveaway_id)
                v = ParticipateView(self, st, ended=True)
                v.reroll_btn.disabled = True
                # Mark embed as cancelled
                emb = self._giveaway_embed(st, count=count)
                emb.title = f"{st.prize} [CANCELLED]"
                emb.description = (emb.description or "") + "\n\n🛑 **Cancelled**"
                await msg.edit(embed=emb, view=v)
            except Exception as e:
                print('Giveaway watcher error:', repr(e))

        # Optional announcement in channel
        try:
            await channel.send(f"🛑 Giveaway **{st.prize}** is gecanceld.")
        except Exception:
            pass
        return True

    async def _reroll_giveaway(self, st: GiveawayState, *, interaction: discord.Interaction) -> bool:
        row = self.bot.db.get_giveaway(st.giveaway_id)
        if not row or int(row["ended"]) != 1:
            return False

        guild = interaction.guild
        if not guild:
            return False
        channel = guild.get_channel(st.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(st.channel_id)
            except Exception:
                channel = None
        if not isinstance(channel, discord.abc.Messageable):
            return False

        entries = self.bot.db.get_giveaway_entries(st.giveaway_id)
        if not entries:
            return False

        # Try to avoid previous winners if possible
        prev = []
        try:
            if _row_get(row, "winner_ids"):
                prev = json.loads(row["winner_ids"]) or []
            elif _row_get(row, "winner_id"):
                prev = [int(row["winner_id"]) ]
        except Exception:
            prev = []
        pool = [uid for uid in entries if uid not in prev] or entries

        winners_count = int(_row_get(row, "winners_count") or 1)
        k = min(max(1, winners_count), len(pool))
        winner_ids = random.sample(pool, k=k)
        winner_members: list[discord.Member] = []
        for uid in winner_ids:
            try:
                m = guild.get_member(uid) or await guild.fetch_member(uid)
                if isinstance(m, discord.Member):
                    winner_members.append(m)
            except Exception as e:
                print('Giveaway watcher error:', repr(e))

        # Store new winners (still ended)
        self.bot.db.end_giveaway(st.giveaway_id, winner_ids=winner_ids)

        # Announce reroll
        tag_line = " ".join(m.mention for m in winner_members) if winner_members else ""
        try:
            emb = self._results_embed(st, winners=winner_members, count=len(entries))
            emb.title = f"{st.prize} [REROLL]"
            await channel.send(content=tag_line, embed=emb)
        except Exception:
            pass

        # DM + role
        role = guild.get_role(WINNER_ROLE_ID)
        for m in winner_members:
            try:
                await m.send(embed=self._winner_dm_embed(st))
            except Exception as e:
                print('Giveaway watcher error:', repr(e))
            try:
                if role:
                    await m.add_roles(role, reason="Giveaway reroll winnaar")
            except Exception as e:
                print('Giveaway watcher error:', repr(e))
        return True



    # ---------------------
    # Dashboard actions
    # ---------------------
    async def dashboard_create(
        self,
        *,
        guild_id: int,
        actor_user_id: int,
        channel_id: int,
        prize: str,
        end_at: int | None = None,
        end_in: str | None = None,
        winners: int = 1,
        description: str | None = None,
        max_participants: int | None = None,
        # Optional thumbnail attachment from dashboard.
        # Provide thumbnail_b64 as a base64-encoded string (no data: prefix), plus thumbnail_name.
        thumbnail_b64: str | None = None,
        thumbnail_name: str | None = None,
    ) -> int:
        """Create a giveaway from the web dashboard.

        Provide either end_at (unix seconds) or end_in (same format as /giveaway maak, e.g. 30m/2h/19:00).
        """
        if end_at is None:
            if not end_in:
                raise ValueError('end_at or end_in required')
            end_at = _parse_endtime(end_in)

        guild = self.bot.get_guild(guild_id) or await self.bot.fetch_guild(guild_id)
        channel = guild.get_channel(channel_id)
        if channel is None:
            ch = await self.bot.fetch_channel(channel_id)
            channel = ch
        if not isinstance(channel, discord.TextChannel):
            raise ValueError('channel must be a text channel')

        tn = (thumbnail_name or None)
        if tn:
            # Ensure a safe filename for Discord attachment.
            tn = re.sub(r"[^a-zA-Z0-9._-]", "_", tn)[:64]
            if not tn.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                tn = tn + ".png"

        tmp_state = GiveawayState(
            giveaway_id=0,
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=0,
            prize=prize,
            description=description,
            max_participants=max_participants,
            end_at=int(end_at),
            created_by=int(actor_user_id),
            thumbnail_name=tn,
            winners_count=int(winners or 1),
        )

        view = ParticipateView(self, tmp_state, ended=False)
        # send message first (optionally with thumbnail attachment)
        files = None
        if thumbnail_b64 and tn:
            try:
                import base64
                from io import BytesIO
                raw = base64.b64decode(thumbnail_b64)
                files = [discord.File(BytesIO(raw), filename=tn)]
            except Exception:
                files = None
                tmp_state.thumbnail_name = None

        msg = await channel.send(embed=self._giveaway_embed(tmp_state, count=0), view=view, files=files)

        giveaway_id = self.bot.db.create_giveaway(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=msg.id,
            prize=prize,
            description=description,
            max_participants=max_participants,
            end_at=int(end_at),
            created_by=int(actor_user_id),
            thumbnail_name=tmp_state.thumbnail_name,
            winners_count=int(winners or 1),
        )

        # update state + message with correct state
        tmp_state.giveaway_id = giveaway_id
        tmp_state.message_id = msg.id
        view.state = tmp_state
        await msg.edit(embed=self._giveaway_embed(tmp_state, count=0), view=view)
        return giveaway_id

    async def dashboard_cancel(self, guild_id: int, giveaway_id: int, actor_user_id: int) -> bool:
        row = self.bot.db.get_giveaway(giveaway_id)
        if not row or int(row["ended"]) == 1:
            return False
        st = GiveawayState.from_row(row)
        guild = self.bot.get_guild(guild_id) or await self.bot.fetch_guild(guild_id)
        channel = guild.get_channel(st.channel_id) or await self.bot.fetch_channel(st.channel_id)
        if not isinstance(channel, discord.abc.Messageable):
            return False
        # mark ended
        self.bot.db.end_giveaway(giveaway_id, winner_ids=None)
        # edit original message
        try:
            msg = await channel.fetch_message(st.message_id)
        except Exception:
            msg = None
        if msg:
            try:
                count = self.bot.db.giveaway_entry_count(giveaway_id)
                v = ParticipateView(self, st, ended=True)
                v.reroll_btn.disabled = True
                emb = self._giveaway_embed(st, count=count)
                emb.title = f"{st.prize} [CANCELLED]"
                emb.description = (emb.description or "") + "\n\n🛑 **Cancelled**"
                await msg.edit(embed=emb, view=v)
            except Exception as e:
                print('Dashboard cancel error:', repr(e))
        try:
            await channel.send(f"🛑 Giveaway **{st.prize}** is gecanceld.")
        except Exception:
            pass
        return True

    async def dashboard_reroll(self, guild_id: int, giveaway_id: int, actor_user_id: int) -> bool:
        row = self.bot.db.get_giveaway(giveaway_id)
        if not row or int(row["ended"]) != 1:
            return False
        st = GiveawayState.from_row(row)
        guild = self.bot.get_guild(guild_id) or await self.bot.fetch_guild(guild_id)
        channel = guild.get_channel(st.channel_id) or await self.bot.fetch_channel(st.channel_id)
        if not isinstance(channel, discord.abc.Messageable):
            return False

        entries = self.bot.db.get_giveaway_entries(giveaway_id)
        if not entries:
            return False

        prev = []
        try:
            wids = _row_get(row, "winner_ids")
            wid = _row_get(row, "winner_id")
            if wids:
                prev = json.loads(wids) or []
            elif wid:
                prev = [int(wid)]
        except Exception:
            prev = []
        pool = [uid for uid in entries if uid not in prev] or entries

        winners_count = int(_row_get(row, "winners_count", 1) or 1)
        k = min(max(1, winners_count), len(pool))
        winner_ids = random.sample(pool, k=k)
        winner_members: list[discord.Member] = []
        for uid in winner_ids:
            try:
                m = guild.get_member(uid) or await guild.fetch_member(uid)
                if isinstance(m, discord.Member):
                    winner_members.append(m)
            except Exception:
                pass

        self.bot.db.end_giveaway(giveaway_id, winner_ids=winner_ids)
        tag_line = " ".join(m.mention for m in winner_members) if winner_members else ""
        try:
            emb = self._results_embed(st, winners=winner_members, count=len(entries))
            emb.title = f"{st.prize} [REROLL]"
            await channel.send(content=tag_line, embed=emb)
        except Exception:
            pass
        role = guild.get_role(WINNER_ROLE_ID)
        for m in winner_members:
            try:
                await m.send(embed=self._winner_dm_embed(st))
            except Exception:
                pass
            try:
                if role:
                    await m.add_roles(role, reason="Giveaway reroll winnaar")
            except Exception:
                pass
        return True
    giveaway = app_commands.Group(name="giveaway", description="Giveaway commands (admins only)")

    @giveaway.command(name="maak", description="Maak een giveaway (admins only)")
    @app_commands.describe(
        prijs="Bijv: 1000 V-Bucks",
        eindtijd="Bijv: 30m, 2h, 1d, 19:00 of 2026-01-12 19:00",
        kanaal="Kanaal waar de giveaway gepost wordt",
        winners="Aantal winnaars (default: 1)",
        deelnemers="Max deelnemers (optioneel)",
        beschrijving="Extra tekst/omschrijving",
        afbeelding="Optionele afbeelding (thumbnail)"
    )
    async def giveaway_maak(
        self,
        interaction: discord.Interaction,
        prijs: str,
        eindtijd: str,
        kanaal: discord.TextChannel,
        winners: int = 1,
        deelnemers: Optional[int] = None,
        beschrijving: Optional[str] = None,
        afbeelding: Optional[discord.Attachment] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Dit kan alleen in een server.", ephemeral=True)
        if not _can_create_giveaway(interaction.user):
            return await interaction.response.send_message("Alleen admins of B-Crew kunnen een giveaway aanmaken.", ephemeral=True)

        try:
            end_at = _parse_endtime(eindtijd)
        except Exception as e:
            return await interaction.response.send_message(str(e), ephemeral=True)


        if winners is None or winners < 1:
            winners = 1
        if deelnemers is not None and deelnemers <= 0:
            deelnemers = None

        await interaction.response.defer(ephemeral=True)
        responded = False

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
            winners_count=int(winners),
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
            winners_count=int(winners),
            winner_ids_json=None,
        )
        self.bot.add_view(ParticipateView(self, st, ended=False))
        try:
            await interaction.followup.send(f"✅ Giveaway geplaatst in {channel.mention}.", ephemeral=True)
        except Exception:
            pass

        # Update message with the real state (button label)
        try:
            v2 = ParticipateView(self, st, ended=False)
            v2.participate_btn.label = "Deelnemen"
            await msg.edit(embed=self._giveaway_embed(st, count=0), view=v2)
        except Exception:
            pass

        await interaction.followup.send(f"✅ Giveaway aangemaakt in {kanaal.mention}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Giveaway(bot))