import os
import json
import time
import asyncio
import datetime as _dt
from typing import Optional, List, Set

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from .db import DB
from .cogs.music import Music
from .cogs.weather import Weather
from .cogs.search_ddg import SearchDDG

STRIKE1_DURATION = 24 * 60 * 60
STRIKE2_DURATION = 7 * 24 * 60 * 60

# --- Hard config (server-specific role IDs) ---
# Strikes
STRIKE1_ROLE_ID = 1459637702055039088
STRIKE2_ROLE_ID = 1459637763652718663
STRIKE3_ROLE_ID = 1459637807281602703
# Muted role
MUTED_ROLE_ID = 1042562173643268230

# Roles we never want to strip during a mute.
# NOTE: MUTED_ROLE_ID is *not* protected during /unmute because we explicitly remove it.
PROTECTED_ROLE_IDS: Set[int] = {
    STRIKE1_ROLE_ID,
    STRIKE2_ROLE_ID,
    STRIKE3_ROLE_ID,
    MUTED_ROLE_ID,
}

def _parse_csv_ids(val: str) -> Set[int]:
    out: Set[int] = set()
    for part in (val or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            pass
    return out

def _parse_csv_names(val: str) -> Set[str]:
    return set([p.strip() for p in (val or "").split(",") if p.strip()])

def _human_duration(seconds: int) -> str:
    if seconds >= 7*24*60*60:
        days = seconds // (24*60*60)
        return f"{days} dagen"
    hours = max(1, seconds // 3600)
    return f"{hours} uur"

class BromeStriker(commands.Bot):
    def __init__(self, *, guild_id: int, db_path: str, modlog_channel_id: Optional[int] = None):
        intents = discord.Intents.default()
        intents.members = True  # needed for role ops
        intents.voice_states = True  # needed for muziek (voice)
        super().__init__(command_prefix="!", intents=intents)

        self.guild_id = guild_id
        self.modlog_channel_id = modlog_channel_id

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db = DB(db_path)

        # config
        self.preserve_role_ids = _parse_csv_ids(os.getenv("PRESERVE_ROLE_IDS", ""))
        self.preserve_role_names = _parse_csv_names(os.getenv("PRESERVE_ROLE_NAMES", "Member"))
        self.hidden_category_ids = _parse_csv_ids(os.getenv("MUTED_HIDDEN_CATEGORY_IDS", ""))

        self.twitch_webhook = (os.getenv("TWITCH_BAN_WEBHOOK_URL", "") or "").strip()

        # role names
        self.role_strike_1 = "Strike 1"
        self.role_strike_2 = "Strike 2"
        self.role_strike_3 = "Strike 3"
        self.role_muted = "Gedempt"

        # background task holder
        self.mute_watcher_task: Optional[asyncio.Task] = None

    async def setup_hook(self) -> None:
        """
        setup_hook runs before on_ready.
        IMPORTANT: load cogs + add commands BEFORE syncing, otherwise slash commands won't appear.
        """
        # 1) Load cogs (async!)
        await self.add_cog(Music(self))
        await self.add_cog(Weather(self))
        await self.add_cog(SearchDDG(self))

        # 2) Register app commands on the tree
        # (Doing this here guarantees they exist before sync.)
        self.tree.add_command(mute_cmd)
        self.tree.add_command(unmute_cmd)
        self.tree.add_command(strikes_cmd)
        self.tree.add_command(reset_strikes_cmd)
        self.tree.add_command(warn_cmd)
        self.tree.add_command(warns_cmd)
        self.tree.add_command(removewarn_cmd)
        self.tree.add_command(resetwarns_cmd)
        self.tree.add_command(purge_cmd)
        self.tree.add_command(kick_cmd)
        self.tree.add_command(ban_cmd)

        # 3) Sync commands (guild sync = instant)
        guild = discord.Object(id=self.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        print(f"✅ Slash commands synced to guild={self.guild_id}")

        # 4) Start background task
        self.mute_watcher_task = asyncio.create_task(self._mute_watcher_loop())

    async def on_ready(self) -> None:
        print(f"Logged in as {self.user} (guild={self.guild_id})")

    async def _ensure_roles(self, guild: discord.Guild) -> None:
        # Ensure roles exist; don't change permissions except for muted role default denies send messages.
        def find_role(name: str) -> Optional[discord.Role]:
            return discord.utils.get(guild.roles, name=name)

        def find_role_by_id_or_name(role_id: int, name: str) -> Optional[discord.Role]:
            return guild.get_role(role_id) or find_role(name)

        # Strike roles (prefer fixed IDs, but fall back to name)
        if not find_role_by_id_or_name(STRIKE1_ROLE_ID, self.role_strike_1):
            await guild.create_role(name=self.role_strike_1, reason="BromeStriker: strike rol aanmaken")
        if not find_role_by_id_or_name(STRIKE2_ROLE_ID, self.role_strike_2):
            await guild.create_role(name=self.role_strike_2, reason="BromeStriker: strike rol aanmaken")
        if not find_role_by_id_or_name(STRIKE3_ROLE_ID, self.role_strike_3):
            await guild.create_role(name=self.role_strike_3, reason="BromeStriker: strike rol aanmaken")

        muted = find_role_by_id_or_name(MUTED_ROLE_ID, self.role_muted)
        if not muted:
            muted = await guild.create_role(name=self.role_muted, reason="BromeStriker: gedempt rol aanmaken")

        # Apply overwrite to block sending in channels where we can
        for ch in guild.channels:
            if isinstance(ch, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
                try:
                    ow = ch.overwrites_for(muted)
                    changed = False
                    if getattr(ow, "send_messages", None) is not False:
                        ow.send_messages = False
                        changed = True
                    if getattr(ow, "add_reactions", None) is not False:
                        ow.add_reactions = False
                        changed = True
                    if changed:
                        await ch.set_permissions(muted, overwrite=ow, reason="BromeStriker: gedempt mag niet praten")
                except Exception:
                    continue

        # Hide specific categories for muted role
        for cat_id in self.hidden_category_ids:
            cat = guild.get_channel(cat_id)
            if isinstance(cat, discord.CategoryChannel):
                try:
                    ow = cat.overwrites_for(muted)
                    ow.view_channel = False
                    await cat.set_permissions(muted, overwrite=ow, reason="BromeStriker: verberg categorie voor gedempt")
                except Exception:
                    continue

    def _is_preserved_role(self, role: discord.Role, guild: discord.Guild) -> bool:
        if role.is_default():
            return True
        if role.id in self.preserve_role_ids:
            return True
        if role.name in self.preserve_role_names:
            return True
        if role.id in PROTECTED_ROLE_IDS:
            return True
        if role.name in {self.role_strike_1, self.role_strike_2, self.role_strike_3, self.role_muted}:
            return True
        return False

    async def _send_modlog(self, guild: discord.Guild, embed: discord.Embed) -> None:
        if not self.modlog_channel_id:
            return
        ch = guild.get_channel(self.modlog_channel_id)
        if isinstance(ch, discord.abc.Messageable):
            try:
                await ch.send(embed=embed)
            except Exception:
                pass

    async def _dm_strike_embed(self, member: discord.Member, strike: int, duur_label: str, reden: str, moderator: discord.abc.User, guild: discord.Guild) -> None:
        title = f"⚠️ Waarschuwing: Strike {strike}"
        if strike == 3:
            title = "⛔ Strike 3 — Je bent eruit"
        embed = discord.Embed(
            title=title,
            description=f"Server: **{guild.name}**",
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Straf", value=duur_label, inline=False)
        embed.add_field(name="Reden", value=reden or "(geen reden opgegeven)", inline=False)
        embed.add_field(name="Moderator", value=str(moderator), inline=False)
        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)
        try:
            await member.send(embed=embed)
        except Exception:
            pass

    async def _dm_mod_embed(
        self,
        member: discord.Member,
        action_title: str,
        action_value: str,
        reden: str,
        moderator: discord.abc.User,
        guild: discord.Guild,
    ) -> None:
        embed = discord.Embed(
            title=action_title,
            description=f"Server: **{guild.name}**",
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Actie", value=action_value, inline=False)
        embed.add_field(name="Reden", value=reden or "(geen reden opgegeven)", inline=False)
        embed.add_field(name="Moderator", value=str(moderator), inline=False)
        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)
        try:
            await member.send(embed=embed)
        except Exception:
            pass

    async def _call_twitch_webhook(self, guild: discord.Guild, user: discord.Member, reason: str) -> None:
        if not self.twitch_webhook:
            return
        try:
            import aiohttp
            async with aiohttp.ClientSession() as sess:
                await sess.post(self.twitch_webhook, json={
                    "guild_id": guild.id,
                    "discord_user_id": user.id,
                    "discord_tag": str(user),
                    "reason": reason,
                    "event": "strike3_ban",
                }, timeout=10)
        except Exception:
            pass

    async def _mute_watcher_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                now = int(time.time())
                for row in self.db.due_mutes(now):
                    guild_id = int(row["guild_id"])
                    user_id = int(row["user_id"])
                    roles_json = row["roles_json"]
                    guild = self.get_guild(guild_id)
                    if not guild:
                        self.db.clear_mute(guild_id, user_id)
                        continue
                    member = guild.get_member(user_id)
                    if not member:
                        self.db.clear_mute(guild_id, user_id)
                        continue
                    await self._restore_roles_after_mute(guild, member, roles_json)
                    self.db.clear_mute(guild_id, user_id)
            except Exception as e:
                print("Mute watcher error:", e)
            await asyncio.sleep(10)

    async def _restore_roles_after_mute(self, guild: discord.Guild, member: discord.Member, roles_json: str) -> None:
        roles_ids = []
        try:
            roles_ids = json.loads(roles_json) or []
        except Exception:
            roles_ids = []
        keep = set()
        for r in member.roles:
            if r.name in {self.role_muted, self.role_strike_1, self.role_strike_2, self.role_strike_3}:
                keep.add(r.id)

        target_roles = []
        for rid in roles_ids:
            role = guild.get_role(int(rid))
            if role:
                target_roles.append(role)

        # also keep strike roles currently on user
        for r in member.roles:
            if r.id in keep:
                role = guild.get_role(r.id)
                if role and role not in target_roles:
                    target_roles.append(role)

        # remove muted role
        muted = discord.utils.get(guild.roles, name=self.role_muted)
        try:
            if muted and muted in target_roles:
                target_roles.remove(muted)
        except Exception:
            pass

        try:
            await member.edit(roles=target_roles, reason="BromeStriker: mute verlopen, rollen hersteld")
        except Exception:
            pass

bot: Optional[BromeStriker] = None

@app_commands.command(name="mute", description="Geef een strike en voer de standaard straf uit (24u / 7 dagen / ban)")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)")
async def mute_cmd(interaction: discord.Interaction, user: discord.Member, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)

    if not interaction.guild or not isinstance(interaction.user, (discord.Member, discord.User)):
        return await interaction.followup.send("Dit commando werkt alleen in een server.", ephemeral=True)

    guild = interaction.guild
    await bot._ensure_roles(guild)

    inter_id = str(interaction.id)
    if bot.db.seen_interaction(inter_id):
        return await interaction.followup.send("⚠️ Dit commando is al verwerkt.", ephemeral=True)
    bot.db.mark_interaction(inter_id)
    bot.db.prune_interactions()

    me = guild.me
    if me is None:
        return await interaction.followup.send("Ik kan mezelf niet vinden in deze server.", ephemeral=True)

    if user.top_role >= me.top_role and user != guild.owner:
        return await interaction.followup.send("❌ Ik kan deze gebruiker niet modereren (rol staat hoger of gelijk aan mij).", ephemeral=True)

    strike = bot.db.increment_strikes(guild.id, user.id)

    muted_role = guild.get_role(MUTED_ROLE_ID) or discord.utils.get(guild.roles, name=bot.role_muted)
    s1 = guild.get_role(STRIKE1_ROLE_ID) or discord.utils.get(guild.roles, name=bot.role_strike_1)
    s2 = guild.get_role(STRIKE2_ROLE_ID) or discord.utils.get(guild.roles, name=bot.role_strike_2)
    s3 = guild.get_role(STRIKE3_ROLE_ID) or discord.utils.get(guild.roles, name=bot.role_strike_3)

    if not muted_role or not s1 or not s2 or not s3:
        return await interaction.followup.send("❌ Rollen ontbreken (Strike 1/2/3 of Gedempt).", ephemeral=True)

    active_strike_role: Optional[discord.Role] = None
    try:
        if strike == 1:
            active_strike_role = s1
            await user.add_roles(s1, reason="BromeStriker: strike 1")
        elif strike == 2:
            active_strike_role = s2
            await user.add_roles(s2, reason="BromeStriker: strike 2")
        else:
            active_strike_role = s3
            await user.add_roles(s3, reason="BromeStriker: strike 3")

        if strike >= 2 and s1 and s1 in user.roles:
            await user.remove_roles(s1, reason="BromeStriker: upgrade strike")
        if strike >= 3 and s2 and s2 in user.roles:
            await user.remove_roles(s2, reason="BromeStriker: upgrade strike")
    except Exception:
        pass

    if strike >= 3:
        await bot._dm_strike_embed(user, 3, "Permanente ban", reden or "", interaction.user, guild)
        await bot._call_twitch_webhook(guild, user, reden or "")
        try:
            await guild.ban(user, reason=f"Strike 3 - {reden or 'geen reden'}", delete_message_seconds=0)
        except Exception as e:
            return await interaction.followup.send(f"❌ Ban mislukt: {e}", ephemeral=True)

        bot.db.delete_strikes(guild.id, user.id)
        bot.db.clear_mute(guild.id, user.id)

        emb = discord.Embed(title="⛔ Strike 3 — Ban", description=f"**{user}** is verbannen.", timestamp=discord.utils.utcnow())
        emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
        emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
        await bot._send_modlog(guild, emb)
        return await interaction.followup.send(f"⛔ **{user}** heeft **Strike 3** gekregen en is **permanent verbannen**.", ephemeral=True)

    duration = STRIKE1_DURATION if strike == 1 else STRIKE2_DURATION
    label = "24 uur" if strike == 1 else "7 dagen"
    unmute_at = int(time.time()) + duration

    current_roles = [r.id for r in user.roles if not bot._is_preserved_role(r, guild)]
    roles_json = json.dumps(current_roles)
    bot.db.upsert_mute(guild.id, user.id, roles_json, unmute_at)

    target_roles = [r for r in user.roles if bot._is_preserved_role(r, guild)]

    if active_strike_role and active_strike_role not in target_roles:
        target_roles.append(active_strike_role)
    if muted_role not in target_roles:
        target_roles.append(muted_role)

    try:
        await user.edit(roles=target_roles, reason=f"BromeStriker: strike {strike} mute")
    except Exception as e:
        return await interaction.followup.send(f"❌ Rollen aanpassen mislukt: {e}", ephemeral=True)

    await bot._dm_strike_embed(user, strike, f"Mute: {label}", reden or "", interaction.user, guild)

    emb = discord.Embed(title=f"🔇 Strike {strike} — Mute", description=f"**{user}** is gedempt.", timestamp=discord.utils.utcnow())
    emb.add_field(name="Duur", value=label, inline=True)
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"🔇 **{user}** kreeg **Strike {strike}** → dempen voor **{label}**.", ephemeral=True)

@app_commands.command(name="unmute", description="Haal demping weg en herstel rollen")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)")
async def unmute_cmd(interaction: discord.Interaction, user: discord.Member, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild
    await bot._ensure_roles(guild)

    cur = bot.db.conn.cursor()
    cur.execute("SELECT roles_json FROM mutes WHERE guild_id=? AND user_id=?", (guild.id, user.id))
    row = cur.fetchone()
    roles_json = row[0] if row else "[]"
    await bot._restore_roles_after_mute(guild, user, roles_json)
    bot.db.clear_mute(guild.id, user.id)
    return await interaction.followup.send(f"✅ **{user}** is ontdempt en rollen zijn hersteld.", ephemeral=True)

@app_commands.command(name="strikes", description="Bekijk het aantal strikes van een gebruiker")
@app_commands.describe(user="Gebruiker")
async def strikes_cmd(interaction: discord.Interaction, user: discord.Member):
    assert bot is not None
    if not interaction.guild:
        return await interaction.response.send_message("Alleen in een server.", ephemeral=True)
    s = bot.db.get_strikes(interaction.guild.id, user.id)
    return await interaction.response.send_message(f"📌 **{user}** heeft **{s}** strike(s).", ephemeral=False)

@app_commands.command(name="resetstrikes", description="Reset strikes van een gebruiker")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)")
async def reset_strikes_cmd(interaction: discord.Interaction, user: discord.Member, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild
    bot.db.delete_strikes(guild.id, user.id)

    s1 = discord.utils.get(guild.roles, name=bot.role_strike_1)
    s2 = discord.utils.get(guild.roles, name=bot.role_strike_2)
    s3 = discord.utils.get(guild.roles, name=bot.role_strike_3)
    try:
        to_remove = [r for r in [s1, s2, s3] if r and r in user.roles]
        if to_remove:
            await user.remove_roles(*to_remove, reason=f"BromeStriker: reset strikes - {reden or ''}")
    except Exception:
        pass
    return await interaction.followup.send(f"♻️ Strikes van **{user}** zijn gereset naar 0.", ephemeral=True)

# -------------------------
# Extra standaard moderation commands
# -------------------------

@app_commands.command(name="warn", description="Geef een waarschuwing (met DM) en log in het modlog kanaal")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)")
@app_commands.checks.has_permissions(moderate_members=True)
async def warn_cmd(interaction: discord.Interaction, user: discord.Member, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild
    warn_count = bot.db.increment_warns(guild.id, user.id)

    await bot._dm_mod_embed(user, "⚠️ Waarschuwing", f"Warning (totaal: {warn_count})", reden or "", interaction.user, guild)

    emb = discord.Embed(title="⚠️ Warning", description=f"**{user}** is gewaarschuwd. (totaal: {warn_count})", timestamp=discord.utils.utcnow())
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"⚠️ **{user}** is gewaarschuwd. (totaal warns: {warn_count})", ephemeral=True)

@app_commands.command(name="warns", description="Bekijk het aantal waarschuwingen van een gebruiker")
@app_commands.describe(user="Gebruiker")
@app_commands.checks.has_permissions(moderate_members=True)
async def warns_cmd(interaction: discord.Interaction, user: discord.Member):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild
    w = bot.db.get_warns(guild.id, user.id)

    emb = discord.Embed(title="📋 Waarschuwingen", description=f"**{user}** heeft **{w}** warn(s).", timestamp=discord.utils.utcnow())
    emb.add_field(name="Gebruiker", value=str(user), inline=False)
    await bot._send_modlog(guild, discord.Embed(title="📋 Warns bekeken", description=f"{interaction.user} bekeek warns van **{user}** (totaal: {w}).", timestamp=discord.utils.utcnow()))

    return await interaction.followup.send(embed=emb, ephemeral=True)

@app_commands.command(name="removewarn", description="Verwijder 1 (of meer) waarschuwing(en) van een gebruiker")
@app_commands.describe(user="Gebruiker", aantal="Hoeveel warns verwijderen (default 1)", reden="Reden (optioneel)")
@app_commands.checks.has_permissions(moderate_members=True)
async def removewarn_cmd(interaction: discord.Interaction, user: discord.Member, aantal: Optional[int] = 1, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild

    amt = int(aantal or 1)
    amt = max(1, min(50, amt))
    new_count = bot.db.decrement_warns(guild.id, user.id, amt)

    emb = discord.Embed(title="➖ Remove warn", description=f"Warn(s) verwijderd bij **{user}**.", timestamp=discord.utils.utcnow())
    emb.add_field(name="Aantal", value=str(amt), inline=True)
    emb.add_field(name="Nieuwe totaal", value=str(new_count), inline=True)
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"➖ Warn(s) verwijderd. **{user}** heeft nu {new_count} warn(s).", ephemeral=True)

@app_commands.command(name="resetwarns", description="Reset waarschuwingen van een gebruiker naar 0")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)")
@app_commands.checks.has_permissions(moderate_members=True)
async def resetwarns_cmd(interaction: discord.Interaction, user: discord.Member, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild

    bot.db.delete_warns(guild.id, user.id)

    emb = discord.Embed(title="♻️ Reset warns", description=f"Warns van **{user}** zijn gereset naar 0.", timestamp=discord.utils.utcnow())
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"♻️ Warns van **{user}** zijn gereset naar 0.", ephemeral=True)

@app_commands.command(name="purge", description="Verwijder berichten (optioneel alleen van een gebruiker)")
@app_commands.describe(aantal="Aantal berichten (1-200)", user="Alleen berichten van deze gebruiker (optioneel)", reden="Reden (optioneel)")
@app_commands.checks.has_permissions(manage_messages=True)
async def purge_cmd(interaction: discord.Interaction, aantal: int, user: Optional[discord.Member] = None, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        return await interaction.followup.send("❌ Purge kan alleen in een tekstkanaal.", ephemeral=True)

    limit = max(1, min(200, int(aantal)))

    def _check(msg: discord.Message) -> bool:
        if user is None:
            return True
        return msg.author.id == user.id

    try:
        deleted = await channel.purge(limit=limit, check=_check, reason=f"Purge - {reden or 'geen reden'}")
    except Exception as e:
        return await interaction.followup.send(f"❌ Purge mislukt: {e}", ephemeral=True)

    count = len(deleted)
    guild = interaction.guild

    emb = discord.Embed(title="🧹 Purge", description=f"{count} bericht(en) verwijderd in {channel.mention}.", timestamp=discord.utils.utcnow())
    emb.add_field(name="Filter", value=(str(user) if user else "(geen, alles)"), inline=False)
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"🧹 Verwijderd: {count} bericht(en).", ephemeral=True)

@app_commands.command(name="kick", description="Kick een gebruiker (met DM) en log in het modlog kanaal")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)")
@app_commands.checks.has_permissions(kick_members=True)
async def kick_cmd(interaction: discord.Interaction, user: discord.Member, reden: Optional[str] = None):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild

    me = guild.me
    if me is None:
        return await interaction.followup.send("Ik kan mezelf niet vinden in deze server.", ephemeral=True)
    if user.top_role >= me.top_role and user != guild.owner:
        return await interaction.followup.send("❌ Ik kan deze gebruiker niet kicken (rol staat hoger of gelijk aan mij).", ephemeral=True)

    await bot._dm_mod_embed(user, "👢 Kick", "Je bent van de server gekickt", reden or "", interaction.user, guild)

    try:
        await user.kick(reason=f"Kick - {reden or 'geen reden'}")
    except Exception as e:
        return await interaction.followup.send(f"❌ Kick mislukt: {e}", ephemeral=True)

    emb = discord.Embed(title="👢 Kick", description=f"**{user}** is gekickt.", timestamp=discord.utils.utcnow())
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"👢 **{user}** is gekickt.", ephemeral=True)

@app_commands.command(name="ban", description="Ban een gebruiker (eerst DM, dan na 5s ban) en log in modlog")
@app_commands.describe(user="Gebruiker", reden="Reden (optioneel)", verwijder_berichten="Verwijder berichten van de laatste X dagen (0-7)")
@app_commands.checks.has_permissions(ban_members=True)
async def ban_cmd(
    interaction: discord.Interaction,
    user: discord.Member,
    reden: Optional[str] = None,
    verwijder_berichten: Optional[int] = 0,
):
    assert bot is not None
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Alleen in een server.", ephemeral=True)
    guild = interaction.guild

    me = guild.me
    if me is None:
        return await interaction.followup.send("Ik kan mezelf niet vinden in deze server.", ephemeral=True)
    if user.top_role >= me.top_role and user != guild.owner:
        return await interaction.followup.send("❌ Ik kan deze gebruiker niet bannen (rol staat hoger of gelijk aan mij).", ephemeral=True)

    days = int(verwijder_berichten or 0)
    days = max(0, min(7, days))
    delete_seconds = days * 24 * 60 * 60

    await bot._dm_mod_embed(user, "⛔ Ban", "Je bent van de server verbannen", reden or "", interaction.user, guild)
    await asyncio.sleep(5)

    try:
        await guild.ban(user, reason=f"Ban - {reden or 'geen reden'}", delete_message_seconds=delete_seconds)
    except Exception as e:
        return await interaction.followup.send(f"❌ Ban mislukt: {e}", ephemeral=True)

    emb = discord.Embed(title="⛔ Ban", description=f"**{user}** is verbannen.", timestamp=discord.utils.utcnow())
    emb.add_field(name="Reden", value=reden or "(geen)", inline=False)
    emb.add_field(name="Berichten verwijderd", value=f"{days} dag(en)", inline=True)
    emb.add_field(name="Moderator", value=str(interaction.user), inline=False)
    await bot._send_modlog(guild, emb)

    return await interaction.followup.send(f"⛔ **{user}** is verbannen.", ephemeral=True)

def main() -> None:
    global bot
    load_dotenv()
    token = (os.getenv("DISCORD_TOKEN", "") or "").strip()
    guild_id = int(os.getenv("GUILD_ID", "0") or "0")
    modlog = os.getenv("MODLOG_CHANNEL_ID", "").strip()
    modlog_id = int(modlog) if modlog.isdigit() else None

    if not token or not guild_id:
        raise SystemExit("DISCORD_TOKEN en GUILD_ID zijn verplicht in .env")

    db_path = os.path.join(os.getcwd(), "data", "bromestriker.db")
    bot = BromeStriker(guild_id=guild_id, db_path=db_path, modlog_channel_id=modlog_id)
    bot.run(token)
