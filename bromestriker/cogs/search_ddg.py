import asyncio
import time
import re
from urllib.parse import urlparse

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from ddgs import DDGS

BRAND_GREEN = discord.Colour.from_rgb(46, 204, 113)

ADMIN_ROLE_ID = 1450553389971800185  # your Discord Admin role id

WIKI_SUMMARY = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"

def is_admin(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    return any(r.id == ADMIN_ROLE_ID for r in member.roles)

class SearchView(discord.ui.View):
    def __init__(self, *, owner_id: int, query: str, results: list[dict], per_page: int = 5):
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.query = query
        self.results = results
        self.per_page = per_page
        self.page = 0

        self.prev_btn.disabled = True
        self.next_btn.disabled = len(results) <= per_page

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("⛔ This menu isn't for you.", ephemeral=True)
            return False
        return True

    def _page_slice(self):
        start = self.page * self.per_page
        end = start + self.per_page
        return self.results[start:end], start, end

    def make_embed(self) -> discord.Embed:
        items, start, end = self._page_slice()
        total = len(self.results)

        e = discord.Embed(
            title=f"🔎 Search — {self.query}",
            description=f"Showing **{start+1}-{min(end,total)}** of **{total}** results",
            colour=BRAND_GREEN,
        )
        for i, it in enumerate(items, start=start+1):
            title = it.get("title") or "Result"
            body = (it.get("body") or "").strip()
            href = it.get("href") or ""
            if body:
                body = re.sub(r"\s+", " ", body)
                body = (body[:240] + "…") if len(body) > 240 else body
            e.add_field(
                name=f"{i}. {title}",
                value=f"{body}\n{href}",
                inline=False,
            )
        e.set_footer(text="Source: DuckDuckGo (via ddgs).")
        return e

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page -= 1
        self.prev_btn.disabled = self.page <= 0
        self.next_btn.disabled = (self.page+1) * self.per_page >= len(self.results)
        await interaction.response.edit_message(embed=self.make_embed(), view=self)

    @discord.ui.button(label="Volgende ▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page += 1
        self.prev_btn.disabled = self.page <= 0
        self.next_btn.disabled = (self.page+1) * self.per_page >= len(self.results)
        await interaction.response.edit_message(embed=self.make_embed(), view=self)

class SearchDDG(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.last_search: dict[int, float] = {}

    async def _ddg_text(self, query: str, max_results: int = 25) -> list[dict]:
        # ddgs is sync; run in a thread to avoid blocking the event loop
        def _run():
            with DDGS() as ddgs:
                return list(ddgs.text(query, max_results=max_results))
        return await asyncio.to_thread(_run)

    async def _wikipedia_summary(self, url: str) -> tuple[str | None, str | None]:
        try:
            parsed = urlparse(url)
            if "wikipedia.org" not in parsed.netloc:
                return None, None
            # /wiki/Title
            m = re.search(r"/wiki/([^#?]+)", parsed.path)
            if not m:
                return None, None
            title = m.group(1)
            async with aiohttp.ClientSession() as session:
                async with session.get(WIKI_SUMMARY.format(title=title), timeout=10, headers={"accept": "application/json"}) as r:
                    if r.status != 200:
                        return None, None
                    js = await r.json()
            extract = (js.get("extract") or "").strip()
            if extract:
                extract = extract[:500] + ("…" if len(extract) > 500 else "")
            return js.get("title"), extract or None
        except Exception:
            return None, None

    @app_commands.command(name="zoek", description="Zoek op het web (DuckDuckGo). 30s cooldown (admins bypass).")
    @app_commands.describe(query="What you want to search for")
    async def search(self, interaction: discord.Interaction, query: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Server only.", ephemeral=True)

        now = time.time()
        if not is_admin(interaction.user):
            last = self.last_search.get(interaction.user.id, 0)
            if now - last < 30:
                wait = int(30 - (now - last))
                return await interaction.response.send_message(f"⏳ Slow down — try again in **{wait}s**.", ephemeral=True)

        await interaction.response.defer()
        self.last_search[interaction.user.id] = now

        try:
            results = await self._ddg_text(query, max_results=25)
        except Exception as e:
            return await interaction.followup.send(f"Search failed: {e}")

        if not results:
            return await interaction.followup.send("No results found.")

        # If top result is Wikipedia, enrich with summary + link button
        top = results[0]
        top_url = top.get("href") or ""
        wiki_title, wiki_extract = await self._wikipedia_summary(top_url)

        view = SearchView(owner_id=interaction.user.id, query=query, results=results, per_page=5)
        embed = view.make_embed()

        if wiki_title and wiki_extract:
            embed.description = f"**Wikipedia:** {wiki_title}\n{wiki_extract}\n\n" + (embed.description or "")
            # add a link button to top wiki page
            view.add_item(discord.ui.Button(label="Open Wikipedia", url=top_url, style=discord.ButtonStyle.link))

        await interaction.followup.send(embed=embed, view=view)

async def setup(bot: commands.Bot):
    await bot.add_cog(SearchDDG(bot))
