import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

BRAND_GREEN = discord.Colour.from_rgb(46, 204, 113)

# Simple mapping for Open-Meteo weather codes -> emoji + label
WEATHER_CODE = {
    0: ("☀️", "Clear sky"),
    1: ("🌤️", "Mainly clear"),
    2: ("⛅", "Partly cloudy"),
    3: ("☁️", "Overcast"),
    45: ("🌫️", "Fog"),
    48: ("🌫️", "Depositing rime fog"),
    51: ("🌦️", "Light drizzle"),
    53: ("🌦️", "Moderate drizzle"),
    55: ("🌧️", "Dense drizzle"),
    61: ("🌧️", "Slight rain"),
    63: ("🌧️", "Moderate rain"),
    65: ("🌧️", "Heavy rain"),
    71: ("🌨️", "Slight snow"),
    73: ("🌨️", "Moderate snow"),
    75: ("❄️", "Heavy snow"),
    80: ("🌦️", "Rain showers"),
    81: ("🌦️", "Moderate rain showers"),
    82: ("⛈️", "Violent rain showers"),
    95: ("⛈️", "Thunderstorm"),
    96: ("⛈️", "Thunderstorm with hail"),
    99: ("⛈️", "Thunderstorm with hail"),
}

def code_to_icon(code: int):
    return WEATHER_CODE.get(code, ("🌡️", "Weer"))

class Weer(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _embed(self, title: str, desc: str = ""):
        return discord.Embed(title=title, description=desc, colour=BRAND_GREEN)

    @app_commands.command(name="weer", description="Toon het actuele weer en de 7-daagse verwachting.")
    @app_commands.describe(location="Stad / plaats", unit="c of f")
    async def weather(self, interaction: discord.Interaction, location: str, unit: str = "c"):
        unit = (unit or "c").strip().lower()
        if unit not in ("c", "f"):
            return await interaction.response.send_message("Unit must be `c` or `f`.", ephemeral=True)

        await interaction.response.defer()

        temp_unit = "fahrenheit" if unit == "f" else "celsius"
        wind_unit = "mph" if unit == "f" else "kmh"

        async with aiohttp.ClientSession() as session:
            # Geocode
            geo_url = "https://geocoding-api.open-meteo.com/v1/search"
            async with session.get(geo_url, params={"name": location, "count": 1, "language": "en", "format": "json"}) as r:
                if r.status != 200:
                    return await interaction.followup.send("❌ Geocoding failed.")
                geo = await r.json()
            if not geo.get("results"):
                return await interaction.followup.send("❌ Location not found.")

            g = geo["results"][0]
            lat, lon = g["latitude"], g["longitude"]
            name = g.get("name", location)
            country = g.get("country", "")

            # Forecast (current + daily 7 days)
            fc_url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m",
                "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
                "forecast_days": 7,
                "timezone": "auto",
                "temperature_unit": temp_unit,
                "windspeed_unit": wind_unit,
            }
            async with session.get(fc_url, params=params) as r:
                if r.status != 200:
                    return await interaction.followup.send("❌ Forecast fetch failed.")
                fc = await r.json()

        cur = fc.get("current", {})
        daily = fc.get("daily", {})

        code = int(cur.get("weather_code", -1))
        icon, label = code_to_icon(code)

        t = cur.get("temperature_2m")
        feels = cur.get("apparent_temperature")
        hum = cur.get("relative_humidity_2m")
        wind = cur.get("wind_speed_10m")

        unit_sym = "°F" if unit == "f" else "°C"
        wind_sym = "mph" if unit == "f" else "km/h"

        title = f"{icon} Weer — {name}{', ' + country if country else ''}"
        desc = f"**Now:** {label}\n**Temp:** {t}{unit_sym} (feels {feels}{unit_sym})\n**Luchtvochtigheid:** {hum}%\n**Wind:** {wind} {wind_sym}"

        e = self._embed(title, desc)

        # 7-day forecast
        times = daily.get("time", [])
        wcodes = daily.get("weather_code", [])
        tmax = daily.get("temperature_2m_max", [])
        tmin = daily.get("temperature_2m_min", [])
        pop = daily.get("precipitation_probability_max", [])

        lines = []
        for i in range(min(7, len(times))):
            ic, _lab = code_to_icon(int(wcodes[i]) if i < len(wcodes) else -1)
            mx = tmax[i] if i < len(tmax) else "-"
            mn = tmin[i] if i < len(tmin) else "-"
            pp = pop[i] if i < len(pop) else "-"
            lines.append(f"`{times[i]}` {ic} **{mn}{unit_sym}**–**{mx}{unit_sym}** • ☔ {pp}%")

        if lines:
            e.add_field(name="7‑day forecast", value="\n".join(lines)[:1024], inline=False)

        await interaction.followup.send(embed=e)

async def setup(bot: commands.Bot):
    await bot.add_cog(Weer(bot))

Weather = Weer

