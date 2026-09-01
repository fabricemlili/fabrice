import os
import aiohttp
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from python_files.constants import WEATHER_CODE, LANGUAGE_KEYBOARD, HOUR_KEYBOARD, COMMANDS_KEYBOARD, TRADUCTIONS

load_dotenv()
BOT_TOKEN = os.getenv("TOKEN_TELEGRAM")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

language_preferences: dict[int, str] = {}
forecast_state: dict[int, dict] = {}  # step: "awaiting_location" | "awaiting_hour"
scheduled_forecasts: dict[int, dict] = {}  # active scheduled forecast tasks


# ---------------------------------------------------------------------------
# Weather API helpers
# ---------------------------------------------------------------------------

async def get_location(
    city_name: str,
    language: str = "en",
    session: aiohttp.ClientSession | None = None,
):
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": city_name, "count": 1, "language": language, "format": "json"}

    async def _fetch(s: aiohttp.ClientSession):
        async with s.get(url, params=params) as response:
            return (await response.json())["results"][0]

    if session is None:
        async with aiohttp.ClientSession() as s:
            return await _fetch(s)
    return await _fetch(session)


async def get_weather(
    latitude: float,
    longitude: float,
    session: aiohttp.ClientSession | None = None,
):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,weather_code,wind_speed_10m",
        "timezone": "auto",
    }

    async def _fetch(s: aiohttp.ClientSession):
        async with s.get(url, params=params) as response:
            return await response.json()

    if session is None:
        async with aiohttp.ClientSession() as s:
            return await _fetch(s)
    return await _fetch(session)


def get_weather_code_description(weather_code: int, language: str = "en") -> str:
    entry = WEATHER_CODE.get(weather_code)
    if entry is None:
        return "❓ Unknown weather code"
    return entry.get(language, entry["en"])


def handle_forecast_data(data: dict, timezone: str):
    tomorrow = (datetime.now(ZoneInfo(timezone)) + timedelta(days=1)).date().strftime("%Y-%m-%d")

    morning = [f"{tomorrow}T0{i}:00" for i in range(6, 10)]
    afternoon = [f"{tomorrow}T{hour:02d}:00" for hour in range(14, 18)]
    evening = [f"{tomorrow}T{hour:02d}:00" for hour in range(18, 22)]

    def get_weather(time_range, func):
        temp, idx = func(
            (
                (data["hourly"]["temperature_2m"][idx], idx)
                for idx, time in enumerate(data["hourly"]["time"])
                if time in time_range
            ),
            key=lambda x: x[0]
        )
        return {
            "temperature": temp,
            "weather_code": data["hourly"]["weather_code"][idx],
            "wind_speed": data["hourly"]["wind_speed_10m"][idx],
            "time": datetime.strptime(data["hourly"]["time"][idx], "%Y-%m-%dT%H:%M").strftime("%H:%M"),
            "units": data["hourly_units"],
        }
    
    morning_weather = get_weather(morning, func=min)
    afternoon_weather = get_weather(afternoon, func=max)
    evening_weather = get_weather(evening, func=min)

    return {
        "morning": morning_weather,
        "afternoon": afternoon_weather,
        "evening": evening_weather,
    }


async def get_forecast_tomorrow(
    latitude: float,
    longitude: float,
    timezone: str,
    session: aiohttp.ClientSession | None = None,
):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,weather_code,wind_speed_10m",
        "timezone": "auto",
        "forecast_days": 2,
    }

    async def _fetch(s: aiohttp.ClientSession):
        async with s.get(url, params=params) as response:
            return await response.json()

    if session is None:
        async with aiohttp.ClientSession() as s:
            data = await _fetch(s)
    else:
        data = await _fetch(session)

    return handle_forecast_data(data, timezone)


async def run_scheduled_forecast(
    session: aiohttp.ClientSession,
    user_id: int,
    lat: float,
    lon: float,
    timezone: str,
    location_name: str,
    hour: int,
):
    try:
        while True:
            now = datetime.now(ZoneInfo(timezone))
            target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())

            lang = language_preferences.get(user_id, "en")
            try:
                forecast = await get_forecast_tomorrow(lat, lon, timezone, session)
                await send_message(
                    session,
                    user_id,
                    TRADUCTIONS["forecast_message_location"][lang].format(location=location_name),
                )
                for period in ["morning", "afternoon", "evening"]:
                    forecast_period = forecast[period]
                    units = forecast_period["units"]
                    await send_message(
                        session,
                        user_id,
                        TRADUCTIONS[f"forecast_message_{period}"][lang].format(
                            temperature=f"{round(forecast_period['temperature'])} {units['temperature_2m']}",
                            weather=get_weather_code_description(forecast_period["weather_code"], lang),
                            wind_speed=f"{round(forecast_period['wind_speed'])} {units['wind_speed_10m']}",
                            time=forecast_period["time"],
                        ),
                    )
            except Exception as e:
                await send_message(session, user_id, TRADUCTIONS["error_forecast"][lang])
                print(f"Error in run_scheduled_forecast loop: {e}")
    except asyncio.CancelledError:
        pass

# ---------------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------------

async def get_updates(session: aiohttp.ClientSession, offset: int | None = None):
    params: dict = {"timeout": 5}
    if offset is not None:
        params["offset"] = offset
    async with session.get(f"{TELEGRAM_API}/getUpdates", params=params) as response:
        response.raise_for_status()
        return await response.json()


async def send_message(
    session: aiohttp.ClientSession,
    user_id: int,
    text: str,
    keyboard: list | None = None,
    reply_keyboard: list | None = None,
):
    data: dict = {"chat_id": user_id, "text": text}
    if keyboard:
        data["reply_markup"] = {"inline_keyboard": keyboard}
    elif reply_keyboard:
        data["reply_markup"] = {
            "keyboard": reply_keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": False,
        }
    async with session.post(f"{TELEGRAM_API}/sendMessage", json=data) as response:
        response.raise_for_status()
        return await response.json()


async def answer_callback_query(session: aiohttp.ClientSession, callback_query_id: str):
    async with session.post(
        f"{TELEGRAM_API}/answerCallbackQuery",
        json={"callback_query_id": callback_query_id},
    ) as response:
        response.raise_for_status()


# ---------------------------------------------------------------------------
# Update handlers
# ---------------------------------------------------------------------------

async def handle_callback_query(session: aiohttp.ClientSession, callback_query: dict):
    user_id = callback_query["from"]["id"]
    await answer_callback_query(session, callback_query["id"])
    data = callback_query["data"]

    if data.startswith("lang_"):
        language_code = data.split("_", 1)[1]
        await handle_language_selection(session, user_id, language_code)
    elif data.startswith("forecast_hour_"):
        hour = int(data.split("_")[2])
        await handle_forecast_hour_selection(session, user_id, hour)


async def handle_language_selection(
    session: aiohttp.ClientSession,
    user_id: int,
    language_code: str,
):
    language_preferences[user_id] = language_code
    await send_message(session, user_id, TRADUCTIONS["lang_set"][language_code], reply_keyboard=COMMANDS_KEYBOARD)


async def handle_forecast_command(session: aiohttp.ClientSession, user_id: int):
    lang = language_preferences.get(user_id, "en")
    forecast_state[user_id] = {"step": "awaiting_forecast_location"}
    await send_message(session, user_id, TRADUCTIONS["forecast_ask_location"][lang])


async def handle_forecast_location(session: aiohttp.ClientSession, user_id: int, text: str):
    lang = language_preferences.get(user_id, "en")
    try:
        location = await get_location(city_name=text, session=session, language=lang)
    except Exception:
        await send_message(session, user_id, TRADUCTIONS["error_location"][lang].format(location=text))
        return
    forecast_state[user_id] = {"step": "awaiting_forecast_hour", "location": location}
    await send_message(session, user_id, TRADUCTIONS["forecast_ask_hour"][lang], keyboard=HOUR_KEYBOARD)


async def handle_forecast_hour_selection(
    session: aiohttp.ClientSession,
    user_id: int,
    hour: int,
):
    lang = language_preferences.get(user_id, "en")
    state = forecast_state.pop(user_id, None)

    if not state or state.get("step") != "awaiting_forecast_hour":
        await send_message(session, user_id, TRADUCTIONS["forecast_no_active"][lang])
        return

    location = state["location"]
    location_parts = [
        location[f"admin{i}"]
        for i in range(10)
        if location.get(f"admin{i}")
    ]
    if location.get("country"):
        location_parts.append(location["country"])
    location_name = ", ".join(location_parts)

    try:
        weather = await get_weather(location["latitude"], location["longitude"], session=session)
        timezone = weather["timezone"]
    except Exception:
        await send_message(session, user_id, TRADUCTIONS["error_weather"][lang].format(location=location_name))
        return

    if user_id in scheduled_forecasts:
        scheduled_forecasts[user_id]["task"].cancel()

    task = asyncio.create_task(
        run_scheduled_forecast(
            session, user_id,
            location["latitude"], location["longitude"],
            timezone, location_name, hour,
        )
    )
    scheduled_forecasts[user_id] = {
        "lat": location["latitude"],
        "lon": location["longitude"],
        "timezone": timezone,
        "location_name": location_name,
        "hour": hour,
        "task": task,
    }
    await send_message(
        session, user_id,
        TRADUCTIONS["forecast_scheduled"][lang].format(location=location_name, hour=hour),
    )


async def handle_cancel_forecast(session: aiohttp.ClientSession, user_id: int):
    lang = language_preferences.get(user_id, "en")
    if user_id not in scheduled_forecasts:
        await send_message(session, user_id, TRADUCTIONS["forecast_no_active"][lang])
        return
    scheduled_forecasts.pop(user_id)["task"].cancel()
    forecast_state.pop(user_id, None)
    await send_message(session, user_id, TRADUCTIONS["forecast_cancelled"][lang])


async def handle_start(session: aiohttp.ClientSession, user_id: int):
    await send_message(
        session,
        user_id,
        "🌍\n"
        "🇬🇧 Choose your language\n"
        "🇪🇸 Elige tu idioma\n"
        "🇫🇷 Choisissez votre langue\n",
        keyboard=LANGUAGE_KEYBOARD,
    )


async def handle_location_request(
    session: aiohttp.ClientSession,
    user_id: int,
    text: str,
):
    lang = language_preferences.get(user_id, "en")

    try:
        location = await get_location(city_name=text, session=session, language=lang)
    except Exception:
        await send_message(session, user_id, TRADUCTIONS["error_location"][lang].format(location=text))
        return

    location_parts = [
        location[f"admin{i}"]
        for i in range(10)
        if location.get(f"admin{i}")
    ]
    if location.get("country"):
        location_parts.append(location["country"])
    location_name = ", ".join(location_parts)

    try:
        weather = await get_weather(location["latitude"], location["longitude"], session=session)
    except Exception:
        await send_message(session, user_id, TRADUCTIONS["error_weather"][lang].format(location=text))
        return

    current = weather["current"]
    units = weather["current_units"]

    await send_message(
        session,
        user_id,
        TRADUCTIONS["weather_info"][lang].format(
            location=location_name,
            current_time=datetime.now(ZoneInfo(weather["timezone"])).strftime("%Y-%m-%d %H:%M:%S"),
            temperature=f"{current['temperature_2m']} {units['temperature_2m']}",
            weather=get_weather_code_description(current["weather_code"], lang),
            wind_speed=f"{current['wind_speed_10m']} {units['wind_speed_10m']}",
        ),
        reply_keyboard=COMMANDS_KEYBOARD,
    )


async def handle_message(session: aiohttp.ClientSession, message: dict):
    user_id = message["from"]["id"]
    text = message.get("text", "").strip()
    lang = language_preferences.get(user_id, "en")

    if text == "/start":
        await handle_start(session, user_id)

    elif text in ["/weather", "🌤️ Weather"]:
        forecast_state.pop(user_id, None)  # Clear any forecast state
        await send_message(session, user_id, TRADUCTIONS["ask_location"][lang])

    elif text in ["/forecast", "📅 Daily forecast"]:
        await handle_forecast_command(session, user_id)

    elif text in ["/cancelforecast", "❌ Cancel forecast"]:
        await handle_cancel_forecast(session, user_id)

    elif forecast_state.get(user_id, {}).get("step") == "awaiting_forecast_location":
        await handle_forecast_location(session, user_id, text)

    elif forecast_state.get(user_id, {}).get("step") == "awaiting_forecast_hour":
        await send_message(session, user_id, TRADUCTIONS["forecast_ask_hour"][lang], keyboard=HOUR_KEYBOARD)

    else:
        try:
            await handle_location_request(session, user_id, text)
        except Exception as e:
            await send_message(session, user_id, f"❌ An error occurred: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    if not BOT_TOKEN:
        raise ValueError("TOKEN_TELEGRAM is missing from .env")

    offset = None

    async with aiohttp.ClientSession() as session:
        print("🤖 Bot started!")

        while True:
            try:
                data = await get_updates(session, offset)

                for update in data.get("result", []):
                    offset = update["update_id"] + 1

                    if message := update.get("message"):
                        await handle_message(session, message)

                    if callback_query := update.get("callback_query"):
                        await handle_callback_query(session, callback_query)

            except Exception as e:
                print(f"❌ Error: {e}")
                await asyncio.sleep(5)  # Wait before retrying


if __name__ == "__main__":
    asyncio.run(main())
