import aiosqlite

DB_PATH = "python_files/src/python_files/database/telegram_weather_bot.db"


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                language TEXT NOT NULL DEFAULT 'en'
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_forecasts (
                user_id INTEGER PRIMARY KEY,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                timezone TEXT NOT NULL,
                location_name TEXT NOT NULL,
                hour INTEGER NOT NULL
            )
        """)

        await db.commit()


async def load_users() -> dict[int, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_id, language FROM users"
        )

        rows = await cursor.fetchall()

    return {
        user_id: language
        for user_id, language in rows
    }

# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

async def save_user_language(user_id: int, language: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, language)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET language = excluded.language
        """, (user_id, language))

        await db.commit()


async def get_user_language(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT language FROM users WHERE user_id = ?",
            (user_id,),
        )

        row = await cursor.fetchone()

        if row is None:
            return "en"

        return row[0]


# ---------------------------------------------------------------------------
# Scheduled forecasts
# ---------------------------------------------------------------------------

async def save_scheduled_forecast(
    user_id: int,
    latitude: float,
    longitude: float,
    timezone: str,
    location_name: str,
    hour: int,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO scheduled_forecasts (
                user_id,
                latitude,
                longitude,
                timezone,
                location_name,
                hour
            )
            VALUES (?, ?, ?, ?, ?, ?)

            ON CONFLICT(user_id)
            DO UPDATE SET
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                timezone = excluded.timezone,
                location_name = excluded.location_name,
                hour = excluded.hour
        """, (
            user_id,
            latitude,
            longitude,
            timezone,
            location_name,
            hour,
        ))

        await db.commit()


async def delete_scheduled_forecast(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM scheduled_forecasts WHERE user_id = ?",
            (user_id,),
        )

        await db.commit()


async def get_scheduled_forecasts() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT
                user_id,
                latitude,
                longitude,
                timezone,
                location_name,
                hour
            FROM scheduled_forecasts
        """)

        rows = await cursor.fetchall()

    return [
        {
            "user_id": row[0],
            "latitude": row[1],
            "longitude": row[2],
            "timezone": row[3],
            "location_name": row[4],
            "hour": row[5],
        }
        for row in rows
    ]