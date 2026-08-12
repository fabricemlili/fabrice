import json
import time
import asyncio
from typing import Optional
import aiohttp
from python_files.logger import log
import os
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv("TOKEN")
GUILD_ID = os.getenv("GUILD_ID")

DISCORD_API_HEADERS = {
    "Authorization": f"Bot {TOKEN}",
    "Content-Type": "application/json"
}
BASE_URL_DISCORD = "https://discord.com/api/v10"

SPORTRADAR_API_HEADERS = {
    "accept": "application/json",
    "x-api-key": os.getenv("SPORTRADAR_API_KEY")
}
SUMMARIES_URL = "https://api.sportradar.com/tennis/trial/v3/en/schedules/live/summaries.json"
TIMELINES_DELTA_URL = "https://api.sportradar.com/tennis/trial/v3/en/schedules/live/timelines_delta.json"

RETRY_DELAY = 5  # seconds


async def fetch_json(
    url: str,
    headers: Optional[dict[str, str]] = None,
    session: Optional[aiohttp.ClientSession] = None,
) -> dict:
    close_session = session is None

    if session is None:
        session = aiohttp.ClientSession()

    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        log(f"API error: {e}", level="ERROR")
        return {}
    finally:
        if close_session:
            await session.close()


async def fetch_json_with_retry(
    url: str,
    headers: Optional[dict[str, str]] = None,
    session: Optional[aiohttp.ClientSession] = None,
    retry_delay: int = RETRY_DELAY,
) -> dict:
    while True:
        data = await fetch_json(url, headers=headers, session=session)
        if data:
            return data
        log(f"No data received from {url}. Retrying in {retry_delay} seconds...", level="WARNING")
        await asyncio.sleep(retry_delay)


def parse_json(data):
    def flatten_dict(data, prefix=""):
        result = {}

        for key, value in data.items():
            key = f"{prefix}_{key}" if prefix else str(key)

            if isinstance(value, dict):
                nested = flatten_dict(value, key)

                for nested_key, nested_value in nested.items():
                    add_unique(result, nested_key, nested_value)

            elif isinstance(value, list):
                result[key] = [
                    flatten_dict(item, key) if isinstance(item, dict) else item
                    for item in value
                ]

            else:
                add_unique(result, key, value)

        return result

    def add_unique(result, key, value):
        new_key = "_".join(key.split("_")[-3:])  # Use the last part of the key as the new key
        if new_key not in result:
            result[new_key] = value
            return

        if key not in result:
            result[key] = value
            return

        i = 2
        new_key = f"{key}_{i}"

        while new_key in result:
            i += 1
            new_key = f"{key}_{i}"

        result[new_key] = value

    if isinstance(data, dict):
        return flatten_dict(data)

    if isinstance(data, list):
        return [
            flatten_dict(item) if isinstance(item, dict) else item
            for item in data
        ]

    return data


class DiscordGuildManager:
    def __init__(self, token, guild_id):
        self.token = token
        self.guild_id = guild_id

    async def _request(
        self,
        url,
        data=None,
        type="POST",
        session: Optional[aiohttp.ClientSession] = None
    ):
        close_session = session is None

        if session is None:
            session = aiohttp.ClientSession()

        try:
            if type == "POST":
                async with session.post(
                    url,
                    json=data,
                    headers=DISCORD_API_HEADERS
                ) as response:
                    response.raise_for_status()
                    return await response.json()

            elif type == "GET":
                async with session.get(
                    url,
                    headers=DISCORD_API_HEADERS
                ) as response:
                    response.raise_for_status()
                    return await response.json()

            elif type == "DELETE":
                async with session.delete(
                    url,
                    headers=DISCORD_API_HEADERS
                ) as response:
                    response.raise_for_status()

                    # Discord returns 204 No Content
                    if response.status == 204:
                        return True

                    return await response.json()

        except aiohttp.ClientError as e:
            log(f"Discord API error: {e}", level="ERROR")
            return None

        finally:
            if close_session:
                await session.close()

    async def create_channel(
        self,
        nom: str,
        category_id: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None
    ) -> tuple[Optional[str], Optional[str]]:
        url = f"{BASE_URL_DISCORD}/guilds/{self.guild_id}/channels"

        data = {
            "name": nom,
            "type": 0,  # 0 = text channel
        }

        if category_id:
            data["parent_id"] = category_id

        response = await self._request(
            url,
            data=data,
            session=session
        )

        if isinstance(response, dict) and "id" in response and "name" in response:
            log(f"Channel created: {response['name']}")
            return response["id"], response["name"]

        log("Failed to create channel.", level="ERROR")
        return None, None

    async def delete_channel(self, channel_id, session: Optional[aiohttp.ClientSession] = None) -> Optional[str]:
        url = f"{BASE_URL_DISCORD}/channels/{channel_id}"

        response = await self._request(
            url,
            type="DELETE",
            session=session
        )

        if response:
            log(f"Channel deleted: {channel_id}")
            return channel_id

        log(f"Failed to delete channel: {channel_id}", level="ERROR")
        return None

    async def delete_all_channels(self) -> list[str]:
        url = f"{BASE_URL_DISCORD}/guilds/{self.guild_id}/channels"

        async with aiohttp.ClientSession() as session:
            channels = await self._request(
                url,
                type="GET",
                session=session
            )

            if not isinstance(channels, list):
                log("Failed to retrieve channels.", level="ERROR")
                return []

            if not channels:
                log("No channels found.")
                return []

            log(f"Found {len(channels)} channel(s).")

            deleted_channels = []

            for channel in channels:
                channel_id = channel["id"]
                channel_name = channel["name"]

                log(f"Deleting channel: {channel_name}")

                deleted_channel_id = await self.delete_channel(channel_id, session=session)
                if deleted_channel_id:
                    deleted_channels.append(deleted_channel_id)
                await asyncio.sleep(1)

            log(f"All channels have been deleted. Deleted channels: {deleted_channels}")
            return deleted_channels
        
    async def send_message(
        self,
        channel_id: str,
        content: str,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[str]:
        url = f"{BASE_URL_DISCORD}/channels/{channel_id}/messages"

        data = {
            "content": content
        }

        response = await self._request(
            url,
            data=data,
            type="POST",
            session=session
        )

        if isinstance(response, dict) and "id" in response:
            log(f"Message sent to channel {channel_id}")
            return response["id"]

        log(f"Failed to send message to channel: {channel_id}", level="ERROR")
        return None

    async def create_category(
        self,
        nom: str,
        session: Optional[aiohttp.ClientSession] = None
    ) -> tuple[Optional[str], Optional[str]]:
        url = f"{BASE_URL_DISCORD}/guilds/{self.guild_id}/channels"

        data = {
            "name": nom,
            "type": 4,  # 4 = category
        }

        response = await self._request(
            url,
            data=data,
            session=session
        )

        if isinstance(response, dict) and "id" in response and "name" in response:
            log(f"Category created: {response['name']}")
            return response["id"], response["name"]

        log("Failed to create category.", level="ERROR")
        return None, None






class TennisLiveMatchTracker:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session
        self.live_matches = {}
        self.generated_at = None
        self.last_point_ids: dict[str, str] = {}
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def get_live_matches(self) -> dict[str, dict]:
        data = await fetch_json_with_retry(
            SUMMARIES_URL, 
            SPORTRADAR_API_HEADERS, 
            session=self.session, 
            retry_delay=RETRY_DELAY
        )

        parsed_data = parse_json(data)
        if not isinstance(parsed_data, dict):
            raise ValueError("Parsed data is not a dictionary")

        live_matches = {}
        for summary in parsed_data["summaries"]:
            status = summary["event_status_status"]
            match_status = summary["status_match_status"]

            if match_status == "not_started":
                continue

            live_matches[summary["sport_event_id"]] = summary

        return live_matches

    async def get_current_scores(self) -> dict[str, dict]:
        data = await fetch_json_with_retry(
            TIMELINES_DELTA_URL, 
            SPORTRADAR_API_HEADERS, 
            session=self.session, 
            retry_delay=RETRY_DELAY
        )

        parsed_data = parse_json(data)
        if not isinstance(parsed_data, dict):
            raise ValueError("Parsed data is not a dictionary")

        generated_at = parsed_data.get("generated_at")
        if not generated_at:
            raise ValueError("Missing 'generated_at' in parsed data")
        if self.generated_at and generated_at == self.generated_at:
            return {}
        self.generated_at = generated_at

        scores = {}
        for delta in parsed_data.get("sport_event_timeline_deltas", []):
            event_id = delta.get("event_timeline_id")

            live_match = self.live_matches.get(event_id)
            if not live_match:
                log(f"Live match not found for event_id: {event_id}", level="WARNING")
                continue

            scores[event_id] = delta

        return scores

    async def _task_update_live_matches(self):
        while True:
            try:
                self.live_matches.update(await self.get_live_matches())
            except Exception as e:
                log(f"Error updating live matches: {e}", level="ERROR")
            finally:
                await asyncio.sleep(60)

    async def run(self):
        asyncio.create_task(self._task_update_live_matches())

        while True:
            try:
                current_scores = await self.get_current_scores()
                for event_id, score_info in current_scores.items():
                    last_point_id = score_info["event_timeline_timeline"][0]["timeline_timeline_id"]

                    if event_id in self.last_point_ids and self.last_point_ids[event_id] == last_point_id:
                        continue
                    self.last_point_ids[event_id] = last_point_id

                    await self.queue.put((event_id, score_info, self.live_matches[event_id]))
            except Exception as e:
                log(f"Error updating current scores: {e}", level="ERROR")
            finally:
                await asyncio.sleep(5)


       
def extract_player_info(competitors, qualifier, match_type) -> dict:
    if match_type == "singles":
        player = next((c for c in competitors if c["event_competitors_qualifier"] == qualifier), None)
        if player:
            name = player["event_competitors_name"]
            first_name, last_name = name.split(", ")[1].strip(), name.split(", ")[0].strip()
            country = player["event_competitors_country"]
            return {
                "name": name,
                "first_name": first_name,
                "last_name": last_name,
                "country": country
            }
    elif match_type == "doubles":
        team = next((c for c in competitors if c["event_competitors_qualifier"] == qualifier), None)
        if team:
            players = team["summaries_sport_event_competitors_players"]
            
            player_1 = players[0]
            player_2 = players[1]

            name_1 = player_1["competitors_players_name"]
            first_name_1, last_name_1 = name_1.split(", ")[1].strip(), name_1.split(", ")[0].strip()
            country_1 = player_1["competitors_players_country"]

            name_2 = player_2["competitors_players_name"]
            first_name_2, last_name_2 = name_2.split(", ")[1].strip(), name_2.split(", ")[0].strip()
            country_2 = player_2["competitors_players_country"]

            return {
                "player_1": {
                    "name": name_1,
                    "first_name": first_name_1,
                    "last_name": last_name_1,
                    "country": country_1
                },
                "player_2": {
                    "name": name_2,
                    "first_name": first_name_2,
                    "last_name": last_name_2,
                    "country": country_2
                }
            }
    return {}

            


        
async def main():
    category_mapping = {}
    channel_mapping = {}
    last_messages = {}
    
    async with aiohttp.ClientSession() as session:
        discord_guild_manager = DiscordGuildManager(TOKEN, GUILD_ID)
        # await discord_guild_manager.delete_all_channels()

        tennis_live_match_tracker = TennisLiveMatchTracker(session=session)
        asyncio.create_task(tennis_live_match_tracker.run())
        queue = tennis_live_match_tracker.queue

        while True:
            event_id, score_info, match_info = await queue.get()

            organization = match_info["context_category_name"]

            if organization in ["UTR Women", "UTR Men"]:
                queue.task_done()
                continue

            last_point_info = score_info["event_timeline_timeline"][0]
            last_point_type = last_point_info["timeline_timeline_type"]

            try:
                last_point_winner = last_point_info["timeline_timeline_competitor"]
            except KeyError:
                print(json.dumps(score_info, indent=4))
                exit(1)

            score = [f"{s['scores_home_score']}/{s['scores_away_score']}" for s in score_info.get("status_period_scores", [])]
            try:
                serving = score_info["game_state_serving"]
            except KeyError:
                print(json.dumps(score_info, indent=4))
                exit(1)

            game_score = (
                f"{score_info['state_home_score']}-{score_info['state_away_score']}"
                if serving == "home"
                else f"{score_info['state_away_score']}-{score_info['state_home_score']}"
            ).replace("50", "A")

            stadium = match_info["event_venue_name"]
            match_type = match_info["context_competition_type"]
            gender = match_info["context_competition_gender"]
            competitors = match_info["sport_event_competitors"]

            round = match_info["context_round_name"]
            tournament = match_info["context_competition_name"]
            category_name = "-".join(tournament.split(" ")).lower().replace(",", "")

            if match_type == "singles":
                home_player = extract_player_info(competitors, "home", match_type)
                home_player_name = home_player["last_name"]

                away_player = extract_player_info(competitors, "away", match_type)
                away_player_name = away_player["last_name"]

                channel_name = "-".join(home_player_name.split(" ") + ["vs"] + away_player_name.split(" ")).lower()

            elif match_type == "doubles":
                home_players = extract_player_info(competitors, "home", match_type)
                home_player_name = f"{home_players['player_1']['last_name']}-{home_players['player_2']['last_name']}"

                away_players = extract_player_info(competitors, "away", match_type)
                away_player_name = f"{away_players['player_1']['last_name']}-{away_players['player_2']['last_name']}"
      
                channel_name = "-".join(home_player_name.split(" ") + ["vs"] + away_player_name.split(" ")).lower()

            if category_name not in category_mapping:
                category_id, name = await discord_guild_manager.create_category(category_name, session=session)
                if category_name != name:
                    raise ValueError(f"Category name mismatch: expected {category_name}, got {name}")
                category_mapping[category_name] = category_id
         
            if channel_name not in channel_mapping:
                category_id = category_mapping[category_name]
                channel_id, name = await discord_guild_manager.create_channel(channel_name, category_id=category_id, session=session)
                if channel_name != name:
                    raise ValueError(f"Channel name mismatch: expected {channel_name}, got {name}")
                channel_mapping[channel_name] = channel_id

            home_display = home_player_name.replace("-", " / ")
            away_display = away_player_name.replace("-", " / ")

            server_tag_home = "🎾 " if serving == "home" else ""
            server_tag_away = " 🎾" if serving == "away" else ""

            sets_display = "  ".join(score) if score else "0/0"

            point_type_display = last_point_type.replace("_", " ").title()
            winner_display = home_display if last_point_winner == "home" else away_display

            message = (
                f"🏆 **{tournament}** — {round}\n"
                f"{gender.title()} {match_type.title()} • 📍 {stadium}\n"
                f"{'─' * 32}\n"
                f"{server_tag_home}**{home_display}**  vs  **{away_display}**{server_tag_away}\n\n"
                f"**Sets:** {sets_display}\n"
                f"**Current Game:** `{game_score}`\n\n"
                # f"📢 **Last Point:** {point_type_display} — won by **{winner_display}**\n"
                f"📢 **Last Point:** won by **{winner_display}**\n"
                f"🆔 `{event_id}`"
            )

            if event_id in last_messages and last_messages[event_id] == message:
                queue.task_done()
                continue

            channel_id = channel_mapping[channel_name]
            await discord_guild_manager.send_message(channel_id, message, session=session)
            last_messages[event_id] = message

            queue.task_done()

if __name__ == "__main__":
    asyncio.run(main())