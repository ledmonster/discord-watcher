import datetime
import os
from logging import getLogger

import requests
from google.cloud import datastore

logger = getLogger(__name__)

DISCORD_API_KEY = os.getenv("DISCORD_API_KEY")
API_ENDPOINT = "https://discord.com/api/v8"

CHANNELS = [
    {"id": "865213099271389185", "channel": "announcements", "server": "World of Women",
     "server_id": "865187610494107658"},
    {"id": "863524941957431308", "channel": "announcements", "server": "Robotos", "server_id": "863524941436420128"},
    {"id": "874970250209288233", "channel": "announcements", "server": "Boss Beauties",
     "server_id": "874970249768873994"},
]

if __name__ == '__main__':
    client = datastore.Client(project="discord-watcher")

    # channel 登録
    for ch in CHANNELS:
        key = client.key("Channel", ch["id"])
        entity = datastore.Entity(key)
        entity.update({
            "channel": ch["channel"],
            "server": ch["server"],
            "server_id": ch["server_id"],
        })
        client.put(entity)

    query = client.query(kind="Channel")
    for ch in query.fetch():  # type: datastore.Entity
        url = f"{API_ENDPOINT}/channels/{ch.key.name}/messages"
        logger.warning(url)
        res = requests.get(url, headers={"Authorization": DISCORD_API_KEY})
        if res.status_code != 200:
            logger.warning("failed to fetch messages")
            break
        for msg in res.json():
            key = client.key("Message", msg["id"])
            entity = datastore.Entity(key, exclude_from_indexes=("content", "url", "thumbnail_url"))
            entity.update({
                "timestamp": datetime.datetime.fromisoformat(msg["timestamp"]),
                "channel_id": msg["channel_id"],
                "content": msg["content"],
                "url": msg["embeds"][0]["url"] if msg["embeds"] else None,
                "thumbnail_url": msg["embeds"][0].get("thumbnail", {}).get("url") if msg["embeds"] else None,
            })
            client.put(entity)
