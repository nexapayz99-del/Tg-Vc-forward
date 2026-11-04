import os
import json
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "12345678"))
API_HASH = os.getenv("API_HASH", "your_api_hash_here")
BOT_TOKEN = os.getenv("BOT_TOKEN", "your_bot_token_here")
OWNER_ID = int(os.getenv("OWNER_ID", "123456789"))
OPEN_FOR_ALL = os.getenv("OPEN_FOR_ALL", "True").lower() == "true"

DB_FILE = "multi_user_db.json"


def load_db() -> dict:
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "approved_users": [OWNER_ID],
        "banned_users": [],
        "open_for_all": OPEN_FOR_ALL,
        "users": {},  # str(user_id) -> {"recorder": str, "broadcasters": [str], "source_chat": int}
    }


def save_db(data: dict):
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4)
