import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "NexaVCForwarder")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")

if not ENCRYPTION_KEY:
    raise ValueError("ENCRYPTION_KEY is required in .env for secure session storage.")
