import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()

DB_HTTP = os.getenv("DB_HTTP", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "27017"))
DB_NAME = os.getenv("DB_NAME", "ocr")
DB_ACC = os.getenv("DB_ACC", "")
DB_PAS = os.getenv("DB_PAS", "")

LLM_BASEAPI = os.getenv("LLM_BASEAPI", "")
API_KEY = os.getenv("API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen35")
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", 16384))

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))

BASE_DIR = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = BASE_DIR / "storage" / "snapshots"
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
