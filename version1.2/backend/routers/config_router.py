
"""System settings router.

Settings are persisted in a JSON file at ``backend/storage/settings.json``.
Runtime falls back to defaults from ``cores.config`` when a field is missing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from cores.config import (
    API_KEY,
    DB_HTTP,
    DB_NAME,
    DB_PORT,
    LLM_BASEAPI,
    LLM_MODEL,
    POLL_INTERVAL,
)
from utils.common import now_utc

router = APIRouter(prefix="/api/config", tags=["config"])

CONFIG_KEY = "system_settings"
SETTINGS_FILE = Path(__file__).resolve().parents[1] / "storage" / "settings.json"


class ConfigRead(BaseModel):
    """Response schema returned to frontend."""

    # Database (read-only via API)
    db_host: str = ""
    db_port: int = 27017
    db_name: str = "ocr"

    # LLM
    llm_base_api: str = ""
    llm_model: str = ""
    api_key: str = ""

    # Pipeline
    poll_interval: int = 300


class ConfigUpdate(BaseModel):
    """Update payload from frontend."""

    # LLM
    llm_base_api: str | None = None
    llm_model: str | None = None
    api_key: str | None = None

    # Pipeline
    poll_interval: int | None = Field(default=None, ge=5, le=86400)


def _env_defaults() -> dict[str, Any]:
    return {
        "db_host": DB_HTTP,
        "db_port": DB_PORT,
        "db_name": DB_NAME,
        "llm_base_api": LLM_BASEAPI,
        "llm_model": LLM_MODEL,
        "api_key": API_KEY,
        "poll_interval": POLL_INTERVAL,
    }


def _read_settings_file() -> dict[str, Any]:
    if not SETTINGS_FILE.exists():
        return {}
    try:
        with SETTINGS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_settings_file(doc: dict[str, Any]) -> None:
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SETTINGS_FILE.open("w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


def _merge_with_defaults(raw_doc: dict[str, Any]) -> dict[str, Any]:
    defaults = _env_defaults()
    merged = defaults.copy()
    for field in defaults:
        if field in raw_doc and raw_doc[field] is not None:
            merged[field] = raw_doc[field]

    # Keep bounds stable even if file was edited manually.
    try:
        merged["poll_interval"] = int(merged.get("poll_interval", POLL_INTERVAL))
    except Exception:
        merged["poll_interval"] = POLL_INTERVAL
    merged["poll_interval"] = max(5, min(86400, merged["poll_interval"]))
    return merged


def ensure_config_document(_db: Any = None) -> dict[str, Any]:
    """Ensure settings JSON exists and contains all required fields.

    The optional parameter is kept for backwards compatibility with startup code.
    """
    raw_doc = _read_settings_file()
    merged = _merge_with_defaults(raw_doc)

    now_iso = now_utc().isoformat()
    file_doc = {
        "_key": CONFIG_KEY,
        **merged,
        "created_at": raw_doc.get("created_at") or now_iso,
        "updated_at": now_iso,
    }
    _write_settings_file(file_doc)
    return merged


@router.get("", response_model=ConfigRead)
def get_config():
    """Return current config (env defaults + JSON overrides)."""
    merged = ensure_config_document()
    return ConfigRead(**merged)


@router.put("")
def update_config(payload: ConfigUpdate):
    """Update config fields and persist to JSON file."""
    current_raw = _read_settings_file()
    current = _merge_with_defaults(current_raw)

    update_fields: dict[str, Any] = {}
    for field, value in payload.model_dump(exclude_none=True).items():
        update_fields[field] = value

    if not update_fields:
        return ConfigRead(**ensure_config_document())

    current.update(update_fields)
    now_iso = now_utc().isoformat()
    file_doc = {
        "_key": CONFIG_KEY,
        **current,
        "created_at": current_raw.get("created_at") or now_iso,
        "updated_at": now_iso,
    }
    _write_settings_file(file_doc)
    return ConfigRead(**current)

@router.post("/reset")
def reset_config():
    """Reset settings JSON to defaults from env/config."""
    defaults = _env_defaults()
    now_iso = now_utc().isoformat()
    _write_settings_file(
        {
            "_key": CONFIG_KEY,
            **defaults,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    )
    return ConfigRead(**defaults)
