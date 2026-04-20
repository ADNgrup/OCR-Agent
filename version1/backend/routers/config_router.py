"""
Router quản lý cài đặt hệ thống (app_config).

Các tham số cấu hình được lưu vào MongoDB collection ``app_config``
dưới dạng 1 document duy nhất (singleton).  Backend vẫn khởi động được
bằng giá trị mặc định từ ``.env`` / ``cores.config`` – nhưng khi có
document trong DB thì ưu tiên lấy từ DB.
"""

from __future__ import annotations

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
from cores.dbconnection.mongo import get_db
from utils.common import now_utc

router = APIRouter(prefix="/api/config", tags=["config"])

# ---- Singleton key cho collection app_config ----
CONFIG_KEY = "system_settings"


# ---- Pydantic schemas ----
class ConfigRead(BaseModel):
    """Schema trả về cho client."""

    # Database (READ-ONLY – chỉ hiển thị, không cho sửa qua API)
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
    """Schema nhận từ client khi cập nhật."""

    # LLM
    llm_base_api: str | None = None
    llm_model: str | None = None
    api_key: str | None = None

    # Pipeline
    poll_interval: int | None = Field(default=None, ge=5, le=86400)


# ---- Default từ .env / config.py ----
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


def _load_config_doc(db) -> dict[str, Any]:
    """Lấy document config từ DB, merge với env defaults."""
    defaults = _env_defaults()
    doc = db.app_config.find_one({"_key": CONFIG_KEY})
    if doc:
        for field in defaults:
            if field in doc and doc[field] is not None:
                defaults[field] = doc[field]
    return defaults


def ensure_config_document(db) -> dict[str, Any]:
    """Ensure app_config exists in MongoDB and backfill missing fields from env defaults."""
    defaults = _env_defaults()
    doc = db.app_config.find_one({"_key": CONFIG_KEY})
    now = now_utc()

    if not doc:
        db.app_config.insert_one(
            {
                "_key": CONFIG_KEY,
                **defaults,
                "created_at": now,
                "updated_at": now,
            }
        )
        return defaults

    missing_fields = {
        field: value
        for field, value in defaults.items()
        if field not in doc or doc[field] is None
    }
    if missing_fields:
        db.app_config.update_one(
            {"_key": CONFIG_KEY},
            {"$set": {**missing_fields, "updated_at": now}},
        )

    merged = defaults.copy()
    for field in defaults:
        if field in doc and doc[field] is not None:
            merged[field] = doc[field]
    return merged


# ---- Endpoints ----

@router.get("", response_model=ConfigRead)
def get_config():
    """Trả về toàn bộ cấu hình hiện tại (env defaults + DB overrides)."""
    db = get_db()
    merged = ensure_config_document(db)
    return ConfigRead(**merged)


@router.put("")
def update_config(payload: ConfigUpdate):
    """Cập nhật cấu hình. Chỉ các trường được gửi (not None) mới được lưu."""
    db = get_db()

    update_fields: dict[str, Any] = {}
    for field, value in payload.model_dump(exclude_none=True).items():
        update_fields[field] = value

    if not update_fields:
        return {"detail": "Nothing to update."}

    update_fields["updated_at"] = now_utc()

    db.app_config.update_one(
        {"_key": CONFIG_KEY},
        {
            "$set": update_fields,
            "$setOnInsert": {"_key": CONFIG_KEY, "created_at": now_utc()},
        },
        upsert=True,
    )

    # Trả lại config mới nhất
    merged = ensure_config_document(db)
    return ConfigRead(**merged)


@router.post("/reset")
def reset_config():
    """Xoá config trong DB → quay về giá trị mặc định từ .env."""
    db = get_db()
    db.app_config.delete_one({"_key": CONFIG_KEY})
    return ConfigRead(**ensure_config_document(db))
