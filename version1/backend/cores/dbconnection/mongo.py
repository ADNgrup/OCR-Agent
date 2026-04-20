from __future__ import annotations

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

from cores.config import DB_ACC, DB_HTTP, DB_NAME, DB_PAS, DB_PORT


class SubEntitySchema(BaseModel):
    col: str = ""
    row: str = ""
    value_type: str = "text"
    unit: str = ""


class IndicatorSchema(BaseModel):
    label: str = ""
    metric: str = ""
    value_type: str = "text"
    unit: str = ""


class EntitySchema(BaseModel):
    id: Optional[str] = None
    main_entity_name: str
    type: str = "HMI Object"
    region: str = "center"  # Should support center, top, bottom, etc.
    indicators: Optional[List[IndicatorSchema]] = None
    subentities: Optional[List[SubEntitySchema]] = None


class ScreenGroupModel(BaseModel):
    source_id: Any
    monitor_key: str
    name: str = ""
    entity_schema: List[EntitySchema] = []
    histogram: Optional[List[float]] = None
    brightness_mean: Optional[float] = None
    brightness_std: Optional[float] = None
    ignored: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SnapshotModel(BaseModel):
    source_id: Any
    screen_group_id: Any
    monitor_key: str
    image_hash: str
    histogram: Optional[List[float]] = None
    brightness_mean: Optional[float] = None
    brightness_std: Optional[float] = None
    image_base64: Optional[str] = None
    entities_values: Optional[List[Dict[str, Any]]] = None
    llm_parse_error: Optional[str] = None
    extracted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class KVMSourceModel(BaseModel):
    name: str  # The friendly name
    host: str  # The IP/hostname of KVM
    port: int  # The port
    credentials: Optional[str] = None  # Secret token or string
    monitor_keys: Optional[List[str]] = None  # List of displays/monitors on this source
    enabled: bool = True
    last_polled_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


def _build_mongo_uri() -> str:
    if DB_ACC and DB_PAS:
        return f"mongodb://{DB_ACC}:{DB_PAS}@{DB_HTTP}:{DB_PORT}/{DB_NAME}?authSource=admin"
    return f"mongodb://{DB_HTTP}:{DB_PORT}/{DB_NAME}"


MONGO_URI = _build_mongo_uri()
client = MongoClient(MONGO_URI, tz_aware=True)


def get_db() -> Database:
    return client[DB_NAME]


def ensure_indexes(db: Database):
    db.app_config.create_index([("_key", ASCENDING)], unique=True)

    db.kvm_sources.create_index([("name", ASCENDING)])
    db.kvm_sources.create_index([("enabled", ASCENDING)])

    db.screen_groups.create_index([("source_id", ASCENDING), ("monitor_key", ASCENDING)])
    db.screen_groups.create_index([("source_id", ASCENDING), ("name", ASCENDING)])

    db.snapshots.create_index([("source_id", ASCENDING), ("monitor_key", ASCENDING), ("created_at", DESCENDING)])
    db.snapshots.create_index([("source_id", ASCENDING), ("monitor_key", ASCENDING), ("image_hash", ASCENDING)])
    db.snapshots.create_index([("screen_group_id", ASCENDING), ("created_at", DESCENDING)])

    db.snapshot_jobs.create_index([("status", ASCENDING)])
    db.snapshot_jobs.create_index([("created_at", DESCENDING)])
    db.snapshot_jobs.create_index([("updated_at", DESCENDING)])
