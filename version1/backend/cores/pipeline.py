from __future__ import annotations

from typing import Any

from pymongo.database import Database

from .pipelines import PipelineService


_service = PipelineService()


def _oid(value: str):
    return _service.oid(value)


def _to_id(value: Any) -> str:
    return _service.to_id(value)


def _save_snapshot(content: bytes, source_id: str, monitor_key: str):
    return _service.save_snapshot(content, source_id, monitor_key)


def _create_job(db: Database, source_id, monitor_key: str):
    return _service.create_job(db, source_id, monitor_key)


def _update_job(db: Database, job_id, status: str, error: str | None = None):
    return _service.update_job(db, job_id, status, error)


def get_queue_stats(db: Database) -> dict:
    return _service.get_queue_stats(db)


def cleanup_old_jobs(db: Database, keep_hours: int = 24):
    return _service.cleanup_old_jobs(db, keep_hours)


def pick_or_create_group(db: Database, source: dict, monitor_key: str, feature: list[float], brightness: tuple[float, float]) -> dict:
    return _service.pick_or_create_group(db, source, monitor_key, feature, brightness)


def map_entities_and_log(db: Database, snapshot: dict, extracted: dict[str, Any]):
    return _service.map_entities_and_log(db, snapshot, extracted)


def backfill_old_data(db: Database) -> dict:
    return _service.backfill_old_data(db)


def process_single_snapshot(db: Database, source: dict, monitor_key: str):
    return _service.process_single_snapshot(db, source, monitor_key)


async def poll_loop(db: Database, stop_event):
    return await _service.poll_loop(db, stop_event)


def serialize_source(source: dict) -> dict:
    return _service.serialize_source(source)


def create_source(db: Database, payload: dict) -> str:
    return _service.create_source(db, payload)


def get_source_or_none(db: Database, source_id: str) -> dict | None:
    return _service.get_source_or_none(db, source_id)


def list_screens(db: Database, source_id: str) -> list[dict]:
    return _service.list_screens(db, source_id)


def list_entities(db: Database, screen_group_id: str) -> list[dict]:
    return _service.list_entities(db, screen_group_id)


def get_screen_preview(db: Database, screen_group_id: str) -> dict | None:
    return _service.get_screen_preview(db, screen_group_id)


def list_logs(db: Database, screen_group_id: str, since, entity_ids: list[str] | None, limit: int) -> list[dict]:
    return _service.list_logs(db, screen_group_id, since, entity_ids, limit)


def get_timeseries(db: Database, screen_group_id: str, since, entity_ids: list[str] | None = None) -> dict[str, Any]:
    return _service.get_timeseries(db, screen_group_id, since, entity_ids)


def latest_snapshots(db: Database, source_id: str, limit: int) -> list[dict]:
    return _service.latest_snapshots(db, source_id, limit)
