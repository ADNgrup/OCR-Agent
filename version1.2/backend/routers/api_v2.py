import base64
import hashlib
import io
import re
from pathlib import Path
from datetime import timedelta
from typing import Any
from uuid import uuid4

from bson import ObjectId
from bson.binary import Binary
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Body, UploadFile, File, Form
from fastapi.responses import FileResponse, Response
from PIL import Image

from cores.config import POLL_INTERVAL
from cores.schema_mongo import get_db
from cores.pipelines.pipeline_service_v2 import PipelineServiceV2
from cores.schemas import SourceCreate, SourceUpdate
from routers.config_router import ConfigUpdate, get_config as get_config_v1, update_config as update_config_v1, reset_config as reset_config_v1
from utils.common import now_utc
from utils.image_features import average_fingerprint, brightness_feature, histogram_feature

router = APIRouter(prefix="/api/v2", tags=["kvm-ocr-v2"])
pipeline_v2 = PipelineServiceV2()


def _norm_pct(value: float, default: float) -> float:
    try:
        out = float(value)
    except Exception:
        out = default
    return max(0.0, min(100.0, out))


def _normalize_list(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(v).strip() for v in values if str(v).strip()]


def _normalize_object_type(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"log", "log/alert", "log table", "log tables"}:
        return "log tables"
    if text in {"table", "fixed table", "fixed table object"}:
        return "fixed table"
    return "scada object"


def _segment_name_to_pascal_id(name: str, fallback: str = "Segment") -> str:
    text = str(name or "").strip()
    if not text:
        return fallback

    tokens = [token for token in re.split(r"[^0-9A-Za-z]+", text) if token]
    if tokens:
        out = "".join(token[:1].upper() + token[1:] for token in tokens)
    else:
        compact = "".join(ch for ch in text if ch.isalnum())
        out = (compact[:1].upper() + compact[1:]) if compact else ""

    if not out:
        out = fallback
    if out[0].isdigit():
        out = f"S{out}"
    return out


def _unique_segment_name(base_name: str, used_names: set[str]) -> str:
    candidate = str(base_name or "").strip() or "Segment"
    if candidate not in used_names:
        used_names.add(candidate)
        return candidate

    suffix = 2
    while True:
        next_name = f"{candidate} {suffix}"
        if next_name not in used_names:
            used_names.add(next_name)
            return next_name
        suffix += 1


def _unique_segment_id(base_id: str, used_ids: set[str]) -> str:
    candidate = str(base_id or "").strip() or "Segment"
    if candidate[0].isdigit():
        candidate = f"S{candidate}"

    if candidate not in used_ids:
        used_ids.add(candidate)
        return candidate

    suffix = 2
    while True:
        next_id = f"{candidate}{suffix}"
        if next_id not in used_ids:
            used_ids.add(next_id)
            return next_id
        suffix += 1


def _normalize_segment_identity(segments: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    used_names: set[str] = set()
    used_ids: set[str] = set()

    for idx, seg in enumerate(segments):
        if not isinstance(seg, dict):
            continue
        row = dict(seg)

        base_name = str(row.get("name") or f"Segment {idx + 1}").strip() or f"Segment {idx + 1}"
        seg_name = _unique_segment_name(base_name, used_names)
        seg_id = _unique_segment_id(
            _segment_name_to_pascal_id(seg_name, fallback=f"Segment{idx + 1}"),
            used_ids,
        )

        row["name"] = seg_name
        row["id"] = seg_id
        normalized.append(row)

    return normalized


def _schema_count(group: dict) -> int:
    segs = group.get("segmentation_schema")
    if isinstance(segs, list) and segs:
        return len(segs)
    legacy = group.get("entity_schema")
    if isinstance(legacy, list):
        return len(legacy)
    return 0


def _schema_status(group: dict) -> str:
    status = str(group.get("schema_status") or "").strip().lower()
    if status in {"classified", "unclassified"}:
        return status
    return "classified" if _schema_count(group) else "unclassified"


def _legacy_entity_to_segment(entity: dict, fallback_id: str, fallback_name: str) -> dict:
    metadata = entity.get("metadata") if isinstance(entity.get("metadata"), dict) else {}
    bbox_src = metadata.get("bbox") if isinstance(metadata.get("bbox"), dict) else {}
    bbox = {
        "x": _norm_pct(bbox_src.get("x", 10), 10),
        "y": _norm_pct(bbox_src.get("y", 10), 10),
        "w": _norm_pct(bbox_src.get("w", 20), 20),
        "h": _norm_pct(bbox_src.get("h", 20), 20),
    }

    seg_type = _normalize_object_type(entity.get("type"))
    columns: list[str] = []
    rows: list[str] = []

    if seg_type == "fixed table":
        raw_columns = metadata.get("value_columns") if isinstance(metadata.get("value_columns"), list) else []
        columns = [str(col).strip() for col in raw_columns if str(col).strip()]
        if not columns:
            seen_cols = set()
            for sub in entity.get("subentities") or []:
                col = str(sub.get("col") or "").strip()
                if col and col not in seen_cols:
                    seen_cols.add(col)
                    columns.append(col)

        raw_rows = metadata.get("rows") if isinstance(metadata.get("rows"), list) else []
        rows = [str(row).strip() for row in raw_rows if str(row).strip()]
        if not rows:
            seen_rows = set()
            for sub in entity.get("subentities") or []:
                row = str(sub.get("row") or "").strip()
                if row and row not in seen_rows:
                    seen_rows.add(row)
                    rows.append(row)

        if not columns:
            columns = ["column_1"]
        if not rows:
            rows = ["row_1"]
    elif seg_type == "log tables":
        columns = ["time", "message"]
    else:
        seen_cols = set()
        for ind in entity.get("indicators") or []:
            label = str(ind.get("label") or ind.get("metric") or "").strip()
            if label and label not in seen_cols:
                seen_cols.add(label)
                columns.append(label)
        if not columns:
            raw_columns = metadata.get("columns") if isinstance(metadata.get("columns"), list) else []
            columns = [str(col).strip() for col in raw_columns if str(col).strip()]

    return {
        "id": str(entity.get("id") or fallback_id),
        "name": str(entity.get("main_entity_name") or fallback_name).strip() or fallback_name,
        "type": seg_type,
        "shape": "rectangle",
        "bbox": bbox,
        "columns": columns,
        "rows": rows,
        "sample_id": str(metadata.get("sample_id") or "").strip(),
    }


def _group_segmentation_schema(group: dict) -> list[dict]:
    segs = group.get("segmentation_schema")
    if isinstance(segs, list) and segs:
        return _normalize_segment_identity([dict(seg) for seg in segs if isinstance(seg, dict)])

    legacy = group.get("entity_schema")
    if not isinstance(legacy, list):
        return []

    converted = []
    for idx, entity in enumerate(legacy):
        if not isinstance(entity, dict):
            continue
        converted.append(
            _legacy_entity_to_segment(
                entity,
                fallback_id=f"seg_{uuid4().hex[:8]}",
                fallback_name=f"Segment {idx + 1}",
            )
        )
    return _normalize_segment_identity(converted)


def _latest_legacy_sample(group: dict) -> dict | None:
    samples = list(group.get("samples") or [])
    if not samples:
        return None
    return samples[-1]


def _group_has_binary_sample(group: dict) -> bool:
    return group.get("sample") is not None


def _screen_sample_payload(group: dict) -> dict | None:
    if _group_has_binary_sample(group):
        sample_meta = group.get("sample_meta") if isinstance(group.get("sample_meta"), dict) else {}
        group_id = group.get("_id")
        image_url = f"/api/v2/screens/{str(group_id)}/sample-image" if group_id else None
        return {
            "id": str(sample_meta.get("id") or "sample"),
            "filename": sample_meta.get("filename") or "sample.png",
            "content_type": sample_meta.get("content_type") or "image/png",
            "image_hash": sample_meta.get("image_hash"),
            "image_base64": image_url,
            "width": sample_meta.get("width"),
            "height": sample_meta.get("height"),
            "created_at": sample_meta.get("created_at") or group.get("created_at"),
        }

    legacy = _latest_legacy_sample(group)
    if not legacy:
        return None

    return {
        "id": str(legacy.get("id") or ""),
        "filename": legacy.get("filename"),
        "content_type": legacy.get("content_type"),
        "image_hash": legacy.get("image_hash"),
        "image_base64": legacy.get("image_base64"),
        "width": legacy.get("width"),
        "height": legacy.get("height"),
        "created_at": legacy.get("created_at"),
    }


def _sample_count(group: dict) -> int:
    if _group_has_binary_sample(group):
        return 1
    return len(list(group.get("samples") or []))


def _sample_image_url(group: dict) -> str | None:
    payload = _screen_sample_payload(group)
    if not payload:
        return None
    return payload.get("image_base64")


def _decode_data_url_image(data_url: str) -> tuple[str, bytes] | None:
    text = str(data_url or "")
    if not text.startswith("data:"):
        return None

    comma_idx = text.find(",")
    if comma_idx <= 5:
        return None

    header = text[:comma_idx]
    payload = text[comma_idx + 1 :]
    if ";base64" not in header:
        return None

    content_type = header[5:].split(";", 1)[0] or "image/png"
    try:
        raw = base64.b64decode(payload)
    except Exception:
        return None

    return content_type, raw


def _ensure_group_schema(db, group: dict) -> tuple[dict, list[dict]]:
    segmentation_schema = _group_segmentation_schema(group)
    existing_schema = group.get("segmentation_schema")
    needs_sync = isinstance(existing_schema, list) and existing_schema != segmentation_schema

    if "entity_schema" in group or not isinstance(existing_schema, list) or needs_sync:
        now = now_utc()
        db.screen_groups.update_one(
            {"_id": group["_id"]},
            {
                "$set": {
                    "segmentation_schema": segmentation_schema,
                    "schema_status": "classified" if segmentation_schema else "unclassified",
                    "classified_at": group.get("classified_at") or (now if segmentation_schema else None),
                    "updated_at": now,
                },
                "$unset": {"entity_schema": ""},
            },
        )
        refreshed = db.screen_groups.find_one({"_id": group["_id"]})
        if refreshed:
            return refreshed, segmentation_schema

    return group, segmentation_schema


def _screen_summary_payload(group: dict) -> dict:
    schema_count = _schema_count(group)
    return {
        "id": str(group.get("_id")),
        "source_id": str(group.get("source_id")),
        "monitor_key": group.get("monitor_key") or "default",
        "name": group.get("name") or "",
        "ignored": bool(group.get("ignored")),
        "schema_status": _schema_status(group),
        "entity_count": schema_count,
        "sample_count": _sample_count(group),
        "sample_image_url": _sample_image_url(group),
        "classified_at": group.get("classified_at"),
        "updated_at": group.get("updated_at"),
    }


def _serialize_source(source: dict) -> dict:
    return {
        "id": str(source.get("_id")),
        "name": source.get("name"),
        "host": source.get("host"),
        "port": source.get("port"),
        "base_path": source.get("base_path"),
        "poll_seconds": source.get("poll_seconds"),
        "enabled": bool(source.get("enabled")),
        "monitor_keys": source.get("monitor_keys") or [],
        "similarity_threshold": source.get("similarity_threshold", 0.92),
        "mode": source.get("mode", "v2"),
        "last_polled_at": source.get("last_polled_at"),
    }


def _screen_library_payload(db, group: dict) -> dict:
    result_query = {"screen_group_id": group.get("_id")}
    latest_result = db.ocr_results.find_one(result_query, sort=[("created_at", -1)]) if group.get("_id") else None
    latest_result = latest_result or {}
    snapshot_count = db.ocr_results.count_documents(result_query) if group.get("_id") else 0

    latest_snapshot_id = latest_result.get("snapshot_id")
    sample_image_url = _sample_image_url(group)
    if not sample_image_url and latest_snapshot_id:
        sample_image_url = f"/api/v2/snapshots/{str(latest_snapshot_id)}/image"

    return {
        "id": str(group.get("_id")),
        "source_id": str(group.get("source_id")),
        "monitor_key": group.get("monitor_key") or "default",
        "name": group.get("name") or "",
        "ignored": bool(group.get("ignored")),
        "schema_status": _schema_status(group),
        "entity_count": _schema_count(group),
        "sample_count": _sample_count(group) or snapshot_count,
        "sample_image_url": sample_image_url,
        "snapshot_count": snapshot_count,
        "last_snapshot_at": latest_result.get("created_at"),
        "classified_at": group.get("classified_at"),
        "updated_at": group.get("updated_at"),
    }


def _run_once_worker_v2(source_id_str: str):
    db = get_db()
    try:
        source_oid = ObjectId(source_id_str)
    except Exception:
        return

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        return

    for monitor_key in source.get("monitor_keys") or ["default"]:
        try:
            pipeline_v2.process_single_snapshot(db, source, monitor_key)
        except Exception:
            pass

    db.kvm_sources.update_one(
        {"_id": source["_id"]},
        {"$set": {"last_polled_at": now_utc(), "updated_at": now_utc()}},
    )


@router.get("/config")
def get_config_v2():
    return get_config_v1()


@router.put("/config")
def update_config_v2(payload: ConfigUpdate):
    return update_config_v1(payload)


@router.post("/config/reset")
def reset_config_v2():
    return reset_config_v1()


@router.get("/queue")
def get_queue_v2():
    db = get_db()
    pipeline_agg = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    results = list(db.snapshot_jobs.aggregate(pipeline_agg))
    stats: dict[str, Any] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
    for row in results:
        status = row.get("_id")
        if status in stats:
            stats[status] = int(row.get("count") or 0)

    recent_errors = list(
        db.snapshot_jobs.find(
            {"status": "failed", "error": {"$ne": None}},
            {"error": 1, "source_id": 1, "monitor_key": 1, "updated_at": 1},
        ).sort("updated_at", -1).limit(10)
    )
    stats["recent_errors"] = [
        {
            "source_id": str(entry.get("source_id", "")),
            "monitor_key": entry.get("monitor_key"),
            "error": entry.get("error"),
            "time": entry.get("updated_at"),
        }
        for entry in recent_errors
    ]
    return stats


@router.get("/kvm-sources")
def list_sources_v2():
    db = get_db()
    sources = list(db.kvm_sources.find().sort("_id", 1))
    return [_serialize_source(src) for src in sources]


@router.post("/kvm-sources")
def create_kvm_source_v2(payload: SourceCreate):
    db = get_db()
    now = now_utc()
    similarity = min(0.999, max(0.5, float(payload.similarity_threshold)))
    document = {
        "name": payload.name,
        "host": payload.host,
        "port": payload.port,
        "base_path": payload.base_path or "kx",
        "poll_seconds": max(5, int(payload.poll_seconds or POLL_INTERVAL)),
        "enabled": bool(payload.enabled),
        "monitor_keys": payload.monitor_keys or ["default"],
        "headers": payload.headers or {},
        "similarity_threshold": similarity,
        "mode": "v2",
        "last_polled_at": None,
        "created_at": now,
        "updated_at": now,
    }
    inserted = db.kvm_sources.insert_one(document)
    return {"id": str(inserted.inserted_id)}


@router.patch("/kvm-sources/{source_id}/toggle")
def toggle_source_v2(source_id: str, enabled: bool):
    db = get_db()
    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    update: dict[str, Any] = {"enabled": bool(enabled), "updated_at": now_utc()}
    if enabled:
        update["last_polled_at"] = None

    db.kvm_sources.update_one({"_id": source_oid}, {"$set": update})
    refreshed = db.kvm_sources.find_one({"_id": source_oid})
    return _serialize_source(refreshed or source)


@router.put("/kvm-sources/{source_id}")
def update_kvm_source_v2(source_id: str, payload: SourceUpdate):
    db = get_db()
    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    update_fields = payload.model_dump(exclude_none=True)
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    if "poll_seconds" in update_fields:
        update_fields["poll_seconds"] = max(5, int(update_fields["poll_seconds"]))
    if "similarity_threshold" in update_fields:
        update_fields["similarity_threshold"] = min(0.999, max(0.5, float(update_fields["similarity_threshold"])))
    if "monitor_keys" in update_fields and not update_fields["monitor_keys"]:
        update_fields["monitor_keys"] = ["default"]
    update_fields["mode"] = "v2"

    update_fields["updated_at"] = now_utc()
    db.kvm_sources.update_one({"_id": source_oid}, {"$set": update_fields})
    refreshed = db.kvm_sources.find_one({"_id": source_oid})
    return _serialize_source(refreshed or source)


@router.delete("/kvm-sources/{source_id}")
def delete_kvm_source_v2(source_id: str):
    db = get_db()
    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    db.kvm_sources.delete_one({"_id": source_oid})
    return {"ok": True}


@router.get("/screens")
def list_screens_v2(source_id: str):
    db = get_db()
    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    groups = list(db.screen_groups.find({"source_id": source_oid}).sort("_id", 1))
    rows = []
    for group in groups:
        group, _ = _ensure_group_schema(db, group)
        rows.append(_screen_library_payload(db, group))
    return rows


@router.patch("/screens/{screen_group_id}/source")
def update_screen_source_v2(screen_group_id: str, payload: dict = Body(...)):
    db = get_db()
    source_id = str(payload.get("source_id") or "").strip()
    if not source_id:
        raise HTTPException(status_code=400, detail="Missing source_id")

    try:
        target_source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source_id")

    source = db.kvm_sources.find_one({"_id": target_source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    try:
        group_oid = ObjectId(screen_group_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen id")

    group = db.screen_groups.find_one({"_id": group_oid})
    if not group:
        raise HTTPException(status_code=404, detail="Screen group not found")

    now = now_utc()
    db.screen_groups.update_one(
        {"_id": group_oid},
        {"$set": {"source_id": target_source_oid, "updated_at": now}},
    )
    db.ocr_results.update_many(
        {"screen_group_id": group_oid},
        {"$set": {"source_id": target_source_oid, "updated_at": now}},
    )

    refreshed = db.screen_groups.find_one({"_id": group_oid}) or group
    refreshed, _ = _ensure_group_schema(db, refreshed)
    return {"ok": True, "screen": _screen_library_payload(db, refreshed)}


@router.delete("/screens/{screen_group_id}")
def delete_screen_group_v2(screen_group_id: str):
    db = get_db()
    try:
        oid = ObjectId(screen_group_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen id")

    group = db.screen_groups.find_one({"_id": oid})
    if not group:
        raise HTTPException(status_code=404, detail="Screen group not found")

    snapshot_ids = [
        row.get("snapshot_id")
        for row in db.ocr_results.find({"screen_group_id": oid}, {"snapshot_id": 1})
        if row.get("snapshot_id") is not None
    ]
    snapshots_deleted = 0
    if snapshot_ids:
        snapshots_deleted = db.snapshots.delete_many({"_id": {"$in": snapshot_ids}}).deleted_count
    # Legacy cleanup: older versions stored screen_group_id directly in snapshots.
    snapshots_deleted += db.snapshots.delete_many({"screen_group_id": oid}).deleted_count
    jobs_deleted = db.jobs.delete_many({"screen_group_id": oid}).deleted_count
    entity_logs_deleted = db.entity_logs.delete_many({"screen_group_id": oid}).deleted_count
    ocr_results_deleted = db.ocr_results.delete_many({"screen_group_id": oid}).deleted_count
    legacy_ocr_entities_deleted = db.ocr_entities.delete_many({"screen_group_id": oid}).deleted_count

    snapshot_storage_deleted = 0
    if snapshot_ids:
        snapshot_storage_deleted = db.snapshot_storage.delete_many({"snapshot_id": {"$in": snapshot_ids}}).deleted_count

    db.screen_groups.delete_one({"_id": oid})

    return {
        "ok": True,
        "deleted_screen_id": screen_group_id,
        "deleted_counts": {
            "snapshots": snapshots_deleted,
            "jobs": jobs_deleted,
            "entity_logs": entity_logs_deleted,
            "ocr_results": ocr_results_deleted,
            "legacy_ocr_entities": legacy_ocr_entities_deleted,
            "snapshot_storage": snapshot_storage_deleted,
        },
    }


@router.post("/screens/{screen_id}/toggle-ignore")
def toggle_screen_ignore_v2(screen_id: str, payload: dict = Body(...)):
    db = get_db()
    ignored = bool(payload.get("ignored", False))
    try:
        oid = ObjectId(screen_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen id")

    group = db.screen_groups.find_one({"_id": oid})
    if not group:
        raise HTTPException(status_code=404, detail="Screen group not found")

    db.screen_groups.update_one(
        {"_id": oid},
        {"$set": {"ignored": ignored, "updated_at": now_utc()}},
    )
    return {"ok": True, "ignored": ignored}


@router.get("/snapshots")
def get_snapshots_v2(
    source_id: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    skip: int = Query(default=0, ge=0),
):
    db = get_db()
    if source_id:
        try:
            ObjectId(source_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid source_id")

    return pipeline_v2.list_snapshots(db, source_id=source_id, limit=limit, skip=skip)


@router.get("/snapshots/latest")
def get_latest_snapshots_v2(source_id: str, limit: int = Query(default=20, ge=1, le=100)):
    db = get_db()

    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    return pipeline_v2.latest_snapshots(db, source_id=source_id, limit=limit)


@router.put("/snapshots/{snapshot_id}/evaluation")
def update_snapshot_evaluation_v2(snapshot_id: str, payload: dict = Body(...)):
    db = get_db()
    try:
        ObjectId(snapshot_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid snapshot id")

    eval_text = payload.get("evaluation")
    try:
        return pipeline_v2.update_snapshot_evaluation(db, snapshot_id, eval_text)
    except KeyError:
        raise HTTPException(status_code=404, detail="Snapshot not found")


@router.get("/snapshots/{snapshot_id}/image")
def get_snapshot_image_v2(snapshot_id: str):
    db = get_db()
    try:
        snap = db.snapshots.find_one({"_id": ObjectId(snapshot_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid snapshot id")
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    image_bytes = snap.get("image_bytes")
    if image_bytes is not None:
        return Response(
            content=bytes(image_bytes),
            media_type=str(snap.get("content_type") or "image/png"),
        )

    storage_doc = db.snapshot_storage.find_one(
        {"snapshot_id": snap["_id"]},
        sort=[("created_at", -1)],
    )
    if storage_doc and storage_doc.get("image_bytes") is not None:
        return Response(
            content=bytes(storage_doc.get("image_bytes")),
            media_type=str(storage_doc.get("content_type") or "image/png"),
        )

    image_path = snap.get("image_path")
    if not image_path or not Path(image_path).exists():
        raise HTTPException(status_code=404, detail="Snapshot image file missing")
    return FileResponse(image_path, media_type="image/png")


@router.get("/screens/{screen_group_id}/sample-image")
def get_screen_sample_image_v2(screen_group_id: str):
    db = get_db()
    try:
        group = db.screen_groups.find_one({"_id": ObjectId(screen_group_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen id")

    if not group:
        raise HTTPException(status_code=404, detail="Screen group not found")

    sample_binary = group.get("sample")
    if sample_binary is not None:
        sample_meta = group.get("sample_meta") if isinstance(group.get("sample_meta"), dict) else {}
        media_type = str(sample_meta.get("content_type") or "image/png")
        return Response(content=bytes(sample_binary), media_type=media_type)

    legacy = _latest_legacy_sample(group)
    if legacy and legacy.get("image_base64"):
        decoded = _decode_data_url_image(str(legacy.get("image_base64") or ""))
        if decoded:
            media_type, raw = decoded
            return Response(content=raw, media_type=media_type)

    raise HTTPException(status_code=404, detail="Sample image not found")


@router.get("/screens/{screen_group_id}/preview")
def screen_preview_v2(screen_group_id: str):
    db = get_db()
    preview = pipeline_v2.get_screen_preview(db, screen_group_id)
    if not preview:
        raise HTTPException(status_code=404, detail="No snapshots for this screen")
    return preview


@router.get("/screens/{screen_group_id}/schema-editor")
def get_screen_schema_editor_v2(screen_group_id: str):
    db = get_db()
    try:
        oid = ObjectId(screen_group_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen id")

    group = db.screen_groups.find_one({"_id": oid})
    if not group:
        raise HTTPException(status_code=404, detail="Screen group not found")

    group, segmentation_schema = _ensure_group_schema(db, group)

    sample_payload = _screen_sample_payload(group)

    return {
        **_screen_summary_payload(group),
        "segmentation_schema": segmentation_schema,
        "samples": [sample_payload] if sample_payload else [],
    }


@router.get("/entities")
def get_entities_v2(screen_group_id: str):
    db = get_db()
    try:
        oid = ObjectId(screen_group_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen_group_id")

    group = db.screen_groups.find_one({"_id": oid})
    if not group:
        return []

    _ensure_group_schema(db, group)
    return pipeline_v2.list_entities(db, screen_group_id)


@router.post("/kvm-sources/{source_id}/run-once")
def run_once_v2(source_id: str, bg: BackgroundTasks):
    db = get_db()
    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    bg.add_task(_run_once_worker_v2, source_id)
    return {"ok": True, "detail": "V2 snapshot job queued in background."}


@router.post("/screens/samples/upload")
async def upload_screen_sample_v2(
    source_id: str = Form(...),
    monitor_key: str = Form("default"),
    screen_group_id: str | None = Form(None),
    screen_name: str | None = Form(None),
    file: UploadFile = File(...),
):
    db = get_db()

    try:
        source_oid = ObjectId(source_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source id")

    source = db.kvm_sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        image = Image.open(io.BytesIO(content)).convert("RGB")
    except Exception:
        raise HTTPException(status_code=400, detail="File is not a valid image")

    now = now_utc()
    image_hash = hashlib.sha256(content).hexdigest()
    histogram = histogram_feature(image)
    brightness = brightness_feature(image)

    group = None
    created_group = False
    if screen_group_id:
        try:
            group_oid = ObjectId(screen_group_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid screen_group_id")
        group = db.screen_groups.find_one({"_id": group_oid})
        if not group:
            raise HTTPException(status_code=404, detail="Screen group not found")
        if _group_has_binary_sample(group):
            raise HTTPException(status_code=409, detail="This screen already has an initialized sample and cannot be updated")
        legacy_samples = list(group.get("samples") or [])
        if legacy_samples:
            raise HTTPException(status_code=409, detail="This screen already has an initialized sample and cannot be updated")
    else:
        sample_id = uuid4().hex[:12]
        content_type = file.content_type or "image/png"
        group = {
            "source_id": source["_id"],
            "monitor_key": monitor_key or "default",
            "name": (screen_name or f"imported_{monitor_key}_{int(now.timestamp())}").strip(),
            "schema_status": "unclassified",
            "segmentation_schema": [],
            "fingerprint": {
                "histogram": histogram,
                "brightness": [brightness[0], brightness[1]],
            },
            "sample": Binary(content),
            "sample_meta": {
                "id": sample_id,
                "filename": file.filename or f"sample_{sample_id}.png",
                "content_type": content_type,
                "image_hash": image_hash,
                "width": int(image.width),
                "height": int(image.height),
                "created_at": now,
            },
            "created_at": now,
            "updated_at": now,
        }
        inserted = db.screen_groups.insert_one(group)
        group["_id"] = inserted.inserted_id
        created_group = True

    fingerprint = group.get("fingerprint") or {}
    updated_fp = average_fingerprint(fingerprint, histogram, brightness)

    set_payload = {
        "fingerprint": updated_fp,
        "updated_at": now,
    }

    if not created_group:
        sample_id = uuid4().hex[:12]
        content_type = file.content_type or "image/png"
        set_payload["sample"] = Binary(content)
        set_payload["sample_meta"] = {
            "id": sample_id,
            "filename": file.filename or f"sample_{sample_id}.png",
            "content_type": content_type,
            "image_hash": image_hash,
            "width": int(image.width),
            "height": int(image.height),
            "created_at": now,
        }

    if screen_name and str(screen_name).strip():
        set_payload["name"] = str(screen_name).strip()
    if monitor_key and str(monitor_key).strip():
        set_payload["monitor_key"] = str(monitor_key).strip()

    db.screen_groups.update_one(
        {"_id": group["_id"]},
        {
            "$set": set_payload,
            "$unset": {"samples": ""},
        },
    )

    refreshed = db.screen_groups.find_one({"_id": group["_id"]})
    if refreshed:
        refreshed, _ = _ensure_group_schema(db, refreshed)

    return {
        "ok": True,
        "screen": _screen_summary_payload(refreshed or group),
        "sample": _screen_sample_payload(refreshed or group),
    }


@router.put("/screens/{screen_group_id}/schema-segments")
def save_screen_schema_segments_v2(screen_group_id: str, payload: dict = Body(...)):
    db = get_db()
    try:
        oid = ObjectId(screen_group_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screen id")

    group = db.screen_groups.find_one({"_id": oid})
    if not group:
        raise HTTPException(status_code=404, detail="Screen group not found")

    incoming_segments = payload.get("segments")
    if not isinstance(incoming_segments, list):
        raise HTTPException(status_code=400, detail="segments must be a list")

    now = now_utc()
    segmentation_schema = []

    for idx, raw_seg in enumerate(incoming_segments):
        if not isinstance(raw_seg, dict):
            continue

        seg_id = str(raw_seg.get("id") or f"seg_{uuid4().hex[:8]}")
        seg_name = str(raw_seg.get("name") or f"Box {idx + 1}").strip() or f"Box {idx + 1}"
        seg_type = _normalize_object_type(raw_seg.get("type"))
        bbox = raw_seg.get("bbox") if isinstance(raw_seg.get("bbox"), dict) else {}
        norm_bbox = {
            "x": _norm_pct(bbox.get("x", 10), 10),
            "y": _norm_pct(bbox.get("y", 10), 10),
            "w": _norm_pct(bbox.get("w", 20), 20),
            "h": _norm_pct(bbox.get("h", 20), 20),
        }

        columns = _normalize_list(raw_seg.get("columns"))
        rows = _normalize_list(raw_seg.get("rows"))
        if seg_type == "log tables":
            columns = ["time", "message"]

        sample_id = str(raw_seg.get("sample_id") or payload.get("sample_id") or "").strip()
        seg_doc = {
            "id": seg_id,
            "name": seg_name,
            "type": seg_type,
            "bbox": norm_bbox,
            "shape": "rectangle",
            "columns": columns,
            "rows": rows,
            "sample_id": sample_id,
        }
        segmentation_schema.append(seg_doc)

    segmentation_schema = _normalize_segment_identity(segmentation_schema)

    db.screen_groups.update_one(
        {"_id": oid},
        {
            "$set": {
                "segmentation_schema": segmentation_schema,
                "schema_status": "classified" if segmentation_schema else "unclassified",
                "classified_at": now if segmentation_schema else None,
                "updated_at": now,
            },
            "$unset": {"entity_schema": ""},
        },
    )

    refreshed = db.screen_groups.find_one({"_id": oid})
    return {
        "ok": True,
        "screen": _screen_summary_payload(refreshed),
        "segmentation_schema": refreshed.get("segmentation_schema") or [],
    }


@router.get("/ocr-entities")
def list_ocr_entities_v2(
    screen_group_id: str | None = Query(default=None),
    source_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    db = get_db()
    query: dict = {}
    if screen_group_id:
        try:
            query["screen_group_id"] = ObjectId(screen_group_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid screen_group_id")
    if source_id:
        try:
            query["source_id"] = ObjectId(source_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid source_id")

    rows = list(db.ocr_results.find(query).sort("created_at", -1).limit(limit))
    for row in rows:
        row["id"] = str(row.pop("_id"))
        if row.get("source_id") is not None:
            row["source_id"] = str(row["source_id"])
        if row.get("screen_group_id") is not None:
            row["screen_group_id"] = str(row["screen_group_id"])
        if row.get("snapshot_id") is not None:
            row["snapshot_id"] = str(row["snapshot_id"])
            row["snapshot_image_url"] = f"/api/v2/snapshots/{row['snapshot_id']}/image"
    return rows


@router.get("/logs")
def get_logs_v2(
    screen_group_id: str,
    hours: int = Query(default=24, ge=1, le=168),
    entity_ids: str | None = Query(default=None, description="Comma-separated entity IDs"),
    limit: int = Query(default=500, ge=1, le=5000),
):
    db = get_db()
    since = now_utc() - timedelta(hours=hours)
    eids = [eid.strip() for eid in entity_ids.split(",") if eid.strip()] if entity_ids else None
    return pipeline_v2.list_logs(db, screen_group_id=screen_group_id, since=since, entity_ids=eids, limit=limit)


@router.get("/timeseries")
def timeseries_v2(
    screen_group_id: str,
    hours: int = Query(default=24, ge=1, le=168),
    entity_ids: str | None = Query(default=None, description="Comma-separated entity IDs"),
):
    db = get_db()
    since = now_utc() - timedelta(hours=max(1, min(168, hours)))
    eids = [eid.strip() for eid in entity_ids.split(",") if eid.strip()] if entity_ids else None
    return pipeline_v2.get_timeseries(db, screen_group_id=screen_group_id, since=since, entity_ids=eids)
