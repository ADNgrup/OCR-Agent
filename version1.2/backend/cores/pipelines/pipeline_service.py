from __future__ import annotations

import asyncio
import difflib
import hashlib
import io
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, List

from bson import ObjectId
from PIL import Image
from pymongo.database import Database


from cores.config import POLL_INTERVAL, SNAPSHOT_DIR
from .pipeline_utils import EntityExtractionNormalizer
from utils.common import classify_value_type, clean_numeric_value, now_utc, _BOOL_TRUE_VALUES, _BOOL_FALSE_VALUES
from utils.image_features import average_fingerprint, brightness_feature, histogram_feature, similarity_score
from utils.kvm_client import fetch_snapshot_bytes
from cores.services.llm_client import call_llm_image_to_markdown, call_llm_markdown_to_json, ensure_llm_name

logger = logging.getLogger("pipeline")


class PipelineService:
    def __init__(self):
        self.normalizer = EntityExtractionNormalizer()

    @staticmethod
    def oid(value: str) -> ObjectId:
        return ObjectId(value)

    @staticmethod
    def to_id(value: Any) -> str:
        return str(value)

    @staticmethod
    def save_snapshot(content: bytes, source_id: str, monitor_key: str) -> Path:
        ts = now_utc().strftime("%Y%m%d_%H%M%S")
        file_name = f"src{source_id}_{monitor_key}_{ts}.png"
        file_path = SNAPSHOT_DIR / file_name
        file_path.write_bytes(content)
        return file_path

    def create_job(self, db: Database, source_id, monitor_key: str):
        job = {
            "source_id": source_id,
            "monitor_key": monitor_key,
            "status": "pending",
            "error": None,
            "created_at": now_utc(),
            "updated_at": now_utc(),
        }
        return db.snapshot_jobs.insert_one(job).inserted_id

    def update_job(self, db: Database, job_id, status: str, error: str | None = None):
        db.snapshot_jobs.update_one(
            {"_id": job_id},
            {"$set": {"status": status, "error": error, "updated_at": now_utc()}},
        )

    def get_queue_stats(self, db: Database) -> dict:
        pipeline_agg = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        results = list(db.snapshot_jobs.aggregate(pipeline_agg))
        stats: dict = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
        for row in results:
            if row["_id"] in stats:
                stats[row["_id"]] = row["count"]
        recent_errors = list(
            db.snapshot_jobs.find(
                {"status": "failed", "error": {"$ne": None}},
                {"error": 1, "source_id": 1, "monitor_key": 1, "updated_at": 1},
            ).sort("updated_at", -1).limit(10)
        )
        stats["recent_errors"] = [
            {
                "source_id": self.to_id(entry.get("source_id", "")),
                "monitor_key": entry.get("monitor_key"),
                "error": entry.get("error"),
                "time": entry.get("updated_at"),
            }
            for entry in recent_errors
        ]
        return stats

    def cleanup_old_jobs(self, db: Database, keep_hours: int = 24):
        cutoff = now_utc() - timedelta(hours=keep_hours)
        db.snapshot_jobs.delete_many(
            {"status": {"$in": ["completed", "failed"]}, "updated_at": {"$lt": cutoff}}
        )

    def pick_or_create_group(self, db: Database, source: dict, monitor_key: str, feature: list[float], brightness: tuple[float, float]) -> dict:
        groups = list(db.screen_groups.find({"source_id": source["_id"], "monitor_key": monitor_key}))
        best_group = None
        best_score = -1.0
        for group in groups:
            fingerprint = group.get("fingerprint", {})
            ref_feat = fingerprint.get("histogram", [])
            ref_brightness = tuple(fingerprint.get("brightness", [0.0, 0.0]))
            score = similarity_score(feature, ref_feat, brightness, ref_brightness)
            if score > best_score:
                best_score = score
                best_group = group

        threshold = float(source.get("similarity_threshold") or 0.92)
        if best_group and best_score >= threshold:
            updated_fp = average_fingerprint(best_group.get("fingerprint", {}), feature, brightness)
            db.screen_groups.update_one(
                {"_id": best_group["_id"]},
                {"$set": {"fingerprint": updated_fp, "updated_at": now_utc()}},
            )
            best_group["fingerprint"] = updated_fp
            return best_group

        new_group = {
            "source_id": source["_id"],
            "monitor_key": monitor_key,
            "name": f"screen_{monitor_key}_{int(now_utc().timestamp())}",
            "fingerprint": {"histogram": feature, "brightness": [brightness[0], brightness[1]]},
            "created_at": now_utc(),
            "updated_at": now_utc(),
        }
        insert = db.screen_groups.insert_one(new_group)
        new_group["_id"] = insert.inserted_id
        return new_group

    def map_entities_and_log(self, db: Database, snapshot: dict, extracted: dict[str, Any]):
        entities_raw = extracted.get("entities", []) if isinstance(extracted, dict) else []
        merged_entities: dict[str, dict] = {}
        
        for item in entities_raw:
            normalized_entity = self.normalizer.normalize_entity_entry(item)
            if not normalized_entity:
                continue
            
            e_key = normalized_entity["entity_key"]
            if e_key in merged_entities:
                existing_ind_keys = {ind["indicator_key"] for ind in merged_entities[e_key]["indicators"]}
                for ind in normalized_entity["indicators"]:
                    if ind["indicator_key"] not in existing_ind_keys:
                        existing_ind_keys.add(ind["indicator_key"])
                        merged_entities[e_key]["indicators"].append(ind)
            else:
                merged_entities[e_key] = normalized_entity

        for normalized_entity in merged_entities.values():
            display_name = normalized_entity["display_name"]
            entity_key = normalized_entity["entity_key"]
            entity_type = normalized_entity["entity_type"]
            regions = normalized_entity.get("regions", [])
            indicators = normalized_entity["indicators"]

            query = {"screen_group_id": snapshot["screen_group_id"], "entity_key": entity_key}
            entity = db.screen_entities.find_one(query)

            if entity:
                existing_metrics = entity.get("metrics") or {}
                if existing_metrics:
                    for indicator in indicators:
                        curr_key = indicator["indicator_key"]
                        if curr_key not in existing_metrics:
                            # 1. exact match label
                            exact_match = None
                            for old_k, old_v in existing_metrics.items():
                                if old_v.get("indicator_label") == indicator["indicator_label"]:
                                    exact_match = old_k
                                    break
                            if exact_match:
                                indicator["indicator_key"] = exact_match
                                continue
                            
                            # 2. fuzzy match label
                            best_match = None
                            best_score = 0
                            for old_k, old_v in existing_metrics.items():
                                score = difflib.SequenceMatcher(None, indicator["indicator_label"], old_v.get("indicator_label", "")).ratio()
                                if score > best_score:
                                    best_score = score
                                    best_match = old_k
                            if best_score >= 0.9:
                                indicator["indicator_key"] = best_match

            metrics_summary: dict[str, dict[str, Any]] = {}
            for indicator in indicators:
                metrics_summary[indicator["indicator_key"]] = {
                    "display_name": indicator["display_name"],
                    "indicator_label": indicator["indicator_label"],
                    "metric": indicator["metric_name"],
                    "metric_key": indicator["metric_key"],
                    "unit": indicator["unit"],
                    "value_type": indicator["value_type"],
                    "last_value": indicator["value"],
                    "confidence": indicator.get("confidence"),
                    "evidence": indicator.get("evidence"),
                }

            if not entity:
                entity = {
                    "screen_group_id": snapshot["screen_group_id"],
                    "entity_key": entity_key,
                    "display_name": display_name,
                    "entity_type": entity_type,
                    "regions": regions,
                    "metrics": metrics_summary,
                    "last_seen_at": now_utc(),
                    "created_at": now_utc(),
                    "updated_at": now_utc(),
                }
                inserted = db.screen_entities.insert_one(entity)
                entity["_id"] = inserted.inserted_id
            else:
                existing_metrics = entity.get("metrics") or {}
                existing_metrics.update(metrics_summary)
                db.screen_entities.update_one(
                    {"_id": entity["_id"]},
                    {"$set": {
                        "display_name": display_name,
                        "entity_type": entity_type or entity.get("entity_type"),
                        "regions": regions,
                        "metrics": existing_metrics,
                        "last_seen_at": now_utc(),
                        "updated_at": now_utc(),
                    }},
                )

            for indicator in indicators:
                num_val = None
                if indicator["value_type"] == "number":
                    num_val = clean_numeric_value(indicator["value"])
                elif indicator["value_type"] == "bool" and indicator.get("value"):
                    raw_lower = str(indicator["value"]).strip().lower()
                    if raw_lower in _BOOL_TRUE_VALUES:
                        num_val = 1
                    elif raw_lower in _BOOL_FALSE_VALUES:
                        num_val = 0

                db.entity_logs.insert_one({
                    "entity_id": entity["_id"],
                    "snapshot_id": snapshot["_id"],
                    "metric": indicator["indicator_key"],
                    "metric_name": indicator["metric_name"],
                    "indicator_label": indicator["indicator_label"],
                    "value_type": indicator["value_type"],
                    "raw_value": indicator["value"],
                    "numeric_value": num_val,
                    "unit": indicator["unit"],
                    "confidence": indicator.get("confidence"),
                    "evidence": indicator.get("evidence"),
                    "recorded_at": now_utc(),
                })

    def backfill_old_data(self, db: Database) -> dict:
        stats = {"logs_updated": 0, "logs_deleted": 0, "entities_updated": 0}

        old_logs = list(db.entity_logs.find({"value_type": {"$exists": False}}))
        for log in old_logs:
            raw = str(log.get("raw_value", "")).strip()
            value_type = classify_value_type(raw)
            old_status = log.get("status")
            old_color = log.get("color")

            if not value_type and old_status and str(old_status).strip().upper() not in ("", "UNKNOWN", "NULL"):
                value_type = "bool"
                raw = str(old_status).strip()
            if not value_type and old_color and str(old_color).strip().lower() not in ("", "null"):
                value_type = "color"
                raw = str(old_color).strip()

            if not value_type:
                db.entity_logs.delete_one({"_id": log["_id"]})
                stats["logs_deleted"] += 1
                continue

            update: dict[str, Any] = {"value_type": value_type, "metric": "value"}
            if value_type == "number":
                num = clean_numeric_value(raw)
                if num is not None:
                    update["raw_value"] = str(num)
                    update["numeric_value"] = num
            db.entity_logs.update_one({"_id": log["_id"]}, {"$set": update})
            stats["logs_updated"] += 1

        for entity in db.screen_entities.find():
            if entity.get("metrics"):
                continue
            recent_logs = list(db.entity_logs.find({"entity_id": entity["_id"]}).sort("recorded_at", -1).limit(20))
            metrics: dict[str, Any] = {}
            for entry in recent_logs:
                metric_key = entry.get("metric") or "value"
                if metric_key not in metrics:
                    metrics[metric_key] = {
                        "display_name": metric_key,
                        "unit": entry.get("unit") or entity.get("unit"),
                        "value_type": entry.get("value_type") or "number",
                        "last_value": entry.get("raw_value"),
                    }
            if metrics:
                db.screen_entities.update_one(
                    {"_id": entity["_id"]},
                    {"$set": {"metrics": metrics, "updated_at": now_utc()}},
                )
                stats["entities_updated"] += 1

        return stats

    def process_single_snapshot(self, db: Database, source: dict, monitor_key: str):
        job_id = self.create_job(db, source["_id"], monitor_key)
        self.update_job(db, job_id, "processing")
        try:
            image_bytes = fetch_snapshot_bytes(source, monitor_key if monitor_key != "default" else None)
            if not image_bytes:
                self.update_job(db, job_id, "failed", "No snapshot data received from KVM")
                return

            image_hash = hashlib.sha256(image_bytes).hexdigest()
            latest = db.snapshots.find_one(
                {"source_id": source["_id"], "monitor_key": monitor_key},
                sort=[("created_at", -1)],
            )
            if latest and latest.get("image_hash") == image_hash:
                self.update_job(db, job_id, "completed", "Duplicate snapshot skipped")
                return

            image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            histogram = histogram_feature(image)
            brightness = brightness_feature(image)
            group = self.pick_or_create_group(db, source, monitor_key, histogram, brightness)

            if group.get("ignored"):
                logger.info("Skipping extraction for ignored screen_group_id=%s name=%s", group["_id"], group.get("name"))
                self.update_job(db, job_id, "completed")
                return

            saved_path = self.save_snapshot(image_bytes, self.to_id(source["_id"]), monitor_key)

            
            # load entities for the screen group to build schema for LLM extraction

            existing_entities = list(db.screen_entities.find({"screen_group_id": group["_id"]}).sort("entity_key", 1))
            schema_str = None
            if existing_entities:
                schema_list = []
                for e in existing_entities:
                    schema_item = {
                        "main_entity_name": e.get("display_name"),
                        "type": e.get("entity_type") or "display",
                        "region": e.get("region") or "center",
                        "indicators": []
                    }
                    metrics = e.get("metrics") or {}
                    for mk, m in metrics.items():
                        schema_item["indicators"].append({
                            "label": m.get("indicator_label") or "",
                            "metric": mk,
                            "value_raw": "[unreadable]",
                            "value_number": None,
                            "unit": m.get("unit") or None,
                            "value_type": m.get("value_type", "text"),
                            "confidence": "Low",
                            "evidence": []
                        })
                    schema_list.append(schema_item)
                
                full_schema = {
                    "screen_title": group.get("name", ""),
                    "entity_count": len(schema_list),
                    "entities": schema_list
                }
                import json
                schema_str = json.dumps(full_schema, ensure_ascii=False, indent=2)
                print("Using schema for LLM extraction: \n", schema_str)

            # OCR + Information extraction with LLM
            markdown = None
            if schema_str:
                extracted_json = call_llm_markdown_to_json("", image_bytes, promptype='extract_from_schema_prompt', schema_str=schema_str)
            else:
                markdown = call_llm_image_to_markdown(image_bytes)
                extracted_json = call_llm_markdown_to_json(markdown) if markdown else {"screen_title": "", "entities": []}

            raw_llm_json_response = extracted_json.get("_raw_response") if isinstance(extracted_json, dict) else None
            llm_parse_error = extracted_json.get("_parse_error") if isinstance(extracted_json, dict) else None
            entity_count = len(extracted_json.get("entities") or []) if isinstance(extracted_json, dict) else 0

            logger.info(
                "LLM extraction summary source=%s monitor=%s markdown_chars=%d entities=%d parse_error=%s",
                source.get("name"),
                monitor_key,
                len(markdown or ""),
                entity_count,
                llm_parse_error,
            )
            if not entity_count:
                logger.warning(
                    "No entities extracted for source=%s monitor=%s. Raw JSON response preview: %s",
                    source.get("name"),
                    monitor_key,
                    (raw_llm_json_response or "")[:1000],
                )

            screen_name = ensure_llm_name(markdown or extracted_json.get("screen_title", ""), group.get("name", ""))
            final_screen_name = self.normalizer.normalize_screen_title(extracted_json, screen_name or group.get("name", ""))
            db.screen_groups.update_one({"_id": group["_id"]}, {"$set": {"name": final_screen_name, "updated_at": now_utc()}})

            snapshot = {
                "source_id": source["_id"],
                "screen_group_id": group["_id"],
                "monitor_key": monitor_key,
                "image_path": str(saved_path),
                "image_hash": image_hash,
                "histogram": histogram,
                "brightness_mean": brightness[0],
                "brightness_std": brightness[1],
                "markdown": markdown,
                "extracted_json": extracted_json,
                "raw_llm_json_response": raw_llm_json_response,
                "llm_parse_error": llm_parse_error,
                "llm_entity_count": entity_count,
                "created_at": now_utc(),
            }
            inserted = db.snapshots.insert_one(snapshot)
            snapshot["_id"] = inserted.inserted_id

            self.map_entities_and_log(db, snapshot, extracted_json)
            self.update_job(db, job_id, "completed")
            logger.info("Snapshot processed: %s/%s", source.get("name"), monitor_key)
        except Exception as exc:
            self.update_job(db, job_id, "failed", str(exc))
            logger.error("Snapshot failed: %s/%s: %s", source.get("name"), monitor_key, exc)
            raise

    async def poll_loop(self, db: Database, stop_event: asyncio.Event):
        cycle = 0
        while not stop_event.is_set():
            cycle += 1
            if cycle % 100 == 0:
                try:
                    self.cleanup_old_jobs(db)
                except Exception:
                    pass

            sources = list(db.kvm_sources.find({"enabled": True}))
            current = now_utc()
            for source in sources:
                poll_seconds = max(5, int(source.get("poll_seconds", POLL_INTERVAL)))
                last_polled_at = source.get("last_polled_at")
                due = last_polled_at is None or (current - last_polled_at) >= timedelta(seconds=poll_seconds)
                if not due:
                    continue
                for monitor_key in source.get("monitor_keys") or ["default"]:
                    try:
                        await asyncio.to_thread(self.process_single_snapshot, db, source, monitor_key)
                    except Exception as exc:
                        logger.error("Poll error %s/%s: %s", source.get("name"), monitor_key, exc)
                        continue
                db.kvm_sources.update_one(
                    {"_id": source["_id"]},
                    {"$set": {"last_polled_at": now_utc(), "updated_at": now_utc()}},
                )

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                continue

    def serialize_source(self, source: dict) -> dict:
        return {
            "id": self.to_id(source["_id"]),
            "name": source.get("name"),
            "host": source.get("host"),
            "port": source.get("port"),
            "base_path": source.get("base_path"),
            "poll_seconds": source.get("poll_seconds"),
            "enabled": source.get("enabled"),
            "monitor_keys": source.get("monitor_keys") or [],
            "similarity_threshold": source.get("similarity_threshold", 0.92),
            "mode": source.get("mode", "v1"),
            "last_polled_at": source.get("last_polled_at"),
        }

    def create_source(self, db: Database, payloads: List[dict]) -> str:
        now = now_utc()
        ids = []
        for payload in payloads:
            document = {
                "name": payload["name"],
                "host": payload["host"],
                "port": payload["port"],
                "base_path": payload.get("base_path") or "kx",
                "poll_seconds": max(5, int(payload.get("poll_seconds", POLL_INTERVAL))),
                "enabled": bool(payload.get("enabled", True)),
                "monitor_keys": payload.get("monitor_keys") or ["default"],
                "headers": payload.get("headers") or {},
                "similarity_threshold": min(0.999, max(0.5, float(payload.get("similarity_threshold", 0.92)))),
                "mode": payload.get("mode", "v1"),
                "last_polled_at": None,
                "created_at": now,
                "updated_at": now,
            }
            inserted = db.kvm_sources.insert_one(document)
            ids.append(str(inserted.inserted_id))
        return ids

    def get_source_or_none(self, db: Database, source_id: str) -> dict | None:
        try:
            return db.kvm_sources.find_one({"_id": self.oid(source_id)})
        except Exception:
            return None

    def list_screens(self, db: Database, source_id: str) -> list[dict]:
        source_obj_id = self.oid(source_id)
        groups = list(db.screen_groups.find({"source_id": source_obj_id}).sort("_id", 1))
        return [
            {
                "id": self.to_id(group["_id"]),
                "name": group.get("name"),
                "monitor_key": group.get("monitor_key"),
                "ignored": bool(group.get("ignored")),
                "snapshot_count": db.snapshots.count_documents({"screen_group_id": group["_id"]}),
            }
            for group in groups
        ]

    def list_entities(self, db: Database, screen_group_id: str) -> list[dict]:
        group_obj_id = self.oid(screen_group_id)
        entities = list(db.screen_entities.find({"screen_group_id": group_obj_id}).sort("entity_key", 1))
        rows: list[dict] = []
        for entity in entities:
            metrics = entity.get("metrics") or {}
            if not metrics and entity.get("last_value") is not None:
                value_type = classify_value_type(entity.get("last_value"))
                if value_type:
                    metrics = {
                        "value": {
                            "display_name": "value",
                            "unit": entity.get("unit"),
                            "value_type": value_type,
                            "last_value": entity.get("last_value"),
                        }
                    }
            rows.append({
                "id": self.to_id(entity["_id"]),
                "entity_key": entity.get("entity_key"),
                "display_name": entity.get("display_name"),
                "entity_type": entity.get("entity_type"),
                "metrics": metrics,
                "indicators": metrics,
                "last_seen_at": entity.get("last_seen_at"),
            })
        return rows

    def get_screen_preview(self, db: Database, screen_group_id: str) -> dict | None:
        group_obj_id = self.oid(screen_group_id)
        snapshot = db.snapshots.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
        if not snapshot:
            return None
        return {
            "snapshot_id": self.to_id(snapshot["_id"]),
            "created_at": snapshot.get("created_at"),
            "image_url": f"/api/snapshots/{self.to_id(snapshot['_id'])}/image",
            "processing_time_ms": snapshot.get("processing_time_ms")
        }

    def list_logs(self, db: Database, screen_group_id: str, since, entity_ids: list[str] | None, limit: int) -> list[dict]:
        group_obj_id = self.oid(screen_group_id)
        if entity_ids:
            entity_object_ids = [self.oid(entity_id) for entity_id in entity_ids]
        else:
            entity_object_ids = [row["_id"] for row in db.screen_entities.find({"screen_group_id": group_obj_id}, {"_id": 1})]
        if not entity_object_ids:
            return []

        entity_map = {entry["_id"]: entry for entry in db.screen_entities.find({"_id": {"$in": entity_object_ids}})}
        logs = list(
            db.entity_logs.find(
                {"entity_id": {"$in": entity_object_ids}, "recorded_at": {"$gte": since}}
            ).sort("recorded_at", -1).limit(limit)
        )

        rows: list[dict] = []
        for log in logs:
            entity = entity_map.get(log.get("entity_id"))
            if not entity:
                continue
            value_type = log.get("value_type")
            unit = log.get("unit") or entity.get("unit")
            raw = log.get("raw_value", "")
            if not value_type:
                value_type = classify_value_type(raw) or "unknown"
            metric_key = log.get("metric") or "value"
            metric_label = log.get("indicator_label") or log.get("metric_name") or metric_key
            rows.append({
                "log_id": self.to_id(log["_id"]),
                "entity_id": self.to_id(entity["_id"]),
                "entity_key": entity.get("entity_key"),
                "entity_name": entity.get("display_name"),
                "entity_type": entity.get("entity_type"),
                "metric": metric_label,
                "metric_key": metric_key,
                "value_type": value_type,
                "value": raw,
                "numeric_value": log.get("numeric_value"),
                "unit": unit,
                "confidence": log.get("confidence"),
                "evidence": log.get("evidence"),
                "snapshot_id": self.to_id(log["snapshot_id"]) if log.get("snapshot_id") else None,
                "recorded_at": log.get("recorded_at"),
            })
        return rows

    def get_timeseries(self, db: Database, screen_group_id: str, since, entity_ids: list[str] | None = None) -> dict[str, Any]:
        group_obj_id = self.oid(screen_group_id)
        if entity_ids:
            entity_object_ids = [self.oid(entity_id) for entity_id in entity_ids]
            entities = list(db.screen_entities.find({"_id": {"$in": entity_object_ids}}))
        else:
            entities = list(db.screen_entities.find({"screen_group_id": group_obj_id}))

        if not entities:
            return {}

        entity_map = {entity["_id"]: entity for entity in entities}
        logs = list(
            db.entity_logs.find(
                {
                    "entity_id": {"$in": list(entity_map.keys())},
                    "recorded_at": {"$gte": since},
                    "$or": [
                        {"value_type": "number"},
                        {"value_type": "bool"},
                        {"value_type": {"$exists": False}, "numeric_value": {"$ne": None}},
                    ],
                }
            ).sort("recorded_at", 1)
        )

        result: dict[str, Any] = {}
        for log in logs:
            entity = entity_map.get(log.get("entity_id"))
            if not entity:
                continue
            metric_key = log.get("metric") or "value"
            metric_label = log.get("indicator_label") or log.get("metric_name") or metric_key
            series_key = f"{entity.get('entity_key')}:{metric_key}"
            if series_key not in result:
                unit = log.get("unit") or entity.get("unit")
                result[series_key] = {
                    "name": f"{entity.get('display_name')} — {metric_label}",
                    "entity_name": entity.get("display_name"),
                    "metric": metric_key,
                    "metric_label": metric_label,
                    "unit": unit,
                    "points": [],
                }
            y_val = log.get("numeric_value")
            if y_val is None:
                if log.get("value_type") == "bool":
                    raw_val_lower = str(log.get("raw_value", "")).strip().lower()
                    if raw_val_lower in _BOOL_TRUE_VALUES:
                        y_val = 1
                    elif raw_val_lower in _BOOL_FALSE_VALUES:
                        y_val = 0
                else:
                    y_val = clean_numeric_value(log.get("raw_value"))
            if y_val is not None:
                result[series_key]["points"].append({"t": log.get("recorded_at"), "y": y_val})
        return result

    def latest_snapshots(self, db: Database, source_id: str, limit: int) -> list[dict]:
        source_obj_id = self.oid(source_id)
        rows = list(db.snapshots.find({"source_id": source_obj_id}).sort("created_at", -1).limit(limit))
        return [
            {
                "id": self.to_id(snapshot["_id"]),
                "screen_group_id": self.to_id(snapshot["screen_group_id"]),
                "monitor_key": snapshot.get("monitor_key"),
                "image_path": snapshot.get("image_path"),
                "created_at": snapshot.get("created_at"),
            }
            for snapshot in rows
        ]
        
        