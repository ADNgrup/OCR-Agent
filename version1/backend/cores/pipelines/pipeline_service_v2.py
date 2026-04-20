from __future__ import annotations

import asyncio
import difflib
import hashlib
import io
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, List
import unicodedata
import numpy as np

import cv2




from bson import ObjectId
from PIL import Image
from pymongo.database import Database


from cores.config import POLL_INTERVAL, SNAPSHOT_DIR
from utils.common import classify_value_type, clean_numeric_value, now_utc, _BOOL_TRUE_VALUES, _BOOL_FALSE_VALUES
from utils.image_features import average_fingerprint, brightness_feature, histogram_feature, similarity_score
from utils.kvm_client import fetch_snapshot_bytes
from cores.services.llm_client import call_llm_image_to_markdown, call_llm_markdown_to_json, ensure_llm_name
from cores.services.llm_client import call_llm_v2_extract
from cores.services import ocr
from . import pipeline_service, pipeline_utils, per_write_detector
import logging

logger = logging.getLogger("pipeline_v2")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

class PipelineServiceV2(pipeline_service.PipelineService):
    """
    new version of pipeline_service that will replace the old one. Main changes:
    - Refactor the main pipeline loop to be more modular and easier to read
    """

    def __init__(self):
        super().__init__()
        self.normalizer = pipeline_utils.EntityExtractionNormalizer()
        

    def _classify_snapshot(self, db: Database, source: dict, job_id, monitor_key: str):
            image_bytes = fetch_snapshot_bytes(source, monitor_key if monitor_key != "default" else None)
            if not image_bytes:
                self.update_job(db, job_id, "failed", f"No snapshot data received from KVM {monitor_key}")
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
            import base64
            image_base64 = base64.b64encode(image_bytes).decode('ascii')
            
            snapshot = {
                "source_id": source["_id"],
                "screen_group_id": group["_id"],
                "monitor_key": monitor_key,
                "image_hash": image_hash,
                "histogram": histogram,
                "brightness_mean": brightness[0],
                "brightness_std": brightness[1],
                "image_base64": f"data:image/jpeg;base64,{image_base64}",
                "created_at": now_utc(),
            }
            inserted = db.snapshots.insert_one(snapshot)
            snapshot["_id"] = inserted.inserted_id

            return group, image_bytes, snapshot

    def process_single_snapshot(self, db: Database, source: dict, monitor_key: str):
        import time
        start_time = time.time()
        logger.info("Processing snapshot V2 for source=%s monitor=%s", source.get("name"), monitor_key)
        job_id = self.create_job(db, source["_id"], monitor_key)
        self.update_job(db, job_id, "processing")
        try:
            result = self._classify_snapshot(db, source, job_id, monitor_key)
            if not result:
                return # Either failed or duplicate, job already updated

            group, image_bytes, snapshot = result
            saved_path = self.save_snapshot(image_bytes, self.to_id(source["_id"]), monitor_key)
            
            if group.get("ignored"):
                logger.info("Skipping extraction for ignored screen_group_id=%s name=%s", group["_id"], group.get("name"))      
                self.update_job(db, job_id, "completed")
                return

            existing_schema = group.get("entity_schema", [])
            schema_str = None
            if existing_schema:
                full_schema = {
                    "screen_title": group.get("name", ""),
                    "entity_count": len(existing_schema),
                    "entities": existing_schema
                }
                import json
                schema_str = json.dumps(full_schema, ensure_ascii=False, indent=2)
                logger.info("Using existing entity_schema for LLM extraction.")

            # OCR + Information extraction with LLM
            layout_text = ocr.generate_layout_text(str(saved_path))
            extracted_json = call_llm_v2_extract(image_bytes, layout_text, schema_str)

            raw_llm_json_response = extracted_json.get("_raw_response") if isinstance(extracted_json, dict) else None
            llm_parse_error = extracted_json.get("_parse_error") if isinstance(extracted_json, dict) else None
            entities = extracted_json.get("entities") or [] if isinstance(extracted_json, dict) else []

            # Post-process Markdown tables to generate subentities list
            for ent in entities:
                if str(ent.get("type", "")).lower() == "table" and "markdown" in ent:
                    md_text = ent.get("markdown", "").strip()
                    metadata = ent.get("metadata", {})
                    sub_list = []
                    lines = md_text.split("\n")
                    if len(lines) >= 3:
                        # Extract headers
                        headers = [h.strip() for h in lines[0].split("|")[1:-1]]
                        
                        row_name_col_indices = []
                        value_col_indices = []
                        
                        val_cols = metadata.get("value_columns", [])
                        
                        for i, h in enumerate(headers):
                            if h.lower() in ["no", "no.", "index"]:
                                continue
                            if h in val_cols or value_col_indices:
                                value_col_indices.append(i)
                            else:
                                row_name_col_indices.append(i)
                                
                        if not row_name_col_indices and headers:
                           row_name_col_indices = [0]
                           if 0 in value_col_indices: value_col_indices.remove(0)
                        if not value_col_indices and len(headers) > 1:
                           value_col_indices = list(range(1, len(headers)))

                        for row_line in lines[2:]:
                            cells = [c.strip() for c in row_line.split("|")[1:-1]]
                            if not cells: continue
                            
                            row_name_parts = []
                            for idx in row_name_col_indices:
                                if idx < len(cells):
                                    val = cells[idx]
                                    # Fallback to ignore digits if it's the very first column and likely an STT missed by header
                                    if idx == 0 and val.isdigit():
                                        continue
                                    if val and val != "-":
                                        row_name_parts.append(val)
                            row_name = " ".join(row_name_parts) if row_name_parts else "Unknown"

                            for col_idx in value_col_indices:
                                if col_idx < len(cells):
                                    cell_value = cells[col_idx]
                                    col_name = headers[col_idx]
                                    val_num = None
                                    try:
                                        import re
                                        # Remove all non-numeric chars except digits, period, minus
                                        num_str = re.sub(r'[^\d\.\-]', '', cell_value)
                                        if num_str and num_str != "-":
                                            val_num = float(num_str)
                                    except:
                                        pass
                                    sub_list.append({
                                        "col": col_name,
                                        "row": row_name,
                                        "value_raw": cell_value,
                                        "value_number": val_num,
                                        "unit": metadata.get("unit", ""),
                                        "value_type": metadata.get("value_type", "number" if val_num is not None else "text")
                                    })
                    ent["subentities"] = sub_list

            logger.info(
                "LLM extraction summary source=%s monitor=%s layout_chars=%d entities=%d parse_error=%s",
                source.get("name"),
                monitor_key,
                len(layout_text or ""),
                len(entities),
                llm_parse_error,
            )

            # If no schema existed, extract schema from result and update group
            if not existing_schema and entities:
                new_schema = []
                for ent in entities:
                    import uuid
                    ent_schema = {
                        "id": f"{str(group['_id'])}_ent_{uuid.uuid4().hex[:6]}",
                        "main_entity_name": ent.get("main_entity_name"),
                        "type": ent.get("type", "HMI Object"),
                        "region": ent.get("region", "center")
                    }
                    if str(ent_schema["type"]).lower() == "table":
                        ent_schema["subentities"] = []
                        for sub in ent.get("subentities", []):
                            ent_schema["subentities"].append({
                                "col": sub.get("col", ""),
                                "row": sub.get("row", ""),
                                "value_type": sub.get("value_type", "text"),
                                "unit": sub.get("unit", "")
                            })
                    elif str(ent_schema["type"]).lower() in ["log/alert", "log"]:
                        pass
                    else:
                        ent_schema["indicators"] = []
                        for ind in ent.get("indicators", []):
                            ent_schema["indicators"].append({
                                "label": ind.get("label", ""),
                                "metric": ind.get("metric", ""),
                                "value_type": ind.get("value_type", "text"),
                                "unit": ind.get("unit", "")
                            })
                    new_schema.append(ent_schema)
                db.screen_groups.update_one({"_id": group["_id"]}, {"$set": {"entity_schema": new_schema, "updated_at": now_utc()}})

            screen_name = ensure_llm_name(extracted_json.get("screen_title", "") if isinstance(extracted_json, dict) else "", group.get("name", ""))
            final_screen_name = self.normalizer.normalize_screen_title(extracted_json, screen_name or group.get("name", ""))    
            db.screen_groups.update_one({"_id": group["_id"]}, {"$set": {"name": final_screen_name, "updated_at": now_utc()}})  
            
            # Save extracted values into snapshot
            processing_time_ms = int((time.time() - start_time) * 1000)
            db.snapshots.update_one(
                {"_id": snapshot["_id"]},
                {"$set": {
                    "entities_values": entities,
                    "llm_parse_error": llm_parse_error,
                    "extracted_at": now_utc(),
                    "processing_time_ms": processing_time_ms
                }}
            )
            
            self.update_job(db, job_id, "completed")
            logger.info("Snapshot processed: %s/%s", source.get("name"), monitor_key)
        except Exception as exc:
            self.update_job(db, job_id, "failed", str(exc))
            logger.error("Snapshot failed: %s/%s: %s", source.get("name"), monitor_key, exc)
            raise

    def list_entities(self, db: Database, screen_group_id: str) -> list[dict]:
        group_obj_id = self.oid(screen_group_id)
        group = db.screen_groups.find_one({"_id": group_obj_id})
        if not group: return []
        
        schema = group.get("entity_schema", [])
        latest_snap = db.snapshots.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
        latest_vals = latest_snap.get("entities_values", []) if latest_snap else []
        val_map = {v.get("main_entity_name"): v for v in latest_vals if v.get("main_entity_name")}
        
        rows = []
        for i, entity in enumerate(schema):
            ent_name = entity.get("main_entity_name")
            evals = val_map.get(ent_name, {})
            
            ent_dict = {
                "id": entity.get("id") or (str(group_obj_id) + f"_ent_{i}"),
                "entity_key": self.normalizer.slugify(ent_name),
                "display_name": ent_name,
                "entity_type": entity.get("type"),
                "region": entity.get("region"),
                "indicators": {},
                "metrics": {},
                "subentities": [],
                "logs": []
            }
            
            if entity.get("type", "").lower() == "table":
                sub_vals = { f"{s.get('col')}_{s.get('row')}": s for s in evals.get("subentities", []) }
                for sub in entity.get("subentities", []):
                    key = f"{sub.get('col')}_{sub.get('row')}"
                    v = sub_vals.get(key, {})
                    ent_dict["subentities"].append({
                        "col": sub.get("col"),
                        "row": sub.get("row"),
                        "unit": sub.get("unit"),
                        "value_type": sub.get("value_type", "number"),
                        "value_raw": v.get("value_raw", "[unreadable]"),
                        "value_number": v.get("value_number", None)
                    })
            elif entity.get("type", "").lower() in ["log/alert", "log"]:
                ent_dict["logs"] = evals.get("logs", [])
            else:
                ind_vals = { s.get("metric") or s.get("label"): s for s in evals.get("indicators", []) }
                for ind in entity.get("indicators", []):
                    key = ind.get("metric") or ind.get("label")
                    v = ind_vals.get(key, {})
                    ent_dict["metrics"][key] = {
                        "indicator_label": ind.get("label"),
                        "metric_key": ind.get("metric"),
                        "unit": ind.get("unit"),
                        "value_type": ind.get("value_type", "text"),
                        "last_value": v.get("value_raw", "[unreadable]"),
                        "last_number": v.get("value_number", None)
                    }
                    ent_dict["indicators"][key] = ent_dict["metrics"][key]
            rows.append(ent_dict)
        return rows

    def get_screen_preview(self, db: Database, screen_group_id: str) -> dict | None:
        group_obj_id = self.oid(screen_group_id)
        snapshot = db.snapshots.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
        if not snapshot:
            return None
        url = snapshot.get("image_base64")
        if not url: url = f"/api/snapshots/{self.to_id(snapshot['_id'])}/image"
        return {
            "snapshot_id": self.to_id(snapshot["_id"]),
            "created_at": snapshot.get("created_at"),
            "image_url": url,
        }

    def list_logs(self, db: Database, screen_group_id: str, since, entity_ids: list[str] | None, limit: int) -> list[dict]:
        group_obj_id = self.oid(screen_group_id)
        snaps = list(db.snapshots.find(
            {"screen_group_id": group_obj_id, "created_at": {"$gte": since}},
            sort=[("created_at", -1)],
            limit=limit
        ))
        
        logs = []
        for snap in snaps:
            for ent in snap.get("entities_values", []):
                ent_name = ent.get("main_entity_name")
                etype = ent.get("type", "HMI Object")
                
                if str(etype).lower() == "table":
                    for sub in ent.get("subentities", []):
                        logs.append({
                            "log_id": str(snap["_id"]) + "_" + str(ent_name) + "_" + str(sub.get("col")) + "_" + str(sub.get("row")),
                            "entity_name": ent_name,
                            "metric": f"{sub.get('col')}_{sub.get('row')}",
                            "value": sub.get("value_raw"),
                            "numeric_value": sub.get("value_number"),
                            "value_type": sub.get("value_type", "text"),
                            "recorded_at": snap.get("created_at")
                        })
                elif str(etype).lower() in ["log/alert", "log"]:
                    for idx, lg in enumerate(ent.get("logs", [])):
                        logs.append({
                            "log_id": str(snap["_id"]) + "_" + str(ent_name) + "_" + str(idx),
                            "entity_name": ent_name,
                            "metric": "log",
                            "value": f"[{lg.get('time')}] {lg.get('name')}: {lg.get('desc')}",
                            "value_type": "text",
                            "recorded_at": snap.get("created_at")
                        })
                else:
                    for ind in ent.get("indicators", []):
                        logs.append({
                            "log_id": str(snap["_id"]) + "_" + str(ent_name) + "_" + str(ind.get("metric")),
                            "entity_name": ent_name,
                            "metric": ind.get("label") or ind.get("metric"),
                            "value": ind.get("value_raw"),
                            "numeric_value": ind.get("value_number"),
                            "value_type": ind.get("value_type", "text"),
                            "recorded_at": snap.get("created_at")
                        })
                        
        return logs[:limit]

    def get_timeseries(self, db: Database, screen_group_id: str, since, entity_ids: list[str] | None = None) -> dict[str, Any]:
        logs = self.list_logs(db, screen_group_id, since, entity_ids, limit=2000)
        logs.reverse() # oldest to newest
        
        result = {}
        for log in logs:
            if log.get("value_type") not in ["number", "bool"]:
                continue
            
            series_key = f"{log.get('entity_name')}:{log.get('metric')}"
            if series_key not in result:
                result[series_key] = {
                    "name": f"{log.get('entity_name')} - {log.get('metric')}",
                    "entity_name": log.get("entity_name"),
                    "metric": log.get("metric"),
                    "metric_label": log.get("metric"),
                    "unit": "",
                    "points": []
                }
            
            y_val = log.get("numeric_value")
            if y_val is None and log.get("value_type") == "bool":
                raw_val = str(log.get("value")).strip().lower()
                if raw_val in _BOOL_TRUE_VALUES: y_val = 1
                elif raw_val in _BOOL_FALSE_VALUES: y_val = 0
            
            if y_val is not None:
                result[series_key]["points"].append({
                    "t": log.get("recorded_at"),
                    "y": y_val
                })
                
        return result


