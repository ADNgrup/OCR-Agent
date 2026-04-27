from __future__ import annotations

import asyncio
import difflib
import hashlib
import io
import json
import logging
import re
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, List
from uuid import uuid4
import unicodedata
import numpy as np

import cv2




from bson import ObjectId
from bson.binary import Binary
from PIL import Image
from pymongo import MongoClient
from pymongo.database import Database


from cores.config import POLL_INTERVAL, SNAPSHOT_DIR
from cores.schema_mongo import MONGO_URI
from utils.common import classify_value_type, clean_numeric_value, now_utc, _BOOL_TRUE_VALUES, _BOOL_FALSE_VALUES
from utils.image_features import average_fingerprint, brightness_feature, histogram_feature, similarity_score, autocrop_image
from utils.kvm_client import fetch_snapshot_bytes
from cores.services.llm_client import (
    call_llm_image_to_markdown,
    call_llm_markdown_to_json,
    call_llm_segment,
    ensure_llm_name,
)
from cores.services.llm_client import call_llm_v2_extract
from cores.services import ocr
from . import pipeline_service, pipeline_utils, per_write_detector

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
    V2 pipeline flow:
    1. Receive snapshot.
    2. Match against existing samples from the same source.
       - matched + schema: OCR by segmentation.
       - matched + no schema: ignore.
       - no match: queue as new sample and ignore.
    3. Save OCR result to `ocr_results` and push to external dashboard.
    """

    def __init__(self):
        super().__init__()
        self.normalizer = pipeline_utils.EntityExtractionNormalizer()
        self._dashboard_insert_fn = None
        self._dashboard_insert_import_attempted = False

    def _get_dashboard_insert_fn(self):
        if self._dashboard_insert_import_attempted:
            return self._dashboard_insert_fn

        self._dashboard_insert_import_attempted = True
        try:
            from push_to_gafana import insert_ocr_result as insert_fn
            self._dashboard_insert_fn = insert_fn
        except Exception as exc:
            self._dashboard_insert_fn = None
            logger.warning("Dashboard push integration is unavailable: %s", exc)
        return self._dashboard_insert_fn

    @staticmethod
    def _normalize_area_token(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return "unknown"
        text = re.sub(r"\s+", "", text)
        text = text.replace("/", "_")
        return text

    def _iter_dashboard_payload_rows(self, source: dict, snapshot: dict, entities: list[dict[str, Any]]):
        kvm_port = source.get("port")
        if kvm_port is None or str(kvm_port).strip() == "":
            kvm_port = "unknown"

        screen_group_id = snapshot.get("screen_group_id")
        if screen_group_id is None:
            screen_group_id = "unknown"

        screen_id = f"{self._normalize_area_token(kvm_port)}_{self.to_id(screen_group_id)}"

        for ent in entities or []:
            if not isinstance(ent, dict):
                continue

            seg_id_raw = ent.get("segment_id") or ent.get("id") or "Segment"
            seg_id = self._normalize_area_token(seg_id_raw)
            seg_type = self._normalize_object_type(ent.get("type"))
            value = ent.get("value") if isinstance(ent.get("value"), dict) else {}

            if seg_type == "log tables":
                logs = value.get("logs") if isinstance(value.get("logs"), list) else []
                if not logs:
                    logs = ent.get("logs") if isinstance(ent.get("logs"), list) else []

                if not logs:
                    raw_csv = str(value.get("raw_csv_table") or ent.get("raw_csv_table") or "").strip()
                    if raw_csv:
                        try:
                            import csv
                            rows = list(csv.reader(io.StringIO(raw_csv)))
                            if rows:
                                header = [str(col).strip().lower() for col in rows[0]]
                                msg_idx = header.index("message") if "message" in header else (1 if len(header) > 1 else 0)
                                for row in rows[1:]:
                                    message_text = str(row[msg_idx]).strip() if msg_idx < len(row) else ""
                                    if message_text:
                                        logs.append({"message": message_text})
                        except Exception:
                            pass

                area_id = f"{seg_id}_message"
                for lg in logs:
                    if not isinstance(lg, dict):
                        continue
                    msg = str(lg.get("message") or lg.get("desc") or lg.get("msg") or lg.get("name") or "").strip()
                    if not msg:
                        continue
                    yield screen_id, area_id, "text", msg
                continue

            fallback_columns = [str(col).strip() for col in (ent.get("columns") or []) if str(col).strip()]
            fallback_rows = [str(row).strip() for row in (ent.get("rows") or []) if str(row).strip()]
            if seg_type != "fixed table" and not fallback_rows:
                fallback_rows = ["value"]

            _, _, sub_rows = self._expand_table_value(
                value,
                fallback_columns=fallback_columns,
                fallback_rows=fallback_rows,
            )

            if not sub_rows and isinstance(ent.get("subentities"), list):
                sub_rows = self._normalize_table_subentities(
                    ent.get("subentities") or [],
                    columns=fallback_columns or None,
                    rows=fallback_rows or None,
                )

            for sub in sub_rows:
                if not isinstance(sub, dict):
                    continue

                raw_value = sub.get("value_raw", sub.get("value"))
                explicit_number = sub.get("value_number")
                if raw_value is None and explicit_number is None:
                    continue
                if isinstance(raw_value, str) and raw_value.strip() == "" and explicit_number is None:
                    continue

                col_name = self._normalize_area_token(sub.get("col"))
                row_name = self._normalize_area_token(sub.get("row") or "value")
                if col_name == "unknown":
                    continue

                number, _ = self._coerce_numeric_pair(
                    raw_value,
                    explicit_number,
                )

                if seg_type == "scada object":
                    area_id = f"{seg_id}_{col_name}"
                else:
                    area_id = f"{seg_id}_{row_name}_{col_name}"

                yield screen_id, area_id, "number", str(number)

    def _push_ocr_result_to_dashboard(self, source: dict, snapshot: dict, entities: list[dict[str, Any]]) -> None:
        insert_fn = self._get_dashboard_insert_fn()
        if insert_fn is None:
            return

        for screen_id, area_id, type_value, ocr_value in self._iter_dashboard_payload_rows(source, snapshot, entities):
            if ocr_value is None:
                continue
            if isinstance(ocr_value, str) and ocr_value.strip() == "":
                continue
            try:
                insert_fn(screen_id, area_id, type_value, ocr_value)
            except Exception as exc:
                logger.warning(
                    "Dashboard push failed (screen_id=%s, area_id=%s): %s",
                    screen_id,
                    area_id,
                    exc,
                )

    @staticmethod
    def _clamp_pct(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
        except Exception:
            out = default
        return max(0.0, min(100.0, out))

    def _normalize_bbox(self, bbox: dict | None) -> dict[str, float]:
        src = bbox if isinstance(bbox, dict) else {}
        return {
            "x": self._clamp_pct(src.get("x", 10), 10),
            "y": self._clamp_pct(src.get("y", 10), 10),
            "w": self._clamp_pct(src.get("w", 20), 20),
            "h": self._clamp_pct(src.get("h", 20), 20),
        }

    @staticmethod
    def _normalize_list(values: Any) -> list[str]:
        if not isinstance(values, list):
            return []
        return [str(item).strip() for item in values if str(item).strip()]

    @staticmethod
    def _normalize_object_type(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"log", "log/alert", "log table", "log tables"}:
            return "log tables"
        if text in {"table", "fixed table", "fixed table object"}:
            return "fixed table"
        return "scada object"

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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

    def _normalize_segment_schema_identity(self, segments: list[dict]) -> list[dict]:
        normalized: list[dict] = []
        used_names: set[str] = set()
        used_ids: set[str] = set()

        for idx, seg in enumerate(segments):
            if not isinstance(seg, dict):
                continue
            row = dict(seg)
            base_name = str(row.get("name") or f"Segment {idx + 1}").strip() or f"Segment {idx + 1}"
            seg_name = self._unique_segment_name(base_name, used_names)
            seg_id = self._unique_segment_id(
                self._segment_name_to_pascal_id(seg_name, fallback=f"Segment{idx + 1}"),
                used_ids,
            )
            row["name"] = seg_name
            row["id"] = seg_id
            normalized.append(row)

        return normalized

    def _find_best_group_match(
        self,
        db: Database,
        source: dict,
        monitor_key: str,
        histogram: list[float],
        brightness: tuple[float, float],
    ) -> tuple[dict | None, float]:
        groups = list(db.screen_groups.find({"source_id": source["_id"]}))
        best_group = None
        best_score = -1.0

        for group in groups:
            fp = group.get("fingerprint") or {}
            ref_hist = fp.get("histogram") or []
            if not ref_hist:
                continue
            ref_brightness_raw = fp.get("brightness") or [0.0, 0.0]
            ref_brightness = (
                float(ref_brightness_raw[0]) if len(ref_brightness_raw) > 0 else 0.0,
                float(ref_brightness_raw[1]) if len(ref_brightness_raw) > 1 else 0.0,
            )
            score = similarity_score(histogram, ref_hist, brightness, ref_brightness)
            if str(group.get("monitor_key") or "default") == monitor_key:
                score = min(1.0, score + 0.01)
            if score > best_score:
                best_score = score
                best_group = group

        threshold = float(source.get("similarity_threshold") or 0.92)
        if best_group and best_score >= threshold:
            return best_group, best_score
        return None, best_score

    def _append_sample_to_group(
        self,
        db: Database,
        group: dict,
        histogram: list[float],
        brightness: tuple[float, float],
    ) -> dict:
        updated_fp = average_fingerprint(group.get("fingerprint") or {}, histogram, brightness)
        db.screen_groups.update_one(
            {"_id": group["_id"]},
            {
                "$set": {
                    "fingerprint": updated_fp,
                    "updated_at": now_utc(),
                },
            },
        )
        return db.screen_groups.find_one({"_id": group["_id"]}) or group

    def _queue_new_unclassified_group(
        self,
        db: Database,
        source: dict,
        monitor_key: str,
        histogram: list[float],
        brightness: tuple[float, float],
    ) -> dict:
        now = now_utc()
        group = {
            "source_id": source["_id"],
            "monitor_key": monitor_key,
            "name": f"queued_{monitor_key}_{int(now.timestamp())}",
            "schema_status": "unclassified",
            "segmentation_schema": [],
            "queue_status": "pending_schema",
            "fingerprint": {"histogram": histogram, "brightness": [brightness[0], brightness[1]]},
            "created_at": now,
            "updated_at": now,
        }
        inserted = db.screen_groups.insert_one(group)
        group["_id"] = inserted.inserted_id
        db.sample_queue.insert_one(
            {
                "source_id": source["_id"],
                "screen_group_id": group["_id"],
                "monitor_key": monitor_key,
                "status": "pending_schema",
                "created_at": now,
                "updated_at": now,
            }
        )
        return group

    def _insert_snapshot(
        self,
        db: Database,
        source: dict,
        group: dict,
        monitor_key: str,
        image_bytes: bytes,
        image_hash: str,
        histogram: list[float],
        brightness: tuple[float, float],
        saved_path: Path,
    ) -> dict:
        snapshot = {
            "screen_group_id": group.get("_id"),
            "content_type": "image/png",
            "image_bytes": Binary(image_bytes),
            "created_at": now_utc(),
        }
        inserted = db.snapshots.insert_one(snapshot)
        snapshot["_id"] = inserted.inserted_id
        return snapshot

    def _crop_segment_bytes(self, image: Image.Image, bbox: dict[str, float]) -> bytes:
        norm = self._normalize_bbox(bbox)
        width, height = image.size

        x1 = int(round((norm["x"] / 100.0) * width))
        y1 = int(round((norm["y"] / 100.0) * height))
        x2 = int(round(((norm["x"] + norm["w"]) / 100.0) * width))
        y2 = int(round(((norm["y"] + norm["h"]) / 100.0) * height))

        x1 = max(0, min(width - 1, x1))
        y1 = max(0, min(height - 1, y1))
        x2 = max(x1 + 1, min(width, x2))
        y2 = max(y1 + 1, min(height, y2))

        cropped = image.crop((x1, y1, x2, y2))
        buffer = io.BytesIO()
        cropped.save(buffer, format="PNG")
        return buffer.getvalue()

    @staticmethod
    def _normalize_numeric_scalar(value: Any) -> int | float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            f_val = float(value)
            if not np.isfinite(f_val):
                return None
            return int(f_val) if f_val.is_integer() else f_val

        text = str(value).strip()
        if not text:
            return None

        lowered = text.lower()
        if lowered == "on":
            return 1
        if lowered == "off":
            return 0
        if lowered in _BOOL_TRUE_VALUES:
            return 1
        if lowered in _BOOL_FALSE_VALUES:
            return 0

        parsed = clean_numeric_value(text)
        if parsed is None:
            return None

        parsed_f = float(parsed)
        return int(parsed_f) if parsed_f.is_integer() else parsed_f

    def _coerce_numeric_pair(self, raw_value: Any, explicit_number: Any = None, default: float = 0.0) -> tuple[int | float, str]:
        for candidate in (explicit_number, raw_value):
            number = self._normalize_numeric_scalar(candidate)
            if number is not None:
                return number, str(number)

        fallback = int(default) if float(default).is_integer() else float(default)
        return fallback, str(fallback)

    def _normalize_indicator_values(
        self,
        indicators: list[Any],
        expected_labels: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for idx, ind in enumerate(indicators or []):
            if not isinstance(ind, dict):
                continue

            label = str(ind.get("label") or ind.get("metric") or "").strip()
            if not label and expected_labels and idx < len(expected_labels):
                label = str(expected_labels[idx]).strip()
            if not label:
                continue

            source_raw = ind.get("value_raw")
            if source_raw is None or source_raw == "":
                source_raw = ind.get("value")

            number, value_raw = self._coerce_numeric_pair(source_raw, ind.get("value_number"))
            key = self._entity_key(label)
            if key in seen_keys:
                continue

            normalized.append(
                {
                    "label": label,
                    "value_type": "number",
                    "unit": str(ind.get("unit") or ""),
                    "value_raw": value_raw,
                    "value_number": number,
                }
            )
            if key:
                seen_keys.add(key)

        for label in expected_labels or []:
            text = str(label).strip()
            if not text:
                continue
            key = self._entity_key(text)
            if key and key in seen_keys:
                continue
            normalized.append(
                {
                    "label": text,
                    "value_type": "number",
                    "unit": "",
                    "value_raw": "0",
                    "value_number": 0,
                }
            )
            if key:
                seen_keys.add(key)

        return normalized

    def _normalize_table_subentities(
        self,
        subentities: list[Any],
        columns: list[str] | None = None,
        rows: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        existing_cells: set[tuple[str, str]] = set()
        allowed_cols = {str(col).strip() for col in (columns or []) if str(col).strip()}
        allowed_rows = {str(row).strip() for row in (rows or []) if str(row).strip()}
        default_row = next(iter(allowed_rows)) if len(allowed_rows) == 1 else None

        for sub in subentities or []:
            if not isinstance(sub, dict):
                continue

            col = str(sub.get("col") or "").strip()
            row = str(sub.get("row") or "Unknown").strip() or "Unknown"
            if not col:
                continue

            if allowed_cols and col not in allowed_cols:
                continue
            if allowed_rows and row not in allowed_rows:
                if default_row is not None:
                    row = default_row
                else:
                    continue

            source_raw = sub.get("value_raw")
            if source_raw is None or source_raw == "":
                source_raw = sub.get("value")

            number, value_raw = self._coerce_numeric_pair(source_raw, sub.get("value_number"))

            normalized.append(
                {
                    "col": col,
                    "row": row,
                    "value_raw": value_raw,
                    "value_number": number,
                    "unit": str(sub.get("unit") or ""),
                    "value_type": "number",
                }
            )
            existing_cells.add((row, col))

        if rows and columns:
            for row in rows:
                row_name = str(row).strip()
                if not row_name:
                    continue
                for col in columns:
                    col_name = str(col).strip()
                    if not col_name:
                        continue
                    key = (row_name, col_name)
                    if key in existing_cells:
                        continue
                    normalized.append(
                        {
                            "col": col_name,
                            "row": row_name,
                            "value_raw": "0",
                            "value_number": 0,
                            "unit": "",
                            "value_type": "number",
                        }
                    )
                    existing_cells.add(key)

        return normalized

    @staticmethod
    def _split_pipe_values(value: Any) -> list[str]:
        text = str(value or "").strip()
        if not text:
            return []
        return [part.strip() for part in text.split("|")]

    def _table_metric_label(self, row: str, col: str) -> str:
        row_text = str(row or "").strip()
        col_text = str(col or "").strip()
        if row_text.lower() in {"value", "default"}:
            return col_text or "value"
        if not row_text:
            return col_text or "value"
        if not col_text:
            return row_text
        return f"{row_text} / {col_text}"

    def _table_metric_key(self, row: str, col: str) -> str:
        label = self._table_metric_label(row, col)
        key = self.normalizer.slugify(label)
        if key:
            return key
        fallback = f"{str(row or '').strip()}_{str(col or '').strip()}".strip("_")
        return self.normalizer.slugify(fallback) or "value"

    def _table_metadata(self, columns: list[str], rows: list[str], unit_by_col: dict[str, str] | None = None) -> dict[str, Any]:
        col_names = [str(col).strip() for col in (columns or []) if str(col).strip()]
        row_names = [str(row).strip() for row in (rows or []) if str(row).strip()]
        if not row_names:
            row_names = ["value"]
        unit_lookup = unit_by_col or {}
        unit_text = "|".join([str(unit_lookup.get(col, "") or "") for col in col_names]) if col_names else ""
        type_text = "|".join(["number" for _ in col_names]) if col_names else ""
        return {
            "value_columns": col_names,
            "rows": row_names,
            "unit": unit_text,
            "value_type": type_text,
        }

    def _compact_table_value(
        self,
        columns: list[str],
        rows: list[str],
        subentities: list[dict[str, Any]],
        raw_csv_table: str = "",
    ) -> dict[str, Any]:
        col_names = [str(col).strip() for col in (columns or []) if str(col).strip()]
        row_names = [str(row).strip() for row in (rows or []) if str(row).strip()]
        if not row_names:
            row_names = ["value"]

        normalized_subs = self._normalize_table_subentities(subentities, columns=col_names, rows=row_names)
        col_index = {col: idx for idx, col in enumerate(col_names)}
        row_index = {row: idx for idx, row in enumerate(row_names)}

        unit_by_col: dict[str, str] = {}
        cells: list[dict[str, Any]] = []
        for sub in normalized_subs:
            col = str(sub.get("col") or "").strip()
            row = str(sub.get("row") or "").strip() or "value"
            if col not in col_index or row not in row_index:
                continue
            number, _ = self._coerce_numeric_pair(sub.get("value_raw", sub.get("value")), sub.get("value_number"))
            unit_text = str(sub.get("unit") or "").strip()
            if unit_text and col not in unit_by_col:
                unit_by_col[col] = unit_text
            cells.append(
                {
                    "r": row_index[row],
                    "c": col_index[col],
                    "v": number,
                }
            )

        metadata = self._table_metadata(col_names, row_names, unit_by_col=unit_by_col)
        csv_text = str(raw_csv_table or "").strip()
        if not csv_text and normalized_subs:
            csv_text = self._convert_entities_to_csv(normalized_subs, col_names)

        return {
            "storage": "table_v2",
            "metadata": metadata,
            "cells": cells,
            "raw_csv_table": csv_text,
        }

    def _expand_table_value(
        self,
        value: dict[str, Any],
        fallback_columns: list[str] | None = None,
        fallback_rows: list[str] | None = None,
    ) -> tuple[list[str], list[str], list[dict[str, Any]]]:
        value_obj = value if isinstance(value, dict) else {}
        metadata = value_obj.get("metadata") if isinstance(value_obj.get("metadata"), dict) else {}

        columns = self._normalize_list(metadata.get("value_columns"))
        if not columns:
            columns = self._normalize_list(value_obj.get("columns"))
        if not columns:
            columns = [str(col).strip() for col in (fallback_columns or []) if str(col).strip()]

        rows = self._normalize_list(metadata.get("rows"))
        if not rows:
            rows = self._normalize_list(value_obj.get("rows"))
        if not rows:
            rows = [str(row).strip() for row in (fallback_rows or []) if str(row).strip()]
        if not rows:
            rows = ["value"]

        unit_parts = self._split_pipe_values(metadata.get("unit"))
        unit_by_col = {col: (unit_parts[idx] if idx < len(unit_parts) else "") for idx, col in enumerate(columns)}

        subentities: list[dict[str, Any]] = []

        cells = value_obj.get("cells") if isinstance(value_obj.get("cells"), list) else []
        for cell in cells:
            if not isinstance(cell, dict):
                continue
            try:
                row_idx = int(cell.get("r"))
                col_idx = int(cell.get("c"))
            except Exception:
                continue
            if row_idx < 0 or row_idx >= len(rows) or col_idx < 0 or col_idx >= len(columns):
                continue
            number, value_raw = self._coerce_numeric_pair(cell.get("v"), cell.get("value_number"))
            col_name = columns[col_idx]
            subentities.append(
                {
                    "col": col_name,
                    "row": rows[row_idx],
                    "value_raw": value_raw,
                    "value_number": number,
                    "unit": unit_by_col.get(col_name, ""),
                    "value_type": "number",
                }
            )

        if not subentities:
            legacy_subs = value_obj.get("subentities") if isinstance(value_obj.get("subentities"), list) else []
            if legacy_subs:
                legacy_norm = self._normalize_table_subentities(legacy_subs)
                if columns:
                    col_set = set(columns)
                    legacy_norm = [item for item in legacy_norm if str(item.get("col") or "").strip() in col_set]
                if rows:
                    row_set = set(rows)
                    legacy_norm = [item for item in legacy_norm if str(item.get("row") or "").strip() in row_set]
                subentities = legacy_norm

        if not subentities:
            raw_csv_table = str(value_obj.get("raw_csv_table") or "").strip()
            if raw_csv_table:
                metadata_hint = self._table_metadata(columns, rows, unit_by_col=unit_by_col)
                subentities = self._parse_table_csv_to_subentities(raw_csv_table, metadata_hint)

        subentities = self._normalize_table_subentities(subentities, columns=columns, rows=rows)
        return columns, rows, subentities

    def _extract_segment_with_llm(self, image: Image.Image, segment: dict) -> dict[str, Any]:
        bbox = self._normalize_bbox(segment.get("bbox"))
        seg_type = self._normalize_object_type(segment.get("type"))
        columns = self._normalize_list(segment.get("columns"))
        rows = self._normalize_list(segment.get("rows"))
        
        parse_error: str | None = None
        llm_out: dict[str, Any] = {}
        
        try:
            crop_bytes = self._crop_segment_bytes(image, bbox)
            llm_out = call_llm_segment(
                image_bytes=crop_bytes,
                seg_type=seg_type,
                columns=columns,
                rows=rows,
            )
            parse_error = llm_out.get("_parse_error")
        except Exception as exc:
            parse_error = f"llm_exception: {exc}"

        value = self._build_segment_value(seg_type, columns, rows, llm_out)

        return {
            "segment_id": str(segment.get("id") or uuid4().hex[:8]),
            "type": seg_type,
            "name": str(segment.get("name") or "").strip() or "Unnamed",
            "bbox": bbox,
            "columns": columns,
            "rows": rows,
            "value": value,
            "raw_llm_entity": llm_out,
            "llm_parse_error": parse_error,
        }

    def _build_segment_value(self, seg_type: str, columns: list[str], rows: list[str], first_entity: dict[str, Any]) -> dict[str, Any]:
        raw_csv_table = str(first_entity.get("raw_csv_table") or "").strip()
        
        if seg_type == "log tables":
            # For log tables, we parse the CSV to a list of log objects for internal processing
            logs = []
            if raw_csv_table:
                try:
                    import io, csv
                    reader = csv.DictReader(io.StringIO(raw_csv_table))
                    for row_item in reader:
                        logs.append({
                            "time": row_item.get("time", ""),
                            "message": row_item.get("message", "")
                        })
                except Exception as exc:
                    logger.error("Failed to parse log CSV: %s", exc)

            compact_table_value = self._convert_entities_to_csv(logs, ["time", "message"])
            return {
                "columns": ["time", "message"],
                "logs": logs,
                "raw_csv_table": compact_table_value,
            }

        if seg_type == "fixed table":
            metadata = self._table_metadata(columns, rows)
            parsed_from_csv = self._parse_table_csv_to_subentities(raw_csv_table, metadata) if raw_csv_table else []
            normalized_subs = self._normalize_table_subentities(parsed_from_csv, columns=columns, rows=rows)
            
            compact_table_value = self._convert_entities_to_csv(normalized_subs, columns)
            return self._compact_table_value(
                columns=columns,
                rows=rows,
                subentities=normalized_subs,
                raw_csv_table=compact_table_value,
            )

        # SCADA Object: handle as a 2-column CSV (label, value)
        metadata = self._table_metadata(columns=["value"], rows=columns)
        parsed_from_csv = self._parse_table_csv_to_subentities(raw_csv_table, metadata) if raw_csv_table else []
        
        scada_subentities = []
        for sub in parsed_from_csv:
            scada_subentities.append({
                "col": "value",
                "row": sub.get("row"),
                "value_raw": sub.get("value_raw"),
                "value_number": sub.get("value_number"),
                "unit": "",
                "value_type": "number",
            })

        compact_table_value = self._convert_entities_to_csv(scada_subentities, ["value"])
        return self._compact_table_value(
            columns=["value"],
            rows=columns,
            subentities=scada_subentities,
            raw_csv_table=compact_table_value,
        )

    def _ensure_segment_schema(self, db: Database, group: dict) -> list[dict]:
        segments = self._segment_schema_from_group(group)
        has_legacy_field = "entity_schema" in group
        existing_schema = group.get("segmentation_schema")
        needs_sync = isinstance(existing_schema, list) and existing_schema != segments

        if has_legacy_field or not isinstance(existing_schema, list) or needs_sync:
            now = now_utc()
            db.screen_groups.update_one(
                {"_id": group["_id"]},
                {
                    "$set": {
                        "segmentation_schema": segments,
                        "schema_status": "classified" if segments else "unclassified",
                        "classified_at": group.get("classified_at") or (now if segments else None),
                        "updated_at": now,
                    },
                    "$unset": {"entity_schema": ""},
                },
            )

        return segments

    def _upsert_ocr_result_new_connection(self, db_name: str, payload: dict[str, Any]) -> None:
        client = MongoClient(MONGO_URI, tz_aware=True)
        try:
            coll = client[db_name].ocr_results
            try:
                coll.create_index([("snapshot_id", 1)], unique=True, name="uniq_snapshot_id")
            except Exception:
                # Index may already exist with equivalent definition.
                pass

            created_at = payload.get("created_at") or now_utc()
            set_payload = dict(payload)
            set_payload.pop("created_at", None)
            set_payload["updated_at"] = now_utc()

            coll.update_one(
                {"snapshot_id": payload.get("snapshot_id")},
                {
                    "$set": set_payload,
                    "$setOnInsert": {"created_at": created_at},
                },
                upsert=True,
            )
        finally:
            client.close()

    def _store_ocr_result(
        self,
        db: Database,
        source: dict,
        group: dict,
        snapshot: dict,
        monitor_key: str,
        image_hash: str,
        entities: list[dict[str, Any]],
        llm_parse_error: str | None,
        processing_time_ms: int,
        status: str,
    ) -> None:
        self._upsert_ocr_result_new_connection(
            db.name,
            {
                "snapshot_id": snapshot.get("_id"),
                "source_id": source.get("_id"),
                "screen_group_id": group.get("_id"),
                "monitor_key": monitor_key,
                "screen_name": group.get("name"),
                "entities": entities,
                "llm_parse_error": llm_parse_error,
                "processing_time_ms": processing_time_ms,
                "status": status,
                "image_hash": image_hash,
                "evaluation": None,
                "created_at": snapshot.get("created_at") or now_utc(),
            },
        )

    def _classify_snapshot(self, db: Database, source: dict, job_id, monitor_key: str):
        image_bytes = fetch_snapshot_bytes(source, monitor_key if monitor_key != "default" else None)
        if not image_bytes:
            self.update_job(db, job_id, "failed", f"No snapshot data received from KVM {monitor_key}")
            return None

        image_bytes = autocrop_image(image_bytes)
        image_hash = hashlib.sha256(image_bytes).hexdigest()

        latest = db.ocr_results.find_one(
            {"source_id": source["_id"], "monitor_key": monitor_key},
            sort=[("created_at", -1)],
        )
        if latest and latest.get("image_hash") == image_hash:
            self.update_job(db, job_id, "completed", "Duplicate snapshot skipped")
            return None

        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        histogram = histogram_feature(image)
        brightness = brightness_feature(image)
        matched_group, match_score = self._find_best_group_match(
            db=db,
            source=source,
            monitor_key=monitor_key,
            histogram=histogram,
            brightness=brightness,
        )

        return {
            "image_bytes": image_bytes,
            "image_hash": image_hash,
            "image": image,
            "histogram": histogram,
            "brightness": brightness,
            "matched_group": matched_group,
            "match_score": match_score,
        }

    @staticmethod
    def _processing_time_ms(start_time: float) -> int:
        return int((time.time() - start_time) * 1000)

    def _store_empty_result_and_complete(
        self,
        db: Database,
        source: dict,
        group: dict,
        snapshot: dict,
        monitor_key: str,
        image_hash: str,
        start_time: float,
        status: str,
        completion_message: str,
        job_id: Any,
    ) -> None:
        self._store_ocr_result(
            db=db,
            source=source,
            group=group,
            snapshot=snapshot,
            monitor_key=monitor_key,
            image_hash=image_hash,
            entities=[],
            llm_parse_error=None,
            processing_time_ms=self._processing_time_ms(start_time),
            status=status,
        )
        self.update_job(db, job_id, "completed", completion_message)

    def _extract_segment_entities(
        self,
        image: Image.Image,
        segments: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], str | None]:
        entities: list[dict[str, Any]] = []
        parse_errors: list[str] = []

        for seg in segments:
            if not isinstance(seg, dict):
                continue
            segment_result = self._extract_segment_with_llm(image, seg)
            entities.append(segment_result)
            if segment_result.get("llm_parse_error"):
                parse_errors.append(
                    f"{segment_result.get('segment_id')}: {segment_result.get('llm_parse_error')}"
                )

        return entities, (" | ".join(parse_errors) if parse_errors else None)

    def _queue_unmatched_snapshot_result(
        self,
        db: Database,
        source: dict,
        monitor_key: str,
        histogram: list[float],
        brightness: tuple[float, float],
        image_bytes: bytes,
        image_hash: str,
        saved_path: Path,
        start_time: float,
        job_id: Any,
    ) -> None:
        queued_group = self._queue_new_unclassified_group(
            db=db,
            source=source,
            monitor_key=monitor_key,
            histogram=histogram,
            brightness=brightness,
        )
        snapshot = self._insert_snapshot(
            db=db,
            source=source,
            group=queued_group,
            monitor_key=monitor_key,
            image_bytes=image_bytes,
            image_hash=image_hash,
            histogram=histogram,
            brightness=brightness,
            saved_path=saved_path,
        )
        self._store_empty_result_and_complete(
            db=db,
            source=source,
            group=queued_group,
            snapshot=snapshot,
            monitor_key=monitor_key,
            image_hash=image_hash,
            start_time=start_time,
            status="queued_new_screen_group",
            completion_message="New sample queued for schema",
            job_id=job_id,
        )

    def process_single_snapshot(self, db: Database, source: dict, monitor_key: str):
        start_time = time.time()
        logger.info("Processing snapshot V2 for source=%s monitor=%s", source.get("name"), monitor_key)
        job_id = self.create_job(db, source["_id"], monitor_key)
        self.update_job(db, job_id, "processing")
        try:
            result = self._classify_snapshot(db, source, job_id, monitor_key)
            if not result:
                return

            image_bytes = result["image_bytes"]
            image_hash = result["image_hash"]
            image = result["image"]
            histogram = result["histogram"]
            brightness = result["brightness"]
            matched_group = result["matched_group"]

            saved_path = self.save_snapshot(image_bytes, self.to_id(source["_id"]), monitor_key)

            if not matched_group:
                self._queue_unmatched_snapshot_result(
                    db=db,
                    source=source,
                    monitor_key=monitor_key,
                    histogram=histogram,
                    brightness=brightness,
                    image_bytes=image_bytes,
                    image_hash=image_hash,
                    saved_path=saved_path,
                    start_time=start_time,
                    job_id=job_id,
                )
                return

            # update fingerprint of matched group with new sample
            group = self._append_sample_to_group(
                db=db,
                group=matched_group,
                histogram=histogram,
                brightness=brightness,
            )
            # insert snapshot linked to matched group for ocr result checking
            snapshot = self._insert_snapshot(
                db=db,
                source=source,
                group=group,
                monitor_key=monitor_key,
                image_bytes=image_bytes,
                image_hash=image_hash,
                histogram=histogram,
                brightness=brightness,
                saved_path=saved_path,
            )
            segments = []
            if not group.get("ignored"): segments = self._ensure_segment_schema(db, group)
            if not segments:
                self._store_empty_result_and_complete(
                    db=db,
                    source=source,
                    group=group,
                    snapshot=snapshot,
                    monitor_key=monitor_key,
                    image_hash=image_hash,
                    start_time=start_time,
                    status="ignored",
                    completion_message="Matched sample has no schema",
                    job_id=job_id,
                )
                return

            entities, llm_parse_error = self._extract_segment_entities(image, segments)
            self._finalize_snapshot(
                db=db,
                source=source,
                group=group,
                snapshot=snapshot,
                monitor_key=monitor_key,
                image_hash=image_hash,
                entities=entities,
                llm_parse_error=llm_parse_error,
                start_time=start_time,
            )

            self.update_job(db, job_id, "completed")
            logger.info("Segmented OCR completed source=%s monitor=%s segments=%d", source.get("name"), monitor_key, len(entities))
        except Exception as exc:
            self.update_job(db, job_id, "failed", str(exc))
            logger.error("Snapshot failed: %s/%s: %s", source.get("name"), monitor_key, exc)
            raise

    def _generate_layout_text(self, saved_path: Path) -> str:
        try:
            return ocr.generate_layout_text(str(saved_path))
        except Exception as exc:
            logger.warning("Failed to generate OCR layout text: %s", exc)
            return ""

    def _entity_key(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return self.normalizer.slugify(text) or text.lower()

    def _parse_table_csv_to_subentities(self, raw_csv_table: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        csv_text = str(raw_csv_table or "").strip()
        if not csv_text:
            return []

        try:
            import io, csv
            rows = list(csv.reader(io.StringIO(csv_text)))
        except Exception:
            return []

        if len(rows) < 2:
            return []

        header = [str(col).strip() for col in rows[0]]
        value_columns = [str(col).strip() for col in metadata.get("value_columns", []) if str(col).strip()]
        metadata_rows = [str(row).strip() for row in metadata.get("rows", []) if str(row).strip()]

        first_col_text = str(header[0] if header else "").strip().lower()
        use_header = first_col_text in {"row", "rows", "item", "name", "label", "time"} or (bool(value_columns) and any(c in value_columns for c in header))
        
        data_rows = rows[1:] if use_header else rows
        if use_header and not value_columns and len(header) > 1:
            value_columns = header[1:]

        if not value_columns:
            width = max((len(row) for row in data_rows), default=len(header))
            value_columns = [f"col_{idx}" for idx in range(1, width)] if width > 1 else ["value"]

        unit_parts = [part.strip() for part in str(metadata.get("unit", "") or "").split("|")] if metadata.get("unit") else []
        unit_by_col = {col: (unit_parts[idx] if idx < len(unit_parts) else "") for idx, col in enumerate(value_columns)}
        col_idx_map = {col: idx for idx, col in enumerate(header)} if use_header else {}

        subentities = []
        for row_idx, row in enumerate(data_rows):
            row_name = str(row[0]).strip() if row else ""
            if not row_name and row_idx < len(metadata_rows):
                row_name = metadata_rows[row_idx]
            if not row_name:
                row_name = "Unknown"

            for col_pos, col in enumerate(value_columns):
                idx = col_idx_map.get(col) if use_header else col_pos + 1
                if idx is None or idx >= len(row):
                    continue
                
                val_raw = str(row[idx]).strip()
                if val_raw == "":
                    continue
                
                val_num, norm_raw = self._coerce_numeric_pair(val_raw)
                subentities.append({
                    "col": col,
                    "row": row_name,
                    "value_raw": norm_raw,
                    "value_number": val_num,
                    "unit": unit_by_col.get(col, ""),
                    "value_type": "number",
                })
        return subentities

    def _convert_entities_to_csv(self, items: list[dict], columns: list[str]) -> str:
        """Unified helper to build CSV from either table segments (subentities) or log segments (list of dicts)."""
        if not items:
            return ""
        
        import io, csv
        out = io.StringIO()
        writer = csv.writer(out)
        
        # Check if items are subentities (have 'row' and 'col') or log-like dicts
        is_subentities = all(isinstance(x, dict) and "row" in x and "col" in x for x in items[:3] if x)
        
        if is_subentities:
            row_order = []
            value_map: dict[str, dict[str, str]] = {}
            cols = [str(c).strip() for c in columns if str(c).strip()]
            if not cols:
                cols = list(dict.fromkeys(str(x.get("col", "")).strip() for x in items if str(x.get("col", "")).strip()))
            
            for sub in items:
                r = str(sub.get("row", "Unknown")).strip() or "Unknown"
                c = str(sub.get("col", "")).strip()
                if not c: continue
                if r not in value_map:
                    value_map[r] = {}
                    row_order.append(r)
                val = sub.get("value_raw", sub.get("value"))
                if val is None: val = sub.get("value_number")
                value_map[r][c] = str(val if val is not None else "")
                
            writer.writerow(["row"] + cols)
            for r in row_order:
                writer.writerow([r] + [value_map[r].get(c, "") for c in cols])
        else:
            # Assume log entries or flat dicts
            header = columns if columns else list(items[0].keys())
            writer.writerow(header)
            for item in items:
                writer.writerow([str(item.get(k, "")).strip() for k in header])
                
        return out.getvalue().strip()

    def _update_schema(self, db: Database, group: dict, entities: list) -> list:
        new_schema = []
        used_segment_names: set[str] = set()
        used_segment_ids: set[str] = set()

        for idx, ent in enumerate(entities):

            ent_type = self._normalize_object_type(ent.get("type"))
            metadata = ent.get("metadata", {}) if isinstance(ent.get("metadata"), dict) else {}
            bbox = self._normalize_bbox(metadata.get("bbox") if isinstance(metadata.get("bbox"), dict) else {})
            seg_name_base = str(ent.get("main_entity_name") or "Unnamed").strip() or "Unnamed"
            seg_name = self._unique_segment_name(seg_name_base, used_segment_names)
            seg_id = self._unique_segment_id(
                self._segment_name_to_pascal_id(seg_name, fallback=f"Segment{idx + 1}"),
                used_segment_ids,
            )

            columns: list[str] = []
            rows: list[str] = []

            if ent_type == "fixed table":
                columns = [str(col).strip() for col in metadata.get("value_columns", []) if str(col).strip()]
                if not columns:
                    seen_cols = set()
                    for sub in ent.get("subentities", []) or []:
                        col = str(sub.get("col", "")).strip()
                        if col and col not in seen_cols:
                            seen_cols.add(col)
                            columns.append(col)

                rows = [str(row).strip() for row in metadata.get("rows", []) if str(row).strip()]
                if not rows:
                    seen_rows = set()
                    for sub in ent.get("subentities", []) or []:
                        row_name = str(sub.get("row", "")).strip()
                        if row_name and row_name not in seen_rows:
                            seen_rows.add(row_name)
                            rows.append(row_name)

                if not columns:
                    columns = ["column_1"]
                if not rows:
                    rows = ["row_1"]
            elif ent_type == "log tables":
                columns = ["time", "message"]
            else:
                columns = [
                    str(ind.get("label") or ind.get("metric") or "").strip()
                    for ind in (ent.get("indicators") or [])
                    if str(ind.get("label") or ind.get("metric") or "").strip()
                ]
                if not columns:
                    columns = ["value"]

            new_schema.append(
                {
                    "id": seg_id,
                    "name": seg_name,
                    "type": ent_type,
                    "shape": "rectangle",
                    "bbox": bbox,
                    "columns": columns,
                    "rows": rows,
                    "sample_id": str(metadata.get("sample_id") or "").strip(),
                }
            )

        db.screen_groups.update_one(
            {"_id": group["_id"]},
            {
                "$set": {
                    "segmentation_schema": new_schema,
                    "schema_status": "classified" if new_schema else "unclassified",
                    "classified_at": now_utc() if new_schema else None,
                    "updated_at": now_utc(),
                },
                "$unset": {"entity_schema": ""},
            },
        )
        return new_schema

    def _update_screen_name(self, db: Database, group: dict, extracted_json: dict):
        screen_name = ensure_llm_name(extracted_json.get("screen_title", "") if isinstance(extracted_json, dict) else "", group.get("name", ""))
        final_screen_name = self.normalizer.normalize_screen_title(extracted_json, screen_name or group.get("name", ""))    
        db.screen_groups.update_one({"_id": group["_id"]}, {"$set": {"name": final_screen_name, "updated_at": now_utc()}})  

    def _finalize_snapshot(
        self,
        db: Database,
        source: dict,
        group: dict,
        snapshot: dict,
        monitor_key: str,
        image_hash: str,
        entities: list,
        llm_parse_error: str | None,
        start_time: float,
    ):
        processing_time_ms = int((time.time() - start_time) * 1000)
        self._store_ocr_result(
            db=db,
            source=source,
            group=group,
            snapshot=snapshot,
            monitor_key=monitor_key,
            image_hash=image_hash,
            entities=entities,
            llm_parse_error=llm_parse_error,
            processing_time_ms=processing_time_ms,
            status="completed",
        )
        self._push_ocr_result_to_dashboard(source=source, snapshot=snapshot, entities=entities)

    def _segment_schema_from_group(self, group: dict) -> list[dict]:
        segmentation = [dict(seg) for seg in (group.get("segmentation_schema") or []) if isinstance(seg, dict)]
        if segmentation:
            return self._normalize_segment_schema_identity(segmentation)

        # Backward compatibility for old docs still storing entity_schema.
        legacy = [dict(ent) for ent in (group.get("entity_schema") or []) if isinstance(ent, dict)]
        if not legacy:
            return []

        converted = []
        for idx, ent in enumerate(legacy):
            ent_type = self._normalize_object_type(ent.get("type"))
            metadata = ent.get("metadata") if isinstance(ent.get("metadata"), dict) else {}
            bbox = self._normalize_bbox(metadata.get("bbox") if isinstance(metadata.get("bbox"), dict) else {})

            columns: list[str] = []
            rows: list[str] = []
            if ent_type == "fixed table":
                columns = [str(col).strip() for col in metadata.get("value_columns", []) if str(col).strip()]
                if not columns:
                    seen_cols = set()
                    for sub in ent.get("subentities") or []:
                        col = str(sub.get("col") or "").strip()
                        if col and col not in seen_cols:
                            seen_cols.add(col)
                            columns.append(col)

                rows = [str(row).strip() for row in metadata.get("rows", []) if str(row).strip()]
                if not rows:
                    seen_rows = set()
                    for sub in ent.get("subentities") or []:
                        row_name = str(sub.get("row") or "").strip()
                        if row_name and row_name not in seen_rows:
                            seen_rows.add(row_name)
                            rows.append(row_name)

                if not columns:
                    columns = ["column_1"]
                if not rows:
                    rows = ["row_1"]
            elif ent_type == "log tables":
                columns = ["time", "message"]
            else:
                for ind in ent.get("indicators") or []:
                    label = str(ind.get("label") or ind.get("metric") or "").strip()
                    if label:
                        columns.append(label)
                if not columns:
                    columns = ["value"]

            converted.append(
                {
                    "id": str(ent.get("id") or f"seg_{idx + 1}"),
                    "name": str(ent.get("main_entity_name") or f"Segment {idx + 1}").strip() or f"Segment {idx + 1}",
                    "type": ent_type,
                    "shape": "rectangle",
                    "bbox": bbox,
                    "columns": columns,
                    "rows": rows,
                    "sample_id": str(metadata.get("sample_id") or "").strip(),
                }
            )

        return self._normalize_segment_schema_identity(converted)

    def list_entities(self, db: Database, screen_group_id: str) -> list[dict]:
        group_obj_id = self.oid(screen_group_id)
        group = db.screen_groups.find_one({"_id": group_obj_id})
        if not group:
            return []

        schema = self._segment_schema_from_group(group)
        latest_result = db.ocr_results.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
        latest_vals = []
        if latest_result:
            latest_vals = latest_result.get("entities") or latest_result.get("entities_values") or []
        if not latest_vals:
            legacy_snap = db.snapshots.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
            if legacy_snap:
                latest_vals = legacy_snap.get("entities_values", []) or []

        val_by_id = {}
        val_by_name = {}
        for row in latest_vals:
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("segment_id") or row.get("id") or "").strip()
            if row_id:
                val_by_id[row_id] = row
            row_name = str(row.get("name") or row.get("main_entity_name") or "").strip()
            if row_name:
                val_by_name[row_name] = row

        rows = []
        for i, segment in enumerate(schema):
            seg_id = str(segment.get("id") or f"{str(group_obj_id)}_seg_{i}")
            seg_name = str(segment.get("name") or f"Segment {i + 1}").strip() or f"Segment {i + 1}"
            seg_type = self._normalize_object_type(segment.get("type"))

            evals = val_by_id.get(seg_id) or val_by_name.get(seg_name) or {}
            value = evals.get("value") if isinstance(evals.get("value"), dict) else {}

            ent_dict = {
                "id": seg_id,
                "entity_key": self.normalizer.slugify(seg_name),
                "display_name": seg_name,
                "entity_type": seg_type,
                "region": segment.get("region") or "center",
                "indicators": {},
                "metrics": {},
                "subentities": [],
                "logs": []
            }

            if seg_type == "log tables":
                logs = value.get("logs") if isinstance(value.get("logs"), list) else []
                if not logs:
                    logs = evals.get("logs") if isinstance(evals.get("logs"), list) else []
                ent_dict["logs"] = logs
            else:
                schema_cols = [str(col).strip() for col in (segment.get("columns") or []) if str(col).strip()]
                schema_rows = [str(row).strip() for row in (segment.get("rows") or []) if str(row).strip()]
                if seg_type != "fixed table" and not schema_rows:
                    schema_rows = ["value"]

                cols, row_names, table_values = self._expand_table_value(
                    value,
                    fallback_columns=schema_cols,
                    fallback_rows=schema_rows,
                )

                if not table_values:
                    legacy_subs = evals.get("subentities") if isinstance(evals.get("subentities"), list) else []
                    table_values = self._normalize_table_subentities(
                        legacy_subs,
                        columns=cols or schema_cols,
                        rows=row_names or schema_rows,
                    )

                ent_dict["subentities"] = table_values

                if seg_type == "scada object":
                    if not cols:
                        seen_cols = set()
                        for sub in table_values:
                            col = str(sub.get("col") or "").strip()
                            if col and col not in seen_cols:
                                seen_cols.add(col)
                                cols.append(col)

                    for col in cols:
                        col_name = str(col).strip()
                        if not col_name:
                            continue

                        source = None
                        for sub in table_values:
                            if str(sub.get("col") or "").strip() != col_name:
                                continue
                            row_text = str(sub.get("row") or "").strip().lower()
                            if row_text in {"value", "default"}:
                                source = sub
                                break
                            if source is None:
                                source = sub

                        source = source or {
                            "value_raw": "0",
                            "value_number": 0,
                            "unit": "",
                        }

                        number, value_raw = self._coerce_numeric_pair(
                            source.get("value_raw", source.get("value")),
                            source.get("value_number"),
                        )
                        metric_key = self.normalizer.slugify(col_name) or col_name.lower().replace(" ", "_")
                        metric_payload = {
                            "indicator_label": col_name,
                            "metric_key": metric_key,
                            "unit": str(source.get("unit") or ""),
                            "value_type": "number",
                            "last_value": value_raw,
                            "last_number": number,
                        }
                        ent_dict["metrics"][metric_key] = metric_payload
                        ent_dict["indicators"][metric_key] = metric_payload

            rows.append(ent_dict)
        return rows

    def get_screen_preview(self, db: Database, screen_group_id: str) -> dict | None:
        group_obj_id = self.oid(screen_group_id)
        latest_result = db.ocr_results.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
        if latest_result:
            snapshot_id = latest_result.get("snapshot_id")
            if snapshot_id is not None:
                url = f"/api/v2/snapshots/{self.to_id(snapshot_id)}/image"
                return {
                    "snapshot_id": self.to_id(snapshot_id),
                    "created_at": latest_result.get("created_at"),
                    "image_url": url,
                }

        legacy_snapshot = db.snapshots.find_one({"screen_group_id": group_obj_id}, sort=[("created_at", -1)])
        if not legacy_snapshot:
            return None
        return {
            "snapshot_id": self.to_id(legacy_snapshot.get("_id")),
            "created_at": legacy_snapshot.get("created_at"),
            "image_url": f"/api/v2/snapshots/{self.to_id(legacy_snapshot.get('_id'))}/image",
        }

    def list_logs(self, db: Database, screen_group_id: str, since, entity_ids: list[str] | None, limit: int) -> list[dict]:
        group_obj_id = self.oid(screen_group_id)
        results = list(db.ocr_results.find(
            {"screen_group_id": group_obj_id, "created_at": {"$gte": since}},
            sort=[("created_at", -1)],
            limit=limit
        ))
        if not results:
            legacy_snaps = list(db.snapshots.find(
                {"screen_group_id": group_obj_id, "created_at": {"$gte": since}},
                sort=[("created_at", -1)],
                limit=limit,
            ))
            results = [
                {
                    "snapshot_id": snap.get("_id"),
                    "created_at": snap.get("created_at"),
                    "entities": snap.get("entities_values", []) or [],
                }
                for snap in legacy_snaps
            ]

        requested_ids = set(entity_ids or [])
        logs = []
        for result_row in results:
            entities_values = result_row.get("entities") or result_row.get("entities_values") or []
            snapshot_ref = result_row.get("snapshot_id") or result_row.get("_id")

            for ent in entities_values:
                if not isinstance(ent, dict):
                    continue

                ent_id = str(ent.get("segment_id") or ent.get("id") or "").strip()
                if requested_ids and (not ent_id or ent_id not in requested_ids):
                    continue

                ent_name = str(ent.get("name") or ent.get("main_entity_name") or "Unknown").strip() or "Unknown"
                etype = self._normalize_object_type(ent.get("type"))
                value = ent.get("value") if isinstance(ent.get("value"), dict) else {}

                if etype == "log tables":
                    log_rows = value.get("logs") if isinstance(value.get("logs"), list) else []
                    if not log_rows:
                        log_rows = ent.get("logs") if isinstance(ent.get("logs"), list) else []

                    for idx, lg in enumerate(log_rows):
                        if not isinstance(lg, dict):
                            continue
                        message_text = lg.get("message")
                        if message_text is None or message_text == "":
                            name_text = str(lg.get('name', '') or '')
                            desc_text = str(lg.get('desc', lg.get('msg', '')) or '')
                            if name_text and desc_text:
                                message_text = f"{name_text}: {desc_text}"
                            else:
                                message_text = name_text or desc_text
                        logs.append({
                            "log_id": str(snapshot_ref) + "_" + str(ent_name) + "_" + str(idx),
                            "entity_name": ent_name,
                            "metric": "log",
                            "value": f"[{lg.get('time')}] {message_text}",
                            "value_type": "text",
                            "recorded_at": result_row.get("created_at")
                        })
                else:
                    fallback_columns = [str(col).strip() for col in (ent.get("columns") or []) if str(col).strip()]
                    fallback_rows = [str(row).strip() for row in (ent.get("rows") or []) if str(row).strip()]
                    if etype != "fixed table" and not fallback_rows:
                        fallback_rows = ["value"]

                    _, _, sub_rows = self._expand_table_value(
                        value,
                        fallback_columns=fallback_columns,
                        fallback_rows=fallback_rows,
                    )

                    if not sub_rows and isinstance(ent.get("subentities"), list):
                        sub_rows = self._normalize_table_subentities(
                            ent.get("subentities") or [],
                            columns=fallback_columns or None,
                            rows=fallback_rows or None,
                        )

                    if (not sub_rows) and etype == "scada object":
                        indicators = value.get("indicators") if isinstance(value.get("indicators"), list) else []
                        if not indicators:
                            indicators = ent.get("indicators") if isinstance(ent.get("indicators"), list) else []
                        for ind in indicators:
                            if not isinstance(ind, dict):
                                continue
                            label = str(ind.get("label") or ind.get("metric") or "").strip()
                            if not label:
                                continue
                            number, value_raw = self._coerce_numeric_pair(
                                ind.get("value_raw", ind.get("value")),
                                ind.get("value_number"),
                            )
                            sub_rows.append(
                                {
                                    "col": label,
                                    "row": "value",
                                    "value_raw": value_raw,
                                    "value_number": number,
                                    "unit": str(ind.get("unit") or ""),
                                    "value_type": "number",
                                }
                            )

                    for sub in sub_rows:
                        if not isinstance(sub, dict):
                            continue

                        col_name = str(sub.get("col") or "").strip()
                        row_name = str(sub.get("row") or "value").strip() or "value"
                        if not col_name:
                            continue

                        number, value_raw = self._coerce_numeric_pair(
                            sub.get("value_raw", sub.get("value")),
                            sub.get("value_number"),
                        )
                        metric_key = self._table_metric_key(row_name, col_name)
                        metric_label = self._table_metric_label(row_name, col_name)

                        logs.append(
                            {
                                "log_id": str(snapshot_ref) + "_" + str(ent_name) + "_" + metric_key,
                                "entity_name": ent_name,
                                "metric": metric_key,
                                "metric_label": metric_label,
                                "value": value_raw,
                                "numeric_value": number,
                                "value_type": "number",
                                "unit": str(sub.get("unit") or ""),
                                "recorded_at": result_row.get("created_at"),
                            }
                        )
                        
        return logs[:limit]

    def get_timeseries(self, db: Database, screen_group_id: str, since, entity_ids: list[str] | None = None) -> dict[str, Any]:
        logs = self.list_logs(db, screen_group_id, since, entity_ids, limit=2000)
        logs.reverse() # oldest to newest
        
        result = {}
        for log in logs:
            if log.get("value_type") not in ["number", "bool"]:
                continue
            
            metric_key = log.get("metric") or "value"
            metric_label = log.get("metric_label") or metric_key
            series_key = f"{log.get('entity_name')}:{metric_key}"
            if series_key not in result:
                result[series_key] = {
                    "name": f"{log.get('entity_name')} - {metric_label}",
                    "entity_name": log.get("entity_name"),
                    "metric": metric_key,
                    "metric_label": metric_label,
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

    def list_snapshots(
        self,
        db: Database,
        source_id: str | None = None,
        limit: int = 20,
        skip: int = 0,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if source_id:
            query["source_id"] = self.oid(source_id)

        safe_limit = max(1, int(limit))
        safe_skip = max(0, int(skip))

        rows = list(
            db.ocr_results.find(query)
            .sort("created_at", -1)
            .skip(safe_skip)
            .limit(safe_limit)
        )
        total = db.ocr_results.count_documents(query)

        if total == 0:
            legacy_query: dict[str, Any] = {}
            if source_id:
                legacy_query["source_id"] = self.oid(source_id)
            legacy_rows = list(
                db.snapshots.find(legacy_query)
                .sort("created_at", -1)
                .skip(safe_skip)
                .limit(safe_limit)
            )
            legacy_total = db.snapshots.count_documents(legacy_query)
            items = []
            for snap in legacy_rows:
                items.append(
                    {
                        "id": self.to_id(snap.get("_id")),
                        "source_id": self.to_id(snap.get("source_id")) if snap.get("source_id") is not None else None,
                        "screen_group_id": self.to_id(snap.get("screen_group_id")) if snap.get("screen_group_id") is not None else None,
                        "monitor_key": snap.get("monitor_key"),
                        "created_at": snap.get("created_at"),
                        "image_url": f"/api/v2/snapshots/{self.to_id(snap.get('_id'))}/image",
                        "entities_values": snap.get("entities_values", []),
                        "llm_parse_error": snap.get("llm_parse_error", False),
                        "evaluation": snap.get("evaluation"),
                        "processing_time_ms": snap.get("processing_time_ms"),
                    }
                )
            return {
                "total": legacy_total,
                "items": items,
                "skip": safe_skip,
                "limit": safe_limit,
            }

        items: list[dict[str, Any]] = []
        for row in rows:
            snapshot_id = row.get("snapshot_id")
            if snapshot_id is None:
                continue
            image_url = f"/api/v2/snapshots/{self.to_id(snapshot_id)}/image"

            items.append(
                {
                    "id": self.to_id(snapshot_id),
                    "source_id": self.to_id(row.get("source_id")) if row.get("source_id") is not None else None,
                    "screen_group_id": self.to_id(row.get("screen_group_id")) if row.get("screen_group_id") is not None else None,
                    "monitor_key": row.get("monitor_key"),
                    "created_at": row.get("created_at"),
                    "image_url": image_url,
                    "entities_values": row.get("entities") or row.get("entities_values") or [],
                    "llm_parse_error": row.get("llm_parse_error", False),
                    "evaluation": row.get("evaluation"),
                    "processing_time_ms": row.get("processing_time_ms"),
                }
            )

        return {
            "total": total,
            "items": items,
            "skip": safe_skip,
            "limit": safe_limit,
        }

    def latest_snapshots(self, db: Database, source_id: str, limit: int) -> list[dict[str, Any]]:
        source_obj_id = self.oid(source_id)
        safe_limit = max(1, int(limit))
        rows = list(
            db.ocr_results.find({"source_id": source_obj_id})
            .sort("created_at", -1)
            .limit(safe_limit)
        )

        if not rows:
            legacy_rows = list(
                db.snapshots.find({"source_id": source_obj_id})
                .sort("created_at", -1)
                .limit(safe_limit)
            )
            return [
                {
                    "id": self.to_id(snap.get("_id")),
                    "screen_group_id": self.to_id(snap.get("screen_group_id")) if snap.get("screen_group_id") is not None else None,
                    "monitor_key": snap.get("monitor_key"),
                    "image_url": f"/api/v2/snapshots/{self.to_id(snap.get('_id'))}/image",
                    "created_at": snap.get("created_at"),
                }
                for snap in legacy_rows
            ]

        out: list[dict[str, Any]] = []
        for row in rows:
            snapshot_id = row.get("snapshot_id")
            if snapshot_id is None:
                continue
            image_url = f"/api/v2/snapshots/{self.to_id(snapshot_id)}/image"

            out.append(
                {
                    "id": self.to_id(snapshot_id),
                    "screen_group_id": self.to_id(row.get("screen_group_id")) if row.get("screen_group_id") is not None else None,
                    "monitor_key": row.get("monitor_key"),
                    "image_url": image_url,
                    "created_at": row.get("created_at"),
                }
            )
        return out

    def update_snapshot_evaluation(self, db: Database, snapshot_id: str, evaluation: Any) -> dict[str, Any]:
        snapshot_obj_id = self.oid(snapshot_id)
        result = db.ocr_results.update_one(
            {"snapshot_id": snapshot_obj_id},
            {"$set": {"evaluation": evaluation, "updated_at": now_utc()}},
        )
        if result.matched_count <= 0:
            # Legacy fallback
            legacy = db.snapshots.update_one(
                {"_id": snapshot_obj_id},
                {"$set": {"evaluation": evaluation, "updated_at": now_utc()}},
            )
            if legacy.matched_count > 0:
                return {"ok": True, "evaluation": evaluation}
        if result.matched_count <= 0:
            raise KeyError("snapshot_not_found")
        return {"ok": True, "evaluation": evaluation}


