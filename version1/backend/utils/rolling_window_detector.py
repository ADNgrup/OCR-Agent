"""
Runs as a background task (started in main.py alongside poll_loop).
Every RUN_INTERVAL_SECONDS it fetches the last WINDOW_SIZE numeric readings
for every active entity and checks for three trend anomalies:

  1. spike      — latest value jumps sharply vs. window mean (z-score)
  2. drift      — window mean has shifted significantly vs. the prior window
  3. freeze     — all values in the window are identical (stuck sensor)

Anomalies are written to the `anomaly_logs` MongoDB collection using the
same rule as per_write_detector.py so the notify stage will have one unified
collection to read from.

Configuration keys (read from MongoDB `system_config` document,
with env-var fallbacks):
  rolling_window_size          int    default 75
  rolling_run_interval_seconds int    default 600  (10 min)
  rolling_spike_z_threshold    float  default 3.0
  rolling_drift_threshold_pct  float  default 0.20  (20% mean shift)
"""

from __future__ import annotations

import asyncio
import logging
import statistics
from datetime import datetime, timezone
from typing import Optional

from pymongo.database import Database

logger = logging.getLogger(__name__)


# Fallback defaults (overridden by values in system_config collection)

DEFAULT_WINDOW_SIZE: int = 75
DEFAULT_RUN_INTERVAL: int = 600       # seconds
DEFAULT_SPIKE_Z: float = 3.0
DEFAULT_DRIFT_PCT: float = 0.20       # 20% shift in mean triggers drift flag



# Config loader  (mirrors pattern used in config_router.py)


def _load_config(db: Database) -> dict:
    doc = db["system_config"].find_one({}) or {}
    return {
        "window_size":  int(doc.get("rolling_window_size",          DEFAULT_WINDOW_SIZE)),
        "run_interval": int(doc.get("rolling_run_interval_seconds", DEFAULT_RUN_INTERVAL)),
        "spike_z":    float(doc.get("rolling_spike_z_threshold",    DEFAULT_SPIKE_Z)),
        "drift_pct":  float(doc.get("rolling_drift_threshold_pct",  DEFAULT_DRIFT_PCT)),
    }


# Anomaly writer  (same as per_write_detector)


def _severity(anomaly_type: str) -> str:
    return {
        "spike":  "high",
        "drift":  "medium",
        "freeze": "medium",
    }.get(anomaly_type, "medium")


def _write_anomaly(
    db: Database,
    entity_id,
    metric: str,
    metric_name: str,
    indicator_label: str,
    unit: str,
    anomaly_type: str,
    description: str,
    extra: Optional[dict] = None,
) -> None:
    doc = {
        "entity_log_id":   None,          # window-level, not a single log
        "entity_id":       entity_id,
        "snapshot_id":     None,
        "metric":          metric,
        "metric_name":     metric_name,
        "indicator_label": indicator_label,
        "unit":            unit,
        "anomaly_type":    anomaly_type,
        "severity":        _severity(anomaly_type),
        "description":     description,
        "detected_at":     datetime.now(timezone.utc),
        "resolved":        False,
        "detector":        "rolling_window",
    }
    if extra:
        doc.update(extra)
    db["anomaly_logs"].insert_one(doc)
    logger.info(
        "rolling_window anomaly [%s] entity=%s — %s",
        anomaly_type, entity_id, description,
    )



# Per-entity analysis


def _analyse_entity(
    db: Database,
    entity_id,
    metric: str,
    metric_name: str,
    indicator_label: str,
    unit: str,
    cfg: dict,
) -> list[str]:
    """
    Pull the last `window_size` numeric readings for this entity and run
    spike / drift / freeze checks. Returns list of anomaly_type strings fired.
    """
    window_size: int = cfg["window_size"]
    spike_z: float   = cfg["spike_z"]
    drift_pct: float = cfg["drift_pct"]

    # Fetch window — newest first
    cursor = (
        db["entity_logs"]
        .find(
            {
                "entity_id": entity_id,
                "value_type": "number",
                "numeric_value": {"$ne": None, "$type": "double"},
            },
            {"numeric_value": 1, "recorded_at": 1, "_id": 0},
        )
        .sort("recorded_at", -1)
        .limit(window_size)
    )
    rows = list(cursor)
    if len(rows) < 10:
        return []   # not enough data for meaningful analysis

    values: list[float] = [r["numeric_value"] for r in rows]
    latest: float = values[0]
    detected: list[str] = []

    
    # Check 1 — FREEZE  (all values in window are identical)
    
    if len(set(values)) == 1:
        _write_anomaly(
            db, entity_id, metric, metric_name, indicator_label, unit,
            anomaly_type="freeze",
            description=(
                f"All {len(values)} readings in window are identical "
                f"(value={latest} {unit}). Possible stuck sensor."
            ),
            extra={"window_size": len(values), "window_value": latest},
        )
        detected.append("freeze")
        return detected  # spike/drift are meaningless if sensor is frozen

    
    # Check 2 — SPIKE  (latest value vs. window mean)
    
    window_mean  = statistics.mean(values)
    window_stdev = statistics.stdev(values)

    if window_stdev > 0:
        z_score = abs(latest - window_mean) / window_stdev
        if z_score > spike_z:
            _write_anomaly(
                db, entity_id, metric, metric_name, indicator_label, unit,
                anomaly_type="spike",
                description=(
                    f"Latest value {latest} {unit} is {z_score:.2f}σ from "
                    f"window mean {window_mean:.2f} "
                    f"(stdev={window_stdev:.2f}, window={len(values)} readings)"
                ),
                extra={
                    "z_score":      round(z_score, 4),
                    "window_mean":  round(window_mean, 4),
                    "window_stdev": round(window_stdev, 4),
                    "window_size":  len(values),
                    "latest_value": latest,
                },
            )
            detected.append("spike")

   
    # Check 3 — DRIFT  (recent half mean vs. older half mean)
    
    half        = len(values) // 2
    recent_half = values[:half]   # newest readings
    older_half  = values[half:]   # older readings

    if len(recent_half) >= 5 and len(older_half) >= 5:
        recent_mean = statistics.mean(recent_half)
        older_mean  = statistics.mean(older_half)

        if older_mean != 0:
            drift_ratio = abs(recent_mean - older_mean) / abs(older_mean)
        else:
            drift_ratio = abs(recent_mean - older_mean)

        if drift_ratio > drift_pct:
            direction = "upward" if recent_mean > older_mean else "downward"
            _write_anomaly(
                db, entity_id, metric, metric_name, indicator_label, unit,
                anomaly_type="drift",
                description=(
                    f"{direction.capitalize()} drift: recent mean {recent_mean:.2f} "
                    f"vs older mean {older_mean:.2f} "
                    f"({drift_ratio * 100:.1f}% shift, "
                    f"threshold={drift_pct * 100:.0f}%, "
                    f"window={len(values)} readings)"
                ),
                extra={
                    "drift_ratio":  round(drift_ratio, 4),
                    "recent_mean":  round(recent_mean, 4),
                    "older_mean":   round(older_mean, 4),
                    "direction":    direction,
                    "window_size":  len(values),
                },
            )
            detected.append("drift")

    return detected



# Main scan — iterates over all distinct numeric entities


def run_scan(db: Database) -> dict[str, int]:
    """
    Synchronous scan of all numeric entities.
    Called from the async loop via asyncio.to_thread() to avoid
    blocking the FastAPI event loop.

    Returns a summary dict:  { anomaly_type: count }
    """
    cfg = _load_config(db)
    summary: dict[str, int] = {}

    # All distinct entity_ids that have numeric readings
    entity_ids = db["entity_logs"].distinct(
        "entity_id",
        {"value_type": "number", "numeric_value": {"$ne": None}},
    )

    logger.info("rolling_window_detector: scanning %d entities", len(entity_ids))

    for entity_id in entity_ids:
        # Grab metadata from the most recent log entry for this entity
        meta = db["entity_logs"].find_one(
            {"entity_id": entity_id, "value_type": "number"},
            {"metric": 1, "metric_name": 1, "indicator_label": 1, "unit": 1},
            sort=[("recorded_at", -1)],
        )
        if not meta:
            continue

        fired = _analyse_entity(
            db,
            entity_id=entity_id,
            metric=meta.get("metric", ""),
            metric_name=meta.get("metric_name", ""),
            indicator_label=meta.get("indicator_label", ""),
            unit=meta.get("unit", ""),
            cfg=cfg,
        )
        for t in fired:
            summary[t] = summary.get(t, 0) + 1

    logger.info("rolling_window_detector scan complete: %s", summary)
    return summary



# Async background loop  (registered in main.py startup)


async def rolling_window_loop(db: Database, stop_event: asyncio.Event) -> None:

    logger.info("rolling_window_detector: background loop started")
    while not stop_event.is_set():
        try:
            cfg = _load_config(db)
            interval = cfg["run_interval"]
            # Run blocking MongoDB scan off the event loop thread
            summary = await asyncio.to_thread(run_scan, db)
            if summary:
                logger.info("rolling_window anomalies this scan: %s", summary)
        except Exception:
            logger.exception("rolling_window_detector: error during scan")

        # Sleep for `interval` seconds, but wake immediately if stop is set
        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=interval,
            )
        except asyncio.TimeoutError:
            pass  # normal — interval elapsed, loop again

    logger.info("rolling_window_detector: background loop stopped")
