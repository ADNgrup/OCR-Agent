"""
Runs anomaly checks on a single entity_log document immediately after it is
written to MongoDB.  Call `detect(log_doc, db)` from main.py after every
entity-log insert.

Checks performed (only on value_type == "number"):
  1. Null / missing numeric_value
  2. Low confidence  (confidence == "Low")
  3. Impossible / domain-rule value  (hard limits per metric_name)
  4. Outlier  (|value - historical_mean| > Z_THRESHOLD * historical_stdev)

Detected anomalies are written to the `anomaly_logs` collection in MongoDB.
"""

from __future__ import annotations

import statistics
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from pymongo.database import Database


# Configuration


# How many standard deviations from the mean before flagging as outlier
Z_THRESHOLD: float = 3.0

# How many historical readings to pull when computing mean / stdev
HISTORY_LIMIT: int = 100

# Minimum number of historical readings required before running outlier check
# (avoids false positives on sparse data)
MIN_HISTORY: int = 10

# Confidence string values treated as "low"
LOW_CONFIDENCE_VALUES: set[str] = {"Low", "low", "LOW"}

# Impossible-value rules
# Each entry:  metric_name (str)  →  (min_value, max_value)
# None means "no bound on that side"
IMPOSSIBLE_VALUE_RULES: dict[str, tuple[Optional[float], Optional[float]]] = {
    # temperatures in ℃
    "temperature":   (-50.0,  250.0),
    # percentage-based flow / valve opening
    "flow_rate":     (0.0,    100.0),
    # water level percentage
    "level_percent": (0.0,    100.0),
    # generic level readings
    "level":         (0.0,    None),
    # volume – non-negative
    "volume":        (0.0,    None),
    # bool encoded as 0/1
    "status":        (0.0,    1.0),
}

# Rules (used when writing anomaly_logs)

def _severity(anomaly_type: str) -> str:
    return {
        "null_value":      "medium",
        "low_confidence":  "low",
        "impossible_value":"high",
        "outlier":         "medium",
    }.get(anomaly_type, "medium")


# Helpers

def _confidence_is_low(confidence: Optional[str]) -> bool:
    if confidence is None:
        return False
    return str(confidence).strip() in LOW_CONFIDENCE_VALUES


def _check_impossible(value: float, metric_name: str) -> Optional[str]:
    """Return a description string if value violates domain rules, else None."""
    rule = IMPOSSIBLE_VALUE_RULES.get(metric_name)
    if rule is None:
        return None
    lo, hi = rule
    if lo is not None and value < lo:
        return f"{value} is below minimum {lo} for metric '{metric_name}'"
    if hi is not None and value > hi:
        return f"{value} is above maximum {hi} for metric '{metric_name}'"
    return None


def _fetch_history(entity_id: str, db: Database) -> list[float]:
    """Fetch recent numeric_value history for the same entity_id."""
    cursor = (
        db["entity_logs"]
        .find(
            {
                "entity_id": ObjectId(entity_id),
                "value_type": "number",
                "numeric_value": {"$ne": None, "$type": "double"},
            },
            {"numeric_value": 1, "_id": 0},
        )
        .sort("recorded_at", -1)
        .limit(HISTORY_LIMIT)
    )
    return [doc["numeric_value"] for doc in cursor if doc.get("numeric_value") is not None]


def _check_outlier(
    value: float, entity_id: str, db: Database
) -> Optional[tuple[float, float, float]]:
    """
    Returns (mean, stdev, z_score) if value is an outlier, else None.
    Requires at least MIN_HISTORY historical readings.
    """
    history = _fetch_history(entity_id, db)
    if len(history) < MIN_HISTORY:
        return None

    mean = statistics.mean(history)
    try:
        stdev = statistics.stdev(history)
    except statistics.StatisticsError:
        return None

    if stdev == 0:
        return None  # all readings identical — no variance to measure

    z_score = abs(value - mean) / stdev
    if z_score > Z_THRESHOLD:
        return (mean, stdev, z_score)
    return None

# Anomaly writer

def _write_anomaly(
    log_doc: dict,
    anomaly_type: str,
    description: str,
    db: Database,
    extra: Optional[dict] = None,
) -> None:

    doc = {
        "entity_log_id":   log_doc.get("_id"),
        "entity_id":       log_doc.get("entity_id"),
        "snapshot_id":     log_doc.get("snapshot_id"),
        "metric":          log_doc.get("metric"),
        "metric_name":     log_doc.get("metric_name"),
        "indicator_label": log_doc.get("indicator_label"),
        "value_type":      log_doc.get("value_type"),
        "raw_value":       log_doc.get("raw_value"),
        "numeric_value":   log_doc.get("numeric_value"),
        "unit":            log_doc.get("unit"),
        "confidence":      log_doc.get("confidence"),
        "anomaly_type":    anomaly_type,
        "severity":        _severity(anomaly_type),
        "description":     description,
        "detected_at":     datetime.now(timezone.utc),
        "resolved":        False,
        "detector":        "per_write",
    }
    if extra:
        doc.update(extra)
    db["anomaly_logs"].insert_one(doc)



def detect(log_doc: dict, db: Database) -> list[str]:
    """
    Run all anomaly checks against a freshly written entity_log document.
    """
    detected: list[str] = []

    # -- Check 1: null / missing value (applies to all value_types)
    if log_doc.get("numeric_value") is None and log_doc.get("value_type") == "number":
        _write_anomaly(
            log_doc,
            anomaly_type="null_value",
            description=(
                f"numeric_value is null for a number-type entity "
                f"(raw_value='{log_doc.get('raw_value')}')"
            ),
            db=db,
        )
        detected.append("null_value")

    # -- Check 2: low confidence (applies to all value_types)
    if _confidence_is_low(log_doc.get("confidence")):
        _write_anomaly(
            log_doc,
            anomaly_type="low_confidence",
            description=(
                f"LLM reported Low confidence "
                f"(evidence={log_doc.get('evidence')})"
            ),
            db=db,
        )
        detected.append("low_confidence")

    # Remaining checks only make sense for numeric readings--> please let me know if we are going to apply to other ypes or not.
    if log_doc.get("value_type") != "number":
        return detected

    numeric_value: Optional[float] = log_doc.get("numeric_value")
    if numeric_value is None:
        return detected  # already flagged above

    metric_name: str = log_doc.get("metric_name", "")

    # -- Check 3: impossible / domain-rule value
    impossible_msg = _check_impossible(numeric_value, metric_name)
    if impossible_msg:
        _write_anomaly(
            log_doc,
            anomaly_type="impossible_value",
            description=impossible_msg,
            db=db,
        )
        detected.append("impossible_value")

    # -- Check 4: statistical outlier vs. entity history
    entity_id = str(log_doc.get("entity_id", ""))
    outlier_result = _check_outlier(numeric_value, entity_id, db)
    if outlier_result:
        mean, stdev, z_score = outlier_result
        _write_anomaly(
            log_doc,
            anomaly_type="outlier",
            description=(
                f"Value {numeric_value} is {z_score:.2f} standard deviations "
                f"from historical mean {mean:.2f} (stdev={stdev:.2f})"
            ),
            db=db,
            extra={"z_score": round(z_score, 4), "historical_mean": round(mean, 4), "historical_stdev": round(stdev, 4)},
        )
        detected.append("outlier")

    return detected
