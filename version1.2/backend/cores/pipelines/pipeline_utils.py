from __future__ import annotations

from typing import Any

from utils.common import classify_value_type, extract_numeric_and_unit, normalize_key


class EntityExtractionNormalizer:
    """Normalize LLM extraction payloads into the internal entity/indicator shape."""

    @staticmethod
    def normalize_screen_title(extracted: dict[str, Any], fallback: str) -> str:
        if not isinstance(extracted, dict):
            return fallback
        return (
            str(extracted.get("screen_title") or "").strip()
            or str(extracted.get("screen_name") or "").strip()
            or fallback
        )

    @classmethod
    def normalize_indicator_entry(cls, entry: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(entry, dict):
            return None

        raw_type = str(entry.get("type") or "").strip()
        value_type = str(entry.get("value_type") or "").strip().lower()
        if not value_type and raw_type.lower() in ("number", "color", "bool", "text"):
            value_type = raw_type.lower()

        label = (
            str(entry.get("label") or "").strip()
            or str(entry.get("indicator") or "").strip()
            or str(entry.get("display_name") or "").strip()
        )
        if not label and raw_type and raw_type.lower() not in ("number", "color", "bool", "text"):
            label = raw_type

        metric_name = str(entry.get("metric") or "").strip()
        raw_val = entry.get("value_raw") if entry.get("value_raw") is not None else entry.get("value")
        raw_val = str(raw_val or "").strip()
        unit = entry.get("unit") or None

        if not value_type:
            value_type = classify_value_type(raw_val)
            if not value_type:
                return None

        if value_type == "number":
            num, extracted_unit = extract_numeric_and_unit(raw_val)
            if num is not None and extracted_unit and not unit:
                unit = extracted_unit
                raw_val = str(num)

        if not metric_name:
            metric_name = label or value_type

        display_name = label or metric_name
        display_key = normalize_key(display_name)
        metric_key = normalize_key(metric_name)

        schema_indicator_key = str(entry.get("indicator_key") or "").strip()  
        exact_metric_from_schema = str(entry.get("metric") or "")

        if schema_indicator_key:
            indicator_key = schema_indicator_key
        else:
            indicator_key = display_key if display_key != metric_key else metric_key
            if display_key and metric_key and display_key != metric_key:
                # If the exact metric matches our known legacy combined format, use it strictly.
                # Otherwise combine them locally for new extractions.
                if exact_metric_from_schema == normalize_key(f"{display_name}_{metric_name}") or "_" in exact_metric_from_schema:
                    indicator_key = exact_metric_from_schema
                else:
                    indicator_key = normalize_key(f"{display_name}_{metric_name}")

        return {
            "indicator_key": indicator_key or metric_key or display_key or value_type,
            "display_name": display_name,
            "indicator_label": label or display_name,
            "metric_name": metric_name,
            "metric_key": metric_key or normalize_key(display_name) or value_type,
            "value": raw_val,
            "unit": unit,
            "value_type": value_type,
            "confidence": entry.get("confidence") or "Unknown",
            "evidence": entry.get("evidence") or [],
        }

    @classmethod
    def normalize_entity_entry(cls, item: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(item, dict):
            return None

        display_name = (
            str(item.get("main_entity_name") or "").strip()
            or str(item.get("name") or "").strip()
            or str(item.get("display_name") or "").strip()
            or "unknown_entity"
        )
        entity_type = str(item.get("entity_type") or item.get("type") or "").strip().lower() or None
        regions = item.get("regions")
        if not isinstance(regions, list):
            single_region = item.get("region")
            regions = [single_region] if single_region else []

        indicators_raw = item.get("indicators")
        if not isinstance(indicators_raw, list):
            indicators_raw = item.get("metrics")
        if not isinstance(indicators_raw, list):
            indicators_raw = []

        if not indicators_raw and item.get("value") is not None:
            indicators_raw = cls.legacy_to_metrics(item)

        indicators: list[dict[str, Any]] = []
        for indicator in indicators_raw:
            normalized = cls.normalize_indicator_entry(indicator)
            if normalized:
                indicators.append(normalized)

        if not indicators:
            return None

        return {
            "display_name": display_name,
            "entity_key": normalize_key(display_name),
            "entity_type": entity_type,
            "regions": regions,
            "indicators": indicators,
        }

    @staticmethod
    def normalize_entity_name(item: dict[str, Any], fallback: str) -> str:
        """Helper to get a normalized entity name directly from an item."""
        if not isinstance(item, dict):
            return fallback
        name = (
            str(item.get("main_entity_name") or "").strip()
            or str(item.get("name") or "").strip()
            or str(item.get("display_name") or "").strip()
            or fallback
        )
        return name

    @staticmethod
    def slugify(text: str) -> str:
        """Helper to safely slugify a key"""
        return normalize_key(text)

    @staticmethod
    def legacy_to_metrics(item: dict) -> list[dict]:
        out: list[dict] = []
        val = str(item.get("value", "")).strip()
        unit = item.get("unit")
        status = item.get("status")
        color = item.get("color")

        if val:
            vtype = classify_value_type(val)
            if vtype == "number":
                num, extracted_unit = extract_numeric_and_unit(val)
                if num is not None:
                    if extracted_unit and not unit:
                        unit = extracted_unit
                    val = str(num)
                out.append({"metric": "value", "value": val, "unit": unit, "type": "number"})
            elif vtype:
                out.append({"metric": "value", "value": val, "unit": unit, "type": vtype})
        if status and str(status).strip().upper() not in ("", "UNKNOWN", "NULL"):
            out.append({"metric": "status", "value": str(status).strip(), "unit": None, "type": "bool"})
        if color and str(color).strip().lower() not in ("", "null"):
            out.append({"metric": "color", "value": str(color).strip(), "unit": None, "type": "color"})
        return out