import base64
import io
import json
import logging
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd
from openai import OpenAI

from cores.config import API_KEY, LLM_BASEAPI, LLM_MODEL, LLM_MAX_TOKENS
from utils.kvm_client import request_with_log
from . import prompts


SETTINGS_FILE = Path(__file__).resolve().parents[2] / "storage" / "settings.json"
logger = logging.getLogger("llm_client")


NUMERIC_NON_LOG_REQUIREMENTS = (
    "CRITICAL OUTPUT REQUIREMENTS: "
    "For every HMI Object indicator and every Table cell/subentity, output numeric values only. "
    "Convert boolean states such as ON/OFF, OPEN/CLOSE, RUN/STOP, TRUE/FALSE to 1/0. "
    "For non-Log entities, do not output a metric field inside indicators. "
    "For every Log/Alert entity, raw_csv_table header must be exactly time,message."
)


def _normalize_openai_base_url(raw_base_url: str) -> str:
    base = str(raw_base_url or "").strip().rstrip("/")
    if not base:
        return ""
    if base.lower().endswith("/v1"):
        return base
    return f"{base}/v1"


def _preview_text(value: str, limit: int = 800) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]..."


def _normalize_numeric_scalar(value: Any) -> int | float | None:
    from utils.common import clean_numeric_value, _BOOL_TRUE_VALUES, _BOOL_FALSE_VALUES

    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        f_val = float(value)
        if not math.isfinite(f_val):
            return None
        return int(f_val) if f_val.is_integer() else f_val

    text = str(value).strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered == "on" or lowered in _BOOL_TRUE_VALUES:
        return 1
    if lowered == "off" or lowered in _BOOL_FALSE_VALUES:
        return 0

    parsed = clean_numeric_value(text)
    if parsed is None:
        return None

    parsed_f = float(parsed)
    if not math.isfinite(parsed_f):
        return None
    return int(parsed_f) if parsed_f.is_integer() else parsed_f


def _coerce_numeric_pair(raw_value: Any, explicit_number: Any = None, default: float = 0.0) -> tuple[int | float, str]:
    for candidate in (explicit_number, raw_value):
        number = _normalize_numeric_scalar(candidate)
        if number is not None:
            return number, str(number)

    fallback = int(default) if float(default).is_integer() else float(default)
    return fallback, str(fallback)


def _load_runtime_settings_file() -> dict[str, Any]:
    if not SETTINGS_FILE.exists():
        return {}
    try:
        with SETTINGS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_runtime_llm_settings() -> dict[str, str]:
    settings = {
        "llm_base_api": LLM_BASEAPI,
        "llm_model": LLM_MODEL,
        "api_key": API_KEY,
        "default_image_prompt": prompts.prompt_v1.DEFAULT_IMAGE_PROMPT,
        "markdown_to_json_prompt": prompts.prompt_v1.MARKDOWN_TO_JSON_PROMPT,
        "extract_from_schema_prompt": prompts.prompt_v1.EXTRACT_FROM_SCHEMA_PROMPT,
        "v2_extract_base_prompt": getattr(prompts.prompt_v2, "V2_EXTRACT_BASE_PROMPT", ""),
        "v2_scada_prompt": getattr(prompts.prompt_v2, "V2_SCADA_OBJECT_PROMPT", ""),
        "v2_fixed_table_prompt": getattr(prompts.prompt_v2, "V2_FIXED_TABLE_PROMPT", ""),
        "v2_log_table_prompt": getattr(prompts.prompt_v2, "V2_LOG_TABLE_PROMPT", ""),
        "v2_merge_prompt": getattr(prompts.prompt_v2, "V2_MERGE_PROMPT", ""),
    }
    doc = _load_runtime_settings_file()
    for key in settings:
        if key in doc and doc.get(key) is not None:
            settings[key] = doc[key]
    
    return settings


def ensure_llm_name(markdown: str, fallback: str) -> str:
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    for line in lines:
        if line.startswith("#"):
            return line.strip("# ")[:255]
    return fallback


def call_llm_image_to_markdown(image_bytes: bytes) -> str:
    settings = _load_runtime_llm_settings()
    base_url = _normalize_openai_base_url(settings.get("llm_base_api", ""))
    if not base_url:
        return ""
    
    client = OpenAI(
        base_url=base_url,
        api_key=settings["api_key"] or "no-key"
    )

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    
    try:
        response = client.chat.completions.create(
            model=settings["llm_model"],
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": settings["default_image_prompt"]},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                    ],
                }
            ],
            temperature=0,
            timeout=600
        )
        content = response.choices[0].message.content.strip()
        logger.info("LLM markdown output (%d chars):\n%s", len(content), _preview_text(content))
        return content
    except Exception as exc:
        logger.exception("image->markdown failed: %s", exc)
        return ""

def _parse_table_csv_to_subentities(csv_text: str, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    import csv
    from utils.common import extract_numeric_and_unit

    metadata = metadata if isinstance(metadata, dict) else {}
    csv_text = (csv_text or "").strip()
    if not csv_text:
        return []

    try:
        rows = list(csv.reader(io.StringIO(csv_text)))
    except Exception as exc:
        logger.error("Failed to parse table CSV: %s", exc)
        return []

    if len(rows) < 2:
        return []

    rows = [[str(cell).strip() for cell in row] for row in rows]
    header = rows[0]
    if not header:
        return []

    meta_value_cols = metadata.get("value_columns", [])
    if not isinstance(meta_value_cols, list):
        meta_value_cols = []
    value_columns = [str(col).strip() for col in meta_value_cols if str(col).strip()]

    meta_rows = metadata.get("rows", [])
    if not isinstance(meta_rows, list):
        meta_rows = []
    meta_rows = [str(row).strip() for row in meta_rows if str(row).strip()]

    first_col_text = str(header[0] if header else "").strip().lower()
    header_has_row_col = first_col_text in {"row", "rows", "item", "name", "label"}
    header_values = [col for col in header[1:] if col]

    header_match_count = 0
    if value_columns and header_values:
        expected_set = set(value_columns)
        header_match_count = len([col for col in header_values if col in expected_set])
    header_looks_like_schema = bool(value_columns) and header_match_count >= max(1, min(2, len(value_columns)))

    use_header = header_has_row_col or header_looks_like_schema
    data_rows = rows[1:] if use_header else rows

    if use_header and not value_columns and len(header) > 1:
        value_columns = header[1:]

    if not value_columns:
        max_width = max((len(row) for row in data_rows), default=len(header))
        if max_width <= 1:
            return []
        value_columns = [f"col_{idx}" for idx in range(1, max_width)]

    unit_text = str(metadata.get("unit", "") or "")
    unit_parts = [part.strip() for part in unit_text.split("|")] if unit_text else []
    unit_by_col = {col: (unit_parts[idx] if idx < len(unit_parts) else "") for idx, col in enumerate(value_columns)}

    col_idx = {col: idx for idx, col in enumerate(header)} if use_header else {}
    subentities: list[dict[str, Any]] = []

    for row_idx, row in enumerate(data_rows):
        row_name = str(row[0]).strip() if row else ""
        if not row_name and row_idx < len(meta_rows):
            row_name = meta_rows[row_idx]
        if not row_name:
            row_name = "Unknown"

        for col_pos, col in enumerate(value_columns):
            if use_header:
                idx = col_idx.get(col)
                if idx is None:
                    continue
            else:
                idx = col_pos + 1

            if idx >= len(row):
                continue

            raw_val = str(row[idx]).strip()
            if not raw_val:
                continue

            detected_number, detected_unit = extract_numeric_and_unit(raw_val)
            value_number, normalized_raw = _coerce_numeric_pair(raw_val, detected_number, default=0)
            subentities.append(
                {
                    "col": col,
                    "row": row_name,
                    "value_raw": normalized_raw,
                    "value_number": value_number,
                    "unit": unit_by_col.get(col) or detected_unit or "",
                    "value_type": "number",
                }
            )

    return subentities


def _parse_log_csv(csv_text: str) -> list[dict[str, Any]]:
    csv_text = (csv_text or "").strip()
    if not csv_text:
        return []

    try:
        df = pd.read_csv(io.StringIO(csv_text), dtype=str, keep_default_na=False)
    except Exception as exc:
        logger.error("Failed to parse log CSV via pandas: %s", exc)
        return []

    if df.empty:
        return []

    df.columns = [str(col).strip() for col in df.columns]
    columns = list(df.columns)
    if not columns:
        return []

    lower_map = {col.lower(): col for col in columns}
    time_col = lower_map.get("time", columns[0])
    message_col = lower_map.get("message")
    name_col = lower_map.get("name")
    desc_col = lower_map.get("desc")

    logs: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        time_text = str(row.get(time_col, "")).strip()
        if message_col:
            message_text = str(row.get(message_col, "")).strip()
        else:
            name_text = str(row.get(name_col, "")).strip() if name_col else ""
            desc_text = str(row.get(desc_col, "")).strip() if desc_col else ""
            if not name_text and len(columns) > 1:
                name_text = str(row.get(columns[1], "")).strip()
            if not desc_text and len(columns) > 2:
                desc_text = str(row.get(columns[2], "")).strip()
            if name_text and desc_text:
                message_text = f"{name_text}: {desc_text}"
            else:
                message_text = name_text or desc_text

        logs.append({
            "time": time_text,
            "message": message_text,
        })
    return logs


def _normalize_indicator_values(indicators: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for ind in indicators or []:
        label = str(ind.get("label", "") or "")
        if not label:
            label = str(ind.get("metric", "") or "")

        value_raw = ind.get("value_raw")
        if value_raw is None or value_raw == "":
            value_raw = ind.get("value")
        if (value_raw is None or value_raw == "") and ind.get("value_number") is not None:
            value_raw = str(ind.get("value_number"))
        if value_raw is None:
            value_raw = ""

        value_number, normalized_raw = _coerce_numeric_pair(
            value_raw,
            ind.get("value_number"),
            default=0,
        )

        normalized.append({
            "label": label,
            "value_type": "number",
            "unit": ind.get("unit", ""),
            "value_raw": normalized_raw,
            "value_number": value_number,
        })

    return normalized


def _extract_entities_from_openai_response(response_or_string) -> dict[str, Any]:
    content = ""
    try:
        if isinstance(response_or_string, str):
            content = response_or_string.strip()
        else:
            content = (response_or_string.choices[0].message.content or "").strip()

        logger.info("LLM raw JSON output (%d chars):\n%s", len(content), _preview_text(content))

        # 1. Strip out reasoning/thinking blocks if present
        if '</think>' in content:
            content = content.split('</think>', 1)[-1].strip()
        else:
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()

        # 2. Extract content from markdown code blocks if the LLM fenced it
        code_block_match = re.search(r'```(?:json)?\s*(.*?)\s*```', content, flags=re.DOTALL | re.IGNORECASE)
        if code_block_match:
            content = code_block_match.group(1).strip()

        # 3. Find the outermost curly braces to guarantee JSON bounds (ignoring conversational padding)
        start_idx = content.find('{')
        end_idx = content.rfind('}')
        if start_idx != -1:
            if end_idx != -1 and end_idx >= start_idx:
                content = content[start_idx:end_idx+1]
            else:
                content = content[start_idx:]

        import json_repair
        try:
            parsed = json.loads(content)
        except Exception:
            parsed = json_repair.loads(content)

        if "entities" not in parsed or not isinstance(parsed["entities"], list):
            parsed["entities"] = []
            
        # Parse CSV objects into normalized runtime fields.
        for ent in parsed.get("entities", []):
            ent_type = str(ent.get("type", "")).strip().lower()
            csv_text = ent.get("raw_csv_table") or ent.get("csv_table") or ""

            if ent_type == "table" and isinstance(csv_text, str) and csv_text.strip():
                ent["raw_csv_table"] = csv_text
                ent["subentities"] = _parse_table_csv_to_subentities(csv_text, ent.get("metadata", {}))
            elif ent_type in ["log/alert", "log"] and isinstance(csv_text, str) and csv_text.strip():
                ent["raw_csv_table"] = csv_text
                ent["logs"] = _parse_log_csv(csv_text)
            elif ent_type == "hmi object":
                ent["indicators"] = _normalize_indicator_values(ent.get("indicators", []) or [])

        parsed["_raw_response"] = content
        parsed["_parse_error"] = None
        return parsed
    except Exception as exc:
        logger.error(f"========== FAILED TO PARSE LLM RESPONSE ==========\n"
                     f"--- EXCEPTION:\n{exc}\n"
                     f"--- ATTEMPTED TO PARSE THIS EXACT STRING:\n{content}\n"
                     f"==================================================")
        logger.exception("JSON parse failed: %s", exc)
        return {"entities": [], "_parse_error": str(exc)}

    
def call_llm_markdown_to_json(markdown: str, image_bytes: bytes | None = None, promptype='markdown_to_json_prompt', schema_str: str | None = None) -> dict[str, Any]:
    settings = _load_runtime_llm_settings()
    base_url = _normalize_openai_base_url(settings.get("llm_base_api", ""))
    if not base_url:
        return {"screen_title": "", "entities": []}
    
    client = OpenAI(
        base_url=base_url,
        api_key=settings["api_key"] or "no-key"
    )

    if image_bytes:
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")
        user_content = []
        if schema_str:
            user_content.append({"type": "text", "text": f"REQUIRED SCHEMA:\n{schema_str}"})
        user_content.append({"type": "text", "text": NUMERIC_NON_LOG_REQUIREMENTS})
        user_content.append(
            {
                "type": "text",
                "text": f"MARKDOWN CONTENT:\n{markdown}" if markdown else "Extract from image and strictly follow REQUIRED SCHEMA.",
            }
        )
        user_content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}})
    else:
        user_content = f"REQUIRED SCHEMA:\n{schema_str}\n\n{markdown}" if schema_str else markdown

    try:
        response = client.chat.completions.create(
            model=settings["llm_model"],
            messages=[
                {"role": "system", "content": settings[promptype]},
                {"role": "user", "content": user_content},
            ],
            temperature=0,
            timeout=600
        )
        return _extract_entities_from_openai_response(response)
    except Exception as exc:
        logger.error("markdown->json failed: %s", exc)
        return {"entities": [], "_parse_error": str(exc)}

def call_llm_v2_extract(
    image_bytes: bytes,
    layout_text: str,
    schema_str: str | None = None,
    schema_bootstrap: bool = False,
) -> dict[str, Any]:
    settings = _load_runtime_llm_settings()
    base_url = _normalize_openai_base_url(settings.get("llm_base_api", ""))
    if not base_url:
        return {"screen_title": "", "entities": []}

    client = OpenAI(
        base_url=base_url,
        api_key=settings["api_key"] or "no-key"
    )

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")

    user_content: list[dict[str, Any]] = []
    user_content.append({"type": "text", "text": NUMERIC_NON_LOG_REQUIREMENTS})
    if schema_bootstrap:
        user_content.append(
            {
                "type": "text",
                "text": (
                    "SCHEMA BOOTSTRAP MODE: Infer stable entities and schema from the current screen first, "
                    "then return current values using that inferred structure."
                ),
            }
        )
    if schema_str:
        user_content.append({"type": "text", "text": f"MANDATORY SCHEMA TO FOLLOW:\n{schema_str}"})
    if layout_text:
        user_content.append({"type": "text", "text": f"SPATIAL TEXT LAYOUT (OCR):\n{layout_text}"})
    user_content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}})

    messages = [
        {"role": "system", "content": settings.get("v2_extract_base_prompt", "")},
        {"role": "user", "content": user_content},
    ]

    try:
        response = client.chat.completions.create(
            model=settings["llm_model"],
            messages=messages,
            temperature=0,
            timeout=1200,
            stream=False,
            max_tokens=LLM_MAX_TOKENS,
        )
        final_content = response.choices[0].message.content or ""
        logger.info("Single-pass V2 extraction completed, output length=%d chars", len(final_content))
        return _extract_entities_from_openai_response(final_content)
    except Exception as exc:
        logger.error("v2 single-pass extract failed: %s", exc)
        return {"entities": [], "_parse_error": str(exc)}

def call_llm_segment(
    image_bytes: bytes,
    seg_type: str,
    columns: list[str] | None = None,
    rows: list[str] | None = None,
) -> dict[str, Any]:
    settings = _load_runtime_llm_settings()
    base_url = _normalize_openai_base_url(settings.get("llm_base_api", ""))
    if not base_url:
        return {}

    client = OpenAI(
        base_url=base_url,
        api_key=settings["api_key"] or "no-key"
    )

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    
    prompt_key = "v2_scada_prompt"
    if seg_type == "fixed table":
        prompt_key = "v2_fixed_table_prompt"
    elif seg_type == "log tables":
        prompt_key = "v2_log_table_prompt"
    
    system_prompt = settings.get(prompt_key, "")
    if prompt_key == "v2_scada_prompt":
        indicators_list = columns or []
        csv_rows = "\n".join([f"{label}," for label in indicators_list])
        system_prompt = system_prompt.format(
            indicators=", ".join(indicators_list),
            indicators_csv_rows=csv_rows
        )
    elif prompt_key == "v2_fixed_table_prompt":
        col_header = ",".join(columns or [])
        row_templates = "\n".join([f"{row_name}," for row_name in (rows or [])])
        system_prompt = system_prompt.format(
            columns=", ".join(columns or []),
            rows=", ".join(rows or []),
            columns_header=col_header,
            rows_csv_template=row_templates
        )

    try:
        response = client.chat.completions.create(
            model=settings["llm_model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                    ],
                },
            ],
            temperature=0,
            timeout=300,
            max_tokens=4096,
        )
        content = response.choices[0].message.content.strip()
        # Remove markdown code blocks if the LLM hallucinated them
        if content.startswith("```"):
            content = re.sub(r"```(csv)?\n?", "", content)
            content = re.sub(r"\n?```", "", content)
        
        return {"raw_csv_table": content.strip()}
    except Exception as exc:
        logger.error("Segment LLM extraction failed (%s): %s", seg_type, exc)
        return {"_parse_error": str(exc)}

