import base64
import json
import logging
import re
from typing import Any
from openai import OpenAI

from cores.config import API_KEY, LLM_BASEAPI, LLM_MODEL, LLM_MAX_TOKENS
from cores.dbconnection.mongo import get_db
from utils.kvm_client import request_with_log
from . import prompts


CONFIG_KEY = "system_settings"
logger = logging.getLogger("llm_client")


def _preview_text(value: str, limit: int = 800) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]..."


def _load_runtime_llm_settings() -> dict[str, str]:
    settings = {
        "llm_base_api": LLM_BASEAPI,
        "llm_model": LLM_MODEL,
        "api_key": API_KEY,
        "default_image_prompt": prompts.prompt_v1.DEFAULT_IMAGE_PROMPT,
        "markdown_to_json_prompt": prompts.prompt_v1.MARKDOWN_TO_JSON_PROMPT,
        "extract_from_schema_prompt": prompts.prompt_v1.EXTRACT_FROM_SCHEMA_PROMPT,
        "v2_extract_base_prompt": getattr(prompts.prompt_v2, "V2_EXTRACT_BASE_PROMPT", ""),
        "v2_merge_prompt": getattr(prompts.prompt_v2, "V2_MERGE_PROMPT", ""),
    }
    try:
        db = get_db()
        doc = db.app_config.find_one({"_key": CONFIG_KEY}) or {}
        for key in settings:
            if key in doc and doc.get(key) is not None:
                settings[key] = doc[key]
    except Exception:
        pass
    
    return settings


def ensure_llm_name(markdown: str, fallback: str) -> str:
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    for line in lines:
        if line.startswith("#"):
            return line.strip("# ")[:255]
    return fallback


def call_llm_image_to_markdown(image_bytes: bytes) -> str:
    settings = _load_runtime_llm_settings()
    if not settings["llm_base_api"]:
        return ""
    
    client = OpenAI(
        base_url=settings["llm_base_api"].rstrip("/") + "/v1",
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

def _extract_entities_from_openai_response(response_or_string) -> dict[str, Any]:
    try:
        if isinstance(response_or_string, str):
            content = response_or_string.strip()
        else:
            content = response_or_string.choices[0].message.content.strip()

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
            
        # --- Pre-process CSV Tables into Subentities ---
        import csv, io
        from utils.common import classify_value_type, extract_numeric_and_unit
        
        for ent in parsed.get("entities", []):
            if ent.get("type") == "Table" and ent.get("raw_csv_table"):
                csv_text = ent.get("raw_csv_table", "")
                subentities = []
                try:
                    reader = csv.reader(io.StringIO(csv_text.strip()))
                    rows = list(reader)
                    if rows and len(rows) >= 2:
                        headers = [str(h).strip() for h in rows[0]]
                        for row in rows[1:]:
                            if not row: continue
                            row_name = str(row[0]).strip()
                            for i, val in enumerate(row[1:], start=1):
                                col_name = headers[i] if i < len(headers) else f"col_{i}"
                                raw_val = str(val).strip()
                                num_val, unit = extract_numeric_and_unit(raw_val)
                                v_type = classify_value_type(raw_val) or "text"
                                
                                subentities.append({
                                    "col": col_name,
                                    "row": row_name,
                                    "value_raw": raw_val,
                                    "value_number": num_val,
                                    "unit": unit or "",
                                    "value_type": v_type,
                                    "confidence": "High"
                                })
                except Exception as e:
                    logger.error(f"Failed to parse CSV string for Table: {e}")
                ent["subentities"] = subentities
                
            elif ent.get("type", "").lower() in ["log/alert", "log"] and ent.get("raw_csv_table"):
                csv_text = ent.get("raw_csv_table", "")
                logs_out = []
                try:
                    reader = csv.reader(io.StringIO(csv_text.strip()))
                    rows = list(reader)
                    if rows and len(rows) >= 2:
                        for row in rows[1:]:
                            if len(row) >= 3:
                                logs_out.append({
                                    "time": str(row[0]).strip(),
                                    "name": str(row[1]).strip(),
                                    "desc": str(row[2]).strip(),
                                })
                            elif len(row) >= 1:
                                logs_out.append({
                                    "time": str(row[0]).strip(),
                                    "name": str(row[1]).strip() if len(row) > 1 else "",
                                    "desc": "",
                                })
                except Exception as e:
                    logger.error(f"Failed to parse CSV string for Log: {e}")
                ent["logs"] = logs_out

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
    if not settings["llm_base_api"]:
        return {"screen_title": "", "entities": []}
    
    client = OpenAI(
        base_url=settings["llm_base_api"].rstrip("/") + "/v1",
        api_key=settings["api_key"] or "no-key"
    )

    if image_bytes:
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")
        user_content = [
            {"type": "text", "text": f"MARKDOWN CONTENT:\n{markdown}" if markdown else "Please extract from image based on the schema below."},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
        ]
        if schema_str:
            user_content.insert(0, {"type": "text", "text": f"REQUIRED SCHEMA:\n{schema_str}"})
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

def call_llm_v2_extract(image_bytes: bytes, layout_text: str, schema_str: str | None = None) -> dict[str, Any]:
    settings = _load_runtime_llm_settings()
    if not settings["llm_base_api"]:
        return {"screen_title": "", "entities": []}

    base_url = settings["llm_base_api"].rstrip("/")

    client = OpenAI(
        base_url=base_url,
        api_key=settings["api_key"] or "no-key"
    )

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    
    import concurrent.futures

    # TASK 1: INITIAL EXTRACTION (without OCR layout)
    def task_image_only():
        user_content = []
        if schema_str:
            user_content.append({"type": "text", "text": f"MANDATORY SCHEMA TO FOLLOW:\n{schema_str}"})
        user_content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}})
        messages = [
            {"role": "system", "content": settings.get("v2_extract_base_prompt", "")},
            {"role": "user", "content": user_content},
        ]
        response = client.chat.completions.create(
            model=settings["llm_model"],
            messages=messages,
            temperature=0,
            timeout=1200, 
            stream=False,  
            max_tokens=LLM_MAX_TOKENS
        )
        return response.choices[0].message.content

    # TASK 2: EXTRACTION (with OCR layout)
    def task_image_and_ocr():
        user_content = []
        if schema_str:
            user_content.append({"type": "text", "text": f"MANDATORY SCHEMA TO FOLLOW:\n{schema_str}"})
        if layout_text:
            user_content.append({"type": "text", "text": f"SPATIAL TEXT LAYOUT (OCR):\n{layout_text}"})
        user_content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}})
        messages = [
            {"role": "system", "content": settings.get("v2_extract_base_prompt", "")},
            {"role": "user", "content": user_content},
        ]
        response = client.chat.completions.create(
            model=settings["llm_model"],
            messages=messages,
            temperature=0,
            timeout=1200, 
            stream=False,  
            max_tokens=LLM_MAX_TOKENS
        )
        return response.choices[0].message.content

    try:
        logger.info("Starting Step 1: Parallel Extractions (Image Only vs Image + OCR)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_img = executor.submit(task_image_only)
            future_ocr = executor.submit(task_image_and_ocr)
            
            output_a = future_img.result()
            output_b = future_ocr.result()
            
        logger.info("Step 1 Complete. Output A length: %d chars, Output B length: %d chars", len(output_a or ""), len(output_b or ""))
        
        # STEP 2: MERGE
        user_content_merge = [
            {"type": "text", "text": f"EXTRACTION OUTPUT A (Vision Only):\n{output_a}\n\nEXTRACTION OUTPUT B (Vision + OCR Layout):\n{output_b}"},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}}
        ]
        
        messages_merge = [
            {"role": "system", "content": settings.get("v2_merge_prompt", "")},
            {"role": "user", "content": user_content_merge},
        ]
        
        logger.info("Starting Step 2: Merge Outputs...")
        response_merge = client.chat.completions.create(
            model=settings["llm_model"],
            messages=messages_merge,
            temperature=0,
            timeout=1200, 
            stream=True,  
            max_tokens=LLM_MAX_TOKENS
        )
        
        final_content = ""
        for chunk in response_merge:
            if hasattr(chunk, "choices") and len(chunk.choices) > 0:
                delta_content = getattr(chunk.choices[0].delta, "content", "")
                if delta_content:
                    final_content += delta_content
                    
        logger.info("Step 2 Complete. Final output length: %d chars", len(final_content))
        return _extract_entities_from_openai_response(final_content)
        
    except Exception as exc:
        logger.error("v2 parallel layout->json failed (2-step process): %s", exc)
        return {"entities": [], "_parse_error": str(exc)}

