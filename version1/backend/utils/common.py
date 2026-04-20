from __future__ import annotations

from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_key(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum() or ch in ["_", "-"]).strip("_-") or "entity"


def parse_numeric(value: str | None) -> float | None:
    if not value:
        return None
    clean = "".join(ch for ch in str(value) if ch.isdigit() or ch in ".-")
    if clean in ["", "-", ".", "-."]:
        return None
    try:
        return float(clean)
    except Exception:
        return None


# ---- value type classification ----

_KNOWN_COLORS = frozenset({
    "red", "green", "blue", "yellow", "cyan", "magenta", "orange",
    "white", "black", "gray", "grey", "purple", "pink",
})

_BOOL_VALUES = frozenset({"on", "off", "true", "false", "yes", "no"})
_BOOL_TRUE_VALUES = frozenset({"on", "true", "yes", "run", "running", "open", "opened", "投入", "運転", "開", "開放"})
_BOOL_FALSE_VALUES = frozenset({"off", "false", "no", "stop", "stopped", "close", "closed", "停止", "切", "閉", "遮断"})

import re as _re
# Match a leading number optionally followed by a unit suffix, e.g. "2132mm", "53.5°C", "21L/min"
_NUM_WITH_UNIT_RE = _re.compile(
    r'^\s*([+-]?\d+(?:\.\d+)?)\s*'
    r'(?:°?[a-zA-Z%/][a-zA-Z0-9/%°]*)?\s*$'
)


def extract_numeric_and_unit(value: str | None) -> tuple[float | None, str | None]:
    """Try to split a value like '2132mm' into (2132.0, 'mm') or '53.5°C' into (53.5, '°C').
    Returns (None, None) if not a match."""
    if not value:
        return None, None
    v = str(value).strip()
    m = _NUM_WITH_UNIT_RE.match(v)
    if m:
        num = float(m.group(1))
        unit_part = v[m.end(1):].strip() or None
        return num, unit_part
    return None, None


def classify_value_type(value: str | None) -> str | None:
    """Return 'number', 'color', 'bool', or 'text' for non-empty values."""
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    if v in _BOOL_VALUES or v in _BOOL_TRUE_VALUES or v in _BOOL_FALSE_VALUES:
        return "bool"
    if v in _KNOWN_COLORS:
        return "color"
    # Direct numeric
    if parse_numeric(v) is not None:
        return "number"
    # Number with unit suffix: "2132mm", "53.5°C", "21L/min"
    num, _ = extract_numeric_and_unit(v)
    if num is not None:
        return "number"
    return "text"


def clean_numeric_value(value: str | None) -> float | None:
    """Parse a numeric value, handling embedded units like '2132mm'."""
    pn = parse_numeric(value)
    if pn is not None:
        return pn
    num, _ = extract_numeric_and_unit(value)
    return num
