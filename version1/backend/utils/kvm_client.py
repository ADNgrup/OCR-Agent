from __future__ import annotations

import logging
import time
from io import BytesIO

import requests
from PIL import Image, ImageStat

logger = logging.getLogger("kvm_client")

DEFAULT_HEADERS = {
    "Cache-Control": "no-cache",
    "User-Agent": "KVM-OCR/2.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

MIN_IMAGE_BYTES = 5_000
STATUS_POLL_TIMEOUT = 60
CONNECTION_CHECK_TIMEOUT = 60
SNAPSHOT_RETRIES = 3


def build_base_url(source: dict) -> str:
    base_path = str(source.get("base_path") or "kx").strip("/")
    return f"http://{source.get('host')}:{source.get('port')}/{base_path}"


# ---------- low-level helpers (session-aware) ----------

def _make_session(extra_headers: dict | None = None) -> requests.Session:
    """Create a requests.Session that preserves cookies across calls."""
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    if extra_headers:
        s.headers.update(extra_headers)
    return s


def _post(session: requests.Session, url: str, params=None, json_body=None, timeout=30):
    try:
        r = session.post(url, params=params, json=json_body or {}, timeout=timeout)
        logger.info("POST %s → %s %s (%d B)", url, r.status_code, r.reason, len(r.content))
        return r
    except Exception as e:
        logger.error("POST %s failed: %s", url, e)
        return None


def _get(session: requests.Session, url: str, params=None, timeout=30, stream=False):
    try:
        r = session.get(url, params=params, timeout=timeout, stream=stream)
        logger.info("GET %s → %s %s", url, r.status_code, r.reason)
        return r
    except Exception as e:
        logger.error("GET %s failed: %s", url, e)
        return None


def _is_black_image(data: bytes, threshold: float = 5.0) -> bool:
    try:
        img = Image.open(BytesIO(data))
        gray = img.convert("L")
        stat = ImageStat.Stat(gray)
        return stat.mean[0] < threshold and stat.stddev[0] < threshold
    except Exception:
        return False


def _ensure_connected_and_ready(session: requests.Session, base_url: str) -> bool:
    r = _post(session, f"{base_url}/connect", timeout=CONNECTION_CHECK_TIMEOUT)
    if not r or r.status_code != 200:
        logger.error("connect failed for %s", base_url)
        return False

    start = time.time()
    while time.time() - start < STATUS_POLL_TIMEOUT:
        r = _get(session, f"{base_url}/status", timeout=15)
        if r and r.status_code == 200:
            try:
                status = r.json()
                if status.get("connected") and status.get("videoReady"):
                    logger.info("videoReady for %s", base_url)
                    return True
            except Exception:
                pass
        time.sleep(0.5)

    logger.warning("videoReady timeout for %s", base_url)
    _post(session, f"{base_url}/disconnect", timeout=15)
    return False


def _wake_screen(session: requests.Session, base_url: str, monitor_key: str | None = None):
    params = {"xCoordinate": 5, "yCoordinate": 5}
    if monitor_key:
        params["monitorKey"] = monitor_key
    _post(session, f"{base_url}/sendmouse", params=params, timeout=30)
    time.sleep(0.5)


# ---------- public API ----------

def fetch_snapshot_bytes(source: dict, monitor_key: str | None = None) -> bytes | None:
    """
    Fetch a screenshot from one KVM monitor.

    Uses a persistent ``requests.Session`` so cookies set by ``/connect``
    are sent on every subsequent request (the KVM endpoint requires this).
    Retries up to SNAPSHOT_RETRIES times, waking the screen between attempts.
    """
    extra_headers = source.get("headers") or {}
    session = _make_session(extra_headers)
    base_url = build_base_url(source)

    try:
        # 1. Connect & wait for videoReady
        if not _ensure_connected_and_ready(session, base_url):
            return None

        # 2. Wake the screen
        _wake_screen(session, base_url, monitor_key)

        # 3. Fetch snapshot with retries
        snapshot_url = f"{base_url}/snapshot"
        snapshot_params = {"monitorKey": monitor_key} if monitor_key else None

        for attempt in range(1, SNAPSHOT_RETRIES + 1):
            logger.info("Snapshot attempt %d/%d for %s", attempt, SNAPSHOT_RETRIES, base_url)

            r = _get(session, snapshot_url, params=snapshot_params, timeout=CONNECTION_CHECK_TIMEOUT, stream=True)
            if not r or r.status_code != 200:
                logger.warning("Snapshot request failed (attempt %d)", attempt)
                continue

            content = r.content
            logger.info(
                "Snapshot: %d bytes  Content-Type=%s",
                len(content),
                r.headers.get("Content-Type"),
            )

            if len(content) < MIN_IMAGE_BYTES:
                logger.warning("Snapshot too small (%d B), waking & retrying", len(content))
                _wake_screen(session, base_url, monitor_key)
                continue

            # validate image
            try:
                img = Image.open(BytesIO(content))
                img.load()
            except Exception as e:
                logger.warning("Invalid image (attempt %d): %s", attempt, e)
                _wake_screen(session, base_url, monitor_key)
                continue

            if _is_black_image(content):
                logger.warning("Black image (attempt %d), retrying", attempt)
                time.sleep(1.5)
                continue

            logger.info("Valid snapshot from %s (%d bytes)", base_url, len(content))
            return content

        logger.error("All %d snapshot attempts failed for %s", SNAPSHOT_RETRIES, base_url)
        return None

    finally:
        _post(session, f"{base_url}/disconnect", timeout=5)


# kept for llm_client.py compatibility
def request_with_log(method: str, url: str, **kwargs) -> requests.Response | None:
    try:
        timeout = kwargs.pop("timeout", 60)
        logger.info("External Req: %s %s (timeout=%s)", method, url, timeout)
        r = requests.request(method, url, timeout=timeout, **kwargs)
        logger.info("External Res: %s %s → %s", method, url, r.status_code)
        return r
    except Exception as e:
        logger.error("External Req Failed: %s %s | Error: %s", method, url, e)
        return None
