from __future__ import annotations

import io
from typing import Any

import numpy as np
from PIL import Image, ImageStat
import cv2


def _largest_bbox_from_mask(mask: np.ndarray, min_area: int) -> tuple[int, int, int, int] | None:
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    best_bbox = None
    best_area = 0
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h
        if area < min_area:
            continue
        if area > best_area:
            best_area = area
            best_bbox = (x, y, w, h)
    return best_bbox


def autocrop_image(image_bytes: bytes) -> bytes:
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        arr = np.asarray(img)
        h, w = arr.shape[:2]
        if h < 60 or w < 60:
            return image_bytes

        strip = max(2, int(min(h, w) * 0.02))
        border_pixels = np.concatenate([
            arr[:strip, :, :].reshape(-1, 3),
            arr[-strip:, :, :].reshape(-1, 3),
            arr[:, :strip, :].reshape(-1, 3),
            arr[:, -strip:, :].reshape(-1, 3),
        ], axis=0)
        bg_color = np.median(border_pixels.astype(np.float32), axis=0)

        dist = np.linalg.norm(arr.astype(np.float32) - bg_color, axis=2)
        fg_mask = (dist > 18.0).astype(np.uint8) * 255

        k = max(3, int(min(h, w) * 0.01))
        if k % 2 == 0:
            k += 1
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (k, k))
        fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_CLOSE, kernel, iterations=2)
        fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_OPEN, kernel, iterations=1)

        min_area = int(h * w * 0.12)
        bbox = _largest_bbox_from_mask(fg_mask, min_area=min_area)

        if bbox is None:
            gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
            edges = cv2.Canny(gray, 50, 150)
            edge_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(3, k // 2), max(3, k // 2)))
            edges = cv2.dilate(edges, edge_kernel, iterations=2)
            bbox = _largest_bbox_from_mask(edges, min_area=min_area)

        if bbox is not None:
            x, y, bw, bh = bbox
            pad = max(4, int(min(h, w) * 0.008))
            x1 = max(0, x - pad)
            y1 = max(0, y - pad)
            x2 = min(w, x + bw + pad)
            y2 = min(h, y + bh + pad)

            crop_w = x2 - x1
            crop_h = y2 - y1
            if crop_w > int(w * 0.35) and crop_h > int(h * 0.35):
                if crop_w < int(w * 0.97) or crop_h < int(h * 0.97):
                    cropped = img.crop((x1, y1, x2, y2))
                    out = io.BytesIO()
                    cropped.save(out, format="JPEG", quality=90)
                    return out.getvalue()
    except Exception:
        pass
    return image_bytes

def histogram_feature(image: Image.Image) -> list[float]:
    resized = image.convert("RGB").resize((128, 128))
    arr = np.asarray(resized)
    hist_parts = []
    for channel in range(3):
        hist, _ = np.histogram(arr[:, :, channel], bins=16, range=(0, 256), density=True)
        hist_parts.append(hist)
    feature = np.concatenate(hist_parts)
    return feature.astype(float).tolist()

def brightness_feature(image: Image.Image) -> tuple[float, float]:
    gray = image.convert("L")
    stat = ImageStat.Stat(gray)
    return float(stat.mean[0]), float(stat.stddev[0])

def similarity_score(f1: list[float], f2: list[float], b1: tuple[float, float], b2: tuple[float, float]) -> float:
    a = np.array(f1, dtype=float)
    b = np.array(f2, dtype=float)
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        hist_sim = 0.0
    else:
        hist_sim = float(np.dot(a, b) / denom)
    brightness_penalty = min(abs(b1[0] - b2[0]) / 255.0, 1.0) * 0.1
    return max(0.0, min(1.0, hist_sim - brightness_penalty))

def average_fingerprint(old_fp: dict[str, Any], new_hist: list[float], new_brightness: tuple[float, float]) -> dict[str, Any]:
    old_hist = np.array(old_fp.get("histogram", new_hist), dtype=float)
    averaged_hist = ((old_hist + np.array(new_hist, dtype=float)) / 2.0).tolist()
    old_brightness = old_fp.get("brightness", [new_brightness[0], new_brightness[1]])
    return {
        "histogram": averaged_hist,
        "brightness": [
            (float(old_brightness[0]) + new_brightness[0]) / 2,
            (float(old_brightness[1]) + new_brightness[1]) / 2,
        ],
    }
