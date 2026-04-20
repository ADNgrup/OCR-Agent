from __future__ import annotations

from typing import Any

import numpy as np
from PIL import Image, ImageStat


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
