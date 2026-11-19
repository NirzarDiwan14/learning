"""Layer [1]: map each vendor's payload shape/units onto the canonical schema.

Canonical sample row: (source, metric, ts, value, unit)
Canonical workout row: dict matching the `workouts` table.

Vendor-specific scores (WHOOP recovery, Oura readiness) keep their own metric
names - they are different algorithms and must not be merged.
"""

from __future__ import annotations

import re
from datetime import datetime

Sample = tuple[str, str, datetime, float, str]


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _day_ts(day: str) -> datetime:
    return datetime.fromisoformat(day + "T07:00:00+00:00")


def normalize_whoop(p: dict) -> list[Sample]:
    t = p["type"]
    if t == "cycle":
        return [("whoop", "strain", _dt(p["start"]), p["score"]["strain"], "0-21")]
    if t == "recovery":
        sc, ts = p["score"], _dt(p["created_at"])
        return [
            ("whoop", "recovery_score", ts, sc["recovery_score"], "%"),
            ("whoop", "hrv_rmssd", ts, sc["hrv_rmssd_milli"], "ms"),
            ("whoop", "resting_hr", ts, sc["resting_heart_rate"], "bpm"),
        ]
    if t == "sleep":
        st, ts = p["score"]["stage_summary"], _dt(p["end"])
        return [
            (
                "whoop",
                "sleep_duration",
                ts,
                st["total_sleep_time_milli"] / 60_000,
                "min",
            ),
            (
                "whoop",
                "deep_sleep",
                ts,
                st["total_slow_wave_sleep_time_milli"] / 60_000,
                "min",
            ),
        ]
    return []


def normalize_oura(p: dict) -> list[Sample]:
    t, ts = p["type"], _day_ts(p["day"])
    if t == "daily_readiness":
        return [("oura", "readiness_score", ts, p["score"], "0-100")]
    if t == "daily_sleep":
        return [("oura", "sleep_score", ts, p["score"], "0-100")]
    if t == "sleep":  # Oura reports seconds
        return [
            ("oura", "sleep_duration", ts, p["total_sleep_duration"] / 60, "min"),
            ("oura", "deep_sleep", ts, p["deep_sleep_duration"] / 60, "min"),
            ("oura", "resting_hr", ts, p["lowest_heart_rate"], "bpm"),
        ]
    return []


_APPLE_MAP = {
    "HKQuantityTypeIdentifierStepCount": ("steps", "steps"),
    "HKQuantityTypeIdentifierActiveEnergyBurned": ("active_energy", "kcal"),
    "HKQuantityTypeIdentifierVO2Max": ("vo2max", "ml/kg/min"),
    "HKQuantityTypeIdentifierHeartRate": ("heart_rate", "bpm"),
}


def normalize_apple(p: dict) -> list[Sample]:
    if p["type"] not in _APPLE_MAP:
        return []
    metric, unit = _APPLE_MAP[p["type"]]
    return [("apple_watch", metric, _dt(p["startDate"]), float(p["value"]), unit)]


def _iso_minutes(duration: str) -> float:
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    h, mi, s = (int(x) if x else 0 for x in m.groups())
    return h * 60 + mi + s / 60


def normalize_polar_workout(p: dict) -> dict:
    return {
        "source": "polar",
        "start_ts": _dt(p["start_time"]),
        "sport": p["sport"].lower(),
        "duration_min": _iso_minutes(p["duration"]),
        "avg_hr": p["heart_rate"]["average"],
        "max_hr": p["heart_rate"]["maximum"],
        "kcal": p["calories"],
        "training_load": p["training_load"],
    }


def normalize(payloads: dict[str, list[dict]]) -> tuple[list[Sample], list[dict]]:
    samples: list[Sample] = []
    workouts: list[dict] = []
    for vendor, items in payloads.items():
        for p in items:
            if vendor == "whoop":
                samples += normalize_whoop(p)
            elif vendor == "oura":
                samples += normalize_oura(p)
            elif vendor == "apple_watch":
                samples += normalize_apple(p)
            elif vendor == "polar":
                workouts.append(normalize_polar_workout(p))
    return samples, workouts
