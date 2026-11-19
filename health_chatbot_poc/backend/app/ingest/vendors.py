"""Synthetic vendor API payloads (WHOOP, Oura, Apple Watch/HealthKit, Polar).

Each vendor returns data in its *own* shape and units, the way the real APIs do.
`normalize.py` maps these to the canonical schema. Data is deterministic per user
and contains deliberate "stories" (illness dip, travel, training block, events) so
the trend/anomaly/change-point layers have something real to find.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

import numpy as np

DAYS = 180


@dataclass
class Story:
    """Day offsets are counted back from today (0 = today)."""

    illness: tuple[int, int] | None = None  # (start_days_ago, end_days_ago)
    travel: tuple[int, int] | None = None
    build_block_days: int = 0  # last N days with rising load
    fitness_gain: float = 0.0  # vo2max gain over the 180 days
    season_start_days_ago: int | None = None
    poor_sleep_recent_days: int = 0


@dataclass
class Athlete:
    id: str
    name: str
    age: int
    sex: str
    sport: str
    goals: str
    injuries: str
    devices: list[str]
    seed: int
    base: dict[str, float]
    story: Story
    events: list[dict] = field(
        default_factory=list
    )  # {days: +future/-past, title, type, notes}
    sleep_source: str = "oura"  # who provides sleep/resting HR


ATHLETES: list[Athlete] = [
    Athlete(
        id="aarav",
        name="Aarav Mehta",
        age=29,
        sex="male",
        sport="Marathon running",
        goals="Run the City Marathon under 3:15; keep Achilles healthy",
        injuries="Mild left Achilles tendinopathy (2025) - avoid sudden plyometric/hill spikes",
        devices=["whoop", "oura", "apple_watch", "polar"],
        seed=7,
        base={
            "hrv": 68,
            "rhr": 50,
            "sleep": 445,
            "deep": 88,
            "steps": 11500,
            "vo2": 48.5,
        },
        story=Story(
            illness=(82, 72),
            build_block_days=42,
            fitness_gain=2.6,
            poor_sleep_recent_days=5,
        ),
        events=[
            {
                "days": 18,
                "title": "City Marathon",
                "type": "race",
                "notes": "Goal pace 4:37/km",
            },
            {
                "days": -46,
                "title": "Lakeside Half Marathon",
                "type": "race",
                "notes": "Finished 1:31:10",
            },
            {
                "days": -78,
                "title": "Flu",
                "type": "illness",
                "notes": "5 days off training",
            },
            {
                "days": 40,
                "title": "Post-marathon VO2max lab test",
                "type": "test",
                "notes": "",
            },
        ],
    ),
    Athlete(
        id="maya",
        name="Maya Fernandes",
        age=24,
        sex="female",
        sport="Football (midfielder)",
        goals="Improve repeated-sprint ability and sleep consistency during the season",
        injuries="None reported",
        devices=["whoop", "apple_watch", "polar"],
        seed=11,
        base={
            "hrv": 82,
            "rhr": 54,
            "sleep": 455,
            "deep": 95,
            "steps": 13000,
            "vo2": 50.0,
        },
        story=Story(travel=(27, 20), season_start_days_ago=60, fitness_gain=1.2),
        events=[
            {
                "days": 3,
                "title": "League match vs Riverside FC",
                "type": "match",
                "notes": "Away, 19:30 kick-off",
            },
            {"days": 10, "title": "Cup semi-final", "type": "match", "notes": "Home"},
            {
                "days": -24,
                "title": "Away tournament (Lisbon)",
                "type": "travel",
                "notes": "4 games in 6 days",
            },
            {
                "days": 17,
                "title": "League match vs Northgate",
                "type": "match",
                "notes": "Home",
            },
        ],
        sleep_source="whoop",
    ),
]


def _in(window: tuple[int, int] | None, ago: int) -> float:
    """1.0 inside the (start_ago, end_ago) window with soft edges, else 0."""
    if not window:
        return 0.0
    start, end = window
    if end <= ago <= start:
        return 1.0
    dist = min(abs(ago - start), abs(ago - end))
    return max(0.0, 1 - dist / 4) if (end - 4) <= ago <= (start + 4) else 0.0


def generate_payloads(a: Athlete, today: date) -> dict[str, list[dict]]:
    """Return raw vendor payloads: {'whoop': [...], 'oura': [...], 'apple_watch': [...], 'polar': [...]}."""
    rng = np.random.default_rng(a.seed)
    s = a.story
    out: dict[str, list[dict]] = {d: [] for d in a.devices}
    fatigue = 0.0

    for i in range(DAYS):
        ago = DAYS - 1 - i
        d = today - timedelta(days=ago)
        weekday = d.weekday()
        progress = i / (DAYS - 1)

        ill = _in(s.illness, ago)
        trav = _in(s.travel, ago)
        block = 1.0 if ago < s.build_block_days else 0.0
        in_season = (
            s.season_start_days_ago is not None and ago <= s.season_start_days_ago
        )
        poor_sleep = 1.0 if ago < s.poor_sleep_recent_days else 0.0

        # ---- training plan for the day ----
        rest_day = weekday == 0 or ill > 0.5
        base_load = 0 if rest_day else rng.normal(95, 25)
        if block and not rest_day:
            base_load *= 1.0 + 0.5 * (1 - ago / max(s.build_block_days, 1))
        if in_season and weekday == 5:  # match day
            base_load = rng.normal(190, 20)
        if weekday == 6 and a.sport.startswith("Marathon") and not ill:
            base_load = rng.normal(210, 30) * (1.15 if block else 1.0)  # long run
        load = max(0.0, base_load * (1 - 0.8 * ill))
        fatigue = 0.8 * fatigue + 0.2 * load

        # ---- physiology driven by the story ----
        hrv = (
            a.base["hrv"] * (1 + 0.04 * progress)
            - 0.06 * (fatigue - 90)
            - 18 * ill
            - 9 * trav
            - 7 * poor_sleep
        )
        hrv += rng.normal(0, 4.5)
        rhr = (
            a.base["rhr"]
            - 1.5 * progress
            + 0.02 * (fatigue - 90)
            + 6 * ill
            + 3 * trav
            + 2 * poor_sleep
            + rng.normal(0, 1.3)
        )
        sleep_min = (
            a.base["sleep"]
            - 75 * trav
            - 70 * poor_sleep
            + 20 * ill
            + (25 if weekday in (5, 6) else 0)
            + rng.normal(0, 28)
        )
        deep_min = (
            a.base["deep"] * (sleep_min / a.base["sleep"])
            - 10 * poor_sleep
            + rng.normal(0, 7)
        )
        steps = a.base["steps"] * (0.45 if ill else 1.0) * (
            1.25 if weekday == 6 else 1.0
        ) + rng.normal(0, 1800)
        recovery = float(
            np.clip(
                55
                + 1.3 * (hrv - a.base["hrv"])
                - 2.2 * (rhr - a.base["rhr"])
                + 0.08 * (sleep_min - 420)
                + rng.normal(0, 6),
                1,
                99,
            )
        )
        strain = float(np.clip(6 + load / 18 + rng.normal(0, 0.8), 2, 21))
        readiness = float(np.clip(recovery * 0.6 + 32 + rng.normal(0, 4), 10, 99))
        sleep_score = float(
            np.clip(
                40 + (sleep_min - 360) / 3.2 + (deep_min - 70) / 3 + rng.normal(0, 4),
                20,
                99,
            )
        )
        vo2 = a.base["vo2"] + s.fitness_gain * progress - 0.8 * ill

        day_iso = d.isoformat()
        wake = datetime.combine(d, time(6, 45), tzinfo=timezone.utc)

        # ---- WHOOP: cycles + recovery (+ sleep when WHOOP is the sleep source) ----
        if "whoop" in out:
            out["whoop"].append(
                {
                    "type": "cycle",
                    "start": datetime.combine(
                        d, time(0, 0), tzinfo=timezone.utc
                    ).isoformat(),
                    "score": {"strain": round(strain, 1)},
                }
            )
            out["whoop"].append(
                {
                    "type": "recovery",
                    "created_at": wake.isoformat(),
                    "score": {
                        "recovery_score": round(recovery),
                        "hrv_rmssd_milli": round(hrv, 1),
                        "resting_heart_rate": round(rhr),
                    },
                }
            )
            if a.sleep_source == "whoop":
                out["whoop"].append(
                    {
                        "type": "sleep",
                        "end": wake.isoformat(),
                        "score": {
                            "stage_summary": {
                                "total_sleep_time_milli": int(sleep_min * 60_000),
                                "total_slow_wave_sleep_time_milli": int(
                                    deep_min * 60_000
                                ),
                            }
                        },
                    }
                )

        # ---- Oura: daily_readiness, daily_sleep, sleep (seconds!) ----
        if "oura" in out:
            out["oura"].append(
                {"type": "daily_readiness", "day": day_iso, "score": round(readiness)}
            )
            out["oura"].append(
                {"type": "daily_sleep", "day": day_iso, "score": round(sleep_score)}
            )
            out["oura"].append(
                {
                    "type": "sleep",
                    "day": day_iso,
                    "total_sleep_duration": int(sleep_min * 60),
                    "deep_sleep_duration": int(deep_min * 60),
                    "lowest_heart_rate": round(rhr),
                }
            )

        # ---- Apple Watch / HealthKit quantity samples ----
        if "apple_watch" in out:
            out["apple_watch"].append(
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "startDate": datetime.combine(
                        d, time(23, 59), tzinfo=timezone.utc
                    ).isoformat(),
                    "value": max(800, round(steps)),
                    "unit": "count",
                }
            )
            out["apple_watch"].append(
                {
                    "type": "HKQuantityTypeIdentifierActiveEnergyBurned",
                    "startDate": datetime.combine(
                        d, time(23, 59), tzinfo=timezone.utc
                    ).isoformat(),
                    "value": round(max(150.0, 280 + load * 3.1 + rng.normal(0, 40))),
                    "unit": "kcal",
                }
            )
            if weekday == 3:
                out["apple_watch"].append(
                    {
                        "type": "HKQuantityTypeIdentifierVO2Max",
                        "startDate": wake.isoformat(),
                        "value": round(vo2, 1),
                        "unit": "mL/min·kg",
                    }
                )
            # intraday heart rate, every 15 minutes (96 samples/day) - the "raw" firehose
            workout_slot = rng.integers(64, 76)  # ~16:00-19:00
            for slot in range(96):
                hour = slot / 4
                circadian = -6 * math.cos((hour - 4) / 24 * 2 * math.pi)
                asleep = hour < 6.5 or hour >= 23
                hr = rhr + (2 if asleep else 22) + circadian + rng.normal(0, 4)
                if load > 0 and workout_slot <= slot < workout_slot + 4:
                    hr = 120 + load / 4 + rng.normal(0, 8)
                out["apple_watch"].append(
                    {
                        "type": "HKQuantityTypeIdentifierHeartRate",
                        "startDate": datetime.combine(
                            d, time(int(hour), (slot % 4) * 15), tzinfo=timezone.utc
                        ).isoformat(),
                        "value": round(max(38.0, hr)),
                        "unit": "count/min",
                    }
                )

        # ---- Polar: exercise sessions (ISO-8601 durations) ----
        if "polar" in out and load > 0:
            minutes = max(20, round(load / 2.2 + rng.normal(0, 6)))
            sport = (
                "RUNNING"
                if a.sport.startswith("Marathon")
                else ("SOCCER" if weekday in (5, 2) else "RUNNING")
            )
            if weekday in (1, 4) and a.sport.startswith("Marathon"):
                sport = "STRENGTH_TRAINING"
            out["polar"].append(
                {
                    "start_time": datetime.combine(
                        d, time(17, 0), tzinfo=timezone.utc
                    ).isoformat(),
                    "sport": sport,
                    "duration": f"PT{minutes}M",
                    "heart_rate": {
                        "average": round(125 + load / 6),
                        "maximum": round(160 + load / 8),
                    },
                    "calories": round(minutes * 11.5),
                    "training_load": round(load, 1),
                }
            )
    return out
