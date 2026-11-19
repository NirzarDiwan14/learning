"""Metric catalog: the semantic layer's single source of truth.

Every tool validates metric names against this catalog. An unknown metric returns
an explicit error listing valid names instead of a guessed (wrong) number.
"""

METRICS: dict[str, dict] = {
    "recovery_score": {
        "unit": "%",
        "agg": "mean",
        "better": "higher",
        "desc": "WHOOP recovery (vendor-specific score, not interchangeable with Oura readiness)",
    },
    "strain": {
        "unit": "0-21",
        "agg": "mean",
        "better": "neutral",
        "desc": "WHOOP day strain (cardiovascular load)",
    },
    "hrv_rmssd": {
        "unit": "ms",
        "agg": "mean",
        "better": "higher",
        "desc": "Overnight heart-rate variability (RMSSD)",
    },
    "readiness_score": {
        "unit": "0-100",
        "agg": "mean",
        "better": "higher",
        "desc": "Oura readiness (vendor-specific score)",
    },
    "sleep_score": {
        "unit": "0-100",
        "agg": "mean",
        "better": "higher",
        "desc": "Oura sleep score (vendor-specific score)",
    },
    "sleep_duration": {
        "unit": "min",
        "agg": "mean",
        "better": "higher",
        "desc": "Total sleep time per night",
    },
    "deep_sleep": {
        "unit": "min",
        "agg": "mean",
        "better": "higher",
        "desc": "Deep / slow-wave sleep per night",
    },
    "resting_hr": {
        "unit": "bpm",
        "agg": "mean",
        "better": "lower",
        "desc": "Resting heart rate",
    },
    "steps": {
        "unit": "steps",
        "agg": "sum",
        "better": "higher",
        "desc": "Daily step count",
    },
    "active_energy": {
        "unit": "kcal",
        "agg": "sum",
        "better": "neutral",
        "desc": "Active energy burned per day",
    },
    "vo2max": {
        "unit": "ml/kg/min",
        "agg": "mean",
        "better": "higher",
        "desc": "Estimated VO2max (Apple Watch, updated ~weekly)",
    },
    "avg_heart_rate": {
        "unit": "bpm",
        "agg": "mean",
        "better": "neutral",
        "desc": "Average heart rate across the day (from intraday samples)",
    },
    "max_heart_rate": {
        "unit": "bpm",
        "agg": "max",
        "better": "neutral",
        "desc": "Highest heart rate of the day (from intraday samples)",
    },
    "training_load": {
        "unit": "AU",
        "agg": "sum",
        "better": "neutral",
        "desc": "Sum of workout training load per day (Polar)",
    },
    "workout_minutes": {
        "unit": "min",
        "agg": "sum",
        "better": "neutral",
        "desc": "Total workout minutes per day",
    },
}

# Metrics used to build the readiness tier / today snapshot (when available for the user).
KEY_METRICS = [
    "hrv_rmssd",
    "resting_hr",
    "sleep_duration",
    "recovery_score",
    "readiness_score",
    "strain",
]


def validate_metric(metric: str) -> str | None:
    """Return an error message if the metric is unknown, else None."""
    if metric in METRICS:
        return None
    return f"Unknown metric '{metric}'. Valid metrics: {', '.join(METRICS)}"


def higher_is_better(metric: str) -> bool | None:
    b = METRICS[metric]["better"]
    return None if b == "neutral" else b == "higher"
