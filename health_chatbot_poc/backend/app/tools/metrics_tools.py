"""Layer [3]: the metrics API / semantic layer, exposed to agents as typed tools.

Rules every tool follows:
  * reads ONLY precomputed Health Facts (never raw samples)
  * returns small JSON with numbers already computed (diffs, % changes, z-scores)
  * user_id comes from the runtime context, never from the model
  * unknown metrics / empty ranges return an explicit error instead of a guess
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Literal

from langchain.tools import ToolRuntime, tool

from backend.app.agents.context import HealthContext
from backend.app.db.connection import fetch_all, fetch_one
from backend.app.tools.catalog import METRICS, validate_metric


def _r(x, nd: int = 1):
    return None if x is None else round(float(x), nd)


def _out(obj) -> str:
    return json.dumps(obj, default=str, separators=(",", ":"))


def _user_metrics(user_id: str) -> list[str]:
    return [r["metric"] for r in fetch_all("SELECT metric FROM metric_baselines WHERE user_id=%s ORDER BY metric", (user_id,))]


def _check(user_id: str, metric: str) -> str | None:
    if err := validate_metric(metric):
        return err
    if metric not in _user_metrics(user_id):
        return f"Metric '{metric}' is not tracked for this user (no device provides it). Available: {', '.join(_user_metrics(user_id))}"
    return None


def _episodes(rows: list[dict], gap_days: int = 4) -> list[dict]:
    """Group anomaly days (same metric + direction, <= gap_days apart) into episodes."""
    eps: list[dict] = []
    for r in sorted(rows, key=lambda r: (r["metric"], r["direction"], r["day"])):
        last = eps[-1] if eps else None
        if last and last["metric"] == r["metric"] and last["direction"] == r["direction"] and (r["day"] - last["end"]).days <= gap_days:
            last["end"] = r["day"]
            last["days"] += 1
            last["peak_z"] = max(last["peak_z"], abs(r["z"]))
            last["values"].append(r["value"])
        else:
            eps.append({"metric": r["metric"], "direction": r["direction"], "start": r["day"], "end": r["day"],
                        "days": 1, "peak_z": abs(r["z"]), "values": [r["value"]], "baseline": r["baseline"]})
    for e in eps:
        e["mean_value"] = _r(sum(e["values"]) / len(e["values"]))
        e["baseline"] = _r(e["baseline"])
        e["peak_z"] = _r(e["peak_z"], 2)
        del e["values"]
    return sorted(eps, key=lambda e: (-e["days"], -e["peak_z"]))


# --------------------------------------------------------------------------------------
@tool
def list_available_metrics(runtime: ToolRuntime[HealthContext]) -> str:
    """List the health metrics tracked for this user, with units and meaning.

    Call this when unsure which metric name to use. Metric names must match exactly.
    """
    ms = _user_metrics(runtime.context.user_id)
    return _out({m: {"unit": METRICS[m]["unit"], "description": METRICS[m]["desc"]} for m in ms})


@tool
def get_today_snapshot(runtime: ToolRuntime[HealthContext]) -> str:
    """Get today's readiness tier (green/amber/red) and the latest value of key metrics
    compared with the user's personal 30-day baseline.

    Use for: "how am I today", "should I train hard today", readiness, recovery status.
    """
    uid = runtime.context.user_id
    rd = fetch_one("SELECT as_of, tier, score, drivers FROM readiness WHERE user_id=%s", (uid,))
    rows = fetch_all(
        """SELECT metric, last_value, mean_7d, mean_30d, z_score, trend, status FROM metric_baselines
           WHERE user_id=%s AND metric = ANY(%s)""",
        (uid, ["hrv_rmssd", "resting_hr", "sleep_duration", "deep_sleep", "recovery_score", "readiness_score", "sleep_score", "strain"]),
    )
    return _out({
        "as_of": rd["as_of"] if rd else None,
        "readiness": {"tier": rd["tier"], "score": _r(rd["score"]), "drivers": rd["drivers"]} if rd else None,
        "metrics": {r["metric"]: {"latest": _r(r["last_value"]), "last_7d_mean": _r(r["mean_7d"]), "baseline_30d": _r(r["mean_30d"]),
                                  "z_vs_baseline": _r(r["z_score"], 2), "trend_30d": r["trend"], "status": r["status"],
                                  "unit": METRICS[r["metric"]]["unit"]} for r in rows},
    })


@tool
def get_metric_summary(metric: str, runtime: ToolRuntime[HealthContext], days: int = 30,
                       granularity: Literal["auto", "day", "week", "month"] = "auto") -> str:
    """Summarise ONE metric over the last N days: mean/min/max, personal baseline and a
    compact bucketed series (max ~26 points).

    Args:
        metric: canonical metric name, e.g. hrv_rmssd, resting_hr, sleep_duration, steps
        days: look-back window in days (1-365)
        granularity: bucket size; "auto" picks day (<=21d), week (<=180d) or month
    """
    uid = runtime.context.user_id
    if err := _check(uid, metric):
        return _out({"error": err})
    days = max(1, min(days, 365))
    if granularity == "auto":
        granularity = "day" if days <= 21 else ("week" if days <= 180 else "month")
    trunc = {"day": "day", "week": "week", "month": "month"}[granularity]
    agg = "SUM" if METRICS[metric]["agg"] == "sum" and granularity == "day" else "AVG"
    series = fetch_all(
        f"""SELECT date_trunc('{trunc}', day)::date AS bucket, {agg}(value) AS v, COUNT(*) AS n
            FROM daily_metrics WHERE user_id=%s AND metric=%s AND day > CURRENT_DATE - %s::int
            GROUP BY 1 ORDER BY 1""",
        (uid, metric, days),
    )
    stats = fetch_one(
        """SELECT AVG(value) mean, MIN(value) min, MAX(value) max, COUNT(*) n FROM daily_metrics
           WHERE user_id=%s AND metric=%s AND day > CURRENT_DATE - %s::int""",
        (uid, metric, days),
    )
    if not stats or not stats["n"]:
        return _out({"error": f"No {metric} data in the last {days} days."})
    base = fetch_one("SELECT mean_30d, mean_90d, trend, slope_per_week FROM metric_baselines WHERE user_id=%s AND metric=%s", (uid, metric))
    return _out({
        "metric": metric, "unit": METRICS[metric]["unit"], "window_days": days, "days_with_data": stats["n"],
        "mean": _r(stats["mean"]), "min": _r(stats["min"]), "max": _r(stats["max"]),
        "baseline_30d": _r(base["mean_30d"]), "baseline_90d": _r(base["mean_90d"]),
        "trend_30d": base["trend"], "slope_per_week": _r(base["slope_per_week"], 2),
        "granularity": granularity,
        "series": [{"start": s["bucket"], "value": _r(s["v"])} for s in series][-26:],
    })


@tool
def compare_periods(metric: str, period_a_start: str, period_a_end: str, period_b_start: str,
                    period_b_end: str, runtime: ToolRuntime[HealthContext]) -> str:
    """Compare the mean of ONE metric between two date ranges (inclusive, YYYY-MM-DD).
    Returns both means, the absolute difference (B - A) and the % change.

    Use for "this month vs last month", "before vs after my race", etc.
    """
    uid = runtime.context.user_id
    if err := _check(uid, metric):
        return _out({"error": err})
    q = "SELECT AVG(value) m, COUNT(*) n FROM daily_metrics WHERE user_id=%s AND metric=%s AND day BETWEEN %s AND %s"
    a = fetch_one(q, (uid, metric, period_a_start, period_a_end))
    b = fetch_one(q, (uid, metric, period_b_start, period_b_end))
    if not a["n"] or not b["n"]:
        return _out({"error": "One of the periods has no data.", "days_a": a["n"], "days_b": b["n"]})
    diff = b["m"] - a["m"]
    return _out({
        "metric": metric, "unit": METRICS[metric]["unit"],
        "period_a": {"start": period_a_start, "end": period_a_end, "mean": _r(a["m"]), "days": a["n"]},
        "period_b": {"start": period_b_start, "end": period_b_end, "mean": _r(b["m"]), "days": b["n"]},
        "difference_b_minus_a": _r(diff), "pct_change": _r(diff / a["m"] * 100 if a["m"] else None),
        "better_direction": METRICS[metric]["better"],
    })


@tool
def get_trends(runtime: ToolRuntime[HealthContext], days: int = 180, metrics: list[str] | None = None) -> str:
    """Long-range "ups and downs": per-metric monthly means, overall change from the first to
    the last month, best/worst month, detected change-points (sustained shifts) and the
    biggest anomaly episodes (e.g. illness, travel, poor-sleep streaks).

    Args:
        days: window, default 180 (6 months)
        metrics: optional subset; default = all key recovery/sleep/fitness metrics
    """
    uid = runtime.context.user_id
    available = _user_metrics(uid)
    default = ["hrv_rmssd", "resting_hr", "sleep_duration", "deep_sleep", "recovery_score", "readiness_score", "vo2max", "training_load", "steps"]
    metrics = [m for m in (metrics or default) if m in available]
    since = date.today() - timedelta(days=days)
    out: dict = {"window_days": days, "metrics": {}}
    for m in metrics:
        months = fetch_all("SELECT month, mean FROM metric_monthly WHERE user_id=%s AND metric=%s AND month >= date_trunc('month', %s::date) ORDER BY month",
                           (uid, m, since))
        if not months:
            continue
        better = METRICS[m]["better"]
        key = (lambda r: r["mean"]) if better != "lower" else (lambda r: -r["mean"])
        first, last = months[0]["mean"], months[-1]["mean"]
        out["metrics"][m] = {
            "unit": METRICS[m]["unit"], "better": better,
            "monthly_mean": {str(r["month"])[:7]: _r(r["mean"]) for r in months},
            "first_to_last_month_pct": _r((last - first) / first * 100 if first else None),
            "best_month": str(max(months, key=key)["month"])[:7] if better != "neutral" else None,
            "worst_month": str(min(months, key=key)["month"])[:7] if better != "neutral" else None,
        }
    cps = fetch_all("SELECT metric, day, before_mean, after_mean, pct_change FROM change_points WHERE user_id=%s AND day >= %s AND metric = ANY(%s)",
                    (uid, since, metrics))
    out["change_points"] = [{"metric": c["metric"], "from": c["day"], "before": _r(c["before_mean"]), "after": _r(c["after_mean"]),
                             "pct_change": _r(c["pct_change"])} for c in cps]
    an = fetch_all("SELECT day, metric, value, baseline, z, direction FROM anomalies WHERE user_id=%s AND day >= %s AND metric = ANY(%s)",
                   (uid, since, metrics))
    out["top_anomaly_episodes"] = _episodes(an)[:6]
    return _out(out)


@tool
def get_anomalies(runtime: ToolRuntime[HealthContext], days: int = 30, metric: str | None = None) -> str:
    """Unusual days (>= 2 standard deviations from the user's trailing 30-day baseline),
    grouped into episodes. Optionally filter to one metric."""
    uid = runtime.context.user_id
    if metric and (err := _check(uid, metric)):
        return _out({"error": err})
    rows = fetch_all(
        "SELECT day, metric, value, baseline, z, direction FROM anomalies WHERE user_id=%s AND day > CURRENT_DATE - %s::int"
        + (" AND metric=%s" if metric else ""),
        (uid, days, metric) if metric else (uid, days),
    )
    return _out({"window_days": days, "episodes": _episodes(rows)[:10]})


@tool
def get_events(runtime: ToolRuntime[HealthContext], direction: Literal["upcoming", "past"] = "upcoming",
               days: int = 120) -> str:
    """Get the user's calendar events (races, matches, travel, illness, tests).

    Args:
        direction: "upcoming" (default) or "past"
        days: how far ahead/back to look
    """
    uid = runtime.context.user_id
    if direction == "upcoming":
        rows = fetch_all("SELECT event_date, title, event_type, notes FROM events WHERE user_id=%s AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + %s::int ORDER BY event_date", (uid, days))
    else:
        rows = fetch_all("SELECT event_date, title, event_type, notes FROM events WHERE user_id=%s AND event_date < CURRENT_DATE AND event_date >= CURRENT_DATE - %s::int ORDER BY event_date DESC", (uid, days))
    today = date.today()
    return _out({"today": today, "events": [{"date": r["event_date"], "days_from_today": (r["event_date"] - today).days,
                                             "title": r["title"], "type": r["event_type"], "notes": r["notes"]} for r in rows]})


@tool
def get_training_load(runtime: ToolRuntime[HealthContext], weeks: int = 6) -> str:
    """Weekly training load, workout minutes and session counts, plus the acute:chronic
    workload ratio (last 7 days vs 28-day weekly average; ~0.8-1.3 is the usual 'sweet spot')."""
    uid = runtime.context.user_id
    weekly = fetch_all(
        """SELECT date_trunc('week', start_ts)::date wk, SUM(training_load) load, SUM(duration_min) minutes, COUNT(*) sessions
           FROM workouts WHERE user_id=%s AND start_ts > now() - (%s::int * interval '1 week') GROUP BY 1 ORDER BY 1""",
        (uid, weeks),
    )
    acute = fetch_one("SELECT COALESCE(SUM(training_load),0) s FROM workouts WHERE user_id=%s AND start_ts > now() - interval '7 days'", (uid,))["s"]
    chronic = fetch_one("SELECT COALESCE(SUM(training_load),0)/4.0 s FROM workouts WHERE user_id=%s AND start_ts > now() - interval '28 days'", (uid,))["s"]
    return _out({
        "weekly": [{"week_start": w["wk"], "load": _r(w["load"]), "minutes": _r(w["minutes"]), "sessions": w["sessions"]} for w in weekly],
        "acute_7d_load": _r(acute), "chronic_weekly_avg_28d": _r(chronic),
        "acute_chronic_ratio": _r(acute / chronic, 2) if chronic else None,
    })


@tool
def get_recent_workouts(runtime: ToolRuntime[HealthContext], days: int = 14) -> str:
    """List workouts from the last N days (sport, duration, heart rate, training load)."""
    rows = fetch_all(
        """SELECT start_ts::date d, sport, duration_min, avg_hr, max_hr, training_load FROM workouts
           WHERE user_id=%s AND start_ts > now() - (%s::int * interval '1 day') ORDER BY start_ts DESC""",
        (runtime.context.user_id, days),
    )
    return _out([{"date": r["d"], "sport": r["sport"], "minutes": _r(r["duration_min"]), "avg_hr": _r(r["avg_hr"], 0),
                  "max_hr": _r(r["max_hr"], 0), "load": _r(r["training_load"])} for r in rows])


_INTENSITY = {"low": 0, "moderate": 1, "high": 2}


@tool
def search_exercises(runtime: ToolRuntime[HealthContext],
                     max_intensity: Literal["low", "moderate", "high"] = "moderate",
                     category: Literal["any", "strength", "endurance", "mobility", "recovery", "hiit"] = "any",
                     focus_keyword: str | None = None) -> str:
    """Search the curated exercise library. Exercises contraindicated by the user's recorded
    injuries are removed automatically.

    Args:
        max_intensity: highest intensity allowed (match it to today's readiness tier:
            red -> low, amber -> moderate, green -> high)
        category: filter by category
        focus_keyword: optional keyword, e.g. "achilles", "sprint", "aerobic", "sleep"
    """
    user = fetch_one("SELECT injuries FROM users WHERE id=%s", (runtime.context.user_id,))
    injuries = (user["injuries"] or "").lower() if user else ""
    rows = fetch_all("SELECT name, category, intensity, focus, duration_min, contraindications, description FROM exercise_library")
    res, excluded = [], []
    for r in rows:
        if _INTENSITY[r["intensity"]] > _INTENSITY[max_intensity]:
            continue
        if category != "any" and r["category"] != category:
            continue
        if focus_keyword and focus_keyword.lower() not in (r["focus"] + " " + r["description"] + " " + r["name"]).lower():
            continue
        if any(c in injuries for c in r["contraindications"]):
            excluded.append(r["name"])
            continue
        res.append({k: r[k] for k in ("name", "category", "intensity", "focus", "duration_min", "description")})
    return _out({"exercises": res[:10], "excluded_due_to_injury": excluded})


LOOKUP_TOOLS = [get_today_snapshot, get_events, get_recent_workouts, get_metric_summary, list_available_metrics]
INSIGHT_TOOLS = [get_trends, get_metric_summary, compare_periods, get_anomalies, get_events, list_available_metrics]
COACHING_TOOLS = [get_today_snapshot, get_training_load, get_events, search_exercises, get_recent_workouts]
ANALYST_TOOLS = [list_available_metrics, get_metric_summary, compare_periods, get_events, get_trends]
