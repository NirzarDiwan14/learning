"""Layer [2]: precompute "Health Facts" from canonical samples.

Runs per user (nightly / on new data in production; once at seed time in the POC).
All maths happens HERE, in pandas - the LLM only ever reads these results.

Outputs:
  daily_metrics     one value per (day, metric)   ~180 rows/metric instead of ~17k raw HR points
  metric_baselines  7d/30d/90d means, z-score vs baseline, 30-day slope, status
  metric_monthly    monthly mean/min/max/p10/p90
  anomalies         days with |z| >= 2 against a trailing 30-day window
  change_points     single best mean-shift per metric (if effect size is large)
  readiness         green/amber/red composite with drivers
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from backend.app.db.connection import conn
from backend.app.tools.catalog import KEY_METRICS, METRICS, higher_is_better

# Which vendor wins when two devices report the same canonical metric.
SOURCE_PRIORITY = {"oura": 0, "whoop": 1, "apple_watch": 2, "polar": 3}


def _load_daily(user_id: str) -> pd.DataFrame:
    """Roll samples + workouts up to one row per (day, metric)."""
    with conn() as c:
        samples = pd.DataFrame(c.execute(
            "SELECT source, metric, ts, value FROM samples WHERE user_id = %s", (user_id,)
        ).fetchall())
        workouts = pd.DataFrame(c.execute(
            "SELECT start_ts, duration_min, training_load FROM workouts WHERE user_id = %s", (user_id,)
        ).fetchall())

    frames = []
    if not samples.empty:
        samples["day"] = pd.to_datetime(samples["ts"], utc=True).dt.date
        hr = samples[samples.metric == "heart_rate"]
        rest = samples[samples.metric != "heart_rate"]
        if not hr.empty:
            g = hr.groupby("day")["value"]
            frames.append(pd.DataFrame({"day": g.mean().index, "metric": "avg_heart_rate", "value": g.mean().values, "source": "apple_watch"}))
            frames.append(pd.DataFrame({"day": g.max().index, "metric": "max_heart_rate", "value": g.max().values, "source": "apple_watch"}))
        if not rest.empty:
            rest = rest.assign(prio=rest.source.map(SOURCE_PRIORITY))
            best = rest.sort_values("prio").groupby(["day", "metric"], as_index=False).first()
            agg = rest.merge(best[["day", "metric", "source"]], on=["day", "metric", "source"])
            agg = agg.groupby(["day", "metric", "source"], as_index=False)["value"].mean()
            frames.append(agg[["day", "metric", "value", "source"]])
    if not workouts.empty:
        workouts["day"] = pd.to_datetime(workouts["start_ts"], utc=True).dt.date
        w = workouts.groupby("day").agg(training_load=("training_load", "sum"), workout_minutes=("duration_min", "sum")).reset_index()
        for m in ("training_load", "workout_minutes"):
            frames.append(pd.DataFrame({"day": w.day, "metric": m, "value": w[m], "source": "polar"}))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day", "metric", "value", "source"])


def _status(z: float, metric: str) -> str:
    if np.isnan(z) or abs(z) < 1:
        return "within_baseline"
    return "above_baseline" if z > 0 else "below_baseline"


def _change_point(series: pd.Series, min_seg: int = 21) -> dict | None:
    """Single best split maximising the mean shift (binary segmentation, one step)."""
    s = series.dropna()
    if len(s) < 2 * min_seg + 1:
        return None
    vals = s.values
    best, best_i = 0.0, None
    sd = vals.std() or 1.0
    for i in range(min_seg, len(vals) - min_seg):
        eff = abs(vals[:i].mean() - vals[i:].mean()) / sd
        if eff > best:
            best, best_i = eff, i
    if best_i is None or best < 0.8:
        return None
    before, after = vals[:best_i].mean(), vals[best_i:].mean()
    return {
        "day": s.index[best_i],
        "before_mean": float(before),
        "after_mean": float(after),
        "pct_change": float((after - before) / before * 100) if before else None,
        "effect_size": float(best),
    }


def _readiness(baselines: dict[str, dict]) -> dict:
    """Composite readiness from key-metric z-scores (sign-corrected so + is good)."""
    drivers, scores = {}, []
    for m in ("hrv_rmssd", "resting_hr", "sleep_duration", "recovery_score", "readiness_score"):
        b = baselines.get(m)
        if not b or b["z_score"] is None or np.isnan(b["z_score"]):
            continue
        sign = 1 if higher_is_better(m) else -1
        z = sign * b["z_score"]
        drivers[m] = {"z_good_direction": round(z, 2), "last_7d_mean": round(b["mean_7d"], 1), "baseline_30d": round(b["mean_30d"], 1)}
        scores.append(z)
    composite = float(np.clip(50 + 15 * np.mean(scores), 0, 100)) if scores else 50.0
    tier = "green" if composite >= 55 else ("amber" if composite >= 40 else "red")
    return {"tier": tier, "score": round(composite, 1), "drivers": drivers}


def precompute_user(user_id: str) -> dict:
    daily = _load_daily(user_id)
    if daily.empty:
        return {"daily_rows": 0}

    wide = daily.pivot_table(index="day", columns="metric", values="value").sort_index()
    wide.index = pd.to_datetime(wide.index)
    full_idx = pd.date_range(wide.index.min(), wide.index.max(), freq="D")
    wide = wide.reindex(full_idx)
    as_of = full_idx.max().date()

    baselines, monthly_rows, anomaly_rows, cp_rows = {}, [], [], []
    for m in wide.columns:
        if m not in METRICS:
            continue
        s = wide[m]
        if METRICS[m]["agg"] == "sum":  # sums: missing day == 0 (rest day), not unknown
            s = s.fillna(0) if m in ("training_load", "workout_minutes") else s
        last30, last7 = s.iloc[-30:], s.iloc[-7:]
        mean30, sd30 = last30.mean(), last30.std()
        mean7 = last7.mean()
        z = (mean7 - mean30) / sd30 if sd30 and not np.isnan(sd30) else float("nan")
        y = last30.dropna()
        slope = float(np.polyfit(np.arange(len(y)), y.values, 1)[0] * 7) if len(y) >= 10 else float("nan")
        rel = abs(slope) / (abs(mean30) or 1)
        trend = "stable" if np.isnan(slope) or rel < 0.01 else ("rising" if slope > 0 else "falling")
        baselines[m] = {
            "last_value": float(s.dropna().iloc[-1]) if s.notna().any() else None,
            "mean_7d": float(mean7), "mean_30d": float(mean30), "sd_30d": float(sd30),
            "mean_90d": float(s.iloc[-90:].mean()), "z_score": float(z),
            "slope_per_week": slope, "trend": trend, "status": _status(z, m),
        }

        # monthly aggregates
        g = s.dropna().groupby(s.dropna().index.to_period("M"))
        for period, grp in g:
            monthly_rows.append((user_id, period.to_timestamp().date(), m, grp.mean(), grp.min(), grp.max(),
                                 grp.quantile(0.1), grp.quantile(0.9), int(grp.count())))

        # anomalies vs trailing 30-day window (excluding today)
        if m not in ("training_load", "workout_minutes", "vo2max"):
            trail_mean = s.shift(1).rolling(30, min_periods=14).mean()
            trail_sd = s.shift(1).rolling(30, min_periods=14).std()
            zz = (s - trail_mean) / trail_sd
            for day, zv in zz[zz.abs() >= 2.0].items():
                anomaly_rows.append((user_id, day.date(), m, float(s[day]), float(trail_mean[day]), float(zv),
                                     "high" if zv > 0 else "low"))

        cp = _change_point(s.rolling(7, min_periods=4).mean())
        if cp and m not in ("training_load", "workout_minutes") and abs(cp["pct_change"] or 0) >= 5:
            cp_rows.append((user_id, m, cp["day"].date(), cp["before_mean"], cp["after_mean"], cp["pct_change"], cp["effect_size"]))

    ready = _readiness(baselines)

    with conn() as c, c.transaction():
        for t in ("daily_metrics", "metric_baselines", "metric_monthly", "anomalies", "change_points", "readiness"):
            c.execute(f"DELETE FROM {t} WHERE user_id = %s", (user_id,))
        with c.cursor().copy("COPY daily_metrics (user_id, day, metric, value, source) FROM STDIN") as cp:
            for r in daily.itertuples(index=False):
                cp.write_row((user_id, r.day, r.metric, float(r.value), r.source))
        for m, b in baselines.items():
            c.execute(
                """INSERT INTO metric_baselines (user_id, metric, as_of, last_value, mean_7d, mean_30d, sd_30d, mean_90d,
                   z_score, slope_per_week, trend, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (user_id, m, as_of, b["last_value"], b["mean_7d"], b["mean_30d"], b["sd_30d"], b["mean_90d"],
                 None if np.isnan(b["z_score"]) else b["z_score"],
                 None if np.isnan(b["slope_per_week"]) else b["slope_per_week"], b["trend"], b["status"]),
            )
        c.cursor().executemany(
            "INSERT INTO metric_monthly VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", monthly_rows)
        c.cursor().executemany(
            "INSERT INTO anomalies VALUES (%s,%s,%s,%s,%s,%s,%s)", anomaly_rows)
        c.cursor().executemany(
            "INSERT INTO change_points VALUES (%s,%s,%s,%s,%s,%s,%s)", cp_rows)
        c.execute(
            "INSERT INTO readiness (user_id, as_of, tier, score, drivers) VALUES (%s,%s,%s,%s,%s)",
            (user_id, as_of, ready["tier"], ready["score"], json.dumps(ready["drivers"])),
        )

    return {
        "daily_rows": len(daily), "metrics": len(baselines), "monthly_rows": len(monthly_rows),
        "anomalies": len(anomaly_rows), "change_points": len(cp_rows), "readiness": ready["tier"],
    }


def load_daily_wide(user_id: str, days: int = 180) -> pd.DataFrame:
    """Daily metrics as a wide DataFrame (day x metric). Used by the sandboxed analysis tool."""
    with conn() as c:
        rows = c.execute(
            "SELECT day, metric, value FROM daily_metrics WHERE user_id = %s AND day > CURRENT_DATE - %s::int ORDER BY day",
            (user_id, days),
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).pivot_table(index="day", columns="metric", values="value")
    df.index = pd.to_datetime(df.index)
    return df.sort_index()
