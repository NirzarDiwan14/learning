"""Seed the database: schema -> users/events/library -> vendor payloads -> normalize -> precompute.

Run:  uv run python -m backend.app.ingest.seed      (from health_chatbot_poc/)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from backend.app.db.connection import conn, init_schema
from backend.app.facts.precompute import precompute_user
from backend.app.ingest.exercise_library import EXERCISES
from backend.app.ingest.normalize import normalize
from backend.app.ingest.vendors import ATHLETES, generate_payloads

log = logging.getLogger(__name__)


def seed(today: date | None = None) -> dict:
    today = today or date.today()
    init_schema()
    summary: dict = {}

    with conn() as c:
        for name, cat, inten, focus, dur, contra, desc in EXERCISES:
            c.execute(
                """INSERT INTO exercise_library (name, category, intensity, focus, duration_min, contraindications, description)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (name) DO NOTHING""",
                (name, cat, inten, focus, dur, contra, desc),
            )

    for a in ATHLETES:
        payloads = generate_payloads(a, today)
        samples, workouts = normalize(payloads)

        with conn() as c, c.transaction():
            c.execute("DELETE FROM users WHERE id = %s", (a.id,))  # cascades
            c.execute(
                "INSERT INTO users (id, name, age, sex, sport, goals, injuries, devices) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (a.id, a.name, a.age, a.sex, a.sport, a.goals, a.injuries, a.devices),
            )
            for e in a.events:
                c.execute(
                    "INSERT INTO events (user_id, event_date, title, event_type, notes) VALUES (%s,%s,%s,%s,%s)",
                    (a.id, today + timedelta(days=e["days"]), e["title"], e["type"], e["notes"]),
                )
            with c.cursor().copy("COPY samples (user_id, source, metric, ts, value, unit) FROM STDIN") as cp:
                for src, metric, ts, val, unit in samples:
                    cp.write_row((a.id, src, metric, ts, val, unit))
            for w in workouts:
                c.execute(
                    """INSERT INTO workouts (user_id, source, start_ts, sport, duration_min, avg_hr, max_hr, kcal, training_load)
                       VALUES (%(user_id)s,%(source)s,%(start_ts)s,%(sport)s,%(duration_min)s,%(avg_hr)s,%(max_hr)s,%(kcal)s,%(training_load)s)""",
                    {**w, "user_id": a.id},
                )

        facts = precompute_user(a.id)
        summary[a.id] = {"raw_samples": len(samples), "workouts": len(workouts), **facts}
        log.info("seeded %s: %s", a.id, summary[a.id])
    return summary


if __name__ == "__main__":
    from backend.app.db.connection import get_pool

    logging.basicConfig(level=logging.INFO)
    for uid, s in seed().items():
        print(uid, s)
    get_pool().close()
