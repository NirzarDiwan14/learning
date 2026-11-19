-- Layer [1]: canonical store + Layer [2]: precomputed "Health Facts"
CREATE EXTENSION IF NOT EXISTS vector;  -- used by mem0 (pgvector)

-- ---------- Profiles & domain data ----------
CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    age         INT,
    sex         TEXT,
    sport       TEXT,
    goals       TEXT,
    injuries    TEXT,
    devices     TEXT[] NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS events (
    id          SERIAL PRIMARY KEY,
    user_id     TEXT REFERENCES users(id) ON DELETE CASCADE,
    event_date  DATE NOT NULL,
    title       TEXT NOT NULL,
    event_type  TEXT NOT NULL,      -- race | match | travel | illness | test
    notes       TEXT
);

-- ---------- [1] Canonical normalized samples (any granularity, tagged by source) ----------
CREATE TABLE IF NOT EXISTS samples (
    user_id     TEXT REFERENCES users(id) ON DELETE CASCADE,
    source      TEXT NOT NULL,      -- whoop | oura | apple_watch | polar
    metric      TEXT NOT NULL,      -- canonical metric name (see tools/catalog.py)
    ts          TIMESTAMPTZ NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS samples_user_metric_ts ON samples (user_id, metric, ts);

CREATE TABLE IF NOT EXISTS workouts (
    id            SERIAL PRIMARY KEY,
    user_id       TEXT REFERENCES users(id) ON DELETE CASCADE,
    source        TEXT NOT NULL,
    start_ts      TIMESTAMPTZ NOT NULL,
    sport         TEXT NOT NULL,
    duration_min  DOUBLE PRECISION NOT NULL,
    avg_hr        DOUBLE PRECISION,
    max_hr        DOUBLE PRECISION,
    kcal          DOUBLE PRECISION,
    training_load DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS workouts_user_ts ON workouts (user_id, start_ts);

-- ---------- [2] Health Facts (precomputed; LLM reads these via tools, never raw samples) ----------
CREATE TABLE IF NOT EXISTS daily_metrics (
    user_id  TEXT REFERENCES users(id) ON DELETE CASCADE,
    day      DATE NOT NULL,
    metric   TEXT NOT NULL,
    value    DOUBLE PRECISION NOT NULL,
    source   TEXT NOT NULL,
    PRIMARY KEY (user_id, day, metric)
);

CREATE TABLE IF NOT EXISTS metric_baselines (
    user_id         TEXT REFERENCES users(id) ON DELETE CASCADE,
    metric          TEXT NOT NULL,
    as_of           DATE NOT NULL,
    last_value      DOUBLE PRECISION,
    mean_7d         DOUBLE PRECISION,
    mean_30d        DOUBLE PRECISION,
    sd_30d          DOUBLE PRECISION,
    mean_90d        DOUBLE PRECISION,
    z_score         DOUBLE PRECISION,   -- last 7d mean vs 30d baseline
    slope_per_week  DOUBLE PRECISION,   -- linear trend over the last 30 days
    trend           TEXT,               -- rising | falling | stable
    status          TEXT,               -- above_baseline | below_baseline | within_baseline
    PRIMARY KEY (user_id, metric)
);

CREATE TABLE IF NOT EXISTS metric_monthly (
    user_id  TEXT REFERENCES users(id) ON DELETE CASCADE,
    month    DATE NOT NULL,
    metric   TEXT NOT NULL,
    mean     DOUBLE PRECISION,
    min      DOUBLE PRECISION,
    max      DOUBLE PRECISION,
    p10      DOUBLE PRECISION,
    p90      DOUBLE PRECISION,
    n_days   INT,
    PRIMARY KEY (user_id, month, metric)
);

CREATE TABLE IF NOT EXISTS anomalies (
    user_id    TEXT REFERENCES users(id) ON DELETE CASCADE,
    day        DATE NOT NULL,
    metric     TEXT NOT NULL,
    value      DOUBLE PRECISION,
    baseline   DOUBLE PRECISION,
    z          DOUBLE PRECISION,
    direction  TEXT,
    PRIMARY KEY (user_id, day, metric)
);

CREATE TABLE IF NOT EXISTS change_points (
    user_id      TEXT REFERENCES users(id) ON DELETE CASCADE,
    metric       TEXT NOT NULL,
    day          DATE NOT NULL,
    before_mean  DOUBLE PRECISION,
    after_mean   DOUBLE PRECISION,
    pct_change   DOUBLE PRECISION,
    effect_size  DOUBLE PRECISION,
    PRIMARY KEY (user_id, metric, day)
);

CREATE TABLE IF NOT EXISTS readiness (
    user_id    TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    as_of      DATE NOT NULL,
    tier       TEXT NOT NULL,        -- green | amber | red
    score      DOUBLE PRECISION,     -- 0-100 composite
    drivers    JSONB NOT NULL        -- which metrics pushed it up/down
);

-- ---------- Cached daily brief (generated once per user per day, reused by every app open) ----------
CREATE TABLE IF NOT EXISTS daily_briefs (
    user_id     TEXT REFERENCES users(id) ON DELETE CASCADE,
    day         DATE NOT NULL,
    content     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, day)
);

-- ---------- Domain knowledge used by the coaching path ----------
CREATE TABLE IF NOT EXISTS exercise_library (
    id                SERIAL PRIMARY KEY,
    name              TEXT UNIQUE NOT NULL,
    category          TEXT NOT NULL,     -- strength | endurance | mobility | recovery | hiit
    intensity         TEXT NOT NULL,     -- low | moderate | high
    focus             TEXT NOT NULL,
    duration_min      INT,
    contraindications TEXT[] NOT NULL DEFAULT '{}',
    description       TEXT NOT NULL
);
