"""Deep-analysis tools: let the data-scientist sub-agent write pandas code that runs in an
isolated subprocess over ONE user's daily Health Facts.

POC isolation (NOT a production sandbox):
  * separate `python -I` process, empty environment (no DB creds, no API keys)
  * only the user's own daily table is provided, as a CSV in a temp dir
  * CPU-time / memory rlimits + wall-clock timeout, output truncated
Production: run this in a real sandbox (container/gVisor/Firecracker, e.g. a
deepagents sandbox backend) with no network access.
"""

from __future__ import annotations

import json
import resource
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

from langchain.tools import ToolRuntime, tool

from backend.app.agents.context import HealthContext
from backend.app.config import get_settings
from backend.app.facts.precompute import load_daily_wide
from backend.app.tools.catalog import METRICS

MAX_OUTPUT = 4000

_RUNNER = textwrap.dedent(
    """
    import json, sys, warnings
    warnings.filterwarnings("ignore")
    import numpy as np, pandas as pd
    df = pd.read_csv("daily.csv", index_col=0, parse_dates=True)
    events = pd.read_json("events.json", convert_dates=["date"]) if open("events.json").read().strip() != "[]" else pd.DataFrame(columns=["date","title","type"])
    code = open("analysis.py").read()
    ns = {"df": df, "events": events, "pd": pd, "np": np}
    exec(compile(code, "analysis.py", "exec"), ns)
    if "result" in ns:
        r = ns["result"]
        print("RESULT:", r.to_string() if hasattr(r, "to_string") else json.dumps(r, default=str, indent=1))
    """
)


def _limits() -> None:  # runs in the child before exec
    resource.setrlimit(resource.RLIMIT_CPU, (20, 20))
    resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))


@tool
def describe_dataset(runtime: ToolRuntime[HealthContext]) -> str:
    """Describe the analysis DataFrame `df` available to run_python_analysis: columns
    (metrics + units), date range, row count and missing-value counts. Call this first."""
    df = load_daily_wide(runtime.context.user_id)
    if df.empty:
        return json.dumps({"error": "no data"})
    return json.dumps({
        "df": "pandas DataFrame, DatetimeIndex (one row per day), one column per metric",
        "rows": len(df), "start": str(df.index.min().date()), "end": str(df.index.max().date()),
        "columns": {c: {"unit": METRICS.get(c, {}).get("unit"), "non_null": int(df[c].notna().sum())} for c in df.columns},
        "events": "pandas DataFrame `events` with columns date, title, type (past and future)",
        "note": "Missing training_load/workout_minutes on a day means rest day (treat as 0).",
    })


@tool
def run_python_analysis(code: str, runtime: ToolRuntime[HealthContext]) -> str:
    """Execute pandas/numpy code against the user's daily metrics and return the output.

    Pre-loaded variables: `df` (daily metrics, DatetimeIndex, one column per metric),
    `events` (DataFrame: date, title, type), `pd`, `np`. Assign the final answer to a
    variable named `result` (dict, number, Series or DataFrame) and/or print() it.
    No file, network or database access. Keep outputs small (aggregate, don't dump rows).

    Args:
        code: Python source code to execute
    """
    uid = runtime.context.user_id
    df = load_daily_wide(uid)
    from backend.app.db.connection import fetch_all

    events = fetch_all("SELECT event_date AS date, title, event_type AS type FROM events WHERE user_id=%s", (uid,))
    with tempfile.TemporaryDirectory(prefix="analysis_") as tmp:
        p = Path(tmp)
        df.to_csv(p / "daily.csv")
        (p / "events.json").write_text(json.dumps(events, default=str))
        (p / "analysis.py").write_text(code)
        (p / "runner.py").write_text(_RUNNER)
        try:
            proc = subprocess.run(
                [sys.executable, "-I", "runner.py"], cwd=tmp, env={"PATH": "/usr/bin:/bin"},
                capture_output=True, text=True, timeout=get_settings().sandbox_timeout_s, preexec_fn=_limits,
            )
        except subprocess.TimeoutExpired:
            return "ERROR: analysis timed out - simplify the computation."
    out = (proc.stdout or "")[-MAX_OUTPUT:]
    if proc.returncode != 0:
        err = (proc.stderr or "").strip().splitlines()
        return f"ERROR (exit {proc.returncode}): " + "\n".join(err[-6:]) + (f"\nPartial stdout:\n{out}" if out else "")
    return out or "Code ran but produced no output. Assign `result` or print()."


DEEP_ANALYSIS_TOOLS = [describe_dataset, run_python_analysis]
