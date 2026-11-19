import pandas as pd
import streamlit as st

from lib import api

user_id = st.session_state.user_id
prof = api.profile(user_id)
user, rd = prof["user"], prof["readiness"]
base = {b["metric"]: b for b in prof["baselines"]}

st.subheader(f"{user['name']} · {user['sport']}")
st.caption(f"Goals: {user['goals']}  \nNotes: {user['injuries']}")

# ---------------------------------------------------------------- KPI row
KPIS = [("hrv_rmssd", "HRV", "ms", "normal"), ("resting_hr", "Resting HR", "bpm", "inverse"),
        ("sleep_duration", "Sleep", "min", "normal"), ("recovery_score", "WHOOP recovery", "%", "normal")]
with st.container(horizontal=True):
    st.metric("Readiness", f"{rd['tier']}", f"{rd['score']}/100", delta_color="off", border=True,
              help="Composite of HRV, resting HR, sleep and vendor recovery scores vs the 30-day baseline")
    for m, label, unit, color in KPIS:
        if m in base:
            b = base[m]
            st.metric(f"{label} (7-day avg)", f"{b['mean_7d']:.1f} {unit}", f"{b['mean_7d'] - b['mean_30d']:+.1f} vs 30-day",
                      delta_color=color, border=True)

# ---------------------------------------------------------------- brief
with st.container(border=True):
    head = st.container(horizontal=True, vertical_alignment="center")
    head.markdown("**:material/wb_sunny: Daily brief** (generated once per day, then cached)")
    regen = head.button("Regenerate", icon=":material/refresh:", key="regen_brief")
    if "brief" not in st.session_state or st.session_state.get("brief_user") != user_id or regen:
        try:
            with st.spinner("Generating brief..."):
                st.session_state.brief = api.brief(user_id, force=regen)
                st.session_state.brief_user = user_id
        except api.BackendError as e:
            st.session_state.pop("brief", None)  # don't cache failures; retry on next run
            st.warning(str(e), title="Brief unavailable")
    if "brief" in st.session_state:
        st.markdown(st.session_state.brief["brief"])
        if st.session_state.brief.get("cached"):
            st.caption("Served from cache (no LLM call).")

# ---------------------------------------------------------------- charts
left, right = st.columns([3, 2])
with left:
    available = sorted(base)
    picked = st.pills("Metrics", available, selection_mode="multi",
                      default=[m for m in ("hrv_rmssd", "resting_hr") if m in available], key="dash_metrics")
    days = st.segmented_control("Window", [30, 90, 180], default=180, format_func=lambda d: f"{d} days", key="dash_days")
    if picked:
        rows = api.series(user_id, tuple(picked), days or 180)
        if rows:
            df = pd.DataFrame(rows).pivot_table(index="day", columns="metric", values="value")
            df.index = pd.to_datetime(df.index)
            for m in picked:
                if m in df:
                    st.line_chart(df[[m]], height=180, x_label=None, y_label=m)

with right:
    st.markdown("**Events**")
    ev = pd.DataFrame(prof["events"])
    if not ev.empty:
        st.dataframe(ev.rename(columns={"event_date": "date", "event_type": "type"}), hide_index=True)

    st.markdown("**What the LLM sees vs what is stored**")
    fp = api.footprint(user_id)
    st.dataframe(pd.DataFrame([
        {"layer": "Raw vendor samples (never sent to the LLM)", "rows": fp["raw_samples"]},
        {"layer": "Daily rollups (analysis sandbox only)", "rows": fp["daily_rows"]},
        {"layer": "Monthly aggregates", "rows": fp["monthly_rows"]},
        {"layer": "Baselines (one per metric)", "rows": fp["baselines"]},
        {"layer": "Anomaly days", "rows": fp["anomalies"]},
        {"layer": "Change-points", "rows": fp["change_points"]},
    ]), hide_index=True)
    st.caption("Tools return compact JSON built from the summary layers, usually 0.3-3 KB per call.")
