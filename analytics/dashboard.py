"""
FitLake Analytics Dashboard — Streamlit

Visualizes recovery scores, strain and sleep analytics. Reads the Gold layer
output written by the Spark job when it is present, and otherwise runs the pure
Pandas scoring engines from pipeline/aggregation/ over data/raw/ — the path the
hosted demo takes, which needs neither Spark nor Docker.

Run locally: streamlit run analytics/dashboard.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Resolve the repo root so imports and data paths work regardless of the
# working directory Streamlit is launched from (local, Docker, or Cloud).
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# E402: these must follow the sys.path setup above so that `pipeline.*` and
# `data.*` resolve when Streamlit runs this file directly.
import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402
from plotly.subplots import make_subplots  # noqa: E402

from quality.bounds import (  # noqa: E402
    ACTIVITY_BOUNDS,
    BOUND_REASONS,
    HRV_BOUNDS,
    SLEEP_BOUNDS,
)

# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FitLake Analytics",
    page_icon="💚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Data Loading ─────────────────────────────────────────────────────────────

DEMO_USERS = 10
DEMO_DAYS = 90


def _bootstrap_demo_data(raw_dir: Path) -> None:
    """
    Generate the synthetic dataset if it isn't present.

    Hosted deployments (Streamlit Cloud) start from a clean checkout, and
    `data/raw/` is gitignored — so there is nothing to read on first boot.
    Generation is seeded (SEED = 42 in generate_data.py) and takes <1s, so the
    hosted demo shows the same scores as a local `make generate-data` run. Only
    the `ingested_at` wall-clock field varies between runs.
    """
    from data.generate_data import generate_dataset

    raw_dir.mkdir(parents=True, exist_ok=True)
    with st.spinner(f"First run — generating {DEMO_USERS} users × {DEMO_DAYS} days…"):
        generate_dataset(DEMO_USERS, DEMO_DAYS, raw_dir)


@st.cache_data(ttl=300)
def load_data() -> dict[str, pd.DataFrame]:
    """
    Load the Gold layer JSON if the Spark job has written it, else compute the
    same metrics inline from data/raw/ with the Pandas engines.

    The Docker stack writes Gold to Iceberg on MinIO, and sql/analytics_queries.sql
    queries those tables from DuckDB. Neither is reachable from a hosted runner,
    so the deployed demo always takes the file-based path.
    """
    raw_dir = REPO_ROOT / "data" / "raw"
    output_dir = REPO_ROOT / "data" / "output"

    dfs = {}

    # Nothing to read at all (fresh clone / hosted deploy) — generate it.
    if (
        not (output_dir / "daily_recovery.json").exists()
        and not (raw_dir / "hrv.json").exists()
    ):
        _bootstrap_demo_data(raw_dir)

    # Try Gold layer output first, fall back to raw
    if (output_dir / "daily_recovery.json").exists():
        dfs["recovery"] = pd.read_json(output_dir / "daily_recovery.json")
        dfs["strain"] = pd.read_json(output_dir / "strain_scores.json")
        dfs["sleep"] = pd.read_json(output_dir / "sleep_analytics.json")
    elif (raw_dir / "hrv.json").exists():
        # Run pipeline inline for demo purposes
        from pipeline.aggregation.recovery_score import compute_recovery_scores
        from pipeline.aggregation.strain_calculator import compute_strain_scores
        from pipeline.aggregation.sleep_analyzer import compute_sleep_metrics

        hrv_df = pd.read_json(raw_dir / "hrv.json")
        sleep_df = pd.read_json(raw_dir / "sleep.json")
        activity_df = pd.read_json(raw_dir / "activity.json")

        merged = hrv_df.merge(
            sleep_df[
                [
                    "user_id",
                    "date",
                    "total_sleep_hours",
                    "sleep_efficiency_pct",
                    "rem_sleep_minutes",
                    "deep_sleep_minutes",
                    "spo2_avg_pct",
                    "resting_hr_bpm",
                ]
            ],
            on=["user_id", "date"],
        )
        dfs["recovery"] = compute_recovery_scores(merged)

        users_df = pd.DataFrame(
            {
                "user_id": activity_df["user_id"].unique(),
                "max_hr": [185] * activity_df["user_id"].nunique(),
            }
        )
        dfs["strain"] = compute_strain_scores(activity_df, users_df)
        dfs["sleep"] = compute_sleep_metrics(sleep_df)
    else:
        st.error("No data found. Run `make generate-data` first.")
        st.stop()

    # Parse dates
    for key in dfs:
        if "date" in dfs[key].columns:
            dfs[key]["date"] = pd.to_datetime(dfs[key]["date"])

    return dfs


@st.cache_data(ttl=300)
def load_quality_report() -> tuple[pd.DataFrame, int, int]:
    """
    Apply the Silver layer's quarantine bounds to the raw sensor tables.

    A hosted runner has no Spark cluster, so it cannot read
    fitlake.silver.quarantine directly. It can apply the same bounds from
    quality/bounds.py to the same raw files Bronze ingests, which reproduces
    exactly which records Silver rejects and why.

    Returns (one row per violated rule, records scanned, rules enforced).
    A record that breaks two bounds appears twice — a strap that loses contact
    reports both its average and its peak HR too low.
    """
    raw_dir = REPO_ROOT / "data" / "raw"
    tables = [
        ("hrv_readings", "hrv.json", HRV_BOUNDS, "reading_id"),
        ("sleep_records", "sleep.json", SLEEP_BOUNDS, "record_id"),
        ("activity_records", "activity.json", ACTIVITY_BOUNDS, "record_id"),
    ]

    rows: list[dict] = []
    scanned = 0
    rules = 0

    for table, filename, bounds, key in tables:
        path = raw_dir / filename
        if not path.exists():
            continue
        df = pd.read_json(path)
        scanned += len(df)

        for column, (lo, hi) in bounds.items():
            if column not in df.columns:
                continue
            rules += 1
            for _, record in df[(df[column] < lo) | (df[column] > hi)].iterrows():
                value = float(record[column])
                side = "low" if value < lo else "high"
                rows.append(
                    {
                        "Source table": f"bronze.{table}",
                        "Record": record.get(key, ""),
                        "Failed rule": f"{column} ∈ [{lo}, {hi}]",
                        "Value": round(value, 1),
                        "Why it fails": BOUND_REASONS.get(column, {}).get(side, ""),
                    }
                )

    return pd.DataFrame(rows), scanned, rules


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("FitLake")
    st.caption("Wearable Analytics Data Lakehouse")
    st.markdown("---")

    data = load_data()

    users = sorted(data["recovery"]["user_id"].unique())
    selected_user = st.selectbox("Select User", users, index=0)

    date_range = st.date_input(
        "Date Range",
        value=[
            data["recovery"]["date"].min().date(),
            data["recovery"]["date"].max().date(),
        ],
    )

    st.markdown("---")
    st.markdown("**Pipeline Stack**")
    st.markdown(
        """
    - Apache Iceberg (open-table)
    - PySpark 3.5 ETL
    - Apache Airflow DAGs
    - Data quality validation
    - OpenLineage + Marquez
    - MinIO (S3-compatible)
    - DuckDB SQL (`sql/`)
    """
    )
    st.caption(
        "This hosted demo runs the Pandas scoring engines from "
        "`pipeline/aggregation/` against a seeded synthetic dataset. The Spark, "
        "Iceberg and Airflow stack above runs locally via `make infra-up`."
    )
    st.markdown("---")
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# ─── Filter Data ──────────────────────────────────────────────────────────────


def filter_user(df: pd.DataFrame, user: str, start, end) -> pd.DataFrame:
    mask = (
        (df["user_id"] == user)
        & (df["date"] >= pd.Timestamp(start))
        & (df["date"] <= pd.Timestamp(end))
    )
    return df[mask].sort_values("date")


start_date, end_date = date_range[0], date_range[-1]
rec_df = filter_user(data["recovery"], selected_user, start_date, end_date)
strain_df = filter_user(data["strain"], selected_user, start_date, end_date)
sleep_df = filter_user(data["sleep"], selected_user, start_date, end_date)


# ─── Header KPIs ─────────────────────────────────────────────────────────────

st.title(f"Recovery Dashboard — {selected_user}")
st.caption(f"{start_date} → {end_date} | {len(rec_df)} days tracked")

col1, col2, col3, col4, col5 = st.columns(5)

avg_recovery = rec_df["recovery_score"].mean() if len(rec_df) > 0 else 0
green_days = (rec_df["recovery_state"] == "Green").sum()
avg_strain = strain_df["strain_score"].mean() if len(strain_df) > 0 else 0
avg_sleep = sleep_df["total_sleep_hours"].mean() if len(sleep_df) > 0 else 0
avg_hrv = rec_df["hrv_rmssd"].mean() if len(rec_df) > 0 else 0

col1.metric(
    "Avg Recovery",
    f"{avg_recovery:.0f}%",
    delta=f"{avg_recovery - 50:.0f} vs 50 baseline",
)
col2.metric(
    "Green Days",
    f"{green_days}/{len(rec_df)}",
    delta=f"{green_days / max(len(rec_df), 1) * 100:.0f}%",
)
col3.metric("Avg Daily Strain", f"{avg_strain:.1f}/21")
col4.metric("Avg Sleep", f"{avg_sleep:.1f}h", delta=f"{avg_sleep - 8:.1f}h vs 8h ideal")
col5.metric("Avg HRV (rMSSD)", f"{avg_hrv:.0f}ms")

st.markdown("---")


# ─── Row 1: Recovery Trend ────────────────────────────────────────────────────

st.subheader("Recovery Score Trend")

if len(rec_df) > 0:
    fig_recovery = go.Figure()

    # Background zones
    fig_recovery.add_hrect(
        y0=0,
        y1=33,
        fillcolor="rgba(255,82,82,0.1)",
        line_width=0,
        annotation_text="Red Zone",
    )
    fig_recovery.add_hrect(
        y0=33,
        y1=66,
        fillcolor="rgba(255,193,7,0.1)",
        line_width=0,
        annotation_text="Yellow Zone",
    )
    fig_recovery.add_hrect(
        y0=66,
        y1=100,
        fillcolor="rgba(76,175,80,0.1)",
        line_width=0,
        annotation_text="Green Zone",
    )

    # Recovery line
    fig_recovery.add_trace(
        go.Scatter(
            x=rec_df["date"],
            y=rec_df["recovery_score"],
            mode="lines+markers",
            name="Recovery Score",
            line=dict(color="#00BCD4", width=2),
            marker=dict(
                color=rec_df["recovery_state"].map(
                    {"Green": "#4CAF50", "Yellow": "#FFC107", "Red": "#F44336"}
                ),
                size=8,
            ),
        )
    )

    # HRV baseline overlay
    if "hrv_baseline" in rec_df.columns:
        fig_recovery.add_trace(
            go.Scatter(
                x=rec_df["date"],
                y=rec_df["hrv_baseline"],
                mode="lines",
                name="HRV Baseline (ms)",
                line=dict(color="#9C27B0", width=1, dash="dot"),
                yaxis="y2",
            )
        )
        fig_recovery.update_layout(
            yaxis2=dict(title="HRV (ms)", overlaying="y", side="right", showgrid=False)
        )

    fig_recovery.update_layout(
        xaxis_title="Date",
        yaxis_title="Recovery Score (0-100)",
        height=350,
        hovermode="x unified",
    )
    st.plotly_chart(fig_recovery, width="stretch")


# ─── Row 2: Strain vs Recovery + Sleep Components ─────────────────────────────

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Strain vs Next-Day Recovery")
    if len(strain_df) > 1 and len(rec_df) > 1:
        # Shift strain by 1 day to see next-day recovery impact
        strain_shifted = strain_df[["date", "strain_score", "strain_category"]].copy()
        strain_shifted["next_date"] = strain_shifted["date"] + pd.Timedelta(days=1)
        recovery_next = rec_df[["date", "recovery_score", "recovery_state"]].copy()

        scatter_data = strain_shifted.merge(
            recovery_next, left_on="next_date", right_on="date", how="inner"
        )

        fig_scatter = px.scatter(
            scatter_data,
            x="strain_score",
            y="recovery_score",
            color="strain_category",
            # One trendline over all points, not one per category: the rarer
            # categories hold as few as two days in a 90-day window, and a
            # two-point OLS fit is noise drawn with a confident line.
            trendline="ols",
            trendline_scope="overall",
            trendline_color_override="#00BCD4",
            category_orders={
                "strain_category": [
                    "Recovery",
                    "Light",
                    "Moderate",
                    "Hard",
                    "All Out",
                ]
            },
            labels={
                "strain_score": "Day N Strain (0-21)",
                "recovery_score": "Day N+1 Recovery (0-100)",
            },
            # Strain categories are ordered magnitude, so they get one hue
            # stepped by lightness (dim = easy day, bright = all out) rather
            # than five separate hues. The previous green→red set left
            # "Hard" and "All Out" 4.7 ΔE apart for normal vision against a
            # readability floor of 15 — indistinguishable even with full
            # colour vision — and it reused the recovery-state greens and reds
            # for an unrelated meaning in the same dashboard.
            color_discrete_map={
                "Recovery": "#A85A15",
                "Light": "#C97A1E",
                "Moderate": "#E39B2F",
                "Hard": "#F5B94F",
                "All Out": "#FFD682",
            },
            height=350,
        )
        st.plotly_chart(fig_scatter, width="stretch")
    else:
        st.info("Not enough data for scatter plot.")

with col_right:
    st.subheader("Recovery Score Components")
    if len(rec_df) > 0:
        components = [
            "hrv_component",
            "rhr_component",
            "sleep_component",
            "spo2_component",
        ]
        labels = ["HRV (40%)", "Resting HR (25%)", "Sleep (25%)", "SpO2 (10%)"]
        avg_components = [
            rec_df[c].mean() if c in rec_df.columns else 0 for c in components
        ]

        fig_radar = go.Figure(
            go.Scatterpolar(
                r=avg_components + [avg_components[0]],
                theta=labels + [labels[0]],
                fill="toself",
                fillcolor="rgba(0,188,212,0.3)",
                line=dict(color="#00BCD4"),
            )
        )
        fig_radar.update_layout(
            # Plotly does not theme the polar face, so it renders white inside
            # the dark card unless the background is cleared explicitly.
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(
                    range=[0, 100],
                    gridcolor="rgba(255,255,255,0.15)",
                    linecolor="rgba(255,255,255,0.15)",
                ),
                angularaxis=dict(gridcolor="rgba(255,255,255,0.15)"),
            ),
            height=350,
            showlegend=False,
        )
        st.plotly_chart(fig_radar, width="stretch")


# ─── Row 3: Sleep Architecture ────────────────────────────────────────────────

st.subheader("Sleep Architecture Breakdown")

if len(sleep_df) > 0:
    # Two panels sharing the date axis, rather than one plot with two y-scales:
    # a night's stages span 0-8h while cumulative debt reaches 30h, so
    # overlaying them either flattens the bars into the floor of the chart or
    # invents an alignment between two scales that have nothing to do with
    # each other.
    fig_sleep = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.68, 0.32],
    )
    stage_colors = {
        "deep_sleep_minutes": ("#1565C0", "Deep Sleep"),
        "rem_sleep_minutes": ("#7B1FA2", "REM"),
        "light_sleep_minutes": ("#0288D1", "Light Sleep"),
        "awake_minutes": ("#B0BEC5", "Awake"),
    }

    for col, (color, label) in stage_colors.items():
        if col in sleep_df.columns:
            fig_sleep.add_trace(
                go.Bar(
                    x=sleep_df["date"],
                    y=sleep_df[col] / 60,  # Convert to hours
                    name=label,
                    marker_color=color,
                ),
                row=1,
                col=1,
            )

    if "sleep_debt_hours" in sleep_df.columns:
        fig_sleep.add_trace(
            go.Scatter(
                x=sleep_df["date"],
                y=sleep_df["sleep_debt_hours"],
                mode="lines",
                name="Sleep Debt (14d rolling)",
                line=dict(color="#FF5722", width=2),
            ),
            row=2,
            col=1,
        )

    fig_sleep.update_layout(
        barmode="stack",
        height=420,
        legend=dict(orientation="h", y=1.08),
        hovermode="x unified",
    )
    fig_sleep.update_yaxes(title_text="Hours asleep", row=1, col=1)
    fig_sleep.update_yaxes(title_text="Debt (h)", row=2, col=1)
    fig_sleep.update_xaxes(title_text="Date", row=2, col=1)

    st.plotly_chart(fig_sleep, width="stretch")


# ─── Row 4: Weekly Pattern Heatmap ────────────────────────────────────────────

st.subheader("Weekly Recovery Pattern")

if len(rec_df) >= 14:
    rec_copy = rec_df.copy()
    rec_copy["week"] = rec_copy["date"].dt.isocalendar().week.astype(int)
    rec_copy["day_of_week"] = rec_copy["date"].dt.day_name()

    pivot = rec_copy.pivot_table(
        values="recovery_score", index="day_of_week", columns="week", aggfunc="mean"
    )
    day_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    pivot = pivot.reindex([d for d in day_order if d in pivot.index])

    fig_heatmap = px.imshow(
        pivot,
        # The zone colours are fixed to the 0-100 score bands so a cell means
        # the same thing here as on the trend chart. That keeps most cells in
        # one band for a consistent athlete, so the score is printed in the
        # cell too rather than left encoded in colour alone.
        text_auto=".0f",
        color_continuous_scale=[
            [0, "#F44336"],
            [0.33, "#FF9800"],
            [0.66, "#FFC107"],
            [1, "#4CAF50"],
        ],
        zmin=0,
        zmax=100,
        labels={"x": "Week Number", "y": "Day of Week", "color": "Recovery"},
        height=300,
    )
    st.plotly_chart(fig_heatmap, width="stretch")
else:
    st.info("At least 2 weeks of data needed for the heatmap.")


# ─── Row 5: HRV Distribution Across Users ─────────────────────────────────────

st.subheader("HRV Distribution — All Users")

all_rec = data["recovery"][
    (data["recovery"]["date"] >= pd.Timestamp(start_date))
    & (data["recovery"]["date"] <= pd.Timestamp(end_date))
]

if len(all_rec) > 0 and "hrv_rmssd" in all_rec.columns:
    fig_hrv = px.violin(
        all_rec,
        x="user_id",
        y="hrv_rmssd",
        box=True,
        points="outliers",
        color="user_id",
        labels={"hrv_rmssd": "HRV rMSSD (ms)", "user_id": "User"},
        height=350,
    )
    # Highlight selected user
    for trace in fig_hrv.data:
        trace.opacity = 1.0 if selected_user in (trace.name or "") else 0.3
    st.plotly_chart(fig_hrv, width="stretch")


# ─── Row 6: Data Quality / Quarantine ─────────────────────────────────────────

st.subheader("Data Quality — Silver Layer Quarantine")

violations_df, records_scanned, rules_enforced = load_quality_report()
quarantined = violations_df["Record"].nunique() if len(violations_df) > 0 else 0

pass_rate = (records_scanned - quarantined) / max(records_scanned, 1) * 100

qcol1, qcol2, qcol3, qcol4 = st.columns(4)
qcol1.metric("Records Scanned", f"{records_scanned:,}")
qcol2.metric("Quarantined", f"{quarantined}")
qcol3.metric("Pass Rate", f"{pass_rate:.2f}%")
qcol4.metric("Bound Rules Enforced", f"{rules_enforced}")

if len(violations_df) > 0:
    st.dataframe(
        violations_df.sort_values(["Source table", "Record"]),
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "Rejected records are written to `fitlake.silver.quarantine` rather than "
        "dropped, so a bad sensor is auditable instead of invisible. These are "
        "injected optical-HR faults — lost skin contact reads implausibly low, "
        "cadence lock spikes the peak — and HR zone minutes are generated before "
        "the fault is applied, so the workout's strain stays coherent while the "
        "reported HR does not."
    )
else:
    st.success("All records passed physiological bound validation.")


# ─── Footer ───────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "FitLake — Open-Source Fitness Analytics Data Lakehouse | "
    "Built with Apache Iceberg, PySpark, Airflow, DuckDB | "
    "[github.com/itsameaditya/fitlake](https://github.com/itsameaditya/fitlake)"
)
