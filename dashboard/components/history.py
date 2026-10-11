"""ModelPulse Monitoring History Component (Phase 6).
Provides filterable, paginated audit table of historical monitoring runs.
"""
from typing import Any, Dict, List, Optional
import pandas as pd
import streamlit as st

from dashboard.components.styles import severity_badge, status_badge, render_df


def render_history(
    all_runs: List[Dict[str, Any]],
    on_select_run_callback=None,
) -> Optional[str]:
    """Render historical monitoring runs table with interactive filtering."""
    st.subheader("Monitoring Runs History")

    if not all_runs:
        st.info("No monitoring runs found. Execute a monitoring run to view historical records.")
        return None

    # Filter Controls
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        severity_filter = st.selectbox(
            "Filter by Severity Band:",
            options=["All", "Low", "Moderate", "High", "Critical"],
            index=0,
        )
    with c2:
        status_filter = st.selectbox(
            "Filter by Status:",
            options=["All", "ok", "quality_failure", "insufficient_evidence"],
            index=0,
        )
    with c3:
        run_type_filter = st.selectbox(
            "Filter by Run Type:",
            options=["All", "streaming_microbatch", "simulator", "manual", "hourly"],
            index=0,
        )

    # Apply filters
    filtered = []
    for r in all_runs:
        sev_band = r.get("severity", {}).get("band", "Low")
        st_val = r.get("status", "ok")
        rt_val = r.get("run_type", "manual")

        if severity_filter != "All" and sev_band.lower() != severity_filter.lower():
            continue
        if status_filter != "All" and st_val.lower() != status_filter.lower():
            continue
        if run_type_filter != "All" and rt_val.lower() != run_type_filter.lower():
            continue
        filtered.append(r)

    st.caption(f"Showing {len(filtered)} of {len(all_runs)} historical monitoring runs")

    if not filtered:
        st.warning("No runs match the selected filter criteria.")
        return None

    # Format table data
    table_rows = []
    for r in filtered:
        sev = r.get("severity", {})
        table_rows.append({
            "Run ID": r.get("run_id"),
            "Created (UTC)": str(r.get("created_at")),
            "Type": r.get("run_type"),
            "Severity Score": sev.get("score"),
            "Band": sev.get("band"),
            "Drift Detected": "Yes" if r.get("dataset_drift_detected") else "No",
            "Samples": r.get("sample_count"),
            "Valid": r.get("valid_sample_count"),
            "Quarantined": r.get("quarantined_sample_count"),
            "Status": r.get("status"),
        })

    df = pd.DataFrame(table_rows)
    render_df(df)

    # Run selector dropdown for inspection
    st.markdown("---")
    st.markdown("#### Drill Down into Run Details")
    run_ids = [r.get("run_id") for r in filtered]
    selected_run_id = st.selectbox("Select a Run ID to inspect detailed diagnostics:", run_ids, index=0)

    return selected_run_id
