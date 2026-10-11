"""ModelPulse Overview Component (Phase 6).
Displays system health KPIs, latest run indicators, persistence notice, and synthetic disclaimers.
"""
from typing import Any, Dict, List, Optional
import streamlit as st

from dashboard.components.styles import severity_badge, status_badge


def render_overview(
    backend_status: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
    all_runs: List[Dict[str, Any]],
    streaming_health: Dict[str, Any],
):
    """Render the dashboard overview tab."""
    st.subheader("System Overview & Live Health")

    # 1. System Disclaimers, Data Provenance & Persistence Banner
    persistence_type = backend_status.get("persistence_type", "in_memory").upper()
    conn_mode = backend_status.get("mode", "direct_local")
    conn_text = "Live REST API (FastAPI)" if conn_mode == "api" else "Local Repository Direct Mode"
    data_src = backend_status.get("data_source", conn_text)
    data_detail = backend_status.get("data_source_detail", "")

    st.markdown(
        f"""
        <div class="disclaimer-banner">
            <strong>System Connection:</strong> {conn_text} &nbsp;|&nbsp;
            <strong>Data Source:</strong> <code>{data_src}</code> &nbsp;|&nbsp;
            <strong>Persistence:</strong> <code>{persistence_type}</code> &nbsp;|&nbsp;
            <strong>Storage Info:</strong> {data_detail}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Freshness / Isolation Warning
    if backend_status.get("freshness_warning"):
        st.warning(f"⚠️ **Storage Isolation & Provenance Alert:** {backend_status['freshness_warning']}")

    # API Connection Diagnostics
    if backend_status.get("api_error") and conn_mode != "api":
        st.info(
            f"ℹ️ **FastAPI Disconnected:** {backend_status.get('failure_reason', 'unreachable')}. "
            f"Diagnostics: `{backend_status['api_error']}`"
        )

    # 2. Synthetic Data Disclosure
    if latest_run and (
        latest_run.get("run_type") == "simulator"
        or "scenario" in latest_run.get("run_id", "").lower()
    ):
        st.info(
            "🧪 **Controlled Synthetic Fixture Notice:** The displayed run was generated using the ModelPulse controlled drift simulator. "
            "These scenarios evaluate platform detection thresholds and must not be interpreted as unperturbed PaySim production performance."
        )

    # 3. Key Health Metrics Row
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        if latest_run:
            sev = latest_run.get("severity", {})
            score = sev.get("score", 0)
            band = sev.get("band", "Low")
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">Latest Severity Score</div>
                    <div class="metric-value">{score}/100</div>
                    <div class="metric-sub">{severity_badge(band)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.metric("Latest Severity", "No Runs")

    with c2:
        if latest_run:
            drift = latest_run.get("dataset_drift_detected", False)
            status_text = "Drift Detected" if drift else "No Drift"
            color = "#ef4444" if drift else "#10b981"
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">Dataset Drift Status</div>
                    <div class="metric-value" style="color: {color};">{status_text}</div>
                    <div class="metric-sub">Feature Ensemble (k>=2 of 3)</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.metric("Drift Status", "N/A")

    with c3:
        if latest_run:
            q_cnt = latest_run.get("quarantined_sample_count") or 0
            tot = latest_run.get("sample_count") or 0
            q_pct = (q_cnt / tot * 100) if tot > 0 else 0
            color = "#f59e0b" if q_cnt > 0 else "#10b981"
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">Data Quality Isolation</div>
                    <div class="metric-value" style="color: {color};">{q_cnt} <span style="font-size: 1rem; color: #94a3b8;">({q_pct:.1f}%)</span></div>
                    <div class="metric-sub">Quarantined records held from model</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.metric("Quarantined", "0")

    with c4:
        total_runs = len(all_runs)
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-title">Total Monitored Runs</div>
                <div class="metric-value">{total_runs}</div>
                <div class="metric-sub">Evaluation checkpoints recorded</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 4. Latest Run Summary Card
    if latest_run:
        st.markdown("---")
        st.markdown("#### Latest Monitoring Run Summary")

        cols = st.columns([2, 1, 1])
        with cols[0]:
            st.write(f"**Run ID:** `{latest_run.get('run_id')}`")
            st.write(f"**Model:** `{latest_run.get('model_name')}` (v`{latest_run.get('model_version')}`)")
            st.write(f"**Execution Type:** `{latest_run.get('run_type')}`")
            st.write(f"**Status:** {status_badge(latest_run.get('status', 'ok'))}", unsafe_allow_html=True)
            st.write(f"**Created:** `{latest_run.get('created_at')}`")

        with cols[1]:
            cur_win = latest_run.get("current_window", {})
            st.write(f"**Window Samples:** {cur_win.get('sample_count', latest_run.get('sample_count'))}")
            st.write(f"**Valid Scored:** {latest_run.get('valid_sample_count', 'N/A')}")
            st.write(f"**Quarantined:** {latest_run.get('quarantined_sample_count', 0)}")
            pred_drift = latest_run.get("prediction_drift", {})
            st.write(f"**Prediction PSI:** `{pred_drift.get('value', 'Unavailable')}`")

        with cols[2]:
            top_feats = latest_run.get("top_contributors", [])
            st.write("**Top Drift Contributors:**")
            if top_feats:
                for tf in top_feats[:3]:
                    st.write(f"- `{tf.get('feature_name')}`: {tf.get('contribution_score', 0):.2f}")
            else:
                st.write("*No significant feature drift*")

        # Recommendations list
        recs = latest_run.get("recommendations", [])
        if recs:
            st.markdown("**Automated Engine Recommendations:**")
            for r in recs:
                st.info(f"💡 {r}")
    else:
        st.warning("No monitoring runs currently recorded in repository. Trigger an evaluation run to begin telemetry.")
