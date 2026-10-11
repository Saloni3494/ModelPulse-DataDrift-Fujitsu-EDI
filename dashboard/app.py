"""ModelPulse v2 — MLOps Monitoring & Drift Telemetry Dashboard.
Streamlit application entrypoint for transaction/fraud model monitoring.
"""
import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st

from dashboard.client import DashboardClient
from dashboard.components.alerting_view import render_alerting_view
from dashboard.components.drift_analysis import render_drift_analysis
from dashboard.components.history import render_history
from dashboard.components.overview import render_overview
from dashboard.components.run_details import render_run_details
from dashboard.components.severity_breakdown import render_severity_breakdown
from dashboard.components.streaming_health import render_streaming_health
from dashboard.components.styles import CUSTOM_CSS, status_badge

# Configure page metadata and layout
st.set_page_config(
    page_title="ModelPulse — MLOps Monitoring",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply sleek visual styles
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def main():
    # Initialize dashboard client
    api_url = os.getenv("MODELPULSE_API_URL", "http://localhost:8000")
    client = DashboardClient(api_base_url=api_url)

    # 1. Sidebar Navigation & Diagnostics
    st.sidebar.title("⚡ ModelPulse v2")
    st.sidebar.caption("Enterprise Fraud Drift & Reliability Monitor")

    # Fetch status
    backend_status = client.get_backend_status()
    conn_mode = backend_status.get("mode")
    if conn_mode == "api":
        st.sidebar.success(f"🟢 Connected to API: `{backend_status.get('url')}`")
    else:
        st.sidebar.info("🟡 Direct Repository Mode (API Offline)")

    st.sidebar.markdown(f"**Persistence:** `{backend_status.get('persistence_type')}`")

    model_details = backend_status.get("details", {})
    st.sidebar.markdown(
        f"**Model:** `{model_details.get('model_name', 'paysim-fraud-model')}`<br>"
        f"**Version:** `v{model_details.get('model_version', '1.0.0')}`",
        unsafe_allow_html=True,
    )

    st.sidebar.markdown("---")

    nav_selection = st.sidebar.radio(
        "Navigation",
        [
            "📊 Overview",
            "📜 Monitoring History",
            "🔍 Run Diagnostics",
            "📈 Statistical Drift",
            "⚖️ Severity Breakdown",
            "🌊 Streaming Telemetry",
            "🔔 Alerting Channels",
        ],
        index=0,
    )

    if st.sidebar.button("🔄 Refresh Telemetry"):
        st.rerun()

    # 2. Data Retrieval
    all_runs = client.list_runs(limit=100)
    latest_run = all_runs[0] if all_runs else None
    streaming_health = client.get_streaming_health()
    alert_status = client.get_alert_status()
    recent_alerts = client.list_alerts(limit=50)

    # Session state tracking for inspected run
    if "selected_run_id" not in st.session_state:
        st.session_state["selected_run_id"] = latest_run.get("run_id") if latest_run else None

    # Retrieve currently selected run object
    current_selected_run = None
    if st.session_state.get("selected_run_id"):
        current_selected_run = client.get_run(st.session_state["selected_run_id"])
    if not current_selected_run and latest_run:
        current_selected_run = latest_run

    # 3. Route to Component View
    if nav_selection == "📊 Overview":
        render_overview(
            backend_status=backend_status,
            latest_run=latest_run,
            all_runs=all_runs,
            streaming_health=streaming_health,
        )

    elif nav_selection == "📜 Monitoring History":
        selected_id = render_history(all_runs=all_runs)
        if selected_id:
            st.session_state["selected_run_id"] = selected_id

    elif nav_selection == "🔍 Run Diagnostics":
        render_run_details(run=current_selected_run)

    elif nav_selection == "📈 Statistical Drift":
        render_drift_analysis(run=current_selected_run)

    elif nav_selection == "⚖️ Severity Breakdown":
        render_severity_breakdown(run=current_selected_run)

    elif nav_selection == "🌊 Streaming Telemetry":
        render_streaming_health(streaming_data=streaming_health)

    elif nav_selection == "🔔 Alerting Channels":
        render_alerting_view(
            alert_status=alert_status,
            recent_alerts=recent_alerts,
        )


if __name__ == "__main__":
    main()
