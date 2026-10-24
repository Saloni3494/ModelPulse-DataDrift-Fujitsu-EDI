"""
ModelPulse — MLOps Monitoring & Data Drift Platform.
Main application shell: top command-bar navigation (no sidebar).
"""

from datetime import datetime
from html import escape

import streamlit as st

from dashboard.client import DashboardClient
from dashboard.components.alerting_view import render_alerting_view
from dashboard.components.drift_analysis import render_drift_analysis
from dashboard.components.history import render_history
from dashboard.components.overview import render_overview
from dashboard.components.run_details import render_run_details
from dashboard.components.severity_breakdown import render_severity_breakdown
from dashboard.components.simulator_view import render_simulator_view
from dashboard.components.streaming_health import render_streaming_health
from dashboard.components.styles import CUSTOM_CSS


# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="ModelPulse | MLOps Observability",
    page_icon="M",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# =========================================================
# SHELL CSS — layout only. Colors, inputs, buttons, tables and
# panels all come from CUSTOM_CSS (styles.py) so there are no
# conflicting overrides.
# =========================================================

SHELL_CSS = """
<style>
/* ---- Remove the sidebar completely ---- */
[data-testid="stSidebar"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"] {
    display: none !important;
}

/* Transparent Streamlit header; leave room for it */
[data-testid="stHeader"] {
    background: transparent !important;
    height: 2.4rem;
}
.block-container {
    max-width: 1600px;
    padding: 3rem 2rem 2rem !important;
}

/* ---- Command bar ---- */
.st-key-mp_topbar {
    position: relative;
    background: linear-gradient(180deg, var(--mp-surface-2), var(--mp-surface));
    border: 1px solid var(--mp-border-strong);
    border-radius: var(--mp-radius);
    padding: 6px 12px;
    box-shadow: 0 8px 28px rgba(0,0,0,.35);
    overflow: hidden;
}
.st-key-mp_topbar::before {
    content: ""; position: absolute; left: 0; right: 0; top: 0; height: 2px;
    background: linear-gradient(90deg, var(--mp-accent), rgba(61,155,255,0) 70%);
}
.st-key-mp_topbar [data-testid="stHorizontalBlock"] {
    align-items: center; gap: .75rem;
}

/* Brand */
.mp-brand-inline { display: flex; align-items: center; gap: 10px; padding: 2px 4px; }
.mp-brand-mark {
    display: grid; place-items: center; width: 34px; height: 34px; flex: 0 0 34px;
    border-radius: 8px; border: 1px solid var(--mp-accent);
    background: var(--mp-accent-soft);
    box-shadow: 0 0 16px rgba(61,155,255,.20);
}
.mp-brand-name {
    color: #fff; font-size: 15px; font-weight: 700; letter-spacing: -.2px; line-height: 1.1;
}
.mp-brand-subtitle {
    color: var(--mp-text-faint); font-family: var(--mp-mono); font-size: 8.5px;
    font-weight: 600; letter-spacing: 1.4px; text-transform: uppercase; margin-top: 3px;
}

/* Navigation: horizontal tab strip built from the radio */
.st-key-modelpulse_navigation [role="radiogroup"] {
    flex-direction: row !important; flex-wrap: nowrap; gap: 2px;
    overflow-x: auto; scrollbar-width: none; justify-content: center;
}
.st-key-modelpulse_navigation [role="radiogroup"]::-webkit-scrollbar { display: none; }
.st-key-modelpulse_navigation label {
    position: relative; height: 38px; padding: 0 14px !important; margin: 0 !important;
    border-radius: var(--mp-radius-sm); border: 1px solid transparent;
    white-space: nowrap; cursor: pointer;
    transition: background var(--mp-fast) var(--mp-ease),
                border-color var(--mp-fast) var(--mp-ease);
}
.st-key-modelpulse_navigation label > div:first-child { display: none !important; }
.st-key-modelpulse_navigation label p {
    color: var(--mp-text-dim); font-size: 12.5px; font-weight: 600; letter-spacing: .2px;
    transition: color var(--mp-fast) var(--mp-ease);
}
.st-key-modelpulse_navigation label::after {
    content: ""; position: absolute; left: 12px; right: 12px; bottom: -7px; height: 2px;
    background: var(--mp-accent); border-radius: 2px;
    transform: scaleX(0); transition: transform 220ms var(--mp-ease);
}
.st-key-modelpulse_navigation label:hover {
    background: var(--mp-surface-3); border-color: var(--mp-border-strong);
}
.st-key-modelpulse_navigation label:hover p { color: var(--mp-text); }
.st-key-modelpulse_navigation label:has(input:checked) {
    background: var(--mp-accent-soft); border-color: rgba(61,155,255,.35);
}
.st-key-modelpulse_navigation label:has(input:checked) p { color: #fff; }
.st-key-modelpulse_navigation label:has(input:checked)::after { transform: scaleX(1); }

/* ---- Context strip (breadcrumb + live status) ---- */
.mp-ctx {
    display: flex; align-items: center; justify-content: space-between;
    flex-wrap: wrap; gap: 8px 18px;
    margin: 10px 0 14px; padding: 7px 14px;
    background: var(--mp-surface); border: 1px solid var(--mp-border);
    border-radius: var(--mp-radius-sm);
    font-family: var(--mp-mono); font-size: 10.5px;
}
.mp-crumbs { display: flex; align-items: center; gap: 8px; color: var(--mp-text-faint);
             letter-spacing: 1px; text-transform: uppercase; }
.mp-crumbs b { color: var(--mp-text); font-weight: 600; }
.mp-crumbs i { color: var(--mp-border-strong); font-style: normal; }
.mp-ctx-items { display: flex; align-items: center; flex-wrap: wrap; gap: 6px 18px; }
.mp-ctx-item { display: flex; align-items: center; gap: 7px; color: var(--mp-text-faint);
               text-transform: uppercase; letter-spacing: .8px; }
.mp-ctx-item strong { color: var(--mp-text); font-weight: 500; text-transform: none;
                      letter-spacing: 0; overflow-wrap: anywhere; }

/* Footer */
.mp-footer {
    margin-top: 32px; padding-top: 12px; border-top: 1px solid var(--mp-border);
    display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px;
    color: var(--mp-text-faint); font-size: 10.5px; line-height: 1.7;
}
.mp-footer strong { color: var(--mp-text-dim); }

@media (max-width: 900px) {
    .block-container { padding: 2.6rem 1rem 1.5rem !important; }
    .st-key-modelpulse_navigation [role="radiogroup"] { justify-content: flex-start; }
}
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
st.markdown(SHELL_CSS, unsafe_allow_html=True)


# =========================================================
# NAVIGATION CONFIGURATION
# =========================================================

NAVIGATION = {
    "overview": {
        "label": "◈  Overview",
        "title": "System Overview",
        "description": (
            "A unified view of monitoring health, recent model runs, "
            "and streaming activity."
        ),
        "group": "PLATFORM",
    },
    "history": {
        "label": "▤  History",
        "title": "Monitoring History",
        "description": (
            "Review previous monitoring runs and changes in model "
            "and data behavior."
        ),
        "group": "MONITORING",
    },
    "diagnostics": {
        "label": "⌘  Diagnostics",
        "title": "Run Diagnostics",
        "description": (
            "Inspect individual runs, drift results, severity, "
            "and available recommendations."
        ),
        "group": "MONITORING",
    },
    "drift": {
        "label": "◉  Drift",
        "title": "Statistical Drift Analysis",
        "description": "Explore statistical drift signals and feature-level behavior.",
        "group": "ANALYTICS",
    },
    "severity": {
        "label": "▥  Severity",
        "title": "Severity & Risk Analysis",
        "description": "Understand severity distribution and monitoring risk signals.",
        "group": "ANALYTICS",
    },
    "telemetry": {
        "label": "≋  Telemetry",
        "title": "Streaming Telemetry",
        "description": "Review ingestion, validation, retries, and reconciliation.",
        "group": "OPERATIONS",
    },
    "alerting": {
        "label": "◎  Alerting",
        "title": "Alerting & Notifications",
        "description": "Inspect notification readiness and delivery history.",
        "group": "OPERATIONS",
    },
    "simulator": {
        "label": "⚡  Simulator",
        "title": "Drift & Stress Simulator",
        "description": "Benchmark synthetic drift scenarios, stress-test pipeline, and replay events.",
        "group": "OPERATIONS",
    },
}

LABEL_TO_PAGE = {page["label"]: page_id for page_id, page in NAVIGATION.items()}

# Pages that already draw their own restyled header. The remaining pages
# get a shared header below until they are restyled too.
PAGES_WITH_OWN_HEADER = {"overview", "history", "diagnostics", "drift", "simulator"}


# =========================================================
# LOAD DASHBOARD DATA
# =========================================================

@st.cache_resource
def get_dashboard_client():
    return DashboardClient()


client = get_dashboard_client()


def _safe_client_call(method_name, fallback):
    """Call an optional backend method without breaking the dashboard."""
    try:
        method = getattr(client, method_name)
        result = method()
        return fallback if result is None else result
    except Exception:
        return fallback


backend_status = _safe_client_call(
    "get_backend_status",
    {"status": "unavailable", "mode": "unknown", "data_source": "Unavailable"},
)

try:
    all_runs = client.list_runs(limit=100) or []
except Exception:
    all_runs = []

try:
    streaming_health = client.get_streaming_health() or {}
except Exception:
    streaming_health = {"summary": {}, "reconciliation": {}}

try:
    alert_status = client.get_alert_status() or {}
except Exception:
    alert_status = {}

try:
    recent_alerts = client.list_alerts(limit=50) or []
except Exception:
    recent_alerts = []

if not isinstance(all_runs, list):
    all_runs = []

latest_run = all_runs[0] if all_runs else None


# =========================================================
# TOP COMMAND BAR
# =========================================================

with st.container(key="mp_topbar"):
    brand_col, nav_col, action_col = st.columns([1.35, 6.2, 1.1], gap="small")

    with brand_col:
        st.markdown(
            """
            <div class="mp-brand-inline">
                <div class="mp-brand-mark">
                    <svg width="20" height="20" viewBox="0 0 28 24" fill="none"
                         stroke="#3d9bff" stroke-width="2.2"
                         stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="1,12 7,12 10,3 14,21 18,8 21,12 27,12"/>
                    </svg>
                </div>
                <div>
                    <div class="mp-brand-name">ModelPulse</div>
                    <div class="mp-brand-subtitle">MLOps Observability</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with nav_col:
        selected_label = st.radio(
            "Application navigation",
            options=[page["label"] for page in NAVIGATION.values()],
            index=0,
            horizontal=True,
            label_visibility="collapsed",
            key="modelpulse_navigation",
        )

    with action_col:
        if st.button("↻  Refresh", use_container_width=True, key="refresh_dashboard"):
            st.cache_data.clear()
            st.rerun()

# Always route using stable page IDs, not display labels.
selected_page = LABEL_TO_PAGE[selected_label]
page = NAVIGATION[selected_page]


# =========================================================
# CONTEXT STRIP
# =========================================================

connection_mode = str(backend_status.get("mode", "unknown"))
connection_status = str(backend_status.get("status", "unknown"))
data_source = str(backend_status.get("data_source", "Not reported"))

api_connected = (
    connection_mode.lower() == "api" and connection_status.lower() == "connected"
)
conn_color = "#34d399" if api_connected else "#fbbf24"
conn_label = "API CONNECTED" if api_connected else "LOCAL / FALLBACK"

st.markdown(
    f"""
    <div class="mp-ctx">
        <div class="mp-crumbs">
            ModelPulse <i>/</i> {escape(page["group"])} <i>/</i>
            <b>{escape(page["title"])}</b>
        </div>
        <div class="mp-ctx-items">
            <span class="mp-ctx-item">
                <span class="mp-live-dot" style="--live-color:{conn_color}"></span>
                <strong style="color:{conn_color}">{conn_label}</strong>
            </span>
            <span class="mp-ctx-item">Source <strong>{escape(data_source)}</strong></span>
            <span class="mp-ctx-item">Runs <strong>{len(all_runs)}</strong></span>
            <span class="mp-ctx-item">Rendered
                <strong>{datetime.now().strftime("%H:%M:%S")}</strong></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Shared header only for pages that do not draw their own yet.
if selected_page not in PAGES_WITH_OWN_HEADER:
    st.markdown(
        f"""
        <div class="mp-page-header" style="margin-bottom:14px">
            <div>
                <div class="mp-eyebrow">ModelPulse / {escape(page["group"])}</div>
                <div class="mp-page-title">{escape(page["title"])}</div>
                <p class="mp-page-sub">{escape(page["description"])}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# PAGE ROUTING
# =========================================================

if selected_page == "overview":
    render_overview(
        backend_status=backend_status,
        latest_run=latest_run,
        all_runs=all_runs,
        streaming_health=streaming_health,
    )


elif selected_page == "history":
    # A selected run can be passed between history and diagnostics.
    selected_run_id = st.session_state.get("selected_run_id")

    try:
        returned_run_id = render_history(
            all_runs=all_runs,
            selected_run_id=selected_run_id,
        )

        if returned_run_id:
            st.session_state["selected_run_id"] = returned_run_id

    except TypeError as exc:
        st.error(
            "The Run History component has an incompatible function "
            "signature. Update render_history() to accept "
            "all_runs and selected_run_id."
        )
        st.caption(str(exc))


elif selected_page == "diagnostics":
    selected_run_id = st.session_state.get("selected_run_id")

    if not selected_run_id and latest_run:
        selected_run_id = latest_run.get("run_id")

    selected_run = None

    if selected_run_id:
        try:
            selected_run = client.get_run(selected_run_id)
        except Exception as exc:
            st.warning("The selected run could not be retrieved from the backend.")
            st.caption(str(exc))

    # Fall back to the already-loaded history if supported by the data.
    if selected_run is None and selected_run_id:
        selected_run = next(
            (
                run for run in all_runs
                if str(run.get("run_id")) == str(selected_run_id)
            ),
            None,
        )

    if selected_run:
        render_run_details(selected_run)
    else:
        st.info(
            "No monitoring run is available to inspect. "
            "Open History and select a run, or execute a monitoring run."
        )


elif selected_page == "drift":
    render_drift_analysis(all_runs)


elif selected_page == "severity":
    render_severity_breakdown(all_runs)


elif selected_page == "telemetry":
    render_streaming_health(streaming_health, client=client)


elif selected_page == "alerting":
    render_alerting_view(
        alert_status=alert_status,
        recent_alerts=recent_alerts,
    )


elif selected_page == "simulator":
    render_simulator_view(client.api_base_url)


# =========================================================
# FOOTER
# =========================================================

st.markdown(
    """
    <div class="mp-footer">
        <span><strong>ModelPulse</strong> · MLOps Monitoring &amp; Data Drift Analysis</span>
        <span>Values reflect data returned by the configured backend or fallback.
        A connected interface alone does not establish production deployment.</span>
    </div>
    """,
    unsafe_allow_html=True,
)