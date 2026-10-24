"""ModelPulse — Overview (industrial command center)."""

from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

from dashboard.components.styles import status_badge  # noqa: F401  (kept for compatibility)


# =========================================================
# TOKENS
# =========================================================

ACCENT = "#3d9bff"
OK = "#34d399"
WARN = "#fbbf24"
HIGH = "#fb923c"
CRIT = "#f87171"

BAND_COLORS = {"low": OK, "moderate": WARN, "high": HIGH, "critical": CRIT}


# =========================================================
# HELPERS
# =========================================================

def _safe(value: Any, default: str = "—") -> str:
    if value is None or value == "":
        value = default
    return escape(str(value))


def _score(run: Dict[str, Any]) -> float:
    severity = run.get("severity") or {}
    if not isinstance(severity, dict):
        return 0.0
    try:
        return max(0.0, min(100.0, float(severity.get("score", 0) or 0)))
    except (TypeError, ValueError):
        return 0.0


def _severity_band(run: Dict[str, Any]) -> str:
    severity = run.get("severity") or {}
    if not isinstance(severity, dict):
        return "Unknown"
    return str(severity.get("band", "Unknown"))


def _format_time(value: Any) -> str:
    if not value:
        return "Timestamp unavailable"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%d %b %Y · %H:%M")
    except (ValueError, TypeError):
        return str(value)


def _is_simulated(run: Dict[str, Any]) -> bool:
    return (
        str(run.get("run_type", "")).lower() == "simulator"
        or "scenario" in str(run.get("run_id", "")).lower()
    )


def _number(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return default


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def _section(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="mp-section-heading">{_safe(title)}</div>
        <div class="mp-section-caption">{_safe(subtitle)}</div>
        """,
        unsafe_allow_html=True,
    )


def _panel_header(title: str, description: str, chip: str) -> None:
    st.markdown(
        f"""
        <div class="mp-panel-heading-row">
            <div>
                <div class="mp-panel-title">{_safe(title)}</div>
                <div class="mp-panel-description">{_safe(description)}</div>
            </div>
            <span class="mp-panel-chip">{_safe(chip)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _kpi_card(title: str, value: str, subtitle: str, accent: str, icon: str,
              fill: float = 100.0) -> str:
    fill = max(4.0, min(100.0, fill))
    return f"""
    <div class="mp-kpi mp-kpi-modern"
         style="--kpi-accent:{accent};--kpi-soft:{_hex_to_rgba(accent, .14)}">
        <div class="mp-kpi-top">
            <span class="mp-kpi-label">{_safe(title)}</span>
            <span class="mp-kpi-icon">{icon}</span>
        </div>
        <div class="mp-kpi-value">{_safe(value)}</div>
        <div class="mp-kpi-subtitle">{_safe(subtitle)}</div>
        <div class="mp-kpi-track">
            <div class="mp-kpi-track-fill"
                 style="width:{fill:.0f}%;background:{accent}"></div>
        </div>
    </div>
    """


# =========================================================
# SEVERITY CHART
# =========================================================

def _render_severity_chart(runs: List[Dict[str, Any]]) -> None:
    if not runs:
        st.markdown(
            """
            <div class="mp-empty-state">
                <div class="mp-empty-icon">⌁</div>
                <div class="mp-empty-title">Waiting for monitoring data</div>
                <div class="mp-empty-copy">
                    Run an evaluation to start building your severity history.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # Runs are assumed to arrive newest first.
    chronological = list(reversed(runs[:30]))

    rows = []
    for index, run in enumerate(chronological, start=1):
        rows.append({
            "Run": f"Run {index}",
            "Run ID": str(run.get("run_id", "Unknown")),
            "Severity": _score(run),
            "Band": _severity_band(run),
            "Drift": "Detected" if run.get("dataset_drift_detected", False) else "Not detected",
            "Timestamp": _format_time(run.get("created_at")),
        })

    df = pd.DataFrame(rows)

    if not PLOTLY_AVAILABLE:
        st.line_chart(df.set_index("Run")[["Severity"]], height=280)
        st.caption("Install Plotly for interactive chart details.")
        return

    fig = go.Figure()

    # Severity bands as subtle horizontal zones
    for y0, y1, color in [
        (0, 25, OK), (25, 50, WARN), (50, 75, HIGH), (75, 100, CRIT),
    ]:
        fig.add_hrect(y0=y0, y1=y1, fillcolor=_hex_to_rgba(color, .04),
                      line_width=0, layer="below")

    fig.add_trace(
        go.Scatter(
            x=df["Run"],
            y=df["Severity"],
            mode="lines+markers",
            name="Severity score",
            customdata=df[["Run ID", "Band", "Drift", "Timestamp"]].values,
            line=dict(color=ACCENT, width=2, shape="linear"),
            marker=dict(
                size=7,
                color=[BAND_COLORS.get(b.lower(), ACCENT) for b in df["Band"]],
                line=dict(color="#0b0e13", width=1.5),
            ),
            fill="tozeroy",
            fillcolor="rgba(61,155,255,0.08)",
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Severity: %{y:.1f}/100<br>"
                "Band: %{customdata[1]}<br>"
                "Drift: %{customdata[2]}<br>"
                "Run: %{customdata[0]}<br>"
                "%{customdata[3]}<extra></extra>"
            ),
        )
    )

    for level, color in [(25, OK), (50, WARN), (75, CRIT)]:
        fig.add_hline(y=level, line_dash="dot",
                      line_color=_hex_to_rgba(color, .45), line_width=1)

    fig.update_layout(
        height=300,
        margin=dict(l=4, r=8, t=8, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans, sans-serif", color="#9aa7bb", size=11),
        showlegend=False,
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#161d28", bordercolor=ACCENT,
                        font=dict(color="#e6ebf3", family="IBM Plex Mono")),
        xaxis=dict(showgrid=False, zeroline=False, title=None,
                   linecolor="#263041", tickfont=dict(size=10)),
        yaxis=dict(title=None, range=[0, 100], dtick=25,
                   gridcolor="rgba(54,66,87,0.45)", zeroline=False,
                   tickfont=dict(family="IBM Plex Mono", size=10)),
        transition=dict(duration=400, easing="cubic-in-out"),
    )

    st.plotly_chart(
        fig,
        width="stretch",
        config={"displayModeBar": False, "displaylogo": False, "responsive": True},
    )

    st.caption(
        "Historical severity scores · Reference levels at 25, 50 and 75 · "
        "Retrieved runs, not live per-second telemetry."
    )


# =========================================================
# LATEST RUN PANEL
# =========================================================

def _render_latest_run(run: Optional[Dict[str, Any]]) -> None:
    if not run:
        st.info("No evaluation has been recorded yet.")
        return

    band = _severity_band(run)
    score = _score(run)
    color = BAND_COLORS.get(band.lower(), ACCENT)
    status = str(run.get("status", "unknown")).upper()

    st.markdown(
        f"""
        <div class="mp-modern-panel" style="--panel-accent:{color}">
            <div class="mp-panel-topline"></div>
            <div class="mp-panel-label">LATEST EVALUATION</div>
            <div class="mp-latest-model">{_safe(run.get('model_name', 'Unknown'))}</div>
            <div class="mp-latest-id">{_safe(run.get('run_id', 'Unknown'))}</div>

            <div class="mp-score-row">
                <div class="mp-score-number" style="color:{color}">
                    {score:g}<span> / 100</span>
                </div>
                <span class="mp-band-pill"
                      style="color:{color};border-color:{color};
                             background:{_hex_to_rgba(color, .10)}">
                    {_safe(band.upper())}
                </span>
            </div>
            <div class="mp-score-track">
                <div class="mp-kpi-track-fill"
                     style="width:{max(score, 3):.0f}%;background:{color}"></div>
            </div>

            <div class="mp-latest-facts">
                <div class="mp-fact"><span>Version</span>
                    <strong>{_safe(run.get('model_version', 'Unknown'))}</strong></div>
                <div class="mp-fact"><span>Run type</span>
                    <strong>{_safe(run.get('run_type', 'Unknown'))}</strong></div>
                <div class="mp-fact"><span>Status</span>
                    <strong>{_safe(status)}</strong></div>
                <div class="mp-fact"><span>Created</span>
                    <strong>{_safe(_format_time(run.get('created_at')))}</strong></div>
            </div>
            {"<div class='mp-muted-text'>Simulator-generated test run; not verified production data.</div>"
             if _is_simulated(run) else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# MAIN OVERVIEW
# =========================================================

def render_overview(
    backend_status: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
    all_runs: List[Dict[str, Any]],
    streaming_health: Dict[str, Any],
) -> None:
    """Render the ModelPulse overview using retrieved backend data."""

    backend_status = backend_status or {}
    all_runs = all_runs or []
    streaming_health = streaming_health or {}

    api_mode = backend_status.get("mode") == "api"
    connection_label = "API CONNECTED" if api_mode else "LOCAL / FALLBACK"
    connection_color = OK if api_mode else WARN

    # ---------------- HERO ----------------
    hero_col, status_col = st.columns([1.8, 1], gap="medium")

    with hero_col:
        st.markdown(
            """
            <div class="mp-page-header">
                <div>
                    <div class="mp-eyebrow">
                        <span class="mp-hero-dot"></span>
                        MODELPULSE / OBSERVABILITY PLATFORM
                    </div>
                    <h1 class="mp-hero-title">Model Intelligence</h1>
                    <p class="mp-hero-subtitle">
                        Monitor model health, identify data drift and investigate
                        changes in model behavior from one workspace.
                    </p>
                    <div class="mp-hero-tags">
                        <span>DRIFT DETECTION</span>
                        <span>SEVERITY ANALYSIS</span>
                        <span>MODEL MONITORING</span>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with status_col:
        latest_time = (
            _format_time(latest_run.get("created_at")) if latest_run else "Not available"
        )
        st.markdown(
            f"""
            <div class="mp-system-card">
                <div class="mp-system-card-header">
                    <span class="mp-panel-label">SYSTEM OVERVIEW</span>
                    <span class="mp-system-symbol">⌘</span>
                </div>
                <div class="mp-system-status">
                    <span class="mp-live-dot" style="--live-color:{connection_color}"></span>
                    <span style="color:{connection_color}">{_safe(connection_label)}</span>
                </div>
                <div class="mp-system-row"><span>Persistence</span>
                    <strong>{_safe(backend_status.get('persistence_type', 'Not reported'))}</strong></div>
                <div class="mp-system-row"><span>Retrieved runs</span>
                    <strong>{len(all_runs):,}</strong></div>
                <div class="mp-system-row"><span>Latest evaluation</span>
                    <strong>{_safe(latest_time)}</strong></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if latest_run and _is_simulated(latest_run):
        st.info(
            "SIMULATED DATA — The latest evaluation uses a controlled "
            "scenario. This demonstrates monitoring behavior, not verified "
            "production performance."
        )

    if backend_status.get("freshness_warning"):
        st.warning("Storage and provenance warning: " + str(backend_status["freshness_warning"]))

    if backend_status.get("api_error") and not api_mode:
        with st.expander("Connection diagnostics"):
            st.write(
                backend_status.get(
                    "failure_reason",
                    "The backend API is not currently connected.",
                )
            )
            st.code(str(backend_status["api_error"]))

    # ---------------- KPI GRID ----------------
    _section("Monitoring at a glance", "Summary of the latest recorded evaluation.")

    if latest_run:
        severity_score = _score(latest_run)
        band = _severity_band(latest_run)
        drift = bool(latest_run.get("dataset_drift_detected", False))
        quarantined = _number(latest_run.get("quarantined_sample_count"))
        samples = _number(latest_run.get("sample_count"))
        quarantine_pct = quarantined / samples * 100 if samples else 0.0
        severity_color = BAND_COLORS.get(band.lower(), ACCENT)

        cards = [
            _kpi_card("Severity score", f"{severity_score:g}/100",
                      f"{band} severity band", severity_color, "◉", severity_score),
            _kpi_card("Dataset drift", "Detected" if drift else "Not detected",
                      "Latest recorded evaluation",
                      CRIT if drift else OK, "⌁", 100),
            _kpi_card("Quarantined samples", f"{quarantined:,}",
                      f"{quarantine_pct:.1f}% of latest-run samples",
                      WARN, "◇", quarantine_pct),
            _kpi_card("Monitoring runs", f"{len(all_runs):,}",
                      "Retrieved from monitoring history", ACCENT, "▤", 100),
        ]
    else:
        cards = [
            _kpi_card("Severity score", "N/A", "No run recorded", ACCENT, "◉", 4),
            _kpi_card("Dataset drift", "Unknown", "No run recorded", ACCENT, "⌁", 4),
            _kpi_card("Quarantined samples", "N/A", "No run recorded", WARN, "◇", 4),
            _kpi_card("Monitoring runs", f"{len(all_runs):,}",
                      "Retrieved from monitoring history", ACCENT, "▤", 100),
        ]

    for column, card in zip(st.columns(4, gap="small"), cards):
        with column:
            st.markdown(card, unsafe_allow_html=True)

    st.markdown("<div class='mp-section-spacer'></div>", unsafe_allow_html=True)

    # ---------------- ANALYTICS GRID ----------------
    chart_col, latest_col = st.columns([1.5, 1], gap="medium")

    with chart_col:
        # A bordered container actually wraps the chart widget,
        # unlike an HTML <div> opened/closed across separate st.markdown calls.
        with st.container(border=True):
            _panel_header(
                "Severity trend",
                "Historical risk score across retrieved runs",
                "HISTORY",
            )
            _render_severity_chart(all_runs)

    with latest_col:
        _render_latest_run(latest_run)