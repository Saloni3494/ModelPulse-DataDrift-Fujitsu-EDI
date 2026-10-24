"""ModelPulse — Statistical Drift Analysis.

Displays recorded drift decisions, cross-run trends, feature contributors,
and statistical-test results. Backend decisions are never recalculated here.
"""

from html import escape
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

from dashboard.components.styles import render_df

ACCENT = "#3d9bff"
OK = "#34d399"
WARN = "#fbbf24"
HIGH = "#fb923c"
CRIT = "#f87171"


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _number(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return default
        result = float(value)
        return result if pd.notna(result) and result not in (float("inf"), float("-inf")) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _text(value: Any, default: str = "—") -> str:
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default


def _display(value: Any, digits: int = 3) -> str:
    value = _number(value)
    return f"{value:,.{digits}f}" if value is not None else "N/A"


def _drift_label(detected: bool) -> str:
    return "Detected" if detected else "Not detected"


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def _kpi(label: str, value: str, sub: str, accent: str, icon: str,
         fill: float = 100.0) -> str:
    fill = max(4.0, min(100.0, fill))
    return f"""
    <div class="mp-kpi-modern"
         style="--kpi-accent:{accent};--kpi-soft:{_rgba(accent, .14)}">
        <div class="mp-kpi-top">
            <span class="mp-kpi-label">{escape(label)}</span>
            <span class="mp-kpi-icon">{icon}</span>
        </div>
        <div class="mp-kpi-value">{escape(value)}</div>
        <div class="mp-kpi-subtitle">{escape(sub)}</div>
        <div class="mp-kpi-track">
            <div class="mp-kpi-track-fill" style="width:{fill:.0f}%;background:{accent}"></div>
        </div>
    </div>
    """


def _pill(text: str, color: str) -> str:
    return (
        f'<span class="mp-pill" style="color:{color};border-color:{color};'
        f'background:{_rgba(color, .10)}">{escape(text.upper())}</span>'
    )


def _section(title: str, caption: str = "") -> None:
    cap = f'<div class="mp-section-caption">{escape(caption)}</div>' if caption else ""
    st.markdown(
        f'<div class="mp-section-heading">{escape(title)}</div>{cap}',
        unsafe_allow_html=True,
    )


def _style_fig(fig, height: int = 250) -> None:
    fig.update_layout(
        height=height,
        margin=dict(l=4, r=8, t=8, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans, sans-serif", color="#9aa7bb", size=11),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#161d28", bordercolor=ACCENT,
                        font=dict(color="#e6ebf3", family="IBM Plex Mono")),
        xaxis=dict(showgrid=False, zeroline=False, title=None, linecolor="#263041"),
        yaxis=dict(gridcolor="rgba(54,66,87,0.45)", zeroline=False, title=None,
                   tickfont=dict(family="IBM Plex Mono", size=10)),
        legend=dict(orientation="h", y=1.12, x=0, font=dict(size=10)),
        transition=dict(duration=400, easing="cubic-in-out"),
    )


def _plot(fig) -> None:
    st.plotly_chart(
        fig, width="stretch",
        config={"displayModeBar": False, "displaylogo": False, "responsive": True},
    )


# ---------------------------------------------------------------------------
# Selected-run analysis
# ---------------------------------------------------------------------------

def _render_selected_run(run: Optional[Dict[str, Any]]) -> None:
    """Render recorded drift evidence for one monitoring run."""

    if not isinstance(run, dict) or not run:
        st.info("Select a monitoring run to inspect its drift evidence.")
        return

    prediction = _as_dict(run.get("prediction_drift"))
    severity = _as_dict(run.get("severity"))
    features = _as_list(run.get("features"))
    contributors = _as_list(run.get("top_contributors"))

    run_id = _text(run.get("run_id"), "Unknown run")
    model_name = _text(run.get("model_name"), "Unknown model")
    model_version = _text(run.get("model_version"))
    run_type = _text(run.get("run_type"), "Unknown").replace("_", " ").title()

    dataset_drift = bool(run.get("dataset_drift_detected", False))
    prediction_drift = bool(prediction.get("drifted", False))
    psi_value = _number(prediction.get("value"))
    psi_threshold = _number(prediction.get("threshold"))
    severity_score = _number(severity.get("score"))
    drifted_features = sum(bool(f.get("drift_detected")) for f in features)

    # Selected-run header
    st.markdown(
        f"""
        <div class="mp-system-card" style="padding:12px 16px">
            <div class="mp-panel-label">SELECTED RUN</div>
            <div class="mp-latest-model">{escape(model_name)}
                <span style="color:var(--mp-text-faint);font-weight:400">
                    · v{escape(model_version)} · {escape(run_type)}</span></div>
            <div class="mp-latest-id">{escape(run_id)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if run_type.casefold() == "simulator":
        st.info(
            "Simulator result: this is a controlled demonstration, "
            "not verified production performance."
        )

    # ---- Summary ----
    _section("Drift summary", "Recorded decisions for the selected run.")

    feat_pct = drifted_features / len(features) * 100 if features else 0
    sev_band = _text(severity.get("band"), "Unknown")
    sev_color = {"low": OK, "moderate": WARN, "high": HIGH, "critical": CRIT}.get(
        sev_band.lower(), ACCENT)

    cards = [
        _kpi("Dataset drift", _drift_label(dataset_drift),
             "Recorded decision", CRIT if dataset_drift else OK, "⌁"),
        _kpi("Prediction PSI", _display(psi_value),
             f"Threshold {_display(psi_threshold)}" if psi_threshold is not None
             else "Threshold unavailable",
             CRIT if prediction_drift else ACCENT, "◉",
             (psi_value / psi_threshold * 100) if psi_value and psi_threshold else 4),
        _kpi("Drifted features", f"{drifted_features}/{len(features)}",
             f"{feat_pct:.0f}% of evaluated features", WARN, "◇", feat_pct),
        _kpi("Severity",
             f"{severity_score:.0f}/100" if severity_score is not None else "N/A",
             f"Band: {sev_band}", sev_color, "▤", severity_score or 4),
    ]
    for column, card in zip(st.columns(4, gap="small"), cards):
        with column:
            st.markdown(card, unsafe_allow_html=True)

    st.caption(
        "Dataset drift and prediction drift are separate recorded decisions. "
        "A distribution shift alone does not establish reduced model accuracy."
    )

    # ---- Prediction distribution ----
    _section("Prediction distribution", "PSI evidence against its configured threshold.")

    pred_color = CRIT if prediction_drift else OK
    ratio = (psi_value / psi_threshold * 100) if psi_value is not None and psi_threshold else 0
    left, right = st.columns(2, gap="small")

    with left:
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{pred_color}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-label">PREDICTION DRIFT DECISION</div>
                <div style="margin-top:10px">{_pill(_drift_label(prediction_drift), pred_color)}</div>
                <div class="mp-system-row" style="margin-top:10px"><span>Metric</span>
                    <strong>{escape(_text(prediction.get('metric'), 'Unknown').upper())}</strong></div>
                <div class="mp-system-row"><span>Evaluation status</span>
                    <strong>{escape(_text(prediction.get('status'), 'Unavailable'))}</strong></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{ACCENT}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-label">RECORDED PSI EVIDENCE</div>
                <div class="mp-big-value">{_display(psi_value, 5)}
                    <small>observed</small></div>
                <div class="mp-score-track" style="margin:10px 0 6px">
                    <div class="mp-kpi-track-fill"
                         style="width:{max(2, min(100, ratio)):.0f}%;background:{pred_color}"></div>
                </div>
                <div class="mp-muted-text">Configured threshold:
                    <span style="font-family:var(--mp-mono);color:var(--mp-text)">
                    {_display(psi_threshold, 5)}</span></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ---- Contributors ----
    _section("Leading drift contributors")

    if contributors:
        contributor_rows = []
        for item in contributors:
            share = _number(item.get("cumulative_share"))
            contributor_rows.append({
                "Feature": _text(item.get("feature_name"), "Unknown"),
                "Contribution score": _number(item.get("contribution_score")),
                "Cumulative share": f"{share * 100:.1f}%" if share is not None else "—",
            })
        render_df(pd.DataFrame(contributor_rows), hide_index=True)
    else:
        st.info("No contributor records were returned for this run.")

    # ---- Feature-level results ----
    _section("Feature-level drift", "Filter evaluated features by name, status or type.")

    if not features:
        st.info("No feature-level drift results were returned.")
        return

    feature_rows = []
    for feature in features:
        unseen = feature.get("unseen_categories")
        if not isinstance(unseen, (list, tuple, set)):
            unseen = []

        votes = feature.get("votes")
        if isinstance(votes, dict):
            votes = str(votes)

        feature_rows.append({
            "Feature": _text(feature.get("feature_name"), "Unknown"),
            "Type": _text(feature.get("feature_type"), "Unknown"),
            "Drift status": _drift_label(bool(feature.get("drift_detected"))),
            "Ensemble votes": _text(votes),
            "Contribution": _number(feature.get("drift_contribution")),
            "Reference mean": _number(feature.get("reference_mean")),
            "Current mean": _number(feature.get("current_mean")),
            "Unseen categories": ", ".join(map(str, unseen)) or "—",
        })

    feature_df = pd.DataFrame(feature_rows)

    with st.container(border=True):
        filters = st.columns([1.6, 1, 1], gap="small")
        with filters[0]:
            search = st.text_input("Search feature", placeholder="Name or type",
                                   key="drift_feature_search")
        with filters[1]:
            status_filter = st.selectbox(
                "Drift status", ["All", "Detected", "Not detected"],
                key="drift_feature_status")
        with filters[2]:
            type_filter = st.selectbox(
                "Feature type",
                ["All"] + sorted(feature_df["Type"].unique().tolist()),
                key="drift_feature_type")

    visible = feature_df.copy()

    if search.strip():
        query = search.strip()
        mask = (
            visible["Feature"].str.contains(query, case=False, regex=False, na=False)
            | visible["Type"].str.contains(query, case=False, regex=False, na=False)
        )
        visible = visible[mask]
    if status_filter != "All":
        visible = visible[visible["Drift status"] == status_filter]
    if type_filter != "All":
        visible = visible[visible["Type"] == type_filter]

    st.caption(f"Showing {len(visible)} of {len(feature_df)} evaluated features.")

    if visible.empty:
        st.warning("No features match the selected filters.")
    else:
        render_df(
            visible,
            hide_index=True,
            column_config={
                "Contribution": st.column_config.NumberColumn(format="%.4f"),
                "Reference mean": st.column_config.NumberColumn(format="%.4f"),
                "Current mean": st.column_config.NumberColumn(format="%.4f"),
            },
        )

    # ---- Individual statistical tests ----
    _section("Statistical test details", "Per-feature tests as returned by the backend.")

    feature_options = [_text(f.get("feature_name"), "Unknown") for f in features]

    with st.container(border=True):
        selected_name = st.selectbox("Select feature", feature_options,
                                     key="drift_test_feature")

    selected_feature = next(
        (f for f in features if _text(f.get("feature_name"), "Unknown") == selected_name),
        None,
    )

    if not selected_feature:
        st.info("No statistical-test details are available for this feature.")
        return

    tests = _as_list(selected_feature.get("tests"))
    feature_drift = bool(selected_feature.get("drift_detected"))

    st.markdown(
        f"{_pill(_drift_label(feature_drift), CRIT if feature_drift else OK)} "
        f'<span class="mp-muted-text" style="margin-left:8px">'
        f"Tests returned: {len(tests)}</span>",
        unsafe_allow_html=True,
    )

    if not tests:
        st.info("The backend did not return individual statistical tests.")
        return

    test_rows = []
    for test in tests:
        test_rows.append({
            "Test": _text(test.get("metric"), "Unknown").upper(),
            "Observed statistic": _number(test.get("value")),
            "Threshold": _number(test.get("threshold")),
            "P-value": _number(test.get("p_value")),
            "Threshold exceeded": (
                "Yes" if test.get("exceeded") is True
                else "No" if test.get("exceeded") is False
                else "—"
            ),
            "Status": _text(test.get("status")),
            "Reference samples": _number(test.get("reference_count")),
            "Current samples": _number(test.get("current_count")),
        })

    render_df(
        pd.DataFrame(test_rows),
        hide_index=True,
        column_config={
            "Observed statistic": st.column_config.NumberColumn(format="%.5f"),
            "Threshold": st.column_config.NumberColumn(format="%.5f"),
            "P-value": st.column_config.NumberColumn(format="%.6g"),
        },
    )

    st.caption(
        "Statistics and decisions are displayed as returned by the backend; "
        "this page does not recompute drift."
    )


# ---------------------------------------------------------------------------
# Page entry point — compatible with app.py
# ---------------------------------------------------------------------------

def render_drift_analysis(all_runs: Any) -> None:
    """Render cross-run drift trends and selected-run analysis."""

    runs = _as_list(all_runs)

    st.markdown(
        """
        <div class="mp-page-header">
            <div>
                <div class="mp-eyebrow">ModelPulse / Statistical Analysis</div>
                <div class="mp-page-title">Statistical Drift Analysis</div>
                <p class="mp-page-sub">
                    Compare recorded monitoring decisions and inspect the evidence
                    behind feature and prediction drift.
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not runs:
        st.info("No monitoring runs are available. Create a run and refresh history.")
        return

    # Newest first; the original list is not mutated.
    runs = sorted(runs, key=lambda item: _text(item.get("created_at"), ""), reverse=True)

    def run_label(item: Dict[str, Any]) -> str:
        run_id = _text(item.get("run_id"), "Unknown run")
        model = _text(item.get("model_name"), "Unknown model")
        created = _text(item.get("created_at"), "Time unavailable")
        scenario = _text(item.get("scenario_id"), "")
        suffix = f" | {scenario}" if scenario else ""
        return f"{created} | {model} | {run_id[:12]}{suffix}"

    with st.container(border=True):
        selected_run = st.selectbox(
            "Monitoring run",
            options=runs,
            index=0,
            format_func=run_label,
            key="drift_analysis_selected_run",
        )

    # ---- Cross-run comparison ----
    _section("Cross-run trends", "Severity and drift decisions across all recorded runs.")

    comparison_rows = []
    for index, run in enumerate(reversed(runs), start=1):
        severity = _as_dict(run.get("severity"))
        prediction = _as_dict(run.get("prediction_drift"))

        comparison_rows.append({
            "Run": f"Run {index}",
            "Run ID": _text(run.get("run_id"), "Unknown"),
            "Created": _text(run.get("created_at"), "Unknown"),
            "Severity score": _number(severity.get("score")),
            "Dataset drift": _drift_label(bool(run.get("dataset_drift_detected", False))),
            "Prediction drift": _drift_label(bool(prediction.get("drifted", False))),
            "_dataset_flag": int(bool(run.get("dataset_drift_detected", False))),
            "_prediction_flag": int(bool(prediction.get("drifted", False))),
        })

    comparison_df = pd.DataFrame(comparison_rows)

    total_runs = len(runs)
    dataset_drift_count = sum(r["_dataset_flag"] for r in comparison_rows)
    prediction_drift_count = sum(r["_prediction_flag"] for r in comparison_rows)

    cards = [
        _kpi("Recorded runs", f"{total_runs:,}", "In monitoring history", ACCENT, "▤"),
        _kpi("Dataset drift detected", f"{dataset_drift_count:,}",
             f"{dataset_drift_count / total_runs * 100:.0f}% of runs", CRIT, "⌁",
             dataset_drift_count / total_runs * 100),
        _kpi("Prediction drift detected", f"{prediction_drift_count:,}",
             f"{prediction_drift_count / total_runs * 100:.0f}% of runs", WARN, "◇",
             prediction_drift_count / total_runs * 100),
    ]
    for column, card in zip(st.columns(3, gap="small"), cards):
        with column:
            st.markdown(card, unsafe_allow_html=True)

    chart_left, chart_right = st.columns(2, gap="small")

    severity_df = comparison_df.dropna(subset=["Severity score"])

    with chart_left:
        with st.container(border=True):
            st.markdown('<div class="mp-panel-title">Severity history</div>',
                        unsafe_allow_html=True)
            if severity_df.empty:
                st.info("No severity scores are available.")
            elif PLOTLY_AVAILABLE:
                fig = go.Figure(go.Scatter(
                    x=severity_df["Run"], y=severity_df["Severity score"],
                    mode="lines+markers", name="Severity",
                    line=dict(color=ACCENT, width=2),
                    marker=dict(size=6, color=ACCENT, line=dict(color="#0b0e13", width=1.5)),
                    fill="tozeroy", fillcolor="rgba(61,155,255,0.08)",
                    hovertemplate="Severity: %{y:.1f}/100<extra></extra>",
                ))
                _style_fig(fig)
                fig.update_yaxes(range=[0, 100], dtick=25)
                fig.update_layout(showlegend=False)
                _plot(fig)
            else:
                st.line_chart(severity_df.set_index("Run")[["Severity score"]], height=250)

    with chart_right:
        with st.container(border=True):
            st.markdown('<div class="mp-panel-title">Drift detections by run</div>',
                        unsafe_allow_html=True)
            if PLOTLY_AVAILABLE:
                fig = go.Figure()
                fig.add_bar(x=comparison_df["Run"], y=comparison_df["_dataset_flag"],
                            name="Dataset drift", marker_color=CRIT,
                            hovertemplate="Dataset: %{y}<extra></extra>")
                fig.add_bar(x=comparison_df["Run"], y=comparison_df["_prediction_flag"],
                            name="Prediction drift", marker_color=WARN,
                            hovertemplate="Prediction: %{y}<extra></extra>")
                _style_fig(fig)
                fig.update_layout(barmode="group", bargap=.35)
                fig.update_yaxes(range=[0, 1.1], dtick=1)
                _plot(fig)
            else:
                st.bar_chart(
                    comparison_df[["Run", "_dataset_flag", "_prediction_flag"]]
                    .rename(columns={"_dataset_flag": "Dataset drift",
                                     "_prediction_flag": "Prediction drift"})
                    .set_index("Run"),
                    height=250,
                )
            st.caption("1 means detected, 0 means not detected.")

    with st.expander("View cross-run records"):
        render_df(
            comparison_df[["Run ID", "Created", "Severity score",
                           "Dataset drift", "Prediction drift"]],
            hide_index=True,
        )

    st.divider()
    _render_selected_run(selected_run)