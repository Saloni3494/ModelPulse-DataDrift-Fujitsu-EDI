"""
ModelPulse — Monitoring History.

Features:
- Summary strip, search and filtering
- Pagination
- Run selection compatible with app.py
- Simulator-generated record disclosure

Styling lives in dashboard.components.styles (CUSTOM_CSS).
"""

from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from dashboard.components.styles import render_df


# =========================================================
# HELPERS
# =========================================================

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError, OverflowError):
        return default


def _severity(run: Dict[str, Any]) -> Dict[str, Any]:
    value = run.get("severity", {})
    return value if isinstance(value, dict) else {}


def _is_simulated(run: Dict[str, Any]) -> bool:
    run_type = str(run.get("run_type", "")).lower()
    run_id = str(run.get("run_id", "")).lower()
    return run_type == "simulator" or "scenario" in run_id


def _format_created(value: Any) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%d %b %Y · %H:%M")
    except (ValueError, TypeError):
        return str(value)


def _matches_search(run: Dict[str, Any], query: str) -> bool:
    if not query:
        return True
    searchable = [
        run.get("run_id", ""),
        run.get("model_name", ""),
        run.get("model_version", ""),
        run.get("run_type", ""),
        run.get("status", ""),
    ]
    text = " ".join(str(item) for item in searchable)
    return query.casefold() in text.casefold()


def _stat_card(label: str, value: str, note: str, color_class: str) -> None:
    # Escape dynamic text because this content is rendered as HTML.
    st.markdown(
        f"""
        <div class="history-stat {color_class}">
            <div class="history-stat-label">{escape(label)}</div>
            <div class="history-stat-value">{escape(value)}</div>
            <div class="history-stat-note">{escape(note)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _section(label: str, title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="mp-panel-label" style="margin-top:6px">{escape(label)}</div>
        <div class="mp-section-heading" style="margin-top:2px">{escape(title)}</div>
        <div class="mp-section-caption">{escape(description)}</div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# MAIN HISTORY PAGE
# =========================================================

def render_history(
    all_runs: List[Dict[str, Any]],
    selected_run_id: Optional[str] = None,
    on_select_run_callback=None,
) -> Optional[str]:
    """
    Render monitoring history.

    Returns the selected run ID, or None when no run is available.
    Compatible with app.py calling render_history(
        all_runs=all_runs,
        selected_run_id=selected_run_id
    ).
    """

    runs = [run for run in (all_runs or []) if isinstance(run, dict)]

    # -----------------------------------------------------
    # Page header
    # -----------------------------------------------------
    st.markdown(
        """
        <div class="mp-page-header">
            <div>
                <div class="mp-eyebrow">ModelPulse / Observability</div>
                <div class="mp-page-title">Monitoring History</div>
                <p class="mp-page-sub">
                    Explore previous evaluations, compare severity levels and
                    inspect the evidence behind monitoring runs.
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not runs:
        st.info(
            "No monitoring runs have been recorded yet. "
            "Execute a monitoring run to start building your history."
        )
        return None

    # -----------------------------------------------------
    # Summary strip
    # -----------------------------------------------------
    scores = [_safe_int(_severity(r).get("score")) for r in runs]
    drift_count = sum(1 for r in runs if r.get("dataset_drift_detected", False))
    risky = sum(
        1 for r in runs
        if str(_severity(r).get("band", "")).lower() in ("high", "critical")
    )
    avg_score = sum(scores) / len(scores) if scores else 0.0
    sim_count = sum(1 for r in runs if _is_simulated(r))

    s1, s2, s3, s4 = st.columns(4, gap="small")
    with s1:
        _stat_card("Total runs", f"{len(runs):,}",
                   f"{sim_count} simulator-generated", "blue")
    with s2:
        _stat_card("Drift detected", f"{drift_count:,}",
                   f"{drift_count / len(runs) * 100:.0f}% of runs", "orange")
    with s3:
        _stat_card("High / critical", f"{risky:,}",
                   "Runs needing attention", "purple")
    with s4:
        _stat_card("Avg severity", f"{avg_score:.1f}",
                   "Mean score across runs", "green")

    # -----------------------------------------------------
    # Filters
    # -----------------------------------------------------
    _section("Explore records", "Run Explorer",
             "Search, filter, paginate, and select a run to inspect diagnostics.")

    with st.container(border=True):
        f1, f2, f3 = st.columns([1.8, 1, 1], gap="small")

        with f1:
            search_query = st.text_input(
                "Search records",
                placeholder="Run ID, model name, status…",
                key="history_search",
                help="Search run ID, model name, version, type, and status.",
            )

        with f2:
            severity_filter = st.selectbox(
                "Severity band",
                ["All", "Low", "Moderate", "High", "Critical"],
                key="history_severity_filter",
            )

        statuses = sorted({str(r.get("status") or "unknown").lower() for r in runs})
        run_types = sorted({str(r.get("run_type") or "manual").lower() for r in runs})

        with f3:
            status_filter = st.selectbox(
                "Run status", ["All"] + statuses, key="history_status_filter",
            )

        f4, f5, f6, f7 = st.columns([1, 1, 1, 0.8], gap="small")

        with f4:
            type_filter = st.selectbox(
                "Execution type", ["All"] + run_types, key="history_type_filter",
            )

        with f5:
            drift_filter = st.selectbox(
                "Drift status",
                ["All", "Drift detected", "No drift"],
                key="history_drift_filter",
            )

        with f6:
            page_size = st.selectbox(
                "Rows per page", [5, 10, 20, 50], index=1, key="history_page_size",
            )

        with f7:
            st.markdown("<div style='height:26px'></div>", unsafe_allow_html=True)
            if st.button("↺ Reset", key="history_reset_filters",
                         use_container_width=True):
                for key in (
                    "history_search",
                    "history_severity_filter",
                    "history_status_filter",
                    "history_type_filter",
                    "history_drift_filter",
                    "history_page_size",
                    "history_page_number",
                ):
                    st.session_state.pop(key, None)
                # Do not remove the currently selected run here.
                st.rerun()

    # -----------------------------------------------------
    # Apply filters
    # -----------------------------------------------------
    filtered: List[Dict[str, Any]] = []

    for run in runs:
        band = str(_severity(run).get("band") or "Low")
        status = str(run.get("status") or "unknown").lower()
        run_type = str(run.get("run_type") or "manual").lower()
        drift = bool(run.get("dataset_drift_detected", False))

        if not _matches_search(run, search_query.strip()):
            continue
        if severity_filter != "All" and band.lower() != severity_filter.lower():
            continue
        if status_filter != "All" and status != status_filter:
            continue
        if type_filter != "All" and run_type != type_filter:
            continue
        if drift_filter == "Drift detected" and not drift:
            continue
        if drift_filter == "No drift" and drift:
            continue

        filtered.append(run)

    total_filtered = len(filtered)

    # -----------------------------------------------------
    # Pagination
    # -----------------------------------------------------
    total_pages = max(1, (total_filtered + page_size - 1) // page_size)

    # IMPORTANT: Define the key before every use.
    selected_key = "history_selected_run"

    available_ids = [
        str(run.get("run_id")) for run in filtered if run.get("run_id") is not None
    ]

    # Synchronize a run ID supplied by app.py with the local selection.
    if selected_run_id is not None and str(selected_run_id) in available_ids:
        st.session_state[selected_key] = str(selected_run_id)

    if (
        selected_key not in st.session_state
        or st.session_state[selected_key] not in available_ids
    ):
        st.session_state[selected_key] = available_ids[0] if available_ids else None

    if "history_page_number" not in st.session_state:
        st.session_state["history_page_number"] = 1

    page_number = min(
        max(1, _safe_int(st.session_state.get("history_page_number"), 1)),
        total_pages,
    )
    st.session_state["history_page_number"] = page_number

    start = (page_number - 1) * page_size
    page_runs = filtered[start:start + page_size]

    st.caption(
        f"Showing {len(page_runs)} of {total_filtered} matching runs "
        f"from {len(runs)} retrieved records."
    )

    if not filtered:
        st.warning(
            "No monitoring runs match these filters. "
            "Try a broader search or reset the filters."
        )
        return None

    # -----------------------------------------------------
    # Results table
    # -----------------------------------------------------
    table_rows = []
    for run in page_runs:
        severity = _severity(run)
        table_rows.append({
            "Run ID": str(run.get("run_id") or "—"),
            "Created": _format_created(run.get("created_at")),
            "Model": str(run.get("model_name") or "—"),
            "Type": str(run.get("run_type") or "—"),
            "Score": _safe_int(severity.get("score")),
            "Severity": str(severity.get("band") or "Unknown"),
            "Drift": "Detected" if run.get("dataset_drift_detected", False) else "Not detected",
            "Samples": run.get("sample_count") if run.get("sample_count") is not None else "—",
            "Valid": run.get("valid_sample_count") if run.get("valid_sample_count") is not None else "—",
            "Quarantined": (
                run.get("quarantined_sample_count")
                if run.get("quarantined_sample_count") is not None else "—"
            ),
            "Status": str(run.get("status") or "unknown"),
            "Data origin": "Simulator" if _is_simulated(run) else "Other",
        })

    df = pd.DataFrame(table_rows)
    render_df(df, hide_index=True, height=min(420, 40 + 35 * len(df)))

    # -----------------------------------------------------
    # Pagination controls
    # -----------------------------------------------------
    if total_pages > 1:
        prev_col, page_col, next_col = st.columns([1, 2, 1], gap="small")

        with prev_col:
            if st.button("← Previous", disabled=page_number <= 1,
                         use_container_width=True, key="history_prev_page"):
                st.session_state["history_page_number"] = page_number - 1
                st.rerun()

        with page_col:
            st.markdown(
                f"""
                <div style="text-align:center;padding:8px 0;color:var(--mp-text-dim);
                            font-family:var(--mp-mono);font-size:12px;">
                    PAGE <strong style="color:var(--mp-text)">{page_number}</strong>
                    / {total_pages}
                </div>
                """,
                unsafe_allow_html=True,
            )

        with next_col:
            if st.button("Next →", disabled=page_number >= total_pages,
                         use_container_width=True, key="history_next_page"):
                st.session_state["history_page_number"] = page_number + 1
                st.rerun()

    # -----------------------------------------------------
    # Select a run
    # -----------------------------------------------------
    st.divider()

    _section("Diagnostics", "Inspect a monitoring run",
             "Choose a record to open its diagnostics page.")

    run_ids = available_ids

    if not run_ids:
        st.warning("The matching records do not contain selectable run IDs.")
        return None

    if st.session_state.get(selected_key) not in run_ids:
        st.session_state[selected_key] = run_ids[0]

    with st.container(border=True):
        selected_id = st.selectbox(
            "Select run ID",
            options=run_ids,
            key=selected_key,
            help="Choose a monitoring run to inspect in Run Diagnostics.",
        )

        # Optional callback for apps that navigate directly on button click.
        if on_select_run_callback is not None:
            if st.button(
                "Open selected run →",
                type="primary",
                use_container_width=True,
                key="history_open_selected_run",
            ):
                on_select_run_callback(selected_id)

    return selected_id