"""ModelPulse — compact diagnostics for a selected monitoring run."""

from datetime import datetime
from html import escape
from typing import Any, Dict, Optional

import streamlit as st

OK = "#34d399"
WARN = "#fbbf24"
CRIT = "#f87171"
ACCENT = "#3d9bff"


# =========================================================
# HELPERS
# =========================================================

def _safe_text(value: Any, fallback: str = "—") -> str:
    if value is None or str(value).strip() == "":
        return fallback
    return str(value)


def _safe_number(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _format_date(value: Any) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%d %b %Y, %H:%M")
    except (ValueError, TypeError):
        return str(value)


def _display_count(value: Any) -> str:
    number = _safe_number(value)
    return "—" if number is None else f"{number:,.0f}"


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def _status_color(status: str) -> str:
    s = status.lower()
    if s in ("ok", "healthy", "completed", "delivered"):
        return OK
    if s in ("failed", "error", "quality_failure"):
        return CRIT
    return WARN


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


def _section(title: str, caption: str = "") -> None:
    cap = f'<div class="mp-section-caption">{escape(caption)}</div>' if caption else ""
    st.markdown(
        f'<div class="mp-section-heading">{escape(title)}</div>{cap}',
        unsafe_allow_html=True,
    )


# =========================================================
# PAGE
# =========================================================

def render_run_details(run: Dict[str, Any]) -> None:
    """Render concise diagnostics for one monitoring run."""

    if not isinstance(run, dict) or not run:
        st.warning("No monitoring run is available. Select a run from history.")
        return

    run_id = _safe_text(run.get("run_id"))
    model_name = _safe_text(run.get("model_name"))
    version = _safe_text(run.get("model_version"))
    status = _safe_text(run.get("status"), "Unknown")
    run_type = _safe_text(run.get("run_type"), "Unknown")
    status_color = _status_color(status)

    # ---------------- Header ----------------
    st.markdown(
        f"""
        <div class="mp-page-header">
            <div>
                <div class="mp-eyebrow">ModelPulse / Run Diagnostics</div>
                <div class="mp-page-title">Execution Details</div>
                <p class="mp-page-sub">
                    {escape(model_name)} · Version {escape(version)}
                    &nbsp;·&nbsp; <span style="font-family:var(--mp-mono);
                    color:var(--mp-accent)">{escape(run_id)}</span>
                </p>
            </div>
            <span class="mp-pill"
                  style="color:{status_color};border-color:{status_color};
                         background:{_rgba(status_color, .10)}">
                {escape(status.upper())}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------------- Summary ----------------
    total = _safe_number(run.get("sample_count"))
    valid = _safe_number(run.get("valid_sample_count"))
    quarantined = _safe_number(run.get("quarantined_sample_count"))

    st.markdown("<div class='mp-section-spacer'></div>", unsafe_allow_html=True)

    cards = [
        _kpi("Execution status", status.upper(), "Reported by backend",
             status_color, "◉"),
        _kpi("Run type", run_type.replace("_", " ").title(),
             "Execution source", ACCENT, "▤"),
        _kpi("Total samples", _display_count(total),
             "Received in this run", ACCENT, "⌁"),
    ]
    for column, card in zip(st.columns(3, gap="small"), cards):
        with column:
            st.markdown(card, unsafe_allow_html=True)

    # ---------------- Metadata ----------------
    _section("Execution metadata", "Identifiers and timing for this run.")

    metadata = [
        ("Model", model_name),
        ("Model version", version),
        ("Created at", _format_date(run.get("created_at"))),
        ("Run ID", run_id),
        ("Execution status", status),
        ("Execution type", run_type),
    ]
    items = "".join(
        f'<div class="mp-meta-item"><span>{escape(label)}</span>'
        f"<strong>{escape(str(value))}</strong></div>"
        for label, value in metadata
    )
    st.markdown(
        f'<div class="mp-modern-panel" style="padding:12px 14px">'
        f'<div class="mp-panel-topline"></div>'
        f'<div class="mp-panel-label">RUN METADATA</div>'
        f'<div class="mp-meta-grid">{items}</div></div>',
        unsafe_allow_html=True,
    )

    # ---------------- Data validation ----------------
    _section("Data validation", "How received samples were classified.")

    q_pct = (quarantined / total * 100) if total and quarantined is not None else 0.0
    v_pct = (valid / total * 100) if total and valid is not None else 0.0

    cards = [
        _kpi("Received", _display_count(total), "Total input samples",
             ACCENT, "▤"),
        _kpi("Valid", _display_count(valid),
             f"{v_pct:.1f}% of received" if total else "—", OK, "✓", v_pct),
        _kpi("Quarantined", _display_count(quarantined),
             f"{q_pct:.1f}% of received" if total else "—", WARN, "◇", q_pct),
    ]
    for column, card in zip(st.columns(3, gap="small"), cards):
        with column:
            st.markdown(card, unsafe_allow_html=True)

    if total and valid is not None and quarantined is not None:
        unaccounted = max(0.0, 100.0 - v_pct - q_pct)
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="padding:12px 14px;margin-top:8px">
                <div class="mp-panel-label">SAMPLE DISTRIBUTION</div>
                <div class="mp-stackbar">
                    <div style="width:{min(v_pct, 100):.2f}%;background:{OK}"></div>
                    <div style="width:{min(q_pct, 100):.2f}%;background:{WARN}"></div>
                    <div style="width:{unaccounted:.2f}%;background:{CRIT}"></div>
                </div>
                <div class="mp-legend">
                    <span><i style="background:{OK}"></i>Valid {v_pct:.1f}%</span>
                    <span><i style="background:{WARN}"></i>Quarantined {q_pct:.1f}%</span>
                    <span><i style="background:{CRIT}"></i>Unaccounted {unaccounted:.1f}%</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if valid + quarantined != total:
            st.info(
                "The reported sample counts do not reconcile exactly. "
                "Review the backend response before drawing conclusions."
            )

    st.caption(
        "Counts and metadata are reported by the monitoring backend. "
        "Unavailable values are shown as —."
    )

    with st.expander("Inspect raw run payload"):
        st.json(run)