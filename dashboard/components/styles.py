"""ModelPulse Dashboard Custom Styles & Visual Tokens (Phase 6).
Provides modern typography, sleek dark mode aesthetics, and badge helpers.
"""

from typing import Optional

CUSTOM_CSS = """

<style>
/* Main dashboard typography and background refinements */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

code, pre {
    font-family: 'JetBrains Mono', monospace !important;
}

/* Metric card styling */
.metric-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    transition: transform 0.15s ease, border-color 0.15s ease;
}
.metric-card:hover {
    border-color: #3b82f6;
    transform: translateY(-2px);
}

.metric-title {
    font-size: 0.82rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #94a3b8;
    margin-bottom: 6px;
}

.metric-value {
    font-size: 1.85rem;
    font-weight: 700;
    color: #f8fafc;
    line-height: 1.2;
}

.metric-sub {
    font-size: 0.78rem;
    color: #64748b;
    margin-top: 4px;
}

/* Severity Badges */
.badge {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.badge-low { background-color: rgba(16, 185, 129, 0.2); color: #10b981; border: 1px solid #10b981; }
.badge-moderate { background-color: rgba(245, 158, 11, 0.2); color: #f59e0b; border: 1px solid #f59e0b; }
.badge-high { background-color: rgba(249, 115, 22, 0.2); color: #f97316; border: 1px solid #f97316; }
.badge-critical { background-color: rgba(239, 68, 68, 0.2); color: #ef4444; border: 1px solid #ef4444; }

/* Disclaimer Banner */
.disclaimer-banner {
    background-color: rgba(30, 41, 59, 0.7);
    border-left: 4px solid #3b82f6;
    padding: 10px 16px;
    border-radius: 0 6px 6px 0;
    margin-bottom: 18px;
    font-size: 0.85rem;
    color: #cbd5e1;
}

/* Data Table styling */
.dataframe {
    border-collapse: collapse !important;
    width: 100% !important;
}
</style>
"""


def severity_badge(band: str, score: Optional[int] = None) -> str:
    """Return HTML badge for severity band."""
    b_lower = band.lower() if band else "low"
    class_map = {
        "low": "badge-low",
        "moderate": "badge-moderate",
        "high": "badge-high",
        "critical": "badge-critical",
    }
    css_class = class_map.get(b_lower, "badge-low")
    score_text = f" ({score})" if score is not None else ""
    return f'<span class="badge {css_class}">{band.upper()}{score_text}</span>'


def status_badge(status: str) -> str:
    """Return status tag HTML."""
    st = status.lower()
    if st in ("ok", "completed", "delivered"):
        color = "#10b981"
        bg = "rgba(16, 185, 129, 0.15)"
    elif st in ("quality_failure", "failed"):
        color = "#ef4444"
        bg = "rgba(239, 68, 68, 0.15)"
    elif st in ("all_quarantined", "suppressed_cooldown"):
        color = "#f59e0b"
        bg = "rgba(245, 158, 11, 0.15)"
    else:
        color = "#94a3b8"
        bg = "rgba(148, 163, 184, 0.15)"

    return f'<span style="background-color: {bg}; color: {color}; border: 1px solid {color}; padding: 3px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 600; text-transform: uppercase;">{status}</span>'


def render_df(df, hide_index: bool = True, **kwargs):
    """Render a dataframe safely with modern width='stretch' and fallback to use_container_width."""
    import streamlit as st
    try:
        return st.dataframe(df, width="stretch", hide_index=hide_index, **kwargs)
    except TypeError:
        return st.dataframe(df, use_container_width=True, hide_index=hide_index, **kwargs)
