"""ModelPulse — Severity & Risk Analysis (compact dashboard).

Severity score, band and component data are shown as recorded by the
backend; nothing here recomputes a severity decision.
"""

import re
from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from dashboard.components.styles import render_df

try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

ACCENT = "#3d9bff"
OK = "#34d399"
WARN = "#fbbf24"
HIGH = "#fb923c"
CRIT = "#f87171"

BANDS = [("Low", 0, 24, OK), ("Moderate", 25, 49, WARN),
         ("High", 50, 74, HIGH), ("Critical", 75, 100, CRIT)]
BAND_COLOR = {name.lower(): color for name, _, _, color in BANDS}

_COMPONENT_KEYS = ("components", "breakdown", "component_scores", "factors",
                   "contributions", "weights", "signals", "drivers")
_NAME_KEYS = ("name", "component", "factor", "label", "metric", "signal",
              "feature_name", "driver")
_BAR_PREFERENCE = ("contribution", "score", "value", "weighted", "weight", "share")

_CSS = """
<style>
.sv-scale { margin: 18px 0 4px; }
.sv-track { position: relative; display: flex; gap: 3px; height: 10px; }
.sv-seg { flex: 1; border-radius: 3px; transition: background 400ms var(--mp-ease); }
.sv-marker {
    position: absolute; top: -7px; width: 4px; height: 24px; border-radius: 2px;
    background: #fff; transform: translateX(-50%);
    box-shadow: 0 0 0 3px rgba(11,14,19,.85), 0 0 12px rgba(255,255,255,.35);
    transition: left 700ms var(--mp-ease);
}
.sv-labels { display: grid; grid-template-columns: repeat(4, 1fr); gap: 3px; margin-top: 12px; }
.sv-label {
    padding: 7px 9px; border: 1px solid var(--mp-border); border-radius: 6px;
    border-top: 2px solid var(--c); background: var(--mp-surface-2);
    transition: border-color var(--mp-fast) var(--mp-ease),
                background var(--mp-fast) var(--mp-ease);
}
.sv-label:hover { border-color: var(--c); }
.sv-label b { display: block; color: var(--mp-text-dim); font-size: 11px; font-weight: 600; }
.sv-label span { color: var(--mp-text-faint); font-family: var(--mp-mono); font-size: 10.5px; }
.sv-label.current { background: color-mix(in srgb, var(--c) 14%, var(--mp-surface-2));
                    border-color: var(--c); }
.sv-label.current b { color: #fff; }
.sv-label.current b::after { content: "  · CURRENT"; color: var(--c);
                             font-family: var(--mp-mono); font-size: 9px; letter-spacing: .8px; }
.sv-row { display: grid; grid-template-columns: 130px 1fr 74px; align-items: center;
          gap: 10px; padding: 7px 0; border-bottom: 1px dashed rgba(54,66,87,.5); }
.sv-row:last-child { border-bottom: 0; }
.sv-name { color: var(--mp-text); font-size: 12px; overflow-wrap: anywhere; }
.sv-bar { height: 6px; border-radius: 99px; background: rgba(145,160,190,.12); overflow: hidden; }
.sv-bar > div { height: 100%; border-radius: inherit; transition: width 600ms var(--mp-ease); }
.sv-val { color: var(--mp-text); font-family: var(--mp-mono); font-size: 11.5px; text-align: right; }
@media (max-width: 900px) { .sv-labels { grid-template-columns: 1fr 1fr; } }
</style>
"""


# =========================================================
# HELPERS
# =========================================================

def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list:
    return [i for i in value if isinstance(i, dict)] if isinstance(value, list) else []


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pretty(text: Any) -> str:
    return re.sub(r"[_\-\s]+", " ", str(text)).strip().title()


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def _score(run: Dict[str, Any]) -> Optional[float]:
    value = _num(_as_dict(run.get("severity")).get("score"))
    return None if value is None else max(0.0, min(100.0, value))


def _band(run: Dict[str, Any]) -> str:
    return str(_as_dict(run.get("severity")).get("band") or "Unknown")


def _created(value: Any) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).strftime("%d %b %Y · %H:%M")
    except (ValueError, TypeError):
        return str(value)


def _section(title: str, caption: str = "") -> None:
    cap = f'<div class="mp-section-caption">{escape(caption)}</div>' if caption else ""
    st.markdown(f'<div class="mp-section-heading">{escape(title)}</div>{cap}',
                unsafe_allow_html=True)


def _panel_title(title: str, description: str, chip: str) -> str:
    return (
        '<div class="mp-panel-heading-row"><div>'
        f'<div class="mp-panel-title">{escape(title)}</div>'
        f'<div class="mp-panel-description">{escape(description)}</div></div>'
        f'<span class="mp-panel-chip">{escape(chip)}</span></div>'
    )


# =========================================================
# COMPONENT BREAKDOWN EXTRACTION
# =========================================================

def _extract_components(run: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    severity = _as_dict(run.get("severity"))

    for source in (severity, run):
        for key in _COMPONENT_KEYS:
            value = source.get(key)

            if isinstance(value, list) and value and all(isinstance(i, dict) for i in value):
                rows = []
                for item in value:
                    name = next((str(item[k]) for k in _NAME_KEYS if item.get(k) not in (None, "")),
                                "Component")
                    row = {"Component": name}
                    for k, v in item.items():
                        if k not in _NAME_KEYS and isinstance(v, (int, float, str, bool)):
                            row[_pretty(k)] = v
                    rows.append(row)
                return pd.DataFrame(rows), _pretty(key)

            if isinstance(value, dict) and value:
                rows = []
                for name, item in value.items():
                    if isinstance(item, dict):
                        row = {"Component": _pretty(name)}
                        row.update({_pretty(k): v for k, v in item.items()
                                    if isinstance(v, (int, float, str, bool))})
                        rows.append(row)
                    elif _num(item) is not None:
                        rows.append({"Component": _pretty(name), "Value": _num(item)})
                if rows:
                    return pd.DataFrame(rows), _pretty(key)

    contributors = _as_list(run.get("top_contributors"))
    if contributors:
        rows = [{"Component": str(c.get("feature_name", "Unknown")),
                 "Contribution score": _num(c.get("contribution_score"))} for c in contributors]
        return pd.DataFrame(rows), "Top contributors"

    return pd.DataFrame(), ""


def _bar_column(df: pd.DataFrame) -> Optional[str]:
    numeric = [c for c in df.columns if c != "Component"
               and pd.to_numeric(df[c], errors="coerce").notna().any()]
    for token in _BAR_PREFERENCE:
        for col in numeric:
            if token in col.lower():
                return col
    return numeric[0] if numeric else None


# =========================================================
# RENDER BLOCKS
# =========================================================

def _ladder_html(score: Optional[float], band: str) -> str:
    current = band.lower()
    segments = "".join(
        f'<div class="sv-seg" style="background:{c if n.lower() == current else _rgba(c, .22)}"></div>'
        for n, _, _, c in BANDS
    )
    labels = "".join(
        f'<div class="sv-label{" current" if n.lower() == current else ""}" style="--c:{c}">'
        f"<b>{n}</b><span>{lo}–{hi}</span></div>"
        for n, lo, hi, c in BANDS
    )
    marker = f'<div class="sv-marker" style="left:{score:.1f}%"></div>' if score is not None else ""
    return (f'<div class="sv-scale"><div class="sv-track">{segments}{marker}</div>'
            f'<div class="sv-labels">{labels}</div></div>')


def _components_html(df: pd.DataFrame, color: str) -> str:
    col = _bar_column(df)
    if col is None:
        return '<div class="mp-muted-text" style="margin-top:12px">No numeric component values were returned.</div>'

    values = pd.to_numeric(df[col], errors="coerce")
    peak = float(values.abs().max() or 1)
    fractional = peak <= 1 and any(t in col.lower() for t in ("weight", "share", "percent", "contribution"))

    rows = []
    for name, value in zip(df["Component"], values):
        if pd.isna(value):
            continue
        width = max(2.0, abs(float(value)) / peak * 100)
        shown = f"{value * 100:.1f}%" if fractional else f"{value:,.2f}"
        rows.append(
            f'<div class="sv-row"><span class="sv-name">{escape(str(name))}</span>'
            f'<div class="sv-bar"><div style="width:{width:.0f}%;background:{color}"></div></div>'
            f'<span class="sv-val">{shown}</span></div>'
        )
    caption = f'<div class="mp-muted-text">Bars show: {escape(col)}</div>'
    return f'<div style="margin-top:8px">{"".join(rows)}</div>{caption}'


def _distribution_html(runs: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for run in runs:
        counts[_band(run)] = counts.get(_band(run), 0) + 1

    total = max(1, len(runs))
    ordered = [n for n, *_ in BANDS if n in counts] + [n for n in counts if n.lower() not in BAND_COLOR]
    rows = []
    for name in ordered:
        color = BAND_COLOR.get(name.lower(), "#9aa7bb")
        count = counts[name]
        rows.append(
            f'<div class="sv-row" style="grid-template-columns:90px 1fr 80px">'
            f'<span class="sv-name" style="color:{color}">{escape(name)}</span>'
            f'<div class="sv-bar"><div style="width:{count / total * 100:.0f}%;background:{color}"></div></div>'
            f'<span class="sv-val">{count} · {count / total * 100:.0f}%</span></div>'
        )
    return f'<div style="margin-top:8px">{"".join(rows)}</div>'


def _trend_chart(runs: List[Dict[str, Any]]) -> None:
    chronological = list(reversed(runs[:30]))
    rows = [{"Run": f"Run {i}", "Severity": _score(r), "Band": _band(r),
             "Run ID": str(r.get("run_id", "Unknown"))}
            for i, r in enumerate(chronological, start=1)]
    df = pd.DataFrame(rows).dropna(subset=["Severity"])

    if df.empty:
        st.info("No severity scores are available.")
        return

    if not PLOTLY_AVAILABLE:
        st.line_chart(df.set_index("Run")[["Severity"]], height=230)
        return

    fig = go.Figure()
    for lo, hi, color in [(0, 25, OK), (25, 50, WARN), (50, 75, HIGH), (75, 100, CRIT)]:
        fig.add_hrect(y0=lo, y1=hi, fillcolor=_rgba(color, .04), line_width=0, layer="below")

    fig.add_trace(go.Scatter(
        x=df["Run"], y=df["Severity"], mode="lines+markers",
        customdata=df[["Band", "Run ID"]].values,
        line=dict(color=ACCENT, width=2),
        marker=dict(size=7, color=[BAND_COLOR.get(b.lower(), ACCENT) for b in df["Band"]],
                    line=dict(color="#0b0e13", width=1.5)),
        fill="tozeroy", fillcolor="rgba(61,155,255,0.08)",
        hovertemplate="Severity: %{y:.1f}/100<br>Band: %{customdata[0]}<br>%{customdata[1]}<extra></extra>",
    ))
    fig.update_layout(
        height=230, margin=dict(l=4, r=8, t=6, b=4), showlegend=False, hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans, sans-serif", color="#9aa7bb", size=11),
        hoverlabel=dict(bgcolor="#161d28", bordercolor=ACCENT,
                        font=dict(color="#e6ebf3", family="IBM Plex Mono")),
        xaxis=dict(showgrid=False, zeroline=False, linecolor="#263041"),
        yaxis=dict(range=[0, 100], dtick=25, gridcolor="rgba(54,66,87,0.45)", zeroline=False,
                   tickfont=dict(family="IBM Plex Mono", size=10)),
        transition=dict(duration=400, easing="cubic-in-out"),
    )
    st.plotly_chart(fig, width="stretch",
                    config={"displayModeBar": False, "displaylogo": False, "responsive": True})


# =========================================================
# PAGE
# =========================================================

def render_severity_breakdown(all_runs: Any) -> None:
    """Render the severity score, band ladder, components and history."""

    st.markdown(_CSS, unsafe_allow_html=True)

    runs = _as_list(all_runs)

    st.markdown(
        """
        <div class="mp-page-header">
            <div>
                <div class="mp-eyebrow">ModelPulse / Analytics</div>
                <div class="mp-page-title">Severity &amp; Risk Analysis</div>
                <p class="mp-page-sub">Severity score, band position, contributing components
                and distribution across recorded runs.</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not runs:
        st.info("No monitoring runs are available. Create a run and refresh history.")
        return

    runs = sorted(runs, key=lambda r: str(r.get("created_at") or ""), reverse=True)

    def label(run: Dict[str, Any]) -> str:
        return (f"{_created(run.get('created_at'))} | {run.get('model_name') or 'Unknown model'} | "
                f"{str(run.get('run_id') or 'Unknown')[:12]}")

    with st.container(border=True):
        run = st.selectbox("Monitoring run", options=runs, index=0,
                           format_func=label, key="severity_selected_run")

    score = _score(run)
    band = _band(run)
    color = BAND_COLOR.get(band.lower(), ACCENT)
    drift = bool(run.get("dataset_drift_detected", False))
    score_text = f"{score:g}" if score is not None else "N/A"

    # ---------------- Score + components ----------------
    left, right = st.columns([1, 1.25], gap="medium")

    with left:
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{color}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-label">SEVERITY SCORE</div>
                <div class="mp-score-row">
                    <div class="mp-score-number" style="color:{color}">{score_text}<span> / 100</span></div>
                    <span class="mp-band-pill" style="color:{color};border-color:{color};
                          background:{_rgba(color, .10)}">{escape(band.upper())}</span>
                </div>
                {_ladder_html(score, band)}
                <div class="mp-latest-facts" style="margin-top:14px">
                    <div class="mp-fact"><span>Run ID</span>
                        <strong>{escape(str(run.get('run_id') or '—'))}</strong></div>
                    <div class="mp-fact"><span>Created</span>
                        <strong>{escape(_created(run.get('created_at')))}</strong></div>
                    <div class="mp-fact"><span>Dataset drift</span>
                        <strong>{'Detected' if drift else 'Not detected'}</strong></div>
                    <div class="mp-fact"><span>Model</span>
                        <strong>{escape(str(run.get('model_name') or '—'))}</strong></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    components, source = _extract_components(run)

    with right:
        body = (_components_html(components, color) if not components.empty else
                '<div class="mp-muted-text" style="margin-top:12px">'
                'No component breakdown was returned for this run.</div>')
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{ACCENT}">
                <div class="mp-panel-topline"></div>
                {_panel_title("Component breakdown", source or "Contributing factors", "COMPONENTS")}
                {body}
            </div>
            """,
            unsafe_allow_html=True,
        )

    if not components.empty:
        with st.expander("View full component table"):
            render_df(components, hide_index=True)

    # ---------------- Distribution + trend ----------------
    _section("Severity across runs", "Band distribution and score history for recorded runs.")

    dist_col, trend_col = st.columns([1, 1.4], gap="medium")

    with dist_col:
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{HIGH}">
                <div class="mp-panel-topline"></div>
                {_panel_title("Band distribution", f"{len(runs)} recorded runs", "BANDS")}
                {_distribution_html(runs)}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with trend_col:
        with st.container(border=True):
            st.markdown(_panel_title("Severity trend", "Most recent 30 runs", "HISTORY"),
                        unsafe_allow_html=True)
            _trend_chart(runs)

    st.caption("Scores and bands are displayed as recorded by the backend; "
               "this page does not recompute severity.")