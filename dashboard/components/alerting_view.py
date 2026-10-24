"""ModelPulse — Alerting & Notifications (compact dashboard).

Channel status, dispatch policy and delivery counters are shown as
reported by the backend. Data lookups are key-name tolerant; adjust the
*_TOKENS constants below if your payload uses different names.
"""

import re
from html import escape
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from dashboard.components.styles import render_df

ACCENT = "#3d9bff"
OK = "#34d399"
WARN = "#fbbf24"
HIGH = "#fb923c"
CRIT = "#f87171"
MUTED = "#9aa7bb"

CHANNELS = [
    ("slack", "Slack Webhook", "◈",
     "Set SLACK_WEBHOOK_URL in the backend environment.", "Webhook is configured."),
    ("email", "Email / SMTP", "✉",
     "Configure SMTP credentials and alert recipient.", "SMTP is configured."),
]

DELIVERY_TOKENS = {
    "success": [("deliver", "success"), ("success",), ("delivered",), ("sent",)],
    "suppressed": [("suppress",)],
    "skipped": [("skip",), ("disabled",)],
    "failed": [("fail",)],
}
SEVERITY_TOKENS = [("min", "severity"), ("minimum", "severity"),
                   ("severity", "threshold"), ("threshold",)]
COOLDOWN_TOKENS = [("cooldown", "second"), ("cooldown", "minute"),
                   ("cooldown", "window"), ("cooldown",)]
_COOLDOWN_EXCLUDE = ("suppress", "skip", "count", "total")

_TRUE = {"true", "yes", "configured", "enabled", "ready", "active", "connected", "ok"}
_FALSE = {"false", "no", "not_configured", "unconfigured", "disabled", "missing", "none", "off"}

_CSS = """
<style>
.al-channel {
    display: flex; align-items: center; gap: 12px; padding: 11px 12px; margin-top: 8px;
    background: var(--mp-surface-2); border: 1px solid var(--mp-border);
    border-left: 3px solid var(--c); border-radius: var(--mp-radius-sm);
    transition: border-color var(--mp-fast) var(--mp-ease),
                transform var(--mp-fast) var(--mp-ease);
}
.al-channel:hover { border-color: var(--c); transform: translateX(3px); }
.al-icon {
    display: grid; place-items: center; width: 34px; height: 34px; flex: 0 0 34px;
    border-radius: 8px; background: rgba(154,167,187,.10); color: var(--c); font-size: 16px;
}
.al-body { flex: 1; min-width: 0; }
.al-name { color: var(--mp-text); font-size: 13px; font-weight: 600; }
.al-hint { color: var(--mp-text-faint); font-size: 11px; margin-top: 2px; }
.al-policy {
    display: flex; align-items: baseline; justify-content: space-between; gap: 10px;
    padding: 10px 0; border-bottom: 1px dashed rgba(54,66,87,.6);
}
.al-policy:last-child { border-bottom: 0; }
.al-policy span { color: var(--mp-text-faint); font-size: 10.5px; font-weight: 600;
                  letter-spacing: .8px; text-transform: uppercase; }
.al-policy strong { color: #fff; font-family: var(--mp-mono); font-size: 18px; font-weight: 600; }
.al-policy em { display: block; font-style: normal; color: var(--mp-text-faint);
                font-size: 10.5px; margin-top: 3px; }
.al-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
.al-card {
    padding: 12px 14px; background: var(--mp-surface); border: 1px solid var(--mp-border);
    border-left: 3px solid var(--c); border-radius: var(--mp-radius);
    transition: transform 200ms var(--mp-ease), border-color 200ms var(--mp-ease),
                box-shadow 200ms var(--mp-ease), background 200ms var(--mp-ease);
}
.al-card:hover { transform: translateY(-3px); border-color: var(--c);
                 background: var(--mp-surface-2); box-shadow: 0 10px 24px rgba(0,0,0,.35); }
.al-label { color: var(--mp-text-dim); font-size: 10.5px; font-weight: 600;
            letter-spacing: .8px; text-transform: uppercase; }
.al-value { color: #fff; font-family: var(--mp-mono); font-size: 26px; font-weight: 600;
            line-height: 1.2; margin: 8px 0 4px; }
.al-sub { color: var(--mp-text-faint); font-size: 10.5px; margin-bottom: 8px; }
@media (max-width: 900px) { .al-grid { grid-template-columns: 1fr 1fr; } }
</style>
"""


# =========================================================
# DATA HELPERS
# =========================================================

def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _norm(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def _pretty(text: Any) -> str:
    return re.sub(r"[_\-\s]+", " ", str(text)).strip().title()


def _flatten(data: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {} if out is None else out
    if isinstance(data, dict):
        for key, value in data.items():
            _flatten(value, f"{prefix}_{key}" if prefix else str(key), out)
    elif isinstance(data, (int, float, str, bool)) or data is None:
        out[_norm(prefix)] = data
    return out


def _to_num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_item(flat: Dict[str, Any], candidates: List[Tuple[str, ...]],
               numeric: bool = False, exclude: Tuple[str, ...] = ()) -> Optional[Tuple[str, Any]]:
    for tokens in candidates:
        for path, value in flat.items():
            if value is None or value == "":
                continue
            if exclude and any(x in path for x in exclude):
                continue
            if numeric and _to_num(value) is None:
                continue
            if all(t in path for t in tokens):
                return path, value
    return None


def _fmt(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.2f}"


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def _pill(text: str, color: str) -> str:
    return (f'<span class="mp-pill" style="color:{color};border-color:{color};'
            f'background:{_rgba(color, .10)}">{escape(text.upper())}</span>')


def _section(title: str, caption: str = "") -> None:
    cap = f'<div class="mp-section-caption">{escape(caption)}</div>' if caption else ""
    st.markdown(f'<div class="mp-section-heading">{escape(title)}</div>{cap}',
                unsafe_allow_html=True)


def _channel_state(flat: Dict[str, Any], name: str) -> Optional[bool]:
    """True / False when the payload says so, None when it is silent."""
    def parse(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            key = _norm(value)
            if key in _TRUE:
                return True
            if key in _FALSE:
                return False
        return None

    flags = ("config", "enabled", "active", "ready", "available", "status")
    for path, value in flat.items():
        if name in path and any(f in path for f in flags):
            state = parse(value)
            if state is not None:
                return state
    for path, value in flat.items():
        if name in path:
            state = parse(value)
            if state is not None:
                return state
    return None


def _fmt_severity(value: Any) -> str:
    text = str(value).strip()
    return text if text.endswith("+") else f"{text.title()}+"


def _fmt_cooldown(path: str, value: Any) -> Tuple[str, str]:
    number = _to_num(value)
    if number is None:
        return str(value), "Anti-storm control"
    seconds = number * 60 if "minute" in path else number
    if seconds >= 3600 and seconds % 3600 == 0:
        return f"{seconds / 3600:g} h", f"{seconds:,.0f} seconds"
    if seconds >= 60 and seconds % 60 == 0:
        return f"{seconds / 60:g} min", f"{seconds:,.0f} seconds"
    return f"{seconds:g} s", "Anti-storm control"


# =========================================================
# ALERT HISTORY
# =========================================================

_PREFERRED = ("created", "time", "severity", "band", "channel", "status", "title", "message", "run")


def _alerts_frame(recent_alerts: Any) -> pd.DataFrame:
    rows = []
    for alert in recent_alerts if isinstance(recent_alerts, list) else []:
        if isinstance(alert, dict):
            rows.append({_pretty(k): v for k, v in _flatten(alert).items()})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.dropna(axis=1, how="all")

    def rank(col: str) -> int:
        low = col.lower()
        for index, token in enumerate(_PREFERRED):
            if token in low:
                return index
        return len(_PREFERRED)

    return df[sorted(df.columns, key=rank)]


def _find_col(df: pd.DataFrame, *tokens: str) -> Optional[str]:
    for col in df.columns:
        if any(t in col.lower() for t in tokens):
            return col
    return None


# =========================================================
# PAGE
# =========================================================

def render_alerting_view(alert_status: Optional[Dict[str, Any]] = None,
                         recent_alerts: Optional[List[Dict[str, Any]]] = None) -> None:
    """Render channels, dispatch policy, delivery counters and history."""

    st.markdown(_CSS, unsafe_allow_html=True)

    status = _as_dict(alert_status)
    flat = _flatten(status)

    states = {key: _channel_state(flat, key) for key, *_ in CHANNELS}
    configured = sum(1 for s in states.values() if s)

    header_color = OK if configured == len(CHANNELS) else WARN if configured else CRIT
    st.markdown(
        f"""
        <div class="mp-page-header">
            <div>
                <div class="mp-eyebrow">ModelPulse / Operations</div>
                <div class="mp-page-title">Alerting &amp; Notifications</div>
                <p class="mp-page-sub">Notification readiness, dispatch policy and delivery history.</p>
            </div>
            {_pill(f"{configured}/{len(CHANNELS)} channels configured", header_color)}
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not status:
        st.info("No alert status was returned by the backend.")

    # ---------------- Channels + policy ----------------
    left, right = st.columns([1.3, 1], gap="medium")

    with left:
        rows = []
        for key, name, icon, hint_off, hint_on in CHANNELS:
            state = states[key]
            color = OK if state else CRIT if state is False else MUTED
            label = "Configured" if state else "Not configured" if state is False else "Unknown"
            rows.append(
                f'<div class="al-channel" style="--c:{color}">'
                f'<div class="al-icon">{icon}</div>'
                f'<div class="al-body"><div class="al-name">{escape(name)}</div>'
                f'<div class="al-hint">{escape(hint_on if state else hint_off)}</div></div>'
                f"{_pill(label, color)}</div>"
            )
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{ACCENT}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-heading-row">
                    <div>
                        <div class="mp-panel-title">Notification channels</div>
                        <div class="mp-panel-description">Channel configuration</div>
                    </div>
                    <span class="mp-panel-chip">01</span>
                </div>
                {"".join(rows)}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        sev_item = _pick_item(flat, SEVERITY_TOKENS)
        cool_item = _pick_item(flat, COOLDOWN_TOKENS, exclude=_COOLDOWN_EXCLUDE)

        sev_value = _fmt_severity(sev_item[1]) if sev_item else "—"
        cool_value, cool_note = _fmt_cooldown(*cool_item) if cool_item else ("—", "Not reported")

        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{WARN}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-heading-row">
                    <div>
                        <div class="mp-panel-title">Dispatch policy</div>
                        <div class="mp-panel-description">Alert thresholds and anti-storm controls</div>
                    </div>
                    <span class="mp-panel-chip">02</span>
                </div>
                <div style="margin-top:6px">
                    <div class="al-policy">
                        <div><span>Minimum alert severity</span>
                             <em>Alerts below this threshold may not be dispatched.</em></div>
                        <strong>{escape(sev_value)}</strong>
                    </div>
                    <div class="al-policy">
                        <div><span>Cooldown window</span><em>{escape(cool_note)}</em></div>
                        <strong>{escape(cool_value)}</strong>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ---------------- Delivery performance ----------------
    _section("Delivery performance", "Recorded dispatch outcomes and delivery status.")

    def counter(key: str) -> Optional[float]:
        item = _pick_item(flat, DELIVERY_TOKENS[key], numeric=True)
        return _to_num(item[1]) if item else None

    success, suppressed, skipped, failed = (counter(k) for k in ("success", "suppressed", "skipped", "failed"))

    cards = [
        ("Successfully delivered", success, "Delivery attempts reported as successful.",
         OK, "Delivery success", OK),
        ("Suppressed by cooldown", suppressed, "Notifications held back by cooldown.",
         WARN, "Duplicate control", WARN),
        ("Skipped — channel disabled", skipped, "Attempts skipped because the channel was disabled.",
         ACCENT, "Unavailable", ACCENT),
        ("Failed deliveries", failed, "Delivery attempts reported as failed.",
         CRIT if (failed or 0) > 0 else OK,
         "Attention" if (failed or 0) > 0 else "No failures",
         CRIT if (failed or 0) > 0 else OK),
    ]
    html_cards = "".join(
        f'<div class="al-card" style="--c:{accent}">'
        f'<div class="al-label">{escape(label)}</div>'
        f'<div class="al-value">{_fmt(value)}</div>'
        f'<div class="al-sub">{escape(sub)}</div>{_pill(tag, tag_color)}</div>'
        for label, value, sub, accent, tag, tag_color in cards
    )
    st.markdown(f'<div class="al-grid">{html_cards}</div>', unsafe_allow_html=True)

    # ---------------- Recent alerts ----------------
    _section("Recent alerts", "Latest notifications returned by the backend.")

    df = _alerts_frame(recent_alerts)

    if df.empty:
        st.info("No alerts have been recorded yet.")
    else:
        sev_col = _find_col(df, "severity", "band")
        status_col = _find_col(df, "status")

        with st.container(border=True):
            f1, f2, f3 = st.columns([1.8, 1, 1], gap="small")
            with f1:
                query = st.text_input("Search alerts", placeholder="Any field…",
                                      key="alert_search")
            with f2:
                sev_filter = st.selectbox(
                    "Severity",
                    ["All"] + (sorted(df[sev_col].dropna().astype(str).unique()) if sev_col else []),
                    key="alert_severity_filter", disabled=sev_col is None)
            with f3:
                status_filter = st.selectbox(
                    "Delivery status",
                    ["All"] + (sorted(df[status_col].dropna().astype(str).unique()) if status_col else []),
                    key="alert_status_filter", disabled=status_col is None)

        view = df
        if query.strip():
            haystack = view.astype(str).agg(" ".join, axis=1)
            view = view[haystack.str.contains(query.strip(), case=False, regex=False)]
        if sev_col and sev_filter != "All":
            view = view[view[sev_col].astype(str) == sev_filter]
        if status_col and status_filter != "All":
            view = view[view[status_col].astype(str) == status_filter]

        st.caption(f"Showing {len(view)} of {len(df)} alerts.")
        if view.empty:
            st.warning("No alerts match the selected filters.")
        else:
            render_df(view, hide_index=True, height=min(380, 44 + 36 * len(view)))

    with st.expander("Raw alert status"):
        st.json(status)