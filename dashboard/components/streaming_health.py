"""ModelPulse — Streaming Telemetry (compact pipeline dashboard).

Counters are displayed exactly as reported by the backend. Nothing here
verifies a live broker connection or recomputes any decision.

Data lookups are key-name tolerant: see METRIC_TOKENS to adjust matching.
"""

import re
from html import escape
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

ACCENT = "#3d9bff"
OK = "#34d399"
WARN = "#fbbf24"
CRIT = "#f87171"

# Documented micro-batching reference settings (shown when the backend
# payload does not override them).
CONFIG_DEFAULTS = [
    ("Window duration", "5 min", "Documented micro-batch window · 300 seconds"),
    ("Maximum batch size", "1,000", "Documented maximum records per batch"),
    ("Deduplication cache", "50,000", "Documented event ID capacity"),
    ("Consumer group", "ModelPulse", "modelpulse-consumer-group"),
    ("Input topic", "events", "Documented input topic · 2 partitions"),
    ("Dead letter topic", "events.dlq", "Documented DLQ topic · acks=all"),
]

# Each metric: list of token tuples, tried in order. A tuple matches a payload
# path when every token is a substring of it (e.g. "summary_events_consumed").
METRIC_TOKENS: Dict[str, List[Tuple[str, ...]]] = {
    "consumed": [("total", "events", "consumed"), ("events", "consumed"),
                 ("consumed",), ("total", "events"), ("received",)],
    "validated": [("validated", "scored"), ("events", "validated"),
                  ("validated",), ("scored",), ("valid", "events")],
    "quarantined": [("quarantine",)],
    "dlq": [("dlq",), ("dead", "letter")],
    "batches_completed": [("batch", "complet"), ("complet", "batch"), ("batch", "success")],
    "batches_failed": [("batch", "fail"), ("fail", "batch")],
    "duplicates": [("duplicate",), ("dedup",)],
    "retries": [("retr",)],
}

STAGES = [
    ("consumed", "Total events consumed", "Events received by ingestion", ACCENT),
    ("validated", "Validated & scored", "Records accepted for scoring", OK),
    ("quarantined", "Quarantined events", "Records isolated by quality controls", WARN),
    ("dlq", "Dead letter queue", "Events reported by the DLQ counter", CRIT),
]

MINIS = [
    ("batches_completed", "Batches completed", "Micro-batches completed successfully", OK, False),
    ("batches_failed", "Batches failed", "Micro-batches reported as failed", CRIT, True),
    ("duplicates", "Duplicates dropped", "Repeated deliveries suppressed", ACCENT, False),
    ("retries", "Transient retries", "Retry attempts reported by the pipeline", WARN, False),
]

_CSS = """
<style>
.tp-flow { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
.tp-stage {
    position: relative; padding: 12px 14px; min-height: 104px;
    background: var(--mp-surface); border: 1px solid var(--mp-border);
    border-left: 3px solid var(--c); border-radius: var(--mp-radius);
    transition: transform 200ms var(--mp-ease), border-color 200ms var(--mp-ease),
                box-shadow 200ms var(--mp-ease), background 200ms var(--mp-ease);
}
.tp-stage:hover {
    transform: translateY(-3px); border-color: var(--c);
    background: var(--mp-surface-2); box-shadow: 0 10px 24px rgba(0,0,0,.35);
}
.tp-stage:not(:last-child)::after {
    content: "›"; position: absolute; right: -11px; top: 50%;
    transform: translateY(-50%); z-index: 3;
    color: var(--mp-text-faint); font-size: 20px; line-height: 1;
}
.tp-label { color: var(--mp-text-dim); font-size: 10.5px; font-weight: 600;
            letter-spacing: .8px; text-transform: uppercase; }
.tp-value { color: #fff; font-family: var(--mp-mono); font-size: 26px;
            font-weight: 600; line-height: 1.2; margin-top: 8px; }
.tp-sub   { color: var(--mp-text-faint); font-size: 10.5px; margin-top: 3px; }
.tp-bar   { height: 3px; border-radius: 99px; background: rgba(145,160,190,.12);
            margin-top: 10px; overflow: hidden; }
.tp-bar > div { height: 100%; border-radius: inherit; background: var(--c);
                transition: width 600ms var(--mp-ease); }
.tp-mini-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 10px; }
.tp-mini {
    padding: 10px 12px; background: var(--mp-surface-2);
    border: 1px solid var(--mp-border); border-left: 3px solid var(--c);
    border-radius: var(--mp-radius-sm);
    transition: border-color var(--mp-fast) var(--mp-ease),
                transform var(--mp-fast) var(--mp-ease);
}
.tp-mini:hover { border-color: var(--c); transform: translateY(-2px); }
.tp-mini .tp-value { font-size: 21px; margin-top: 5px; }
.mp-meta-item em { display: block; font-style: normal; color: var(--mp-text-faint);
                   font-size: 10.5px; margin-top: 2px; }
@media (max-width: 900px) {
    .tp-flow { grid-template-columns: 1fr 1fr; }
    .tp-stage::after { display: none; }
}
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


def _pick_number(flats: List[Dict[str, Any]], candidates: List[Tuple[str, ...]]) -> Optional[float]:
    for flat in flats:
        for tokens in candidates:
            for path, value in flat.items():
                number = _to_num(value)
                if number is not None and all(t in path for t in tokens):
                    return number
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


# =========================================================
# RENDER BLOCKS
# =========================================================

def _stage_html(label: str, value: Optional[float], sub: str, color: str, pct: Optional[float]) -> str:
    fill = 4 if pct is None else max(4, min(100, pct))
    return (
        f'<div class="tp-stage" style="--c:{color}">'
        f'<div class="tp-label">{escape(label)}</div>'
        f'<div class="tp-value">{_fmt(value)}</div>'
        f'<div class="tp-sub">{escape(sub)}</div>'
        f'<div class="tp-bar"><div style="width:{fill:.0f}%"></div></div></div>'
    )


def _reconciliation_rows(reconciliation: Dict[str, Any]) -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    for path, value in _flatten(reconciliation).items():
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            shown = _pill("Yes" if value else "No", OK if value else WARN)
        elif _to_num(value) is not None and not isinstance(value, str):
            shown = f"<strong>{_fmt(_to_num(value))}</strong>"
        else:
            shown = f"<strong>{escape(str(value))}</strong>"
        rows.append((_pretty(path), shown))
    return rows


# =========================================================
# PAGE
# =========================================================

def render_streaming_health(streaming_health: Optional[Dict[str, Any]] = None) -> None:
    """Render ingestion, processing, reconciliation and configuration."""

    st.markdown(_CSS, unsafe_allow_html=True)

    data = _as_dict(streaming_health)
    summary = _as_dict(data.get("summary"))
    reconciliation = _as_dict(data.get("reconciliation"))
    flats = [_flatten(summary), _flatten(data)]

    values = {key: _pick_number(flats, cands) for key, cands in METRIC_TOKENS.items()}
    missing = [k for k, v in values.items() if v is None]

    st.markdown(
        f"""
        <div class="mp-page-header">
            <div>
                <div class="mp-eyebrow">ModelPulse / Operations</div>
                <div class="mp-page-title">Streaming Telemetry</div>
                <p class="mp-page-sub">Ingestion, validation, retries and reconciliation.
                Cards show counters supplied by the configured backend; they do not
                independently verify a live broker connection.</p>
            </div>
            {_pill("Backend-reported", ACCENT)}
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not data:
        st.info("No streaming telemetry was returned by the backend.")
    elif missing:
        st.warning(
            f"{len(missing)} counter(s) were not found in the payload "
            f"({', '.join(missing)}). Open 'Raw payload' at the bottom to check key "
            "names, then adjust METRIC_TOKENS at the top of this file."
        )

    # ---------------- 01 Ingestion pipeline ----------------
    _section("Ingestion pipeline", "Event flow from consumption to scoring and isolation.")

    consumed = values["consumed"]
    stage_cards = []
    for key, label, sub, color in STAGES:
        value = values[key]
        pct = None
        shown_sub = sub
        if key == "consumed":
            pct = 100 if value is not None else None
        elif consumed and value is not None:
            pct = value / consumed * 100
            shown_sub = f"{pct:.1f}% of consumed"
        stage_cards.append(_stage_html(label, value, shown_sub, color, pct))
    st.markdown(f'<div class="tp-flow">{"".join(stage_cards)}</div>', unsafe_allow_html=True)

    st.markdown("<div class='mp-section-spacer'></div>", unsafe_allow_html=True)

    # ---------------- 02/03 Processing + reconciliation ----------------
    left, right = st.columns([1.15, 1], gap="medium")

    with left:
        minis = []
        for key, label, sub, color, alert_if_positive in MINIS:
            value = values[key]
            tone = CRIT if (alert_if_positive and (value or 0) > 0) else color
            minis.append(
                f'<div class="tp-mini" style="--c:{tone}">'
                f'<div class="tp-label">{escape(label)}</div>'
                f'<div class="tp-value">{_fmt(value)}</div>'
                f'<div class="tp-sub">{escape(sub)}</div></div>'
            )
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{OK}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-heading-row">
                    <div>
                        <div class="mp-panel-title">Processing &amp; reliability</div>
                        <div class="mp-panel-description">Batch completion, duplicate filtering and retry activity</div>
                    </div>
                    <span class="mp-panel-chip">02</span>
                </div>
                <div class="tp-mini-grid">{"".join(minis)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        rows = _reconciliation_rows(reconciliation)
        if rows:
            shown = "".join(
                f'<div class="mp-system-row"><span>{escape(k)}</span>{v}</div>'
                for k, v in rows[:8]
            )
            more = f'<div class="mp-muted-text">+{len(rows) - 8} more in raw payload</div>' if len(rows) > 8 else ""
        else:
            shown = '<div class="mp-muted-text" style="margin-top:12px">No reconciliation records were reported.</div>'
            more = ""
        st.markdown(
            f"""
            <div class="mp-modern-panel" style="--panel-accent:{ACCENT}">
                <div class="mp-panel-topline"></div>
                <div class="mp-panel-heading-row">
                    <div>
                        <div class="mp-panel-title">Event accounting &amp; reconciliation</div>
                        <div class="mp-panel-description">Backend-reported event-accounting outcomes</div>
                    </div>
                    <span class="mp-panel-chip">03</span>
                </div>
                <div style="margin-top:8px">{shown}{more}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ---------------- 04 Configuration ----------------
    _section("Micro-batching configuration", "Reference settings documented by the implementation.")

    items = "".join(
        f'<div class="mp-meta-item"><span>{escape(label)}</span>'
        f"<strong>{escape(value)}</strong><em>{escape(sub)}</em></div>"
        for label, value, sub in CONFIG_DEFAULTS
    )
    st.markdown(
        f'<div class="mp-modern-panel" style="padding:12px 14px">'
        f'<div class="mp-panel-topline"></div>'
        f'<div class="mp-panel-label">CONFIGURATION REFERENCE</div>'
        f'<div class="mp-meta-grid">{items}</div></div>',
        unsafe_allow_html=True,
    )

    with st.expander("View detailed configuration reference"):
        st.table({
            "Setting": [c[0] for c in CONFIG_DEFAULTS],
            "Value": [c[1] for c in CONFIG_DEFAULTS],
            "Note": [c[2] for c in CONFIG_DEFAULTS],
        })

    with st.expander("Raw payload"):
        st.json(data)