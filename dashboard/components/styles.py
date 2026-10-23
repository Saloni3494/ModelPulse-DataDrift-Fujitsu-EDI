"""ModelPulse — shared industrial design system.

Single source of truth for tokens, Streamlit widget theming and
reusable component classes (mp-*). Inject CUSTOM_CSS once from app.py.
"""

from html import escape
from typing import Optional

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

/* =========================================================
   DESIGN TOKENS
   ========================================================= */
:root {
    --mp-bg:            #0b0e13;
    --mp-surface:       #121821;
    --mp-surface-2:     #161d28;
    --mp-surface-3:     #1b2432;
    --mp-border:        #263041;
    --mp-border-strong: #364257;
    --mp-text:          #e6ebf3;
    --mp-text-dim:      #9aa7bb;
    --mp-text-faint:    #6b788d;

    --mp-accent:        #3d9bff;
    --mp-accent-soft:   rgba(61,155,255,.12);
    --mp-accent-ring:   rgba(61,155,255,.28);

    --mp-ok:            #34d399;
    --mp-warn:          #fbbf24;
    --mp-high:          #fb923c;
    --mp-crit:          #f87171;

    --mp-radius:        8px;
    --mp-radius-sm:     6px;
    --mp-ease:          cubic-bezier(.4, 0, .2, 1);
    --mp-fast:          160ms;
    --mp-mono:          'IBM Plex Mono', ui-monospace, monospace;
}

/* =========================================================
   GLOBAL / STREAMLIT CHROME
   ========================================================= */
html, body, .stApp, [class*="css"] {
    font-family: 'IBM Plex Sans', system-ui, sans-serif;
}
.stApp {
    background:
        linear-gradient(rgba(61,155,255,.025) 1px, transparent 1px) 0 0 / 32px 32px,
        linear-gradient(90deg, rgba(61,155,255,.025) 1px, transparent 1px) 0 0 / 32px 32px,
        var(--mp-bg);
    color: var(--mp-text);
}
.block-container {
    padding-top: 1.4rem !important;
    padding-bottom: 2rem !important;
    max-width: 1500px;
}
[data-testid="stVerticalBlock"] { gap: .75rem; }
hr { border-color: var(--mp-border) !important; margin: 1rem 0 !important; }

::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-thumb { background: var(--mp-border-strong); border-radius: 8px; }
::-webkit-scrollbar-track { background: transparent; }

/* =========================================================
   INPUTS — highlighted borders, smooth focus
   ========================================================= */
.stTextInput label, .stSelectbox label, .stNumberInput label,
.stMultiSelect label, .stDateInput label, .stSlider label,
.stTextArea label {
    color: var(--mp-text-dim) !important;
    font-size: 11px !important;
    font-weight: 600 !important;
    letter-spacing: .6px;
    text-transform: uppercase;
}

[data-baseweb="input"], [data-baseweb="base-input"],
[data-baseweb="textarea"], [data-baseweb="select"] > div {
    background: var(--mp-surface-2) !important;
    border: 1px solid var(--mp-border-strong) !important;
    border-radius: var(--mp-radius-sm) !important;
    min-height: 36px;
    transition: border-color var(--mp-fast) var(--mp-ease),
                box-shadow var(--mp-fast) var(--mp-ease),
                background var(--mp-fast) var(--mp-ease);
}
[data-baseweb="input"] input, [data-baseweb="textarea"] textarea {
    color: var(--mp-text) !important;
    font-size: 13px !important;
    padding-top: 6px !important;
    padding-bottom: 6px !important;
}
[data-baseweb="input"]:hover, [data-baseweb="textarea"]:hover,
[data-baseweb="select"] > div:hover {
    border-color: var(--mp-accent) !important;
    background: var(--mp-surface-3) !important;
}
[data-baseweb="input"]:focus-within, [data-baseweb="textarea"]:focus-within,
[data-baseweb="select"] > div:focus-within {
    border-color: var(--mp-accent) !important;
    box-shadow: 0 0 0 3px var(--mp-accent-ring) !important;
}
[data-baseweb="select"] div[value], [data-baseweb="select"] span {
    font-size: 13px;
}
[data-baseweb="popover"] ul {
    background: var(--mp-surface-2) !important;
    border: 1px solid var(--mp-border-strong);
    border-radius: var(--mp-radius-sm);
}
[data-baseweb="popover"] li { transition: background var(--mp-fast) var(--mp-ease); }
[data-baseweb="popover"] li:hover { background: var(--mp-accent-soft) !important; }

/* =========================================================
   BUTTONS
   ========================================================= */
.stButton > button, .stDownloadButton > button {
    background: var(--mp-surface-3);
    color: var(--mp-text);
    border: 1px solid var(--mp-border-strong);
    border-radius: var(--mp-radius-sm);
    min-height: 36px;
    padding: 0 14px;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: .4px;
    transition: all var(--mp-fast) var(--mp-ease);
}
.stButton > button:hover, .stDownloadButton > button:hover {
    border-color: var(--mp-accent);
    color: #fff;
    background: var(--mp-accent-soft);
    transform: translateY(-1px);
    box-shadow: 0 4px 14px rgba(0,0,0,.35);
}
.stButton > button:active { transform: translateY(0); }
.stButton > button:focus-visible {
    outline: none;
    box-shadow: 0 0 0 3px var(--mp-accent-ring);
}
.stButton > button[kind="primary"] {
    background: linear-gradient(180deg, #4aa5ff, #2f86e6);
    border-color: #4aa5ff;
    color: #04101f;
}
.stButton > button[kind="primary"]:hover {
    background: linear-gradient(180deg, #62b2ff, #3d93f0);
    color: #04101f;
}
.stButton > button:disabled { opacity: .4; transform: none; box-shadow: none; }

/* =========================================================
   BORDERED CONTAINERS, ALERTS, EXPANDERS, TABLES
   ========================================================= */
[data-testid="stVerticalBlockBorderWrapper"]:has(> div > [data-testid="stVerticalBlock"]) {
    border-radius: var(--mp-radius);
}
div[data-testid="stVerticalBlockBorderWrapper"] {
    border-color: var(--mp-border) !important;
    background: var(--mp-surface);
    border-radius: var(--mp-radius) !important;
    transition: border-color 220ms var(--mp-ease);
}
div[data-testid="stVerticalBlockBorderWrapper"]:hover {
    border-color: var(--mp-border-strong) !important;
}

[data-testid="stAlert"] {
    background: var(--mp-surface-2);
    border: 1px solid var(--mp-border-strong);
    border-left: 3px solid var(--mp-accent);
    border-radius: var(--mp-radius-sm);
    padding: 8px 12px;
    font-size: 12.5px;
}
[data-testid="stExpander"] {
    border: 1px solid var(--mp-border) !important;
    border-radius: var(--mp-radius-sm) !important;
    background: var(--mp-surface);
    transition: border-color var(--mp-fast) var(--mp-ease);
}
[data-testid="stExpander"]:hover { border-color: var(--mp-accent) !important; }

[data-testid="stDataFrame"] {
    border: 1px solid var(--mp-border-strong);
    border-radius: var(--mp-radius-sm);
    overflow: hidden;
    transition: border-color 220ms var(--mp-ease), box-shadow 220ms var(--mp-ease);
}
[data-testid="stDataFrame"]:hover {
    border-color: var(--mp-accent);
    box-shadow: 0 0 0 3px var(--mp-accent-soft);
}
.stCaption, [data-testid="stCaptionContainer"] {
    color: var(--mp-text-faint) !important;
    font-size: 11px !important;
}

/* =========================================================
   PAGE HEADER (shared)
   ========================================================= */
.mp-page-header {
    position: relative;
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    gap: 16px;
    padding: 14px 18px;
    background: linear-gradient(180deg, var(--mp-surface-2), var(--mp-surface));
    border: 1px solid var(--mp-border);
    border-left: 3px solid var(--mp-accent);
    border-radius: var(--mp-radius);
}
.mp-eyebrow {
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--mp-accent);
    font-family: var(--mp-mono);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.6px;
    text-transform: uppercase;
}
.mp-hero-title, .mp-page-title {
    color: var(--mp-text);
    font-size: clamp(22px, 2.2vw, 28px);
    font-weight: 700;
    letter-spacing: -.5px;
    line-height: 1.2;
    margin: 6px 0 4px;
}
.mp-gradient-accent { background: none; -webkit-text-fill-color: initial; }
.mp-hero-subtitle, .mp-page-sub {
    color: var(--mp-text-dim);
    font-size: 13px;
    line-height: 1.6;
    max-width: 640px;
    margin: 0;
}
.mp-hero { padding: 0; }
.mp-hero-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 12px; }
.mp-hero-tags span, .mp-panel-chip, .mp-tag {
    border: 1px solid var(--mp-border-strong);
    background: var(--mp-surface-3);
    color: var(--mp-text-dim);
    border-radius: 4px;
    padding: 3px 8px;
    font-family: var(--mp-mono);
    font-size: 9.5px;
    font-weight: 600;
    letter-spacing: .8px;
    transition: all var(--mp-fast) var(--mp-ease);
}
.mp-hero-tags span:hover, .mp-tag:hover {
    border-color: var(--mp-accent);
    color: var(--mp-accent);
}

/* live indicator */
.mp-hero-dot, .mp-live-dot {
    display: inline-block;
    width: 7px; height: 7px; flex: 0 0 7px;
    border-radius: 50%;
    background: var(--live-color, var(--mp-ok));
    box-shadow: 0 0 0 0 var(--live-color, var(--mp-ok));
    animation: mpPulse 2s ease-out infinite;
}
@keyframes mpPulse {
    0%   { box-shadow: 0 0 0 0 rgba(52,211,153,.55); }
    70%  { box-shadow: 0 0 0 7px rgba(52,211,153,0); }
    100% { box-shadow: 0 0 0 0 rgba(52,211,153,0); }
}

/* =========================================================
   PANELS
   ========================================================= */
.mp-system-card, .mp-modern-panel, .mp-content-panel {
    position: relative;
    background: var(--mp-surface);
    border: 1px solid var(--mp-border);
    border-radius: var(--mp-radius);
    padding: 14px 16px;
    transition: border-color 220ms var(--mp-ease), box-shadow 220ms var(--mp-ease);
}
.mp-system-card:hover, .mp-modern-panel:hover {
    border-color: var(--mp-border-strong);
    box-shadow: 0 6px 22px rgba(0,0,0,.28);
}
.mp-system-card-header, .mp-panel-heading-row {
    display: flex; align-items: center; justify-content: space-between; gap: 10px;
}
.mp-panel-label {
    color: var(--mp-text-faint);
    font-family: var(--mp-mono);
    font-size: 9.5px;
    font-weight: 600;
    letter-spacing: 1.4px;
    text-transform: uppercase;
}
.mp-panel-title { color: var(--mp-text); font-size: 14px; font-weight: 600; }
.mp-panel-description { color: var(--mp-text-faint); font-size: 11px; margin-top: 2px; }
.mp-panel-topline {
    position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, var(--panel-accent, var(--mp-accent)), transparent);
    border-radius: var(--mp-radius) var(--mp-radius) 0 0;
}
.mp-system-symbol {
    display: grid; place-items: center;
    width: 26px; height: 26px; border-radius: 6px;
    color: var(--mp-accent); background: var(--mp-accent-soft); font-size: 14px;
}
.mp-system-status {
    display: flex; align-items: center; gap: 8px;
    font-family: var(--mp-mono); font-size: 11.5px; font-weight: 600;
    margin: 12px 0;
}
.mp-system-divider { height: 1px; background: var(--mp-border); margin: 10px 0; }
.mp-system-row {
    display: flex; justify-content: space-between; align-items: baseline;
    gap: 12px; padding: 5px 0;
    border-bottom: 1px dashed rgba(54,66,87,.5);
}
.mp-system-row:last-child { border-bottom: 0; }
.mp-system-row span, .mp-fact span { color: var(--mp-text-faint); font-size: 11px; }
.mp-system-row strong, .mp-fact strong {
    color: var(--mp-text); font-size: 12px; font-weight: 500;
    font-family: var(--mp-mono); overflow-wrap: anywhere; text-align: right;
}

/* section headings */
.mp-section-heading {
    display: flex; align-items: center; gap: 8px;
    color: var(--mp-text); font-size: 13px; font-weight: 600;
    letter-spacing: .8px; text-transform: uppercase; margin-top: 6px;
}
.mp-section-heading::before {
    content: ""; width: 3px; height: 13px; border-radius: 2px;
    background: var(--mp-accent);
}
.mp-section-caption { color: var(--mp-text-faint); font-size: 11.5px; margin: 3px 0 10px 11px; }
.mp-section-spacer { height: 8px; }

/* =========================================================
   KPI CARDS (compact)
   ========================================================= */
.mp-kpi-modern {
    position: relative;
    overflow: hidden;
    min-height: 104px;
    padding: 12px 14px;
    background: var(--mp-surface);
    border: 1px solid var(--mp-border);
    border-left: 3px solid var(--kpi-accent, var(--mp-accent));
    border-radius: var(--mp-radius);
    transition: transform 200ms var(--mp-ease), border-color 200ms var(--mp-ease),
                box-shadow 200ms var(--mp-ease), background 200ms var(--mp-ease);
}
.mp-kpi-modern::after {
    content: ""; position: absolute; inset: 0; pointer-events: none;
    background: radial-gradient(circle at 100% 0%, var(--kpi-soft, transparent), transparent 55%);
}
.mp-kpi-modern:hover {
    transform: translateY(-3px);
    border-color: var(--kpi-accent, var(--mp-accent));
    background: var(--mp-surface-2);
    box-shadow: 0 10px 26px rgba(0,0,0,.35);
}
.mp-kpi-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; position: relative; z-index: 1; }
.mp-kpi-label {
    color: var(--mp-text-dim); font-size: 10.5px; font-weight: 600;
    letter-spacing: .8px; text-transform: uppercase;
}
.mp-kpi-icon {
    display: grid; place-items: center; width: 26px; height: 26px; flex: 0 0 26px;
    border-radius: 6px; color: var(--kpi-accent); background: var(--kpi-soft);
    font-size: 14px;
}
.mp-kpi-value {
    position: relative; z-index: 1;
    color: #fff; font-family: var(--mp-mono);
    font-size: clamp(20px, 1.8vw, 26px); font-weight: 600;
    letter-spacing: -.5px; line-height: 1.2; margin-top: 10px;
    overflow-wrap: anywhere;
}
.mp-kpi-subtitle { position: relative; z-index: 1; color: var(--mp-text-faint); font-size: 10.5px; margin-top: 3px; }
.mp-kpi-track, .mp-score-track {
    height: 3px; border-radius: 99px; overflow: hidden;
    background: rgba(145,160,190,.12);
}
.mp-kpi-track { margin-top: 10px; position: relative; z-index: 1; }
.mp-kpi-track-fill { height: 100%; border-radius: inherit; transition: width 600ms var(--mp-ease); }

/* latest run panel */
.mp-latest-id { color: var(--mp-accent); font-family: var(--mp-mono); font-size: 11.5px; overflow-wrap: anywhere; margin-top: 8px; }
.mp-latest-model { color: var(--mp-text); font-size: 15px; font-weight: 600; margin-top: 6px; overflow-wrap: anywhere; }
.mp-muted-text { color: var(--mp-text-faint); font-size: 11px; line-height: 1.5; margin-top: 4px; }
.mp-score-row { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-top: 14px; }
.mp-score-number { font-family: var(--mp-mono); font-size: 30px; font-weight: 600; line-height: 1.2; }
.mp-score-number span { font-size: 12px; color: var(--mp-text-faint); font-weight: 400; }
.mp-band-pill {
    border: 1px solid; border-radius: 4px; padding: 3px 8px;
    font-family: var(--mp-mono); font-size: 9.5px; font-weight: 600; letter-spacing: .8px;
}
.mp-score-track { height: 5px; margin: 8px 0 12px; }
.mp-latest-facts {
    display: grid; grid-template-columns: 1fr 1fr; gap: 10px 14px;
    padding: 10px 0; border-top: 1px solid var(--mp-border);
}
.mp-fact { display: flex; flex-direction: column; gap: 3px; }
.mp-fact strong { text-align: left; }

.mp-contributor-header { display: flex; justify-content: space-between; gap: 10px; color: var(--mp-text); font-size: 12px; margin: 10px 0 5px; }
.mp-contributor-header strong { color: var(--mp-accent); font-family: var(--mp-mono); }
.mp-recommendation {
    display: flex; align-items: flex-start; gap: 10px; padding: 10px 12px; margin: 6px 0;
    background: var(--mp-surface-2); border: 1px solid var(--mp-border);
    border-left: 3px solid var(--mp-accent); border-radius: var(--mp-radius-sm);
    color: var(--mp-text); font-size: 12px; line-height: 1.6;
    transition: border-color var(--mp-fast) var(--mp-ease), transform var(--mp-fast) var(--mp-ease);
}
.mp-recommendation:hover { border-color: var(--mp-accent); transform: translateX(2px); }
.mp-recommendation-number { color: var(--mp-accent); font-family: var(--mp-mono); font-size: 11px; font-weight: 600; }

.mp-empty-state {
    text-align: center; padding: 26px 16px;
    border: 1px dashed var(--mp-border-strong); border-radius: var(--mp-radius);
    background: var(--mp-surface-2);
}
.mp-empty-icon { color: var(--mp-accent); font-size: 24px; margin-bottom: 6px; }
.mp-empty-title { color: var(--mp-text); font-size: 13px; font-weight: 600; }
.mp-empty-copy { color: var(--mp-text-faint); font-size: 11.5px; line-height: 1.6; margin-top: 4px; }

/* =========================================================
   HISTORY-SPECIFIC (shared tokens)
   ========================================================= */
.history-stat {
    min-height: 92px; padding: 12px 14px;
    background: var(--mp-surface); border: 1px solid var(--mp-border);
    border-left: 3px solid var(--h-accent, var(--mp-accent));
    border-radius: var(--mp-radius); box-sizing: border-box;
    transition: transform 200ms var(--mp-ease), border-color 200ms var(--mp-ease),
                box-shadow 200ms var(--mp-ease);
}
.history-stat:hover {
    transform: translateY(-3px); border-color: var(--h-accent, var(--mp-accent));
    box-shadow: 0 10px 24px rgba(0,0,0,.32);
}
.history-stat.blue   { --h-accent: #3d9bff; }
.history-stat.green  { --h-accent: #34d399; }
.history-stat.orange { --h-accent: #fb923c; }
.history-stat.purple { --h-accent: #a78bfa; }
.history-stat-label {
    color: var(--mp-text-dim); font-size: 10px; font-weight: 600;
    letter-spacing: .9px; text-transform: uppercase;
}
.history-stat-value {
    color: #fff; font-family: var(--mp-mono); font-size: 24px; font-weight: 600;
    line-height: 1.2; margin: 8px 0 3px; overflow-wrap: anywhere;
}
.history-stat-note { color: var(--mp-text-faint); font-size: 10.5px; }

/* =========================================================
   RESPONSIVE / ACCESSIBILITY
   ========================================================= */
/* =========================================================
   NATIVE METRICS, SUBHEADINGS, TABS
   ========================================================= */
[data-testid="stMetric"] {
    background: var(--mp-surface);
    border: 1px solid var(--mp-border);
    border-left: 3px solid var(--mp-accent);
    border-radius: var(--mp-radius);
    padding: 10px 14px;
    transition: transform 200ms var(--mp-ease), border-color 200ms var(--mp-ease),
                box-shadow 200ms var(--mp-ease);
}
[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    border-color: var(--mp-accent);
    box-shadow: 0 8px 20px rgba(0,0,0,.3);
}
[data-testid="stMetricLabel"] p {
    color: var(--mp-text-dim) !important;
    font-size: 10.5px !important;
    font-weight: 600 !important;
    letter-spacing: .8px;
    text-transform: uppercase;
}
[data-testid="stMetricValue"] {
    font-family: var(--mp-mono);
    font-size: 22px !important;
    font-weight: 600 !important;
    color: #fff;
}
[data-testid="stMetricDelta"] { font-size: 11px !important; }

.stApp h2, .stApp h3, [data-testid="stHeading"] h3 {
    display: flex; align-items: center; gap: 8px;
    font-size: 13px !important; font-weight: 600 !important;
    letter-spacing: .8px; text-transform: uppercase;
    color: var(--mp-text) !important;
    padding: 0 !important; margin-top: .6rem;
}
.stApp h3::before {
    content: ""; width: 3px; height: 13px; border-radius: 2px; background: var(--mp-accent);
}

[data-baseweb="tab-list"] { gap: 4px; border-bottom: 1px solid var(--mp-border); }
[data-baseweb="tab"] {
    height: 36px; padding: 0 14px; font-size: 12px; font-weight: 600;
    color: var(--mp-text-dim); border-radius: var(--mp-radius-sm) var(--mp-radius-sm) 0 0;
    transition: color var(--mp-fast) var(--mp-ease), background var(--mp-fast) var(--mp-ease);
}
[data-baseweb="tab"]:hover { color: var(--mp-text); background: var(--mp-accent-soft); }
[data-baseweb="tab"][aria-selected="true"] { color: var(--mp-accent); }
[data-baseweb="tab-highlight"] { background: var(--mp-accent) !important; height: 2px; }

/* =========================================================
   DIAGNOSTIC PANELS (run details / drift analysis)
   ========================================================= */
.mp-meta-grid {
    display: grid; grid-template-columns: repeat(3, 1fr);
    gap: 0; margin-top: 10px;
    border: 1px solid var(--mp-border); border-radius: var(--mp-radius-sm);
    overflow: hidden;
}
.mp-meta-item {
    padding: 10px 14px; border-right: 1px solid var(--mp-border);
    border-bottom: 1px solid var(--mp-border);
    transition: background var(--mp-fast) var(--mp-ease);
}
.mp-meta-item:hover { background: var(--mp-surface-2); }
.mp-meta-item:nth-child(3n) { border-right: 0; }
.mp-meta-item:nth-last-child(-n+3) { border-bottom: 0; }
.mp-meta-item span {
    display: block; color: var(--mp-text-faint); font-size: 10px; font-weight: 600;
    letter-spacing: .8px; text-transform: uppercase; margin-bottom: 3px;
}
.mp-meta-item strong {
    color: var(--mp-text); font-family: var(--mp-mono); font-size: 12.5px;
    font-weight: 500; overflow-wrap: anywhere;
}
.mp-stackbar {
    display: flex; height: 8px; border-radius: 99px; overflow: hidden;
    background: rgba(145,160,190,.12); margin: 10px 0 6px;
}
.mp-stackbar > div { height: 100%; transition: width 600ms var(--mp-ease); }
.mp-legend { display: flex; flex-wrap: wrap; gap: 14px; font-size: 11px; color: var(--mp-text-dim); }
.mp-legend i { display: inline-block; width: 8px; height: 8px; border-radius: 2px; margin-right: 6px; }
.mp-pill {
    display: inline-block; border: 1px solid; border-radius: 4px; padding: 3px 8px;
    font-family: var(--mp-mono); font-size: 10px; font-weight: 600; letter-spacing: .8px;
}
.mp-big-value { font-family: var(--mp-mono); font-size: 26px; font-weight: 600; color: #fff; margin-top: 8px; }
.mp-big-value small { font-size: 11px; color: var(--mp-text-faint); font-weight: 400; margin-left: 6px; }

@media (max-width: 760px) {
    .mp-meta-grid { grid-template-columns: 1fr 1fr; }
    .mp-meta-item:nth-child(3n) { border-right: 1px solid var(--mp-border); }
    .mp-meta-item:nth-child(2n) { border-right: 0; }
    .mp-page-header { flex-direction: column; align-items: flex-start; }
    .mp-kpi-modern { min-height: 94px; padding: 10px 12px; }
    .mp-kpi-value { font-size: 20px; }
}
@media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation: none !important; transition: none !important; }
}
</style>
"""


# =========================================================
# BADGES
# =========================================================

_SEVERITY_COLORS = {
    "low": ("#34d399", "rgba(52,211,153,.10)"),
    "moderate": ("#fbbf24", "rgba(251,191,36,.10)"),
    "high": ("#fb923c", "rgba(251,146,60,.10)"),
    "critical": ("#f87171", "rgba(248,113,113,.12)"),
}


def severity_badge(band: str, score: Optional[int] = None) -> str:
    label = (band or "Unknown").strip()
    normalized = label.lower()
    css_class = f"badge-{normalized}" if normalized in _SEVERITY_COLORS else "badge-low"
    color, background = _SEVERITY_COLORS.get(normalized, ("#9aa7bb", "rgba(154,167,187,.10)"))
    score_text = f" · {score}" if score is not None else ""

    return (
        f'<span class="badge {css_class}" style="color:{color};'
        f'background:{background};border:1px solid {color};'
        f'padding:3px 8px;border-radius:4px;font-size:.68rem;'
        f"font-family:'IBM Plex Mono',monospace;"
        f'font-weight:600;letter-spacing:.6px;white-space:nowrap;">'
        f'{escape(label.upper())}{score_text}</span>'
    )


def status_badge(status: str) -> str:
    label = (status or "unknown").strip()
    normalized = label.lower()

    ok = ("#34d399", "rgba(52,211,153,.10)")
    bad = ("#f87171", "rgba(248,113,113,.12)")
    warn = ("#fbbf24", "rgba(251,191,36,.10)")

    styles = {
        "ok": ok, "healthy": ok, "completed": ok, "delivered": ok,
        "quality_failure": bad, "failed": bad, "error": bad,
        "all_quarantined": warn, "suppressed_cooldown": warn, "degraded": warn,
    }
    color, background = styles.get(normalized, ("#9aa7bb", "rgba(154,167,187,.10)"))

    return (
        f'<span style="background:{background};color:{color};'
        f'border:1px solid {color};padding:3px 8px;border-radius:4px;'
        f"font-family:'IBM Plex Mono',monospace;"
        f'font-size:.68rem;font-weight:600;letter-spacing:.6px;'
        f'white-space:nowrap;">{escape(label.upper())}</span>'
    )


# =========================================================
# DATAFRAME HELPER
# =========================================================

def render_df(df, hide_index: bool = True, **kwargs):
    import streamlit as st

    try:
        return st.dataframe(df, width="stretch", hide_index=hide_index, **kwargs)
    except TypeError:
        return st.dataframe(df, use_container_width=True, hide_index=hide_index, **kwargs)