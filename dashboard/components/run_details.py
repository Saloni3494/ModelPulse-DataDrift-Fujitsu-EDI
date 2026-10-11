"""ModelPulse Run Details Component (Phase 6).
Displays deep-dive diagnostics for an individual monitoring run:
- Event-time vs ingestion-time vs evaluation window.
- Sample sizes, valid vs quarantined counts.
- Data-quality findings.
- Performance degradation observations (clearly distinguishing delayed labels from zero performance).
"""
from typing import Any, Dict, Optional
import pandas as pd
import streamlit as st

from dashboard.components.styles import severity_badge, status_badge


def render_run_details(run: Optional[Dict[str, Any]]):
    """Render comprehensive diagnostic view for an individual monitoring run."""
    if not run:
        st.info("Select a monitoring run from History or Overview to inspect detailed diagnostics.")
        return

    st.subheader(f"Run Diagnostics: `{run.get('run_id')}`")

    # Header Card
    sev = run.get("severity", {})
    score = sev.get("score", 0)
    band = sev.get("band", "Low")

    col_h1, col_h2, col_h3, col_h4 = st.columns(4)
    with col_h1:
        st.metric("Composite Severity", f"{score}/100", delta=band)
    with col_h2:
        st.metric("Total Evaluation Samples", run.get("sample_count", 0))
    with col_h3:
        st.metric("Valid Scored Records", run.get("valid_sample_count", "N/A"))
    with col_h4:
        st.metric("Quarantined Records", run.get("quarantined_sample_count", 0))

    st.markdown("---")

    # 1. Temporal & Window Specifications
    st.markdown("#### 1. Temporal Coordinates & Time Semantics")
    st.caption(
        "ModelPulse strictly separates **Event Time** (transaction generation), "
        "**Ingestion Time** (Kafka broker arrival), and **Monitoring Window** (5-minute micro-batch evaluation bounds)."
    )

    cur_win = run.get("current_window", {})
    ref_win = run.get("reference_window", {})

    t_cols = st.columns(3)
    with t_cols[0]:
        st.write("**Evaluation Window Start:**")
        st.code(str(cur_win.get("start", run.get("created_at"))))
    with t_cols[1]:
        st.write("**Evaluation Window End:**")
        st.code(str(cur_win.get("end", run.get("created_at"))))
    with t_cols[2]:
        st.write("**Execution Timestamp (UTC):**")
        st.code(str(run.get("created_at")))

    # 2. Data Quality & Quarantine Findings
    st.markdown("#### 2. Data Quality & Feature Quarantine Analysis")
    q_count = run.get("quarantined_sample_count", 0)
    if q_count > 0:
        st.warning(
            f"⚠️ **{q_count} record(s) isolated from model inference.** "
            "Invalid or out-of-bounds features were quarantined to protect model scoring and statistical distributions from corrupted inputs."
        )
    else:
        st.success("✅ **Zero records quarantined.** All incoming evaluation records satisfied required schema and domain bounds.")

    # 3. Model Performance vs Data Drift
    st.markdown("#### 3. Model Performance Evaluation (Component M)")
    st.info(
        "ℹ️ **Performance Degradation vs Data Drift Distinction:** "
        "Statistical data drift measures covariate distribution shift between reference and current inference traffic. "
        "Actual model performance metrics (e.g. PR-AUC degradation) can **only** be evaluated when ground-truth fraud labels are confirmed. "
        "In production financial systems, fraud feedback is inherently delayed (30–90 days chargeback cycle)."
    )

    comp_m = sev.get("components", {}).get("M")
    if comp_m is not None:
        st.metric("Observed Component M (PR-AUC Drop)", f"{comp_m:.2f}/100")
    else:
        st.markdown(
            """
            <div style="background-color: rgba(30, 41, 59, 0.7); border: 1px dashed #64748b; padding: 12px 16px; border-radius: 6px;">
                <strong>Performance Metrics Status:</strong> <span style="color: #94a3b8;">Unavailable (Delayed Ground-Truth Labels)</span><br>
                <span style="font-size: 0.85rem; color: #94a3b8;">
                    No verified ground-truth labels accompanied this micro-batch.
                    ModelPulse dynamically renormalizes component weights across available evidence (D, P, Q) rather than assuming zero performance impact.
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 4. Recommendations
    recs = run.get("recommendations", [])
    if recs:
        st.markdown("---")
        st.markdown("#### 4. Engine Automated Recommendations")
        for r in recs:
            st.markdown(f"- 💡 {r}")
