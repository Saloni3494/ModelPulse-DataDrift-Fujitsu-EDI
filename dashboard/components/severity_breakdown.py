"""ModelPulse Severity Breakdown Component (Phase 6).
Displays multi-component severity scores (D, P, Q, M) and dynamic weight renormalization.
"""
from typing import Any, Dict, Optional
import pandas as pd
import streamlit as st

from dashboard.components.styles import severity_badge, render_df


def render_severity_breakdown(run: Optional[Dict[str, Any]]):
    """Render transparent severity score decomposition."""
    if not run:
        st.info("Select a monitoring run to view severity score breakdown.")
        return

    sev = run.get("severity", {})
    score = sev.get("score", 0)
    raw_score = sev.get("raw_score")
    band = sev.get("band", "Low")
    confidence = sev.get("confidence", "normal")
    components = sev.get("components", {})
    weights_used = sev.get("weights_used", {})

    st.subheader(f"Multi-Component Severity Engine Breakdown")

    # Header Card
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Final Composite Score", f"{score}/100", delta=band)
    with c2:
        raw_str = f"{raw_score:.2f}" if raw_score is not None else str(score)
        st.metric("Unrounded Raw Score", raw_str)
    with c3:
        st.metric("Evaluation Confidence", confidence.upper())

    st.markdown("---")
    st.markdown("#### Mathematical Formulation & Dynamic Renormalization")
    st.latex(
        r"S = \frac{w_D \cdot D + w_P \cdot P + w_Q \cdot Q + w_M \cdot M}{\sum_{i \in \text{Available}} w_i}"
    )
    st.caption(
        "When ground-truth labels are delayed (Component M unavailable), ModelPulse dynamically renormalizes "
        "the available component weights so the denominator equals the sum of available weights, preventing artificial score dampening."
    )

    # Component Breakdown Table
    component_metadata = [
        {
            "Key": "D",
            "Name": "Data Drift",
            "Description": "Aggregate feature covariate shift weighted by feature importance",
            "Default Weight": 0.40,
        },
        {
            "Key": "P",
            "Name": "Prediction Drift",
            "Description": "Inference score distribution PSI and positive prediction rate delta",
            "Default Weight": 0.20,
        },
        {
            "Key": "Q",
            "Name": "Data Quality Isolation",
            "Description": "Quarantine rate, schema violations, nulls, and out-of-range features",
            "Default Weight": 0.15,
        },
        {
            "Key": "M",
            "Name": "Performance Degradation",
            "Description": "Observed drop in PR-AUC against validation baseline",
            "Default Weight": 0.25,
        },
    ]

    rows = []
    for meta in component_metadata:
        k = meta["Key"]
        val = components.get(k)
        eff_weight = weights_used.get(k, 0.0)

        if val is not None:
            val_str = f"{val:.2f}/100"
            status_text = "Evaluated"
            weighted_contrib = round(val * eff_weight, 2)
            contrib_str = f"{weighted_contrib:.2f}"
        else:
            val_str = "Unavailable"
            status_text = "Missing (Labels Pending)" if k == "M" else "Unavailable"
            contrib_str = "N/A"

        rows.append({
            "Component": f"[{k}] {meta['Name']}",
            "Score": val_str,
            "Configured Base Weight": f"{meta['Default Weight'] * 100:.0f}%",
            "Effective Weight Used": f"{eff_weight * 100:.1f}%",
            "Weighted Contribution": contrib_str,
            "Evaluation Status": status_text,
            "Description": meta["Description"],
        })

    render_df(pd.DataFrame(rows))

    # Band Threshold Reference
    st.markdown("---")
    st.markdown("#### Severity Band Decision Thresholds")
    b_cols = st.columns(4)
    with b_cols[0]:
        st.markdown(f"**Low:** `0 – 24` &nbsp; {severity_badge('Low')}", unsafe_allow_html=True)
    with b_cols[1]:
        st.markdown(f"**Moderate:** `25 – 49` &nbsp; {severity_badge('Moderate')}", unsafe_allow_html=True)
    with b_cols[2]:
        st.markdown(f"**High:** `50 – 74` &nbsp; {severity_badge('High')}", unsafe_allow_html=True)
    with b_cols[3]:
        st.markdown(f"**Critical:** `75 – 100` &nbsp; {severity_badge('Critical')}", unsafe_allow_html=True)
