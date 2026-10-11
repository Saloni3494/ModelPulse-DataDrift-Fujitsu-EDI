"""ModelPulse Drift Analysis Component (Phase 6).
Renders per-feature multi-test ensemble results, top contributors, and reference-vs-current distribution comparisons.
"""
from typing import Any, Dict, List, Optional
import pandas as pd
import streamlit as st

from dashboard.components.styles import status_badge, render_df


def render_drift_analysis(run: Optional[Dict[str, Any]]):
    """Render feature-level and prediction drift diagnostics."""
    if not run:
        st.info("Select a monitoring run to inspect feature drift analysis.")
        return

    st.subheader(f"Statistical Drift Analysis: `{run.get('run_id')}`")

    # 1. Prediction Drift Summary
    pred_drift = run.get("prediction_drift", {})
    p_cols = st.columns(3)
    with p_cols[0]:
        st.metric(
            "Prediction Score PSI",
            f"{pred_drift.get('value', 'Unavailable')}",
            help="Population Stability Index comparing inference model score distributions.",
        )
    with p_cols[1]:
        st.metric(
            "PSI Decision Threshold",
            f"{pred_drift.get('threshold', 0.25)}",
        )
    with p_cols[2]:
        p_drifted = pred_drift.get("drifted", False)
        st.markdown(
            f"""
            <div style="padding-top: 10px;">
                <strong>Prediction Drift:</strong> {status_badge('DRIFT DETECTED' if p_drifted else 'OK')}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # 2. Top Feature Contributors
    st.markdown("#### Top Drifted Feature Contributors")
    top_contributors = run.get("top_contributors", [])
    if top_contributors:
        tc_rows = []
        for tc in top_contributors:
            tc_rows.append({
                "Feature Name": tc.get("feature_name"),
                "Contribution Score": round(tc.get("contribution_score", 0.0), 4),
                "Cumulative Share": f"{tc.get('cumulative_share', 0.0) * 100:.1f}%",
            })
        render_df(pd.DataFrame(tc_rows))
    else:
        st.write("No features exceeded individual drift thresholds.")

    st.markdown("---")

    # 3. Comprehensive Feature Ensemble Results
    st.markdown("#### Multi-Method Feature Drift Ensemble")
    st.caption(
        "Each feature is evaluated using multiple statistical distance and goodness-of-fit metrics. "
        r"Ensemble voting rule: Drift is flagged if at least 2 of 3 evaluated tests exceed their threshold ($k \ge 2/3$)."
    )

    features = run.get("features", [])
    if not features:
        st.info("No feature drift evaluations recorded for this run.")
        return

    feature_summary_rows = []
    for f in features:
        ref_mean = f.get("reference_mean")
        cur_mean = f.get("current_mean")
        feature_summary_rows.append({
            "Feature": f.get("feature_name"),
            "Type": f.get("feature_type"),
            "Ensemble Votes": f.get("votes"),
            "Drift Detected": "YES" if f.get("drift_detected") else "NO",
            "Contribution": round(f.get("drift_contribution", 0.0), 4),
            "Ref Mean": f"{ref_mean:,.2f}" if ref_mean is not None else "—",
            "Cur Mean": f"{cur_mean:,.2f}" if cur_mean is not None else "—",
            "Unseen Categories": ", ".join(f.get("unseen_categories", [])) if f.get("unseen_categories") else "None",
        })

    render_df(pd.DataFrame(feature_summary_rows))

    # 4. Individual Statistical Test Inspector
    st.markdown("---")
    st.markdown("#### Detailed Test-Level Metric Breakdown")
    feat_names = [f.get("feature_name") for f in features]
    selected_feat_name = st.selectbox("Inspect individual statistical tests for feature:", feat_names, index=0)

    selected_feat = next((f for f in features if f.get("feature_name") == selected_feat_name), None)
    if selected_feat:
        tests = selected_feat.get("tests", [])
        test_rows = []
        for t in tests:
            p_val_str = f"{t.get('p_value'):.5f}" if t.get("p_value") is not None else "N/A"
            test_rows.append({
                "Statistical Test": t.get("metric").upper(),
                "Observed Statistic": round(t.get("value", 0.0), 4),
                "Decision Threshold": t.get("threshold"),
                "p-value": p_val_str,
                "Exceeded Threshold": "YES" if t.get("exceeded") else "NO",
                "Status": t.get("status"),
                "Reference Samples": t.get("reference_count"),
                "Current Samples": t.get("current_count"),
            })
        render_df(pd.DataFrame(test_rows))
