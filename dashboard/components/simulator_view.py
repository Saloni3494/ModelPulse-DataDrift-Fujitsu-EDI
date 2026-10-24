"""ModelPulse Simulator Component (Phase 6).
Provides interactive UI for generating controlled drift scenarios and benchmarking the monitoring engine.
"""
from typing import Any, Dict, Optional
import httpx
import pandas as pd
import streamlit as st

from dashboard.components.styles import CUSTOM_CSS, severity_badge, status_badge, render_df
from simulator.scenarios import (
    SYNTHETIC_DISCLAIMER,
    load_or_create_reference,
    scenario_no_drift,
    scenario_numeric_mean_variance_shift,
    scenario_categorical_proportion_shift,
    scenario_unseen_categories,
    scenario_sudden_drift,
    scenario_gradual_drift,
    scenario_data_quality_failures,
)

SCENARIOS_META = {
    "numeric_mean_variance_shift": {
        "name": "Numeric Mean & Variance Shift",
        "description": "Scales transaction amounts (1.8x) and shifts account balances to test numeric drift metrics.",
        "expected_band": "High (Severity ~50-60)",
        "features": ["amount", "oldbalanceOrg", "newbalanceOrig"],
    },
    "no_drift": {
        "name": "No Drift (Clean Baseline)",
        "description": "Draws samples directly from reference distribution to verify false positive suppression.",
        "expected_band": "Low (Severity ~0-15)",
        "features": [],
    },
    "categorical_proportion_shift": {
        "name": "Categorical Proportion Shift",
        "description": "Substantially alters transaction type distribution (e.g. transfers spike to 60%).",
        "expected_band": "Moderate/High",
        "features": ["type"],
    },
    "unseen_categories": {
        "name": "Unseen Categories Injection",
        "description": "Injects brand-new category values (e.g. CRYPTO_OUT) to evaluate zero-count handling.",
        "expected_band": "Moderate",
        "features": ["type"],
    },
    "sudden_drift": {
        "name": "Sudden Concept Drift",
        "description": "Abrupt distribution shock injected halfway through the sample window.",
        "expected_band": "High",
        "features": ["amount", "oldbalanceOrg"],
    },
    "gradual_drift": {
        "name": "Gradual Concept Drift",
        "description": "Linearly ramps shift magnitude across the batch from 0% to 100%.",
        "expected_band": "Moderate/High",
        "features": ["amount", "oldbalanceOrg"],
    },
    "data_quality_failures": {
        "name": "Data Quality Corruptions",
        "description": "Injects negative amounts, null balances, invalid step coordinates, and schema errors.",
        "expected_band": "Critical (Quality Failure / Component Q ~100)",
        "features": ["amount", "step", "oldbalanceOrg"],
    },
}


def render_simulator_view(api_url: str = "http://localhost:8000"):
    """Render the controlled drift simulator interface."""
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    st.title("⚡ ModelPulse Controlled Drift Simulator")
    st.caption("Interactive benchmarking console for evaluating drift detection, data quality checks, and severity elevation.")

    st.info(
        f"🧪 **Synthetic Fixture Notice:** {SYNTHETIC_DISCLAIMER}. "
        "Generated scenarios test platform detection boundaries and never modify clean baseline artifacts."
    )

    # 1. Configuration Controls
    st.subheader("1. Scenario Configuration")
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        scenario_key = st.selectbox(
            "Select Drift Scenario:",
            list(SCENARIOS_META.keys()),
            format_func=lambda k: f"{SCENARIOS_META[k]['name']} ({k})",
            index=0,
        )

    with col2:
        n_samples = st.slider("Sample Count:", min_value=50, max_value=2000, value=500, step=50)

    with col3:
        seed = st.number_input("Random Seed:", min_value=1, max_value=99999, value=42, step=1)

    meta = SCENARIOS_META[scenario_key]
    st.markdown(
        f"""
        <div style="background-color: rgba(30, 41, 59, 0.7); border-left: 4px solid #3b82f6; padding: 10px 16px; border-radius: 4px; margin: 10px 0 20px 0;">
            <strong>Scenario:</strong> {meta['name']}<br>
            <strong>Description:</strong> {meta['description']}<br>
            <strong>Expected Operational Band:</strong> <code>{meta['expected_band']}</code> &nbsp;|&nbsp;
            <strong>Targeted Features:</strong> <code>{', '.join(meta['features']) if meta['features'] else 'None (Clean)'}</code>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2. Execution Triggers
    st.subheader("2. Execute Benchmark Run")
    run_col1, run_col2, run_col3 = st.columns(3)

    with run_col1:
        if st.button("🚀 Run On-Demand Monitoring Cycle", use_container_width=True):
            with st.spinner("Executing monitoring run via API..."):
                payload = {
                    "scenario_id": scenario_key,
                    "scenario_seed": seed,
                    "scenario_samples": n_samples,
                    "run_type": "simulator",
                }
                try:
                    with httpx.Client(timeout=15.0) as client:
                        resp = client.post(f"{api_url}/monitoring/runs", json=payload)
                        if resp.status_code in (200, 201):
                            st.session_state["sim_run_result"] = resp.json()
                            st.success(f"Run completed successfully! ID: `{resp.json().get('run_id')}`")
                        else:
                            st.error(f"API Error ({resp.status_code}): {resp.text}")
                except Exception as exc:
                    st.error(f"Failed to connect to backend at {api_url}: {exc}")

    with run_col2:
        if st.button("🌊 Replay to Live Streaming Bus", use_container_width=True):
            with st.spinner("Publishing events to message bus..."):
                payload = {
                    "scenario_id": scenario_key,
                    "seed": seed,
                    "n_samples": n_samples,
                }
                try:
                    with httpx.Client(timeout=15.0) as client:
                        resp = client.post(f"{api_url}/monitoring/streaming/replay", json=payload)
                        if resp.status_code == 200:
                            data = resp.json()
                            st.success(f"Published {data.get('published_count')} events to stream topic in {data.get('duration_seconds')}s!")
                        else:
                            st.error(f"Streaming Replay Error ({resp.status_code}): {resp.text}")
                except Exception as exc:
                    st.error(f"Failed to connect to backend at {api_url}: {exc}")

    with run_col3:
        if st.button("📦 Flush Streaming Micro-Batch", use_container_width=True):
            try:
                with httpx.Client(timeout=15.0) as client:
                    resp = client.post(f"{api_url}/monitoring/streaming/flush")
                    if resp.status_code == 200:
                        data = resp.json()
                        st.info(f"Micro-batch flushed! Status: `{data.get('status')}` | Batch ID: `{data.get('batch_id')}`")
                    else:
                        st.error(f"Flush Error ({resp.status_code}): {resp.text}")
            except Exception as exc:
                st.error(f"Connection error: {exc}")

    # 3. Benchmark Results Display
    if "sim_run_result" in st.session_state:
        res = st.session_state["sim_run_result"]
        st.markdown("---")
        st.subheader("3. Monitoring Run Assessment")

        sev = res.get("severity", {})
        score = sev.get("score", 0)
        band = sev.get("band", "Low")
        drift_detected = res.get("dataset_drift_detected", False)
        status_val = res.get("status", "ok")

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Composite Severity Score", f"{score}/100")
        with m2:
            st.markdown(f"**Severity Band:**<br>{severity_badge(band)}", unsafe_allow_html=True)
        with m3:
            st.markdown(f"**Drift Detected:**<br>{status_badge('DRIFT DETECTED' if drift_detected else 'OK')}", unsafe_allow_html=True)
        with m4:
            st.markdown(f"**Run Status:**<br>{status_badge(status_val)}", unsafe_allow_html=True)

        # Severity Component Breakdown
        comps = sev.get("components", {})
        c_cols = st.columns(4)
        with c_cols[0]:
            d_val = comps.get("D")
            st.metric("Component D (Data Drift)", f"{d_val:.2f}" if d_val is not None else "N/A")
        with c_cols[1]:
            p_val = comps.get("P")
            st.metric("Component P (Prediction Drift)", f"{p_val:.2f}" if p_val is not None else "N/A")
        with c_cols[2]:
            q_val = comps.get("Q")
            st.metric("Component Q (Data Quality)", f"{q_val:.2f}" if q_val is not None else "N/A")
        with c_cols[3]:
            m_val = comps.get("M")
            st.metric("Component M (Model Accuracy)", f"{m_val:.2f}" if m_val is not None else "Labels Pending")

        # Top Contributors Table
        top_contribs = res.get("top_contributors", [])
        if top_contribs:
            st.markdown("#### Top Drifted Feature Contributors")
            tc_data = [
                {
                    "Feature Name": c.get("feature_name"),
                    "Contribution Score": round(c.get("contribution_score", 0.0), 4),
                    "Cumulative Share": f"{c.get('cumulative_share', 0.0) * 100:.1f}%",
                }
                for c in top_contribs
            ]
            render_df(pd.DataFrame(tc_data))

    # 4. Scenario Ground-Truth Preview
    st.markdown("---")
    with st.expander("🔍 Inspect Ground-Truth Synthetic Batch Preview"):
        ref_df = load_or_create_reference()
        if scenario_key == "numeric_mean_variance_shift":
            sample_df, manifest = scenario_numeric_mean_variance_shift(ref_df, seed=seed, n_samples=min(100, n_samples))
        elif scenario_key == "no_drift":
            sample_df, manifest = scenario_no_drift(ref_df, seed=seed, n_samples=min(100, n_samples))
        elif scenario_key == "categorical_proportion_shift":
            sample_df, manifest = scenario_categorical_proportion_shift(ref_df, seed=seed, n_samples=min(100, n_samples))
        elif scenario_key == "unseen_categories":
            sample_df, manifest = scenario_unseen_categories(ref_df, seed=seed, n_samples=min(100, n_samples))
        elif scenario_key == "sudden_drift":
            sample_df, manifest = scenario_sudden_drift(ref_df, seed=seed, n_samples=min(100, n_samples))
        elif scenario_key == "gradual_drift":
            sample_df, manifest = scenario_gradual_drift(ref_df, seed=seed, n_samples=min(100, n_samples))
        elif scenario_key == "data_quality_failures":
            sample_df, manifest = scenario_data_quality_failures(ref_df, seed=seed, n_samples=min(100, n_samples))
        else:
            sample_df, manifest = ref_df.head(100), None

        st.caption(f"Showing first {len(sample_df)} generated records:")
        render_df(sample_df.head(20))
        if manifest:
            st.json(manifest.model_dump())
