"""ModelPulse Streaming Ingestion & Reconciliation Component (Phase 6).
Displays live streaming telemetry, consumer status, micro-batching health, and the mathematically audited reconciliation table.
"""
from typing import Any, Dict
import pandas as pd
import streamlit as st

from dashboard.components.styles import render_df


def render_streaming_health(streaming_data: Dict[str, Any]):
    """Render streaming telemetry counters and mathematical reconciliation table."""
    st.subheader("Streaming Telemetry & Pipeline Health")

    summary = streaming_data.get("summary", {})
    reconciliation = streaming_data.get("reconciliation", {})

    # 1. Pipeline Telemetry KPI Cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Events Consumed", summary.get("events_consumed", 0))
    with c2:
        st.metric("Events Validated & Scored", summary.get("events_validated", 0))
    with c3:
        st.metric("Quarantined Events", summary.get("events_quarantined", 0))
    with c4:
        st.metric("Dead Letter Queue (DLQ)", summary.get("events_dlq", 0))

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.metric("Duplicate Events Dropped", summary.get("events_duplicated", 0))
    with c6:
        st.metric("Batches Completed", summary.get("batches_completed", 0))
    with c7:
        st.metric("Batches Failed", summary.get("batches_failed", 0))
    with c8:
        st.metric("Transient Retries", summary.get("events_retried", 0))

    st.markdown("---")

    # 2. Mathematical Event Reconciliation Table
    st.markdown("#### Audited Event Accounting & Reconciliation")
    st.caption(
        "Conservation Law: All consumed stream arrivals must strictly map to mutually exclusive terminal states "
        "or non-terminal filters ($Total = Valid + Quarantined + DLQ + Uncommitted + Duplicates$). Discrepancy must equal zero."
    )

    if reconciliation:
        recon_rows = [
            {
                "Accounting Category": "Total Ingested Arrivals",
                "Metric Description": "Raw messages received from Kafka topic",
                "Count": reconciliation.get("total_ingested_arrivals", 0),
                "Classification": "Total Stream Volume",
            },
            {
                "Accounting Category": "PROCESSED_VALID",
                "Metric Description": "Clean transactions validated and scored by model",
                "Count": reconciliation.get("processed_valid", 0),
                "Classification": "Terminal State",
            },
            {
                "Accounting Category": "PROCESSED_QUARANTINED",
                "Metric Description": "Schema-valid records with domain-level errors",
                "Count": reconciliation.get("processed_quarantined", 0),
                "Classification": "Terminal State",
            },
            {
                "Accounting Category": "DLQ_TERMINAL",
                "Metric Description": "Malformed payloads durably acknowledged in events.dlq",
                "Count": reconciliation.get("dlq_terminal", 0),
                "Classification": "Terminal State",
            },
            {
                "Accounting Category": "FAILED_UNCOMMITTED",
                "Metric Description": "Failed records with offsets held back for redelivery",
                "Count": reconciliation.get("failed_uncommitted", 0),
                "Classification": "Terminal State",
            },
            {
                "Accounting Category": "OBSERVED_DUPLICATE_DELIVERIES",
                "Metric Description": "Duplicate event IDs suppressed by deduplication filter",
                "Count": reconciliation.get("observed_duplicate_deliveries", 0),
                "Classification": "Operational Filter",
            },
            {
                "Accounting Category": "RETRY_ATTEMPTS",
                "Metric Description": "Broker write retries prior to DLQ acknowledgment",
                "Count": reconciliation.get("retry_attempts", 0),
                "Classification": "Network Metric (Isolated)",
            },
        ]
        render_df(pd.DataFrame(recon_rows))

        discrepancy = reconciliation.get("conservation_discrepancy", 0)
        if discrepancy == 0:
            st.success("✅ **Mathematical Conservation Verified:** Zero event loss or double counting (Discrepancy: 0).")
        else:
            st.error(f"❌ **Conservation Discrepancy Detected:** {discrepancy} unaccounted events!")
    else:
        st.info("No active reconciliation counters recorded yet.")

    st.markdown("---")

    # 3. Micro-Batching Specifications
    st.markdown("#### Micro-Batching Engine Parameters")
    b_cols = st.columns(3)
    with b_cols[0]:
        st.write("**Window Duration:** 5 Minutes (`300s`)")
        st.write("**Max Batch Size:** 1,000 Records")
    with b_cols[1]:
        st.write("**Deduplication Cache:** 50,000 Event IDs (In-Memory LRU)")
        st.write("**Consumer Group:** `modelpulse-consumer-group`")
    with b_cols[2]:
        st.write("**Input Topic:** `events` (2 Partitions)")
        st.write("**DLQ Topic:** `events.dlq` (`acks=all`)")
