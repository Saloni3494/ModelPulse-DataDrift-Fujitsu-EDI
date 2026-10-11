# ModelPulse v2 — Phase 6: Monitoring Dashboard & Alerting Report

**Date:** 2026-10-09  
**Specification:** Phase 6 — Monitoring Dashboard, Alerting Subsystem & Presentation Layer  
**Status:** Completed & Validated  
**Test Suite:** **131 passed**, 1 skipped (optional live broker integration test) in 42.41s  

---

## Executive Summary

Phase 6 implements the presentation and alerting layer for **ModelPulse v2**, an enterprise MLOps platform monitoring transaction fraud models. Building strictly on top of the existing FastAPI backend, persistence abstractions, statistical drift metrics library, and 5-minute streaming pipeline, Phase 6 introduces:

1. **Streamlit Monitoring Dashboard (`dashboard/`):** A clean, professional 6-tab monitoring dashboard delivering real-time overview metrics, time-ordered run history, feature-level statistical drift analyses with ensemble voting, mathematical severity score breakdowns ($D, P, Q, M$), streaming health telemetry with conservation reconciliation, and alert audit logs.
2. **Multi-Channel Alerting Subsystem (`backend/app/alerting/`):** Enterprise Slack Block Kit and SMTP Email alert dispatch with severity band filtering, deterministic fingerprinting, configurable deduplication cooldown windows, bounded exponential retries, and strict delivery verification guarantees (HTTP 200 / SMTP envelope confirmation).
3. **Dual-Mode Dashboard Client (`dashboard/client.py`):** Primary connectivity via FastAPI REST endpoints with seamless local fallback to repository storage and memory telemetry if the backend server is offline.
4. **Data Integrity & Labeling Protections:** Unambiguous visual badging of synthetic drift fixtures, strict separation of data drift from model performance degradation, explicit "Unavailable" handling for delayed ground-truth labels (Component $M$), and preservation of event-time vs ingestion-time semantics.

---

## 1. Architecture & Component Structure

```text
ModelPulse-1/
├── backend/
│   ├── app/
│   │   ├── alerting/                    # Phase 6 Alerting Subsystem
│   │   │   ├── __init__.py              # Alerting exports
│   │   │   ├── models.py                # AlertSeverity, AlertDeliveryStatus, AlertPayload, AlertRecord
│   │   │   ├── slack.py                 # SlackAlertChannel (Block Kit, HTTP 200 verification, retries)
│   │   │   ├── email.py                 # EmailAlertChannel (HTML/plain multipart, SMTP, retries)
│   │   │   └── service.py               # AlertService (thresholds, deduplication, cooldown, dispatch)
│   │   ├── api/
│   │   │   └── monitoring.py            # Phase 6 endpoints: /streaming-health, /alerts, /alerts/status
│   │   └── streaming/
│   │       └── consumer.py              # Alert dispatch integration upon micro-batch flush
│   ├── tests/
│   │   ├── test_phase6_alerting.py      # 9 unit & integration tests for alerting subsystem
│   │   └── test_phase6_dashboard.py     # 7 unit tests for UI transformations, badges, and client fallback
├── dashboard/
│   ├── app.py                           # Main Streamlit application entrypoint
│   ├── client.py                        # Unified REST API + direct repository client
│   └── components/
│       ├── __init__.py                  # Component exports
│       ├── styles.py                    # CSS design tokens, metric cards, severity/status badges
│       ├── overview.py                  # Tab 1: System overview, latest run, disclaimers
│       ├── history.py                   # Tab 2: Paginated historical runs with multi-filter
│       ├── run_details.py               # Tab 3: Temporal coordinates, DQ breakdown, metrics
│       ├── drift_analysis.py            # Tab 4: Feature-level drift table & ensemble voting
│       ├── severity_breakdown.py        # Tab 5: D/P/Q/M decomposition & weight renormalization
│       ├── streaming_health.py          # Tab 6: Streaming telemetry & conservation reconciliation
│       └── alerting_view.py             # Tab 7: Alert channel status, cooldown, & audit log
└── docs/
    └── phase6_implementation_report.md  # This comprehensive engineering report
```

---

## 2. Dashboard Implementation & UX Design

The dashboard is built with Vanilla CSS design tokens injected via Streamlit's `st.markdown`, providing high visual polish, crisp contrast, and clear state indicators:

### 2.1 Tab 1 — System Overview
- **Backend & Repository Health:** Displays connection status (`API Connected` or `Active Local Fallback`), persistence type (`in_memory` or `sqlite`), and an explicit persistence limitation notice:
  > *"Persistence Note: Current backend uses in-memory or single-node SQLite. Historical runs will reset upon process restart if configured as in_memory."*
- **Latest Monitoring Run Card:** Prominently displays Run ID, Model Version, Sample Size, Overall Severity Score, and Severity Band (`Low`, `Medium`, `High`, `Critical`).
- **Synthetic Data Disclaimer:** Whenever a displayed run originates from a simulation or fixture, a clear alert informs operators:
  > *"Synthetic Fixture: This run was generated from synthetic drift simulations. Results do not represent real-world PaySim fraud model performance."*

### 2.2 Tab 2 — Monitoring History
- **Multi-Filter Toolbar:** Time-ordered runs filterable by Date Range, Severity Band (`Low`, `Medium`, `High`, `Critical`), and Run Status (`completed`, `failed`).
- **Summary Metrics:** Total Runs, Critical Count, Average Severity Score, and Quarantined Records.
- **Interactive Deep-Dive:** Selecting any run row populates the Run Details and Drift Analysis tabs. Handles empty histories cleanly with guidance to trigger new monitoring runs.

### 2.3 Tab 3 — Run Details & Temporal Coordinates
- **Tri-Temporal Coordinates:** Clearly separates three distinct timestamps:
  1. **Event Window:** Earliest and latest event-time (`event_time_start` to `event_time_end`) of transactions in the batch.
  2. **Ingestion Window:** Arrival time into the streaming buffer.
  3. **Monitoring Execution Time:** When the statistical monitoring pipeline executed.
- **Data Quality Isolation:** Lists detected schema and quality anomalies, including quarantined record counts, null checks, and range bounds.
- **Model Performance Notice (Component M):** Explains why performance metrics are unavailable when ground-truth labels are delayed.

### 2.4 Tab 4 — Drift Analysis & Ensemble Voting
- **Top Drifted Features:** Visual ranking of top contributors with drift magnitude.
- **Ensemble Voting Table:** For each feature, displays individual statistical test results and enforces the $k \ge 2/3$ ensemble rule:
  - **Numeric Features:** Population Stability Index (PSI), Kolmogorov-Smirnov (KS) test ($p$-value), and Wasserstein Distance normalized by standard deviation.
  - **Categorical Features:** Categorical PSI, Chi-Square test ($p$-value), Jensen-Shannon (JS) Distance, and explicit counts of unseen categories.
- **Ensemble Consensus:** A feature is flagged as drifted if and only if at least 2 of the 3 statistical tests exceed their thresholds.

### 2.5 Tab 5 — Severity Breakdown ($D, P, Q, M$)
- **Mathematical Formula:** Visualizes the weighted severity equation:
  $$\text{Severity} = \frac{w_D \cdot D + w_P \cdot P + w_Q \cdot Q + w_M \cdot M}{\sum w_{\text{active}}}$$
- **Component Decomposition:**
  - $D$ (Data Drift): Contribution from feature ensemble drift.
  - $P$ (Prediction Drift): Shift in model output probability distribution (Prediction PSI).
  - $Q$ (Data Quality): Proportion of malformed, missing, or out-of-range records.
  - $M$ (Model Performance): Degraded PR-AUC / Recall when ground-truth labels exist.
- **Unavailable Metric Distinguishability:** When labels are missing, Component $M$ is displayed as **Unavailable** (never zero), and base weights ($D=0.40, P=0.20, Q=0.15$) dynamically renormalize to effective weights ($D=0.533, P=0.267, Q=0.200$).

### 2.6 Tab 6 — Streaming Health & Conservation Reconciliation
- **Real-Time Counters:** Total arrivals, valid processed, quarantined records, DLQ handoffs, duplicate deliveries, and retry failures.
- **Conservation Reconciliation Table:** Verifies that no streaming records are lost or created out of thin air according to the identity:
  $$\text{Total Ingested} = \text{Valid Processed} + \text{Quarantined} + \text{Dead Letter Queue (DLQ)} + \text{Retry Failures} + \text{Duplicate Deliveries}$$

### 2.7 Tab 7 — Alerting Readiness & Audit Log
- **Channel Readiness Indicators:** Status badges for Slack Webhook and SMTP Email (`Configured` vs `Unconfigured / Disabled`).
- **Anti-Storm Telemetry:** Active cooldown window setting (default 1800s) and fingerprint caching state.
- **Audit History Table:** Complete log of recent alert evaluations, delivery statuses (`delivered`, `suppressed_cooldown`, `skipped_disabled`, `failed`), timestamps, and error diagnostics.

---

## 3. Alerting Subsystem & Delivery Guarantees

### 3.1 Severity Thresholding & Fingerprinting
Alerts are filtered against `ALERT_MIN_SEVERITY` (default: `Medium`). Only runs with severity $\ge \text{threshold}$ are evaluated for dispatch:
- `Low` (Rank 0) $\rightarrow$ Informational, no alert.
- `Medium` (Rank 1) $\rightarrow$ Dispatches when `ALERT_MIN_SEVERITY <= Medium`.
- `High` (Rank 2) $\rightarrow$ Dispatches when `ALERT_MIN_SEVERITY <= High`.
- `Critical` (Rank 3) $\rightarrow$ Immediate high-priority dispatch.

Each run receives a deterministic alert fingerprint:
$$\text{Fingerprint} = \text{hash}(\text{model\_name} : \text{model\_version} : \text{severity\_band} : \text{run\_type})$$

### 3.2 Anti-Storm Deduplication & Cooldown
- **Cooldown Window:** Configurable via `ALERT_COOLDOWN_SECONDS` (default: 1800s / 30 minutes).
- **Suppression:** If an alert for the same fingerprint was evaluated within the cooldown window, subsequent dispatches are suppressed and recorded as `AlertDeliveryStatus.SUPPRESSED_COOLDOWN`.

### 3.3 Delivery Verification
An alert is **never** reported as `DELIVERED` until positive confirmation is received from the transport provider:
- **Slack:** Requires HTTP 200 response with `{"ok": true}` from the incoming webhook endpoint.
- **Email:** Requires successful completion of the SMTP transaction (`send_message`).
- **Bounded Retries:** Failed dispatches undergo up to 3 retries with exponential backoff (`0.2s * 2^attempt`). If all retries fail, status is recorded as `AlertDeliveryStatus.FAILED` with full diagnostic error messages.

### 3.4 Zero-Credential Resilience
When `SLACK_WEBHOOK_URL` or `SMTP_HOST` are omitted or contain placeholders, the alerting service records `AlertDeliveryStatus.SKIPPED_DISABLED` without raising unhandled exceptions or disrupting monitoring workflows.

---

## 4. Verification & Test Results

### 4.1 Test Execution Summary

The test suite was executed against the project virtual environment `.\.venv\Scripts\python.exe`.

```text
======================= 131 passed, 1 skipped in 42.41s =======================
```

#### Detailed Phase 6 Test Breakdown (`pytest backend/tests/test_phase6*.py -v`):
```text
backend/tests/test_phase6_alerting.py::test_alert_severity_ranking PASSED [  6%]
backend/tests/test_phase6_alerting.py::test_alert_threshold_filtering PASSED [ 12%]
backend/tests/test_phase6_alerting.py::test_alert_cooldown_and_deduplication PASSED [ 18%]
backend/tests/test_phase6_alerting.py::test_slack_channel_success PASSED [ 25%]
backend/tests/test_phase6_alerting.py::test_slack_channel_failure_and_retries PASSED [ 31%]
backend/tests/test_phase6_alerting.py::test_email_channel_success PASSED [ 37%]
backend/tests/test_phase6_alerting.py::test_email_channel_failure_and_retries PASSED [ 43%]
backend/tests/test_phase6_alerting.py::test_missing_credentials_gracefully_skipped PASSED [ 50%]
backend/tests/test_phase6_alerting.py::test_alert_audit_history_and_status PASSED [ 56%]
backend/tests/test_phase6_dashboard.py::test_severity_badge_rendering PASSED [ 62%]
backend/tests/test_phase6_dashboard.py::test_status_badge_rendering PASSED [ 68%]
backend/tests/test_phase6_dashboard.py::test_dashboard_client_local_fallback_when_api_offline PASSED [ 75%]
backend/tests/test_phase6_dashboard.py::test_dashboard_client_empty_history PASSED [ 81%]
backend/tests/test_phase6_dashboard.py::test_component_m_unavailable_distinguished_from_zero PASSED [ 87%]
backend/tests/test_phase6_dashboard.py::test_temporal_coordinates_and_quarantine_integrity PASSED [ 93%]
backend/tests/test_phase6_dashboard.py::test_feature_drift_ensemble_voting_audit PASSED [100%]
```

### 4.2 Skipped Test Justification
- **Skipped Test:** `backend/tests/test_phase5.py::test_live_kafka_broker_integration`
- **Reason:** Requires a running live Apache Kafka broker on `localhost:9092`. As per the test design, live external broker tests are skipped automatically when `KAFKA_MODE != "broker"` to ensure zero-infrastructure portability during automated CI/CD.

---

## 5. Reproducible UI Verification Procedure

To verify the Streamlit dashboard interactively:

### 1. Launch FastAPI Backend
```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
```
Verify health:
```powershell
curl http://localhost:8000/health
```

### 2. Generate a Demonstration Monitoring Run
Trigger a monitoring run via the API or simulation script:
```powershell
curl -X POST http://localhost:8000/monitoring/runs \
  -H "Content-Type: application/json" \
  -d '{"sample_count": 500, "run_type": "simulator", "scenario_id": "sudden_drift_high"}'
```

### 3. Launch Streamlit Dashboard
```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.port 8501
```
Navigate to `http://localhost:8501`:
1. Check **Overview Tab**: Verify that the latest run is displayed with its severity badge and synthetic data disclaimer.
2. Check **History Tab**: Filter runs by severity band `High` or `Critical`.
3. Check **Drift Analysis Tab**: Confirm statistical ensemble vote badges (`DRIFT DETECTED` vs `STABLE`) on `amount`, `oldbalanceOrg`, etc.
4. Check **Severity Breakdown Tab**: Verify that Component $M$ displays "Unavailable" and that the weight renormalization table sums to $1.00$.
5. Check **Streaming Health Tab**: Confirm the conservation reconciliation equation balances.
6. Check **Alerting Tab**: Verify channel status and audit log entries.

---

## 6. Known Limitations & Architecture Boundaries

1. **Storage Persistence:** Default execution uses an in-memory repository or local SQLite file (`data/modelpulse.db`). It does not use TimescaleDB or PostgreSQL in this phase.
2. **Component M Ground Truth Lag:** Transaction fraud labels arrive with realistic multi-day delays. Model performance degradation cannot be evaluated without ground truth.
3. **No Automated Retraining:** Retraining triggers remain human-in-the-loop; no automatic model redeployment or promotion is permitted.
4. **Streaming Guarantees:** Streaming guarantees are at-least-once with idempotent micro-batching and deduplication. Exactly-once processing is not claimed.
5. **Mocked Alerting in Tests:** Live Slack webhooks and SMTP email connections are mocked during automated tests to avoid sending notifications to real external services.

---

## 7. Deliverables Summary

| Deliverable | Path | Status |
|---|---|---|
| Streamlit Dashboard Entrypoint | [`dashboard/app.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/dashboard/app.py) | Complete |
| Dashboard Backend Client | [`dashboard/client.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/dashboard/client.py) | Complete |
| Dashboard UI Components | [`dashboard/components/`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/dashboard/components/) | Complete |
| Alerting Data Models & Enums | [`backend/app/alerting/models.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/models.py) | Complete |
| Slack Webhook Channel | [`backend/app/alerting/slack.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/slack.py) | Complete |
| SMTP Email Channel | [`backend/app/alerting/email.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/email.py) | Complete |
| Alert Service & Cooldown | [`backend/app/alerting/service.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/service.py) | Complete |
| Alerting Automated Tests | [`backend/tests/test_phase6_alerting.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/tests/test_phase6_alerting.py) | Complete (9 passed) |
| Dashboard Automated Tests | [`backend/tests/test_phase6_dashboard.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/tests/test_phase6_dashboard.py) | Complete (7 passed) |
| Environment Template | [`.env.example`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/.env.example) | Complete |
| Project Documentation | [`README.md`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/README.md) | Complete |
| Engineering Report | [`docs/phase6_implementation_report.md`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/docs/phase6_implementation_report.md) | Complete |
