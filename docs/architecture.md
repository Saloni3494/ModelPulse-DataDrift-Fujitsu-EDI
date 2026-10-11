# ModelPulse — System Architecture & Interface Specification

**Specification Version:** 2.0 (Phase 0 Foundation)  
**Status:** Architecture Design & Validated Interface Proposals  
**Reference Alignment:** [ModelPulse_Approach_v2_Full.md](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/ModelPulse_Approach_v2_Full.md)

---

## 1. Architectural Overview

ModelPulse is an end-to-end MLOps platform for continuous data drift, prediction drift, and concept drift monitoring. It bridges real-time streaming ingestion with deep statistical validation, explainable root-cause attribution, and human-in-the-loop candidate model evaluation.

### 1.1 Guiding Principles
1. **Single Modular Application:** Implemented as a clean, modular FastAPI backend rather than fragile distributed microservices.
2. **Honest States:** Missing data, small sample sizes ($n < \text{min\_samples}$), or absent ground-truth labels result in explicit `"insufficient_evidence"` or `"unavailable"` states—never masked as zero or healthy.
3. **Quality Precedes Drift:** Data quality failures (schema errors, missing columns, corrupt values) are quarantined with explicit failure reasons; data quality faults are never misreported as statistical drift.
4. **Ensemble-Driven Confirmation:** Drift is declared only when multi-method statistical tests agree ($k$-of-$m$ voting rule), mitigating single-test sensitivity and false positives.
5. **Separation of Generation & Delivery:** Alerts are permanently recorded regardless of external delivery channel (Slack/email) availability.
6. **Human-in-the-Loop Governance:** Candidate models produced during retraining are evaluated against the current champion and require explicit operator approval before promotion.

---

## 2. System Architecture & Data Flow

```text
Dataset Replay / Drift Simulator ──► Kafka Topic: events
                                           │   (invalid rows ──► Topic: events.dlq)
                                           ▼
                       Consumer: Validate (Pandera) ──► Score (Model)
                                           │
                                           ▼
                      TimescaleDB Hypertable: prediction_events
                                           │
                      5-minute micro-batch continuous aggregates
                                           │
             ┌─────────────────────────────┴──────────────────────────────┐
             ▼                                                            ▼
   Fast Path (Per Micro-Batch):                              Hourly Job (APScheduler):
   EWMA / PSI Early Warning                                  Full Ensemble (Trailing Window)
             └─────────────────────────────┬──────────────────────────────┘
                                           ▼
                     Data Drift · Prediction Drift · Data Quality
                                           ▼
                   Attribution (Importance × Drift, SHAP Shift)
                                           ▼
                         Deterministic Severity (0–100)
                                           ▼
                     Alert Rules ──► Persist ──► Slack / Email
                                           ▼
                         Rule-Based Recommendation Engine
                                           ▼
                                Streamlit Dashboard
```

---

## 3. Modular Backend Service Boundaries

The backend (`backend/app/`) is structured into decoupled domain services:

| Service Module | Location | Primary Responsibility |
|---|---|---|
| `prediction_service` | `backend/app/services/prediction_service.py` | Loads approved champion model; executes batch/single inference; formats responses. |
| `validation_service` | `backend/app/services/validation_service.py` | Validates ingestion schemas, ranges, and types; quarantines corrupt records with reason codes. |
| `drift_service` | `backend/app/services/drift_service.py` | Extracts reference and production windows; computes PSI, KS, Wasserstein, $\chi^2$, JS; executes ensemble voting. |
| `severity_service` | `backend/app/services/severity_service.py` | Computes transparent 0–100 score across $D, P, Q, M$ components with dynamic weight renormalisation. |
| `explanation_service`| `backend/app/services/explanation_service.py`| Computes TreeExplainer SHAP values on samples; calculates $\Delta\text{SHAP}$; ranks top contributors. |
| `alert_service` | `backend/app/services/alert_service.py` | Evaluates rule criteria; persists alerts; dispatches Slack webhooks and SMTP emails; logs delivery outcomes. |
| `retraining_service` | `backend/app/services/retraining_service.py` | Prepares holdouts; trains challenger models; evaluates champion vs. challenger; exposes approval gate. |
| `simulator` | `simulator/` | Injects controlled sudden, gradual, seasonal, category, and quality shifts; generates ground-truth JSON manifests. |

---

## 4. Validated Interface & Contract Proposals

> [!NOTE]
> The schemas below have been defined in `backend/app/schemas/` during Phase 0 to ensure team alignment. They represent the agreed interface contracts that endpoints and database tables will implement in Phases 1 and 2.

### 4.1 Ingestion Contracts (`POST /predict` and `POST /predict/batch`)
- **Location:** [`backend/app/schemas/predictions.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/predictions.py)

```json
// Request Payload (Single)
{
  "event_id": "4a713b18-b2a6-4c3a-9669-79a613cfeb86",
  "model_name": "fraud-detector",
  "model_version": "1.0.0",
  "features": {
    "amount": 1250.50,
    "type": "TRANSFER",
    "oldbalanceOrg": 5000.0,
    "newbalanceOrig": 3749.50
  },
  "timestamp": "2026-10-09T18:00:00Z"
}

// Response Payload (Single)
{
  "event_id": "4a713b18-b2a6-4c3a-9669-79a613cfeb86",
  "model_name": "fraud-detector",
  "model_version": "1.0.0",
  "prediction": 0,
  "score": 0.035,
  "validation_status": "valid",
  "error_reason": null,
  "timestamp": "2026-10-09T18:00:00.120Z"
}
```

### 4.2 Per-Feature Multi-Method Drift Contract
- **Location:** [`backend/app/schemas/drift.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/drift.py)

```json
{
  "feature_name": "amount",
  "feature_type": "numeric",
  "tests": [
    {
      "metric": "psi",
      "value": 0.28,
      "threshold": 0.25,
      "p_value": null,
      "exceeded": true
    },
    {
      "metric": "ks",
      "value": 0.18,
      "threshold": 0.10,
      "p_value": 0.0002,
      "exceeded": true
    },
    {
      "metric": "wasserstein",
      "value": 0.15,
      "threshold": 0.30,
      "p_value": null,
      "exceeded": false
    }
  ],
  "votes": "2/3",
  "votes_count": 2,
  "total_tests": 3,
  "drift_detected": true,
  "drift_contribution": 0.32,
  "reference_mean": 820.15,
  "current_mean": 1340.80
}
```

### 4.3 Monitoring Run & Severity Report Contract (`GET /monitoring/runs/{run_id}`)
- **Location:** [`backend/app/schemas/monitoring.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/monitoring.py)

```json
{
  "run_id": "936d8d85-f129-4d6f-99e0-81f185ef4396",
  "model_name": "fraud-detector",
  "model_version": "1.0.0",
  "run_type": "hourly",
  "reference_window": {
    "start": "2026-09-01T00:00:00Z",
    "end": "2026-09-30T23:59:59Z",
    "sample_count": 50000
  },
  "current_window": {
    "start": "2026-10-09T17:00:00Z",
    "end": "2026-10-09T18:00:00Z",
    "sample_count": 4800
  },
  "sample_count": 4800,
  "status": "ok",
  "dataset_drift_detected": true,
  "prediction_drift": {
    "metric": "psi",
    "value": 0.19,
    "threshold": 0.25,
    "drifted": false
  },
  "severity": {
    "score": 62,
    "band": "High",
    "confidence": "normal",
    "components": {
      "D": 68.5,
      "P": 38.0,
      "Q": 4.0,
      "M": null
    },
    "weights_used": {
      "D": 0.53,
      "P": 0.27,
      "Q": 0.20
    }
  },
  "features": [ /* Array of FeatureDriftResult */ ],
  "top_contributors": [
    { "feature_name": "amount", "contribution_score": 0.32, "cumulative_share": 0.47 },
    { "feature_name": "oldbalanceOrg", "contribution_score": 0.21, "cumulative_share": 0.78 }
  ],
  "recommendations": [
    "Data drift confirmed on features: amount, oldbalanceOrg.",
    "Performance impact unknown (labels delayed). Collect recent ground truth before initiating candidate retraining."
  ],
  "created_at": "2026-10-09T18:00:05Z"
}
```

---

## 5. Severity Score Formula & Deterministic Computation

The overall severity score $S \in [0, 100]$ is computed as:

$$\text{Severity} = \frac{\sum_{c \in \text{available}} W_c \cdot C}{\sum_{c \in \text{available}} W_c}$$

### Component Definitions:
1. **Data Drift ($D \in [0, 100]$):**
   $$D = 100 \times \frac{\sum_f w_f \cdot \text{clip}\left(\frac{\text{PSI}_f}{\text{PSI}_{\text{crit}}}, 0, 1\right)}{\sum_f w_f}$$
   Where $w_f$ is feature importance (uniform fallback if unassigned). Features that fail ensemble voting contribute 0.
2. **Prediction Drift ($P \in [0, 100]$):**
   $$P = 100 \times \text{clip}\left(\frac{\text{PSI}_{\text{score}}}{\text{PSI}_{\text{crit}}}, 0, 1\right)$$
3. **Data Quality ($Q \in [0, 100]$):**
   $$Q = 100 \times \text{clip}\left(\frac{\text{failure\_rate}}{q_{\max}}, 0, 1\right)$$
4. **Performance Degradation ($M \in [0, 100]$):**
   $$M = 100 \times \text{clip}\left(\frac{\text{baseline} - \text{current}}{\text{baseline} - \text{critical\_floor}}, 0, 1\right)$$
   **Status:** Explicitly flagged as `null` / `"unavailable"` when delayed labels are absent. Weights automatically renormalise across $D, P, Q$.

### Severity Bands:
- **0–24:** Low (Normal operations)
- **25–49:** Moderate (Early warning monitoring)
- **50–74:** High (Investigation recommended)
- **75–100:** Critical (Alert triggered, operator intervention required)

---

## 6. Implementation Staging Roadmap & Status

- **Phase 0 (Complete):** Unified workspace, Git setup, pinned core dependencies, smoke testing, interface validation.
- **Phase 1 (Complete):** PaySim financial dataset pipeline, baseline HistGradientBoosting classifier, chronological non-overlapping splits, zero leakage, validation-tuned threshold ($\tau=0.05$), baseline artifacts (`model_pipeline.joblib`, `baseline_metrics.json`, `reference_features.csv`).
- **Phase 2 (Complete):** Standalone statistical drift metrics library: numeric (PSI, KS, normalized Wasserstein), categorical (PSI, $\chi^2$, Jensen-Shannon), unseen category tracking, $k$-of-$m$ ensemble voting rule.
- **Phase 3 (Complete):** Controlled drift simulator (7 reproducible scenarios with `ScenarioManifest`), multi-component severity engine ($D, P, Q, M$ with dynamic weight renormalisation, $[0, 100]$ bounding, monotonicity guarantee).
- **Phase 4 (Complete):** Inference API (`POST /predict`, `POST /predict/batch`, `GET /health`), monitoring orchestration (`POST /monitoring/runs`, `GET /monitoring/runs`, `GET /monitoring/runs/{run_id}`), and persistence repository foundation (`MonitoringRepository` interface, `InMemoryMonitoringRepository`, `SQLiteMonitoringRepository`, TimescaleDB DDL migration `001_create_monitoring_schema.sql`).
- **Phase 4.1 (Complete):** Correctness audit resolving severity score documentation discrepancy, strict quarantine isolation in inference, and explicit sample denominators.
- **Phase 5 (Current - Complete):** Kafka streaming pipeline (`aiokafka`), deterministic producer replay (`DeterministicReplayService`), Dead Letter Queue (`DeadLetterQueueService`), 5-minute micro-batching (`MicroBatcher`), strict manual offset commit policy, bounded retries, and operational telemetry (`StreamingMetrics`).
- **Phase 6 (Next):** Alert dispatch (Slack/email), Streamlit dashboard reading persistent storage.
