# ModelPulse — MLOps Model & Data Drift Monitoring Platform

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Framework: FastAPI](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

ModelPulse is an end-to-end MLOps monitoring platform designed to continuously detect **data drift**, **prediction drift**, and **concept drift**. It features multi-method statistical ensemble voting, transparent 0–100 severity scoring, root-cause drift attribution (importance-weighted and SHAP shifts), automated alerts with delivery guarantees, and human-in-the-loop model retraining.

Specification and design details: [ModelPulse Approach v2 (Full)](ModelPulse_Approach_v2_Full.md) and [docs/architecture.md](docs/architecture.md).

---

## 1. Project Structure

```text
ModelPulse-1/
├── backend/
│   ├── app/
│   │   ├── main.py             # FastAPI entrypoint
│   │   ├── api/                # API route definitions
│   │   ├── core/               # Configuration and application settings
│   │   ├── schemas/            # Pydantic request/response/monitoring contracts
│   │   ├── services/           # Prediction, validation, drift, severity services
│   │   ├── streaming/          # Kafka producer, consumer, and DLQ
│   │   ├── db/                 # Database models, sessions, and Alembic migrations
│   │   └── jobs/               # Hourly detection and fast-path monitoring jobs
│   ├── tests/                  # Backend unit, integration, and smoke tests
│   └── requirements.txt        # Pinned core backend dependencies
├── ml/                         # Baseline model training, evaluation, and artifacts
├── simulator/                  # Drift scenarios and ground-truth manifest generator
├── dashboard/                  # Streamlit monitoring dashboard
├── docs/                       # Architecture, audit reports, and evaluation results
├── references/                 # Upstream reference repositories (isolated)
├── .env.example                # Template environment variables
├── .gitignore                  # Git hygiene rules
└── requirements.txt            # Root dependencies pointer
```

---

## 2. Quickstart & Local Setup

### Prerequisites
- Python 3.10+ (tested on Python 3.12)
- Git
- Docker Desktop (for TimescaleDB and Kafka in Phase 1+)

### Installation

1. **Create and activate the virtual environment:**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
   *(If PowerShell displays a script execution policy error, run: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`)*

2. **Install pinned dependencies:**
   ```powershell
   pip install -r backend/requirements.txt
   ```

3. **Initialize Model Artifacts (Clean Checkout):**
   If `ml/artifacts/model_pipeline.joblib` is not present, train and audit the baseline in ~7 seconds:
   ```powershell
   python ml/train.py --synthetic-demo
   ```

4. **Configure environment variables (Optional):**
   ```powershell
   Copy-Item .env.example .env
   ```
   *ModelPulse runs out of the box with safe defaults (in-memory or SQLite persistence) even without a `.env` file.*

---

## 3. Running the Test Suite

Run the full regression test suite (178 tests passing across Phases 0–8, 1 skipped live Kafka gate):

```powershell
.\.venv\Scripts\python.exe -m pytest backend/tests -v
```

To run Phase 7 end-to-end integration tests specifically:
```powershell
.\.venv\Scripts\python.exe -m pytest backend/tests/test_phase7_e2e.py -v
```

---

## 4. Local Startup & Demonstration Workflow (Phase 8)

### Step 1: Start the FastAPI Backend Service
In terminal 1:
```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```
- API health check: `http://localhost:8000/health`
- Interactive OpenAPI documentation: `http://localhost:8000/docs`

### Step 2: Start the Streamlit Monitoring Dashboard
In terminal 2:
```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.port 8501
```
The dashboard automatically connects to `http://localhost:8000` (or falls back cleanly to local SQLite storage if the backend is stopped).

### Step 3: Trigger an End-to-End Monitoring Cycle
In terminal 3, generate controlled drift via the REST API:
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/monitoring/runs" -Method Post -ContentType "application/json" -Body '{"scenario_id": "numeric_mean_variance_shift", "scenario_seed": 42, "scenario_samples": 500, "run_type": "simulator"}'
```
Refresh the dashboard at `http://localhost:8501` to view:
- Elevated severity band (`High` or `Critical`)
- Multi-method ensemble voting drift detection
- Top drift contributors (`amount`, `oldbalanceOrg`)
- Dynamic weight renormalization ($M$ marked `Unavailable`)
- Staged alert outbox records and delivery status

---

## 5. Troubleshooting Common Issues

| Issue / Error | Root Cause | Solution |
| :--- | :--- | :--- |
| **Port 8000 or 8501 already in use** | A prior uvicorn or streamlit process is still active. | Terminate process via PowerShell: `Get-Process -Id (Get-NetTCPConnection -LocalPort 8000).OwningProcess -ErrorAction SilentlyContinue \| Stop-Process -Force` |
| **`FileNotFoundError: ml/artifacts/...`** | Clean repository checkout without trained model artifacts. | Run `python ml/train.py --synthetic-demo` to generate all 5 versioned artifacts in ~7 seconds. |
| **`ExecutionPolicy` error on `Activate.ps1`** | Windows PowerShell default execution policy blocks scripts. | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` in PowerShell. |
| **Dashboard shows "Process Isolated" warning** | Streamlit is running with `PERSISTENCE_TYPE=in_memory` while FastAPI is offline. | Start FastAPI or set `PERSISTENCE_TYPE=sqlite` in `.env` to share persistent SQLite history across processes. |
| **Kafka live broker test skipped** | Docker or local Kafka broker on port 9092 is offline. | Normal behavior. In-memory bus is 100% verified. Start broker with `docker-compose up -d kafka` to unblock live gate. |

---

## 6. Dashboard Features & Architecture

The Streamlit dashboard (`dashboard/app.py`) provides:
- **System Overview:** Real-time health, backend connectivity, persistence limitation warnings, latest monitoring run card, and synthetic data disclaimers.
- **Monitoring History:** Time-ordered historical run table with date range, severity band, and status filtering, plus individual run deep-dives.
- **Drift Analysis:** Feature-level drift table with statistical ensemble voting ($k \ge 2/3$ rule across PSI, Kolmogorov-Smirnov, and Wasserstein distance for numeric; PSI, Chi-Square, and Jensen-Shannon for categorical), top drift contributors, and p-values.
- **Severity Breakdown:** Mathematical breakdown of components $D$ (Data Drift), $P$ (Prediction Drift), $Q$ (Data Quality), and $M$ (Model Performance). Displays dynamic weight renormalization when Component $M$ is unavailable due to delayed ground truth.
- **Streaming Health & Reconciliation:** Micro-batch window metadata, counters for arrivals, valid, quarantined, DLQ, failures, and duplicates, with a formal conservation reconciliation table ($Arrivals = Valid + Quarantined + DLQ + Failures + Duplicates$).
- **Alert Audit & History:** Live channel status (Slack & Email readiness), cooldown window telemetry, and paginated delivery audit trail.

---

## 7. Alerting Subsystem & Delivery Guarantees

ModelPulse v2 includes an enterprise alerting subsystem (`backend/app/alerting/`):
- **Slack Webhook Channel:** Delivers structured Block Kit alerts featuring severity color-coding, model metadata, top drifted features, and quarantine counts.
- **Email SMTP Channel:** Delivers multipart (HTML + plain text) notifications with tabular component breakdowns.
- **Transactional Outbox:** Alerts are persisted atomically with monitoring runs in the same SQLite transaction before dispatch.
- **Lease Fencing:** Recovery workers acquire expiring leases with monotonic `claim_token` fencing, preventing duplicate dispatches across concurrent or resurrected workers.
- **Delivery Verification:** Alerts transition to `DELIVERED` only when the provider confirms acceptance.
- **Anti-Storm Protection:** Enforces configurable deduplication and cooldown windows (default: 1800s) based on canonical alert fingerprints (`model_name:model_version:severity_band:run_type`).
- **Zero-Credential Resilience:** When webhook URLs or SMTP credentials are unconfigured, alerting status is recorded as `SKIPPED_DISABLED` without throwing runtime exceptions or crashing monitoring workflows.

---

## 8. Known Architectural Limitations & Release Gates

1. **Live Kafka Broker Release Gate (BLOCKED on local host without Docker):** The in-memory bus and offset management are 100% verified (178 unit/integration tests). However, end-to-end delivery against a live Apache Kafka broker requires an active broker listening on `localhost:9092`. Start with `docker-compose up -d kafka` before running `test_live_kafka_broker_integration`.
2. **Storage Persistence:** Default mode is in-memory or single-node SQLite (`data/modelpulse.db`). History does not survive process restarts unless SQLite is selected.
3. **Delayed Ground Truth (Component M):** Ground truth labels for fraudulent transactions arrive with realistic days-to-weeks delays. When ground truth is unavailable, Component $M$ is displayed as **Unavailable** (never zero), and weights dynamically renormalize across $D, P, Q$.
4. **Synthetic Data Labeling:** Synthetic simulations and fixture data are explicitly badged as synthetic and must not be interpreted as real-world PaySim fraud performance.
5. **No Automated Retraining:** Model promotion and retraining are strictly human-in-the-loop and are not automatically triggered.
6. **Streaming Invariants:** Event processing adheres to at-least-once streaming semantics with deterministic batch fingerprint deduplication.

---

## 9. Implementation Status

- **Phase 0 (Complete):** Workspace hygiene, dependency management, and smoke tests.
- **Phase 1 (Complete):** Reproducible baseline PaySim fraud model training pipeline and artifact serialization.
- **Phase 2 (Complete):** Standalone statistical drift-metrics library (numeric: PSI, KS, Wasserstein; categorical: PSI, Chi2, JS).
- **Phase 3 (Complete):** Controlled drift simulator and 4-component ($D, P, Q, M$) severity engine.
- **Phase 4 (Complete):** FastAPI REST API, model inference, and persistence foundation.
- **Phase 4.1 (Complete):** Severity calculation correction, prediction schema quarantine integrity, and acid audit fixes.
- **Phase 5 (Complete):** Kafka event streaming, 5-minute micro-batching, DLQ, and deduplication cache.
- **Phase 5.1 (Complete):** Live consumer offset safety, independent partition tracking, and watermark persistence audit.
- **Phase 5.2 (Complete):** Offset tracker gap safety, broker seek handling, and event-time quarantine integrity.
- **Phase 6 (Complete):** Streamlit monitoring dashboard, Slack/Email alerting subsystem, delivery verification, and health reconciliation.
- **Phase 6.1 (Complete):** Dashboard fallback provenance, alert cooldown caching, and transport failure hardening.
- **Phase 6.2 (Complete):** Durable alert outbox foundation, crash-after-commit recovery, and fingerprint isolation.
- **Phase 6.3 (Complete):** Atomic run + outbox transactions, worker concurrency lease locking, and SMTP partial retry.
- **Phase 6.4 (Complete):** Monotonic lease fencing tokens, terminal state protection, and crash injection safety.
- **Phase 7 (Complete):** End-to-end integration audit, normal/drift/injected failure workflows, dashboard/API consistency, and live Kafka release gate verification.
- **Phase 8 (Complete):** Deployment readiness, reproducible demo validation, security and git packaging audit, troubleshooting guide, and release readiness verification.


---

## 9. License & Attribution

ModelPulse is licensed under the MIT License. Upstream design references (`references/Sentinel` and `references/mlops-drift-detection-platform`) are kept isolated under their respective MIT licenses.

