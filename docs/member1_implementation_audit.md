# ModelPulse — Member 1 Implementation & Workspace Audit Report

**Specification Document:** [ModelPulse_Approach_v2_Full.md](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/ModelPulse_Approach_v2_Full.md)  
**Audit Date:** October 9, 2026  
**Auditor:** ModelPulse Engineering (Member 1 - Ingestion, Data Pipeline & Architecture Lead)  
**Workspace Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1`

---

## 1. Executive Summary

This audit evaluates the current state of the `ModelPulse-1` workspace against the strict specification laid out in **ModelPulse Approach v2 (Standalone)**.

### Key Audit Findings:
1. **Workspace & Git Architecture:**
   - The workspace root `ModelPulse-1` is **not a Git repository**.
   - Instead, the workspace currently houses two cloned reference repositories:
     - `Sentinel/` (Debddj/Sentinel, 30 commits, MIT license).
     - `mlops-drift-detection-platform/` (Faheem Ahmad, 1 commit, MIT license).
   - Neither codebase currently represents the unified target ModelPulse architecture specified in v2 Section 7.
2. **Implementation Reality vs. Claims:**
   - **Baseline Model:** No time-based split fraud baseline exists. Sentinel trains on Kaggle's "Give Me Some Credit" dataset via random 80/20 split; Faheem's platform trains a regressor on NYC Taxi data. Neither saves the required reference set metadata or `baseline_metrics.json`.
   - **Drift Detection:** Both repositories implement isolated statistical tests (PSI, KS, Wasserstein), but **neither possesses an ensemble voting mechanism** ($k$-of-$m$ rule). In Faheem's codebase, drift classification is hard-coded strictly to PSI, completely discarding KS and Wasserstein results.
   - **Severity Scoring:** Completely absent. Faheem's platform has no severity scoring. Sentinel uses a primitive binary condition (`critical` if $|\text{score}| > 0.5$ else `warn`). The v2 deterministic 0–100 multi-component formula ($D, P, Q, M$) is not implemented anywhere.
   - **API & Data Storage:** No persistent hypertable or TimescaleDB continuous aggregates exist. Faheem's API returns in-memory predictions with zero database persistence. Sentinel uses standard PostgreSQL tables (`predictions`, `drift_events`), tightly coupled to Celery and Redis. Neither implements the unified ModelPulse v2 API contracts (`/monitoring/run`, `/labels`, `/retrain`, `/simulate`).
3. **Test Suite Integrity:**
   - Running the test suites reveals major breakage and hollow test stubs.
   - Faheem's test suite fails collection in `test_drift_detector.py` due to an unimported `pd` (`NameError: name 'pd' is not defined`), and 15 out of 44 tests fail in `test_model_serving.py` because FastAPI lifespan overrides test mocks with a non-existent MLflow registry model.
   - Sentinel's test suite crashes collection on 3 modules due to missing dependencies (`celery`, `prometheus_fastapi_instrumentator`). Furthermore, multiple detector unit tests (`test_ks.py`, `test_psi.py`, `test_cusum.py`) and integration tests (`test_alerts_api.py`, `test_drift_api.py`, `test_websocket.py`) are trivial 1-line stubs (`assert detector is not None` or `pass`).

---

## 2. Workspace & Environment Audit

### 2.1 Workspace Structure
```text
d:\BTech\TY\Sem 5\EDI\ModelPulse-1
├── ModelPulse_Approach_v2_Full.md       # Target v2 Specification
├── Sentinel/                            # Reference Repo A (Debddj/Sentinel)
│   ├── .git/                            # Git repo (branch main, clean)
│   ├── backend/                         # FastAPI + Celery + SQLAlchemy + Alembic
│   ├── frontend/                        # React/TypeScript UI
│   ├── ml/                              # Kaggle Credit XGBoost models & explainer
│   ├── infra/                           # Docker compose configs
│   ├── scripts/                         # Seeding & simulation scripts
│   └── docs/                            # Architecture & novelties docs
└── mlops-drift-detection-platform/      # Reference Repo B (Faheem Ahmad)
    ├── .git/                            # Git repo (branch main, clean)
    ├── app/                             # Streamlit dashboard
    ├── feast_repo/                      # Feast local feature store configuration
    ├── src/                             # Python ML pipeline & FastAPI serving
    ├── tests/                           # Pytest test suite
    ├── Dockerfile & docker-compose.yml  # Service containerization
    └── requirements.txt                 # Dependencies
```

### 2.2 Git Status
| Directory | Git Status | Branch / Upstream | Working Tree |
|---|---|---|---|
| `.` (Workspace Root) | **Not a Git Repository** (`fatal: not a git repository`) | N/A | Untracked root |
| `Sentinel/` | Git Repository | `main` (up to date with `origin/main`) | Clean (30+ commits) |
| `mlops-drift-detection-platform/` | Git Repository | `main` (up to date with `origin/main`) | Clean (1 initial commit) |

### 2.3 Dependencies & Runtime Environment
- **Python Runtime:** Python 3.12.4 (`C:\Users\Saloni\anaconda3\python.exe` on Windows).
- **Installed Packages:** `numpy 1.26.4`, `pandas 2.2.2`, `scipy 1.13.1`, `scikit-learn 1.5.1`, `xgboost 3.2.0`, `shap 0.44.1`, `fastapi 0.115.6`, `uvicorn 0.34.0`, `pydantic 2.10.4`, `pytest 8.3.4`.
- **Missing / Incompatible Packages for Reference Repos:**
  - `celery` (Required by Sentinel backend tasks): Not installed.
  - `prometheus-fastapi-instrumentator` (Required by Sentinel `app.main`): Not installed.
  - `psycopg` / `asyncpg` (Required by Sentinel DB layer): Not installed in active environment.
  - `evidently`: Installed, but API changes between versions require pinned calls.

### 2.4 License Assessment
- `mlops-drift-detection-platform/LICENSE`: **MIT License** (Author: Faheem Ahmad). Permissive; components can be ported provided copyright and license notices are retained.
- `Sentinel/LICENSE`: **MIT License** (Copyright 2026 Sentinel Contributors). Permissive; architectural concepts, SHAP delta, and EWMA logic can be ported with license attribution.

---

## 3. Actual Implementation Status vs. ModelPulse v2 Specification

| Feature Area | Specification in ModelPulse v2 | Status in `mlops-platform` | Status in `Sentinel` | Actual Implementation Verdict |
|---|---|---|---|---|
| **Dataset & Task** | Fraud/financial classification with real time-axis (PaySim / IEEE-CIS) | NYC Taxi trip duration regression (Parquet) | "Give Me Some Credit" binary classification (`cs-training.csv`) | ❌ **Non-compliant.** Neither uses PaySim/IEEE-CIS; no time-based splits implemented. |
| **Baseline Model & Training** | Time-based split, gradient boosting, baseline metrics in `baseline_metrics.json`, saved reference set with schema | LightGBM/XGBoost via Optuna; logs to MLflow; no saved baseline reference window schema | XGBClassifier trained with random 80/20 split; exports `xgb_model.pkl`, `explainer.pkl`; no metrics JSON | ⚠️ **Partial.** Models train, but lack time-based splitting, reference window artifacts, and baseline metric JSON records. |
| **Inference Ingestion & Persistence** | `POST /predict`, `POST /predict/batch`; persist to TimescaleDB `prediction_events` hypertable with event ID, version, timestamp, validation | Endpoints exist in `src/model_serving.py`; purely in-memory; **no persistence** | Endpoints exist (`/api/models/{id}/predict`, `/api/predictions/ingest`); writes to standard PostgreSQL; Redis stream | ⚠️ **Partial.** Neither uses TimescaleDB hypertables or ModelPulse event contracts. Faheem doesn't persist; Sentinel couples to Celery/Redis. |
| **Data Quality Validation** | Pandera validation: required columns, types, null limits, numeric bounds, unseen categories; quarantine with reasons | Custom checks in `src/data_quality.py` (Great Expectations style); not integrated into serving | Basic Pydantic validation on request payload; no statistical quarantine or reason logging | ⚠️ **Incomplete.** Faheem has offline validation; neither pipeline validates and quarantines online ingestion rows with error reasons. |
| **Drift Metrics Engine** | Numeric: PSI, KS, Wasserstein. Categorical: PSI, $\chi^2$, JS. Prediction drift: PSI/KS on scores | PSI, CSI, KS, Wasserstein in `src/drift_detector.py`. Streaming ADWIN, Page-Hinkley | PSI, KS, JS-Div, CUSUM, Page-Hinkley, IForest in `app/detectors/` | ⚠️ **Unfinished.** Tests exist in isolation, but $\chi^2$, categorical PSI, and normalized Wasserstein are missing. |
| **Ensemble Voting Logic** | Multi-method ensemble voting: drifted if $\ge k$ of $m$ tests exceed threshold (default 2 of 3); persistence check | ❌ **None.** `detect_drift_batch` classifies drift strictly by PSI. KS and Wasserstein are logged but ignored. | ❌ **None.** Individual detectors trigger individual `DriftEvent` DB rows independently. No voting rule. | ❌ **Missing.** Multi-method ensemble voting does not exist in either repository. |
| **Severity Score (0–100)** | Transparent, deterministic formula: $W_D D + W_P P + W_Q Q + W_M M$; configurable weights; unavailable flags | ❌ **None.** No composite severity scoring. | ❌ **Crude binary.** `severity = "critical" if abs(score) > 0.5 else "warn"` in Celery task. | ❌ **Missing.** No 0–100 multi-component severity engine exists. |
| **Drift Attribution & SHAP** | Rank by importance $\times$ drift; cumulative top-5 share; SHAP mean-absolute attribution shift | ❌ Listed as future work in README; no implementation. | ✅ `app/novelties/shap_attribution.py` computes $\Delta\text{SHAP}$ and top-movers; good design. | ⚠️ **Partial.** Sentinel has working $\Delta\text{SHAP}$ logic, but importance $\times$ drift ranking and cumulative top-5 share are missing. |
| **Streaming & Message Queue** | Kafka broker (KRaft), `events` and `events.dlq` topics; 5-min micro-batches; idempotent consumer | ❌ No Kafka; uses Prefect batch flows. | ❌ No Kafka; uses Celery + Redis Streams. | ❌ **Missing.** Kafka producer/consumer with DLQ must be built as an original contribution. |
| **Feature Store** | Feast local provider (Parquet offline, SQLite online) for serving consistency | Feast repo defined in `feast_repo/`, but not integrated with real-time serving | ❌ None. | ⚠️ **Incomplete.** Feast configuration exists in Faheem's repo, but lacks integration with the serving layer. |
| **Alerting & Notifications** | Decoupled generation & delivery; Slack webhook + SMTP email; delivery status tracked | ❌ Only logs alerts to JSON file; no dispatch. | ❌ Only persists alerts to DB and Redis pub/sub; no external dispatch. | ❌ **Missing.** Neither repository implements Slack webhook or SMTP delivery. |
| **Retraining & Promotion** | Candidate trained on holdout; evaluated vs champion; **human approval required** | `AutoRetrainer` promotes automatically without approval; NYC taxi regression only | ❌ None. | ⚠️ **Non-compliant.** Faheem has champion/challenger logic, but auto-promotes without the required human-in-the-loop gate. |
| **Simulation & Testing** | Controlled drift injection (sudden, gradual, seasonal, quality faults); **ground-truth manifest** | ❌ None. | `scripts/generate_real_predictions.py` injects noise, but creates no ground-truth manifest. | ❌ **Missing.** Ground-truth manifest generation is completely absent. |

---

## 4. Test Suite Execution & Defect Analysis

As mandated by project guidelines, all test suites were executed directly on the system. Below are the verbatim commands, exact outcomes, and root-cause analyses.

### 4.1 Test Run 1: `mlops-drift-detection-platform` Full Test Suite
- **Working Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1\mlops-drift-detection-platform`
- **Command:**
  ```powershell
  python -m pytest tests -v
  ```
- **Result:** **FAILED (Interrupted during collection)**
  - Tests collected: 80 items before interruption
  - Collection Errors: 1
  - Tests Run: 0
- **Exact Error Output:**
  ```text
  =================================== ERRORS ====================================
  ________________ ERROR collecting tests/test_drift_detector.py ________________
  tests\test_drift_detector.py:14: in <module>
      from src.drift_detector import DriftDetector, DriftLevel, DriftAlert
  src\drift_detector.py:62: in <module>
      class DriftDetector:
  src\drift_detector.py:485: in DriftDetector
      reference_df: pd.DataFrame,
  E   NameError: name 'pd' is not defined
  =========================== short test summary info ===========================
  ERROR tests/test_drift_detector.py - NameError: name 'pd' is not defined
  !!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
  ============================== 1 error in 6.08s ===============================
  ```
- **Root Cause:** In `src/drift_detector.py`, type annotation `reference_df: pd.DataFrame` is used at line 485 inside class definition `DriftDetector`, but `import pandas as pd` was omitted from module imports (placed only locally inside `detect_drift_batch` at line 502). This prevents the entire drift test module from loading.

---

### 4.2 Test Run 2: `mlops-drift-detection-platform` Feature Engineering Unit Tests
- **Working Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1\mlops-drift-detection-platform`
- **Command:**
  ```powershell
  python -m pytest tests/test_feature_engineering.py -v
  ```
- **Result:** **PASSED (36 / 36 passed in 3.33s)**
  - Tests Passed: 36
  - Tests Failed: 0
  - Errors: 0
- **Summary:** Verified temporal, geographic, trip-derived, interaction, and null-handling feature functions. All 36 tests execute cleanly.

---

### 4.3 Test Run 3: `mlops-drift-detection-platform` Model Serving Unit Tests
- **Working Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1\mlops-drift-detection-platform`
- **Command:**
  ```powershell
  python -m pytest tests/test_model_serving.py -v
  ```
- **Result:** **FAILED (29 passed, 15 failed in 24.88s)**
  - Total Tests: 44
  - Tests Passed: 29
  - Tests Failed: 15
- **Failed Test Cases:**
  1. `TestHealthEndpoint::test_health_status_healthy_with_model`
  2. `TestPredictEndpoint::test_predict_returns_200`
  3. `TestPredictEndpoint::test_predict_returns_duration`
  4. `TestPredictEndpoint::test_predict_value_is_correct`
  5. `TestPredictEndpoint::test_predict_includes_model_version`
  6. `TestPredictEndpoint::test_predict_includes_timestamp`
  7. `TestPredictEndpoint::test_predict_increments_counter`
  8. `TestPredictEndpoint::test_predict_with_defaults`
  9. `TestPredictBatchEndpoint::test_batch_predict_returns_200`
  10. `TestPredictBatchEndpoint::test_batch_predict_returns_correct_count`
  11. `TestPredictBatchEndpoint::test_batch_predict_values_correct`
  12. `TestPredictBatchEndpoint::test_batch_predict_single_trip`
  13. `TestPredictBatchEndpoint::test_batch_predict_includes_metadata`
  14. `TestErrorHandling::test_predict_model_exception_returns_500`
  15. `TestErrorHandling::test_batch_predict_model_exception_returns_500`
- **Sample Error Output:**
  ```text
  KeyError: 'count' (due to response status 503 instead of 200)
  AttributeError: 'NoneType' object has no attribute 'predict'
  WARNING src.model_serving: Could not load from MLflow registry: Registered Model with name=nyc-taxi-duration-model not found. Model serving will return errors until a model is loaded.
  ```
- **Root Cause:** When `TestClient(app)` is instantiated, FastAPI's `lifespan` context executes `model_state.load_model()`. Because the local MLflow registry does not have `nyc-taxi-duration-model`, `model_state.model` is overwritten with `None`, destroying the mock model set up by the test fixture `setup_mock_model`.

---

### 4.4 Test Run 4: `Sentinel` Backend Unit Tests (Default Execution)
- **Working Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1`
- **Command:**
  ```powershell
  python -m pytest Sentinel/backend/tests/unit -v
  ```
- **Result:** **FAILED (Interrupted during collection)**
  - Total Collected: 124 items before interruption
  - Collection Errors: 3
- **Exact Error Output:**
  ```text
  ERROR Sentinel\backend\tests\unit\test_rate_limit_metrics.py
    ModuleNotFoundError: No module named 'prometheus_fastapi_instrumentator'
  ERROR Sentinel\backend\tests\unit\test_shap_compute.py
    ModuleNotFoundError: No module named 'celery'
  ERROR Sentinel\backend\tests\unit\test_stl_integration.py
    ModuleNotFoundError: No module named 'celery'
  ```
- **Root Cause:** Sentinel hard-depends on `celery` and `prometheus-fastapi-instrumentator`, which are not installed in the Windows environment.

---

### 4.5 Test Run 5: `Sentinel` Backend Unit Tests (Excluding Missing Dependency Modules)
- **Working Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1`
- **Command:**
  ```powershell
  python -m pytest Sentinel/backend/tests/unit --ignore=Sentinel/backend/tests/unit/test_rate_limit_metrics.py --ignore=Sentinel/backend/tests/unit/test_shap_compute.py --ignore=Sentinel/backend/tests/unit/test_stl_integration.py -v
  ```
- **Result:** **PASSED (124 / 124 passed, 1 warning in 23.75s)**
  - Tests Passed: 124
  - Tests Failed: 0
  - Errors: 0
- **Critical Code Quality Finding (Hollow / Stub Tests):**
  While 124 unit tests pass, inspection of the test implementations reveals severe false confidence:
  - `Sentinel/backend/tests/unit/test_ks.py`:
    ```python
    def test_ks_detector():
        detector = KSDetector()
        assert detector is not None  # Does not test statistical correctness or p-value!
    ```
  - `Sentinel/backend/tests/unit/test_psi.py`:
    ```python
    def test_psi_detector():
        detector = PSIDetector()
        assert detector is not None  # Does not test mathematical PSI computation!
    ```
  - `Sentinel/backend/tests/unit/test_cusum.py`:
    ```python
    def test_cusum_detector():
        detector = CUSUMDetector()
        assert detector is not None  # Does not test drift detection behavior!
    ```
  These tests provide 0% statistical verification. Genuine statistical unit tests on known distributions (as mandated by v2 Section 15) must be written.

---

### 4.6 Test Run 6: `Sentinel` Integration Tests
- **Working Directory:** `d:\BTech\TY\Sem 5\EDI\ModelPulse-1`
- **Command:**
  ```powershell
  python -m pytest Sentinel/backend/tests/integration -v
  ```
- **Result:** **FAILED (Interrupted during collection)**
  - Collection Error: `test_ingest_api.py` failed with `ModuleNotFoundError: No module named 'prometheus_fastapi_instrumentator'`.
- **Critical Finding (Empty Integration Stubs):**
  Inspection of the other three integration test files shows they are completely empty:
  - `Sentinel/backend/tests/integration/test_alerts_api.py`:
    ```python
    def test_alerts_api():
        pass
    ```
  - `Sentinel/backend/tests/integration/test_drift_api.py`:
    ```python
    def test_drift_detection_api():
        pass
    ```
  - `Sentinel/backend/tests/integration/test_websocket.py`:
    ```python
    def test_websocket_connection():
        pass
    ```
  **Verdict:** Sentinel contains zero working integration tests.

---

## 5. Comprehensive Feature Inventory & Reuse Strategy

Following Section 2 of the specification, here is the verified code inventory:

| Capability | Exists in Code? | Relevant File(s) | Verified Test? | Reuse Decision |
|---|---|---|---|---|
| **Prediction Endpoint** | Yes | `mlops/src/model_serving.py`, `Sentinel/backend/app/routers/inference.py` | 15 Failures in mlops; Ingestion works | **Adopt Faheem's Pydantic validation structure**, but adapt to generic tabular payload and add database persistence. |
| **Statistical Drift Tests** | Yes | `mlops/src/drift_detector.py`, `Sentinel/backend/app/detectors/` | Broken imports in mlops; Stub assertions in Sentinel | **Extract pure numpy/scipy implementations** (PSI, KS, Wasserstein from Faheem; JS, EWMA from Sentinel). Write real mathematical unit tests. |
| **Ensemble Voting** | **No** | None | None | **Build fresh.** Implement $k$-of-$m$ voting engine with per-feature vote records. |
| **Severity Scoring (0–100)** | **No** | None | None | **Build fresh.** Implement Section 10 formula ($D, P, Q, M$ components, dynamic weight renormalisation). |
| **SHAP Attribution** | Yes | `Sentinel/backend/app/novelties/shap_attribution.py` | 13 passing unit tests | **Reuse Sentinel's TreeExplainer delta calculation**; add importance $\times$ drift ranking and cumulative top-5 share. |
| **Data Quality Validation** | Yes | `mlops/src/data_quality.py` | None | **Reuse validation concepts**; implement lightweight Pandera schema validator for ingestion micro-batches. |
| **Alert Rules & Delivery** | Partial | `Sentinel/backend/app/services/alert_service.py` | Stubs only | **Port Sentinel's cooldown/deduplication logic**; add external Slack webhook and SMTP notification dispatcher. |
| **Streamlit Dashboard** | Yes | `mlops/app/dashboard.py` | Manual UI | **Reuse Faheem's Streamlit UI layout and Plotly charts**, updated to query TimescaleDB instead of disk. |
| **Retraining Pipeline** | Yes | `mlops/src/auto_retrain.py` | None | **Reuse champion/challenger comparison logic**, but enforce mandatory human-approval state before promotion. |
| **TimescaleDB Migrations** | **No** | `Sentinel/backend/migrations/` (standard Postgres) | None | **Build fresh Alembic migrations** targeting TimescaleDB hypertables and continuous aggregates. |
| **Kafka Streaming** | **No** | None | None | **Build fresh.** Kafka producer (dataset replayer with accelerated clock) + consumer with DLQ. |
| **Drift Simulator** | Partial | `Sentinel/scripts/generate_real_predictions.py` | None | **Build fresh module.** Support sudden, gradual, seasonal, category, and quality shifts; generate ground-truth JSON manifest. |

---

## 6. Single Consistent Interface Proposal

Currently, the two repositories have incompatible data structures:
- Sentinel requires `{ "model_id": 1, "features": { ... } }` returning `{ "prediction": 0.8, "shap_values": { ... } }`.
- Faheem requires NYC Taxi fields (`trip_distance`, `pickup_hour`, etc.) returning `{ "predicted_duration_minutes": 15.2 }`.
- Neither repository produces a monitoring result matching the v2 specification.

To ensure consistency across the four team members, here is the **unified interface specification**.

### 6.1 Prediction Ingestion Interface (`POST /predict` & `POST /predict/batch`)

#### Request Schemas:
```python
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid

class PredictionRequest(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str = Field(..., example="fraud-detector")
    model_version: str = Field(..., example="1.0.0")
    features: Dict[str, Any] = Field(..., example={"amount": 1250.0, "type": "TRANSFER", "oldbalanceOrg": 5000.0})
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class BatchPredictionRequest(BaseModel):
    model_name: str
    model_version: str
    predictions: List[PredictionRequest]
```

#### Response Schemas:
```python
class PredictionResponse(BaseModel):
    event_id: str
    model_name: str
    model_version: str
    prediction: int = Field(..., description="Binary class (0 or 1) or integer label")
    score: float = Field(..., description="Prediction probability or model confidence [0.0, 1.0]")
    validation_status: str = Field(..., example="valid")  # "valid" | "quarantined"
    timestamp: datetime

class BatchPredictionResponse(BaseModel):
    model_name: str
    model_version: str
    count: int
    results: List[PredictionResponse]
```

---

### 6.2 Per-Feature Drift Result Interface

Each monitored feature produces a structured result containing all statistical test votes:

```python
class TestResult(BaseModel):
    metric: str = Field(..., example="psi")  # "psi" | "ks" | "wasserstein" | "chi2" | "js"
    value: float
    threshold: float
    p_value: Optional[float] = None
    exceeded: bool

class FeatureDriftResult(BaseModel):
    feature_name: str
    feature_type: str = Field(..., example="numeric")  # "numeric" | "categorical"
    tests: List[TestResult]
    votes: str = Field(..., example="2/3")  # votes_exceeded / total_tests
    votes_count: int
    total_tests: int
    drift_detected: bool  # True if votes_count >= k
    drift_contribution: float  # Normalized importance * drift
    unseen_categories: Optional[List[str]] = None
    reference_mean: Optional[float] = None
    current_mean: Optional[float] = None
```

---

### 6.3 Monitoring-Run Result Interface (`GET /monitoring/runs/{run_id}`)

Matches the illustrative contract in v2 Section 12.3:

```python
class WindowSpec(BaseModel):
    start: datetime
    end: datetime
    sample_count: int

class PredictionDriftSummary(BaseModel):
    metric: str = "psi"
    value: float
    threshold: float = 0.25
    drifted: bool

class SeverityBreakdown(BaseModel):
    score: int = Field(..., ge=0, le=100)
    band: str = Field(..., example="High")  # "Low" | "Moderate" | "High" | "Critical"
    confidence: str = Field(..., example="normal")  # "normal" | "low_sample" | "degraded"
    components: Dict[str, Optional[float]] = Field(
        ..., example={"D": 71.0, "P": 38.0, "Q": 5.0, "M": None}
    )
    weights_used: Dict[str, float] = Field(
        ..., example={"D": 0.53, "P": 0.27, "Q": 0.20}
    )

class TopContributor(BaseModel):
    feature_name: str
    contribution_score: float
    cumulative_share: float

class MonitoringRunResponse(BaseModel):
    run_id: str
    model_name: str
    model_version: str
    run_type: str = Field(..., example="hourly")  # "hourly" | "fast_path" | "manual"
    reference_window: WindowSpec
    current_window: WindowSpec
    sample_count: int
    status: str = Field(..., example="ok")  # "ok" | "insufficient_evidence" | "quality_failure"
    dataset_drift_detected: bool
    prediction_drift: PredictionDriftSummary
    severity: SeverityBreakdown
    features: List[FeatureDriftResult]
    top_contributors: List[TopContributor]
    recommendations: List[str]
    created_at: datetime
```

---

### 6.4 Proposed Database Schema Changes (TimescaleDB)

To support this unified interface, the database schema must migrate from Sentinel's basic Postgres tables to TimescaleDB hypertables:

```sql
-- 1. Predictions Hypertable (Partitioned by timestamp)
CREATE TABLE prediction_events (
    ts TIMESTAMPTZ NOT NULL,
    event_id UUID NOT NULL,
    model_name VARCHAR(64) NOT NULL,
    model_version VARCHAR(32) NOT NULL,
    features JSONB NOT NULL,
    prediction INT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    validation_status VARCHAR(16) NOT NULL, -- 'valid', 'quarantined'
    error_reason TEXT,
    PRIMARY KEY (ts, event_id)
);
SELECT create_hypertable('prediction_events', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX idx_pred_model_ts ON prediction_events (model_name, model_version, ts DESC);

-- 2. Ground Truth Labels
CREATE TABLE labels (
    event_id UUID NOT NULL PRIMARY KEY,
    label INT NOT NULL,
    label_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Monitoring Runs
CREATE TABLE monitoring_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_name VARCHAR(64) NOT NULL,
    model_version VARCHAR(32) NOT NULL,
    run_type VARCHAR(16) NOT NULL,
    ref_window_start TIMESTAMPTZ NOT NULL,
    ref_window_end TIMESTAMPTZ NOT NULL,
    cur_window_start TIMESTAMPTZ NOT NULL,
    cur_window_end TIMESTAMPTZ NOT NULL,
    sample_count INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    dataset_drift_detected BOOLEAN NOT NULL,
    prediction_drift_value DOUBLE PRECISION,
    severity_score INT NOT NULL,
    severity_band VARCHAR(16) NOT NULL,
    components JSONB NOT NULL,
    weights_used JSONB NOT NULL,
    recommendations JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Feature Drift Results (Child of Monitoring Run)
CREATE TABLE feature_drift_results (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES monitoring_runs(run_id) ON DELETE CASCADE,
    feature_name VARCHAR(64) NOT NULL,
    feature_type VARCHAR(16) NOT NULL,
    tests JSONB NOT NULL,
    votes_count INT NOT NULL,
    total_tests INT NOT NULL,
    drift_detected BOOLEAN NOT NULL,
    drift_contribution DOUBLE PRECISION NOT NULL
);

-- 5. Alerts Table
CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES monitoring_runs(run_id),
    severity_band VARCHAR(16) NOT NULL,
    severity_score INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    details JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active', -- 'active', 'acknowledged', 'resolved'
    notification_status JSONB NOT NULL, -- {"slack": "delivered", "email": "delivered"}
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ
);
```

---

## 7. Phased Implementation Plan for Member 1

Based on the target architecture (Section 7) and timeline (Section 17):

### Phase 0: Workspace Unification & Hygiene (Days 1–3)
- Initialize clean root Git repository for `ModelPulse`.
- Create standardized root directory structure: `backend/`, `ml/`, `simulator/`, `docs/`, `dashboard/`.
- Isolate reference repositories into an ignored/archive directory (`references/`) to avoid dependency pollution.
- Establish clean Python virtual environment (`.venv`) with pinned `requirements.txt`.
- Set up Docker Compose for TimescaleDB (`timescale/timescaledb:latest-pg16`) and single-broker Kafka (KRaft).

### Phase 1: Database Layer & Data Model (Days 4–7)
- Initialize Alembic migration environment in `backend/app/db/migrations`.
- Implement SQLAlchemy 2.0 async models for `prediction_events`, `labels`, `monitoring_runs`, `feature_drift_results`, and `alerts`.
- Write migration script executing hypertable creation for `prediction_events`.
- Write unit tests verifying idempotent inserts and query filters on time ranges.

### Phase 2: Ingestion & Validation API (Days 8–11)
- Implement FastAPI endpoints `POST /predict`, `POST /predict/batch`, and `POST /labels`.
- Build `validation_service.py` using Pandera:
  - Required fields, numeric bounds, null limits, categorical set validation.
  - Quarantines invalid records with structured error reasons without failing entire batch.
- Persist valid records directly into `prediction_events` hypertable.
- Create automated test suite for endpoints with 100% test coverage on input validation and DB persistence.

### Phase 3: Kafka Streaming Ingestion (Phase 5 of v2 Roadmap) (Days 12–16)
- Build Kafka Producer (`backend/app/streaming/producer.py`):
  - Replays PaySim financial dataset with accelerated clock simulator.
- Build Kafka Consumer (`backend/app/streaming/consumer.py`):
  - Consumes from `events` topic in micro-batches (5-minute chunks).
  - Validates schema and routes corrupt/malformed records to `events.dlq`.
  - Idempotently writes valid records to `prediction_events`.
- Implement integration tests proving streaming path yields identical data in DB as REST API batch path.

---

## 8. Conclusion & Immediate Recommendations

1. **Do not attempt to salvage broken tests in reference repositories.** The reference repos are learning aids, not production codebases.
2. **Standardize on the PaySim financial fraud dataset.** NYC Taxi does not align with the fraud/credit risk motivation and cannot support concept drift monitoring with delayed ground-truth labels.
3. **Begin Phase 0 immediately.** Initialize the clean root Git repository, write the TimescaleDB docker configuration, and agree on the schemas proposed in Section 6.
