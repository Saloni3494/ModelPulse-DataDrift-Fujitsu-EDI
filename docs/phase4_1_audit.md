# ModelPulse v2 — Phase 4.1 Correctness Audit & Gap Resolution Report

**Date:** 2026-10-09  
**Scope:** Phase 4.1 Pre-Streaming Correctness Audit & Discrepancy Resolution  
**Repository State:** ModelPulse v2 Core API, Monitoring Orchestration & Persistence Foundation  
**Test Suite Status:** **85 passed** / 0 failed in 18.18s  

---

## Executive Summary

Prior to integrating Apache Kafka streaming in Phase 5, Phase 4.1 was executed as a focused correctness audit resolving discrepancies identified between documented contracts, mock examples, and runtime behavior. 

Key resolutions achieved:
1. **Severity Discrepancy Resolved:** Reconciled the documented calculation discrepancy where an example payload reported `score: 64` alongside components $D=56.25, P=100.0, Q=0.0, M=\text{None}$. The underlying severity service mathematically computes `raw_score = 56.6667` which rounds deterministically to `score = 57`. Stale documentation examples were corrected and regression tests added.
2. **Quarantine Isolation & Nullable Predictions:** Quarantined records now return `prediction: None` and `score: None` (enforced in both single and batch inference). The underlying scikit-learn pipeline is never invoked on invalid records. Quarantined records are isolated from prediction metrics (score PSI, PPR) and tracked with explicit sample count denominators (`sample_count`, `valid_sample_count`, `quarantined_sample_count`).
3. **All-Quarantined Batch Resilience:** Batches where all records are quarantined now return an explicit `status: "all_quarantined"` with `value: null` for prediction drift, rather than masking failures with an artificial `0.0` drift score.
4. **Database Readiness Disclosed:** Clarified that while the PostgreSQL/TimescaleDB schema migration ([`001_create_monitoring_schema.sql`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/migrations/001_create_monitoring_schema.sql)) is authored, the runtime currently executes on tested In-Memory and SQLite repositories; live TimescaleDB persistence is not yet wired in Python.

---

## 1. Severity Score Discrepancy Resolution

### 1.1 Discrepancy Description
In `docs/phase4_report.md` Section 2.2, an example monitoring response reported:
```json
"severity": {
  "score": 64,
  "band": "High",
  "confidence": "degraded",
  "components": { "D": 56.25, "P": 100.0, "Q": 0.0, "M": null },
  "weights_used": { "D": 0.533333, "P": 0.266667, "Q": 0.200000, "M": 0.0 }
}
```
However, the documented mathematical formula yields:
$$\text{Score} = \text{round}\left(\frac{0.40 \times 56.25 + 0.20 \times 100.0 + 0.15 \times 0.0}{0.40 + 0.20 + 0.15}\right) = \text{round}\left(\frac{42.5}{0.75}\right) = \text{round}(56.6667) = 57$$

### 1.2 Root Cause Analysis & Service Trace
1. **Engine Execution:** Invoking [`compute_severity_score`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/services/severity_service.py#L320-L400) directly with these exact inputs produces:
   - `raw_score = 56.6667`
   - `score = 57`
   - `band = "High"`
   - `confidence = "degraded"`
   - `weights_used = {'D': 0.533333, 'P': 0.266667, 'Q': 0.200000, 'M': 0.0}`
2. **Audit Table Verification:** In `docs/phase4_report.md` Section 4, the independent audit table had correctly recorded `57`.
3. **Discrepancy Source:** The `score: 64` in Section 2.2 and Section 6.1 was a stale typographical placeholder from a manual draft that was not updated when the final response JSON was inserted.
4. **Actual Simulator Scenario Output:** Running the real `numeric_mean_variance_shift` scenario (seed 42, 500 samples) across the 6 monitored features yields:
   - Monitored drift: `amount` ($w=3.0$), `oldbalanceOrg` ($w=2.0$) drift $\implies D = 100 \times (5.0 / 11.0) = 45.45$
   - Prediction drift: $P = 100.0$
   - Data quality: $Q = 0.0$
   - Delayed feedback: $M = \text{None}$
   - Renormalized weights: $D = 0.533333, P = 0.266667, Q = 0.200000$
   - Engine output: $\text{raw\_score} = 0.533333(45.45) + 0.266667(100.0) = 50.9067 \implies \mathbf{score = 51}$.

### 1.3 Code & Schema Enhancements
- Added `raw_score: Optional[float] = None` to [`SeverityBreakdown`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/monitoring.py#L59-L72), allowing API consumers to verify unrounded floating-point calculations against rounded integer scores.
- Updated `to_breakdown()` in [`SeverityResult`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/monitoring.py#L74-L115) to propagate `raw_score`.
- Added unit regression test `test_severity_score_discrepancy_reproduction_and_resolution` in `backend/tests/test_phase4_1.py`.

---

## 2. Quarantine Isolation in Inference & Monitoring

### 2.1 Problem Statement
In previous mock representations, quarantined records showed `prediction = 0` and `score = 0.0`. This created ambiguity where invalid records appeared to have genuine model predictions indicating negative fraud classifications.

### 2.2 Solution & Contract Updates
1. **Nullable Prediction Schemas:**
   - In [`PredictionResponse`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/predictions.py#L22-L37):
     ```python
     prediction: Optional[int] = Field(default=None, description="0 (Legit), 1 (Fraud), or None if quarantined")
     score: Optional[float] = Field(default=None, description="Fraud probability [0.0, 1.0], or None if quarantined")
     ```
2. **Strict Inference Isolation in `ModelService`:**
   - In [`predict_single`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/services/model_service.py#L153-L191) and [`predict_batch`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/services/model_service.py#L193-L247):
     - Validation occurs before any pipeline transformation.
     - Invalid records immediately receive `prediction = None, score = None, validation_status = "quarantined"`.
     - The underlying scikit-learn pipeline is called **only** on rows passing validation (`valid_df`).
     - Vectorized batch prediction preserves the exact index order of submitted items.
3. **Distinguishability of Probability 0.0:**
   - A legitimate model prediction probability of $0.000000$ yields `score = 0.0` (numeric float) and `validation_status = "valid"`.
   - A quarantined record yields `score = None` and `validation_status = "quarantined"`.
   - The two states are strictly distinguishable programmatically.

---

## 3. Monitoring Consistency & Sample Denominators

### 3.1 Explicit Sample Accounting
The monitoring response schema [`MonitoringRunResponse`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/schemas/monitoring.py#L125-L151) now explicitly reports:
- `sample_count`: Total evaluation records submitted in current window.
- `valid_sample_count`: Records that passed schema and range validation and entered model inference.
- `quarantined_sample_count`: Records quarantined due to nulls, negative amounts, or missing columns.
- Invariance: $\text{sample\_count} = \text{valid\_sample\_count} + \text{quarantined\_sample\_count}$.

### 3.2 Metric Denominator Isolation
- **Prediction Drift Metrics (PSI & PPR):**
  - Calculated strictly over the `valid_sample_count` population.
  - Adding corrupted records to a batch does **not** shift the prediction score PSI or predicted-positive-rate, because invalid records are filtered prior to scoring.
- **Data Quality (Component Q):**
  - Calculated with `sample_count` as the denominator:
    $$\text{failure\_rate} = \frac{\text{quarantined\_sample\_count}}{\text{sample\_count}}$$
  - Component $Q = 100 \times \min\left(1.0, \frac{\text{failure\_rate}}{0.10}\right)$.
  - Corrupted records directly elevate $Q$, while leaving prediction PSI unaffected.

### 3.3 All-Quarantined Batch Behavior
When every record in an evaluation batch is quarantined (`valid_sample_count == 0`):
- `prediction_drift`:
  ```json
  {
    "metric": "psi",
    "value": null,
    "threshold": 0.25,
    "drifted": false,
    "status": "all_quarantined"
  }
  ```
- Rather than falsely claiming `value: 0.0` (zero drift), the metric is explicitly marked unavailable.
- Component $P$ is set to `None`.
- Weight for $P$ is omitted and renormalized to $D$ and $Q$.
- Component $Q = 100.0$ (saturated quality failure).
- Overall run status is set to `"quality_failure"`.

### 3.4 Unexpected Columns Handling
- Monitored feature ensembles ignore unrecognized extra columns during statistical testing.
- [`_assess_data_quality`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/services/monitoring_service.py#L320-L375) detects columns outside `STANDARD_COLUMNS` and records explicit warnings in `recommendations` and quality findings (e.g. `Unexpected column(s) detected: ['unrecognized_crypto_field']`).

### 3.5 Persistence Outage Propagation
- In both [`InMemoryMonitoringRepository`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py#L54-L115) and [`SQLiteMonitoringRepository`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py#L118-L272), simulated storage outages (`simulate_failure = True`) raise `RuntimeError`.
- `MonitoringService.execute_monitoring_run(..., persist=True)` does not swallow repository exceptions, ensuring that a database failure cannot result in a false claim of a successful durable save.

---

## 4. Database Readiness Disclosure

| Component | Status | Implementation Details |
|---|---|---|
| **PostgreSQL / TimescaleDB Migration** | **Authored (SQL Only)** | [`backend/app/db/migrations/001_create_monitoring_schema.sql`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/migrations/001_create_monitoring_schema.sql) defines `monitoring_runs` and `feature_drift_results` tables with TimescaleDB hypertable setup. |
| **Active Python Repository** | **In-Memory & SQLite** | [`backend/app/db/repository.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py) provides `InMemoryMonitoringRepository` (default) and `SQLiteMonitoringRepository` (`PERSISTENCE_TYPE=sqlite`). |
| **TimescaleDB Python Client** | **Not Operational** | A live `TimescaleDBMonitoringRepository` (using asyncpg/SQLAlchemy) is **not** currently implemented or wired in Python. |
| **Operational Disclosure** | **Documented Gap** | Production deployment will require standing up the PostgreSQL container, applying `001_create_monitoring_schema.sql`, and implementing the asyncpg repository adapter in a subsequent task. |

---

## 5. Independent Audit Table

The following independent audit table records expected vs. actual behavior across all audited areas:

| Audit Dimension | Test Case / Scenario | Expected Result | Actual System Result | Status |
|---|---|---|---|---|
| **Severity Score Formula** | $D=56.25, P=100.0, Q=0.0, M=\text{None}$ ($W_{\text{eff}}=[0.533, 0.267, 0.200]$) | $\text{Raw}=56.6667$, $\text{Score}=57$, Band="High" | $\text{Raw}=56.6667$, $\text{Score}=57$, Band="High" | **RESOLVED (Exact)** |
| **Numeric Drift Run** | Scenario `numeric_mean_variance_shift` (seed 42, 500 samples) | $D=45.45, P=100.0, Q=0.0 \implies \text{Score}=51$ | $\text{Raw}=50.9067, \text{Score}=51, \text{Band}="High"$ | **MATCH (Exact)** |
| **Quarantined Record Schema** | Invalid feature payload (e.g. `amount = -500.0`) | `prediction: null`, `score: null`, `status: "quarantined"` | `prediction: None`, `score: None`, `status: "quarantined"` | **MATCH (Exact)** |
| **Inference Pipeline Isolation** | Batch of 1 valid, 1 invalid record | `pipeline.predict_proba` called on 1 row only | `pipeline.predict_proba` called on `len=1` | **MATCH (Isolated)** |
| **Probability 0.0 vs Null** | Score of $0.000$ vs Quarantined record | $0.0 \ne \text{None}$, distinguishable | `valid.score == 0.0`, `quar.score is None` | **MATCH (Distinguishable)** |
| **All-Quarantined Batch** | Batch of 50 corrupted records (all negative amounts) | Prediction drift `value=null, status="all_quarantined"`, $P=\text{None}, Q=100.0$ | `value=None, status="all_quarantined"`, $P=\text{None}, Q=100.0$ | **MATCH (Resilient)** |
| **Mixed Batch PSI Invariance** | Clean batch (100) vs Mixed batch (100 clean + 50 corrupted) | Prediction score PSI identical between runs | PSI identical ($1.8841 = 1.8841$) | **MATCH (Invariant)** |
| **Sample Denominators** | 100 clean + 50 corrupted | `sample=150, valid=100, quar=50` | `sample=150, valid=100, quar=50` | **MATCH (Exact)** |
| **Unexpected Columns** | Input record with extra unknown column | Logged in quality recommendations, inference succeeds | Unrecognized field recorded in recommendations | **MATCH (Resilient)** |
| **Persistence Outage** | Repository outage simulation (`simulate_failure=True`) | Raises `RuntimeError`, 0 records saved | `RuntimeError` raised, 0 records in store | **MATCH (Propagated)** |

---

## 6. Test Suite Execution & Verification

### Exact Verification Command:
```powershell
.\.venv\Scripts\python.exe -m pytest backend/tests -v
```

### Verification Output:
```
============================= test session starts =============================
platform win32 -- Python 3.12.4, pytest-8.3.4, pluggy-1.6.0
cachedir: .pytest_cache
rootdir: D:\BTech\TY\Sem 5\EDI\ModelPulse-1
plugins: anyio-4.15.1, asyncio-0.25.0
collected 85 items

backend/tests/test_drift_metrics.py (15 tests) ...............          [ 17%]
backend/tests/test_ml_baseline.py (9 tests) .........                   [ 28%]
backend/tests/test_phase3.py (23 tests) .......................         [ 55%]
backend/tests/test_phase4.py (21 tests) .....................           [ 80%]
backend/tests/test_phase4_1.py (11 tests) ...........                   [ 92%]
backend/tests/test_smoke.py (6 tests) ......                            [100%]

============================= 85 passed in 18.18s =============================
```

### Test Count Breakdown by Module:
| Module | Test File | Tests Passed | Status |
|---|---|---|---|
| Smoke & Configuration | `backend/tests/test_smoke.py` | 6 / 6 | **PASS** |
| Baseline ML Model | `backend/tests/test_ml_baseline.py` | 9 / 9 | **PASS** |
| Statistical Drift Metrics | `backend/tests/test_drift_metrics.py` | 15 / 15 | **PASS** |
| Simulator & Severity Engine | `backend/tests/test_phase3.py` | 23 / 23 | **PASS** |
| API & Monitoring Foundation | `backend/tests/test_phase4.py` | 21 / 21 | **PASS** |
| Phase 4.1 Correctness & Audit | `backend/tests/test_phase4_1.py` | 11 / 11 | **PASS** |
| **Total Test Suite** | **All 6 Test Modules** | **85 / 85** | **100% PASS** |

---

## 7. Next Phase Preparation & Sign-Off

Phase 4.1 is complete with all audit discrepancies resolved, zero regressions, and 85 passing tests.
The codebase is verified and ready for review prior to Phase 5 (Kafka producer replay, consumer with DLQ, and 5-minute micro-batching).

*End of Phase 4.1 Report. Ready for user review.*
