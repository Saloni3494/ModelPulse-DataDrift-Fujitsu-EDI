# ModelPulse — Complete Implementation Approach (v2, standalone)

**Project:** ModelPulse: MLOps Model & Data Drift Monitoring Platform
**Aligned to:** the EDI review proposal deck (objectives 01–06, 8-stage methodology, technologies table, architecture, conclusion targets)
**Status:** replaces "ModelPulse — Complete Implementation Roadmap" (v1). This document is self-contained; you do not need v1.

---

## Contents

1. Verdict on v1 and what changed
2. What the two reference repositories really give you
3. Scope traceable to the proposal
4. Architecture
5. Final technology decisions
6. Dataset
7. Repository structure and environment setup
8. Phase-by-phase implementation
9. Drift detection design
10. Severity score, attribution and recommender
11. Data model
12. API and event contracts
13. Configuration
14. Evaluation plan (how to get real numbers)
15. Testing strategy
16. Corrections to the proposal deck
17. Team split and timeline
18. Risks, cut-lines and hygiene
19. Definition of done and demo script
20. Master checklist

---

## 1. Verdict on v1 and what changed

v1's principles were right: one backend, staged build, never equate data drift with model failure, human-approved retraining, honest "unavailable" states. Its **scope and alignment** needed correction:

| # | Gap in v1 | Fix in v2 |
|---|-----------|-----------|
| 1 | Deferred Kafka/Pub-Sub, Feast and TimescaleDB, but your methodology and technology slides commit to them | Committed milestones, built after a working batch core, in their lightest viable form |
| 2 | No drift **simulation & testing** module (Objective 04) | Simulator is a first-class component and doubles as the evaluation harness |
| 3 | No explicit **prediction drift** (Objective 01 names data, prediction and concept drift) | Added |
| 4 | Single tests; proposal says multi-method ensemble | Ensemble of PSI, KS/χ², JS divergence, Wasserstein with a documented voting rule |
| 5 | No **recommendation engine** (Objective 03) | Rule-based recommender attached to alerts |
| 6 | No plan to produce the numbers on the Conclusion slide | Evaluation plan (Section 14); unsupported claims flagged (Section 16) |
| 7 | Added tools the proposal never mentions (Celery+Redis, React/TS, Prometheus/Grafana, MLflow as core) | Trimmed: APScheduler, Streamlit; Grafana/MLflow optional |
| 8 | Telco churn dataset does not match the fraud/finance motivation and has no natural time axis | Dataset guidance revised (Section 6) |
| 9 | No team split or cut-lines | Added (Sections 17–18) |

---

## 2. What the two reference repositories really give you

Checked against their GitHub pages (October 2026).

| | Faheem — mlops-drift-detection-platform | Sentinel — Debddj/Sentinel |
|---|---|---|
| Maturity | 1 commit, 0 stars, single author | 30 commits, 0 stars, single author; contains scratch files and default credentials in docs |
| License | MIT (reuse allowed; keep the notice) | A LICENSE file exists — **open it and confirm the type before reusing code** |
| Stack (per README) | LightGBM/XGBoost, MLflow, Optuna, Feast, FastAPI, Evidently, Prefect, Prometheus/Grafana, Streamlit, GitHub Actions | FastAPI, React/TS, PostgreSQL, Celery+Redis, Prometheus/Grafana, WebSockets |
| Drift | PSI/CSI, ADWIN, Page-Hinkley, Evidently reports | PSI, KS, importance-weighted PSI, EWMA adaptive limits, drift-type classifier, STL decomposition |
| Extras | Champion/challenger retraining, NYC Taxi pipeline | Calibration (Brier, ROC/AUC), SHAP, drift simulator script |
| **Not present per README** | Kafka/Pub-Sub, TimescaleDB, SHAP (listed as future), Slack/email (listed as future) | Kafka/Pub-Sub, Feast, TimescaleDB, retraining, Slack/email delivery |
| Caution | README result tables (PSI scores, "34.4% RMSE reduction") are not backed by shown outputs — do not quote them as evidence | Verify schema, tests and auth before porting anything |

**How to use them**

- Neither covers streaming, TimescaleDB or notifications. Those are your original contribution.
- Faheem's repo is the better reuse base (MIT, Python-only, has a Streamlit dashboard, drift code, champion/challenger logic).
- Sentinel is the better design reference (SHAP, importance-weighted drift, EWMA thresholds, simulator).
- Do not merge two backends. Use your own schema (Section 11) and port ideas or small, license-checked modules.
- Do not infer that a feature works from a README heading. Fill this inventory from the code:

| Feature | Exists in code? | File/module | Test exists? | Reuse decision |
|---|---|---|---|---|
| Prediction endpoint | | | | |
| Drift calculation | | | | |
| SHAP | | | | |
| Alert delivery | | | | |
| Dashboard | | | | |
| Retraining | | | | |
| DB migrations | | | | |

---

## 3. Scope traceable to the proposal

Priority: **M** = must (core demo), **S** = should, **C** = could (cut first).

| Proposal item | Deliverable | Priority |
|---|---|---|
| O1 Continuously monitor (data, prediction, concept drift) | Data-drift engine, prediction-drift monitor, label-based concept-drift/performance monitor | M |
| O2 Identify root causes | Drift attribution: drift × importance, SHAP shift, domain-classifier check | M |
| O3 Recommend actions | Rule-based recommender + candidate-model training with human approval | S |
| O4 Simulation & testing | Drift simulator (sudden, gradual, seasonal, quality faults, concept drift) | M |
| O5 Real-time alerts with severity | Alert rules, persisted alerts, Slack webhook + email | M |
| O6 Trust & transparency | Every score shows inputs, thresholds, sample sizes, missing-data flags | M |
| Stage 01 Ingestion (Kafka/Pub-Sub, 5-min batches) | Kafka producer/consumer, micro-batches, dead-letter topic | M (Phase 5) |
| Stage 02 Quality validation | Schema, outliers, duplicates; quarantine with reason | M |
| Stage 03 Feature store | Feast (local) for model features; rolling stats in TimescaleDB | S |
| Stage 04 Drift detection (multi-method, hourly) | Ensemble on a trailing window, hourly job | M |
| Stage 06 Severity 0–100 | Documented deterministic score | M |
| Stage 08 Persistence (TimescaleDB) | Hypertables + continuous aggregates | M |
| Dashboard & analytics | Streamlit app on persisted data | M |

---

## 4. Architecture

```text
Dataset replay / simulator ──► Kafka topic: events
                                   │   (invalid → topic: events.dlq)
                                   ▼
             Consumer: validate ─► score with model ─► write to TimescaleDB
                                   │                    (prediction_events hypertable)
                                   ▼
                  5-min micro-batch stats (continuous aggregate)
                                   │
              ┌────────────────────┴─────────────────────┐
              ▼                                          ▼
   Fast path (per micro-batch):                 Hourly job (APScheduler):
   EWMA/PSI early warning                       full ensemble on trailing window
              └────────────────────┬─────────────────────┘
                                   ▼
        Data drift · Prediction drift · Quality · Performance (if labels)
                                   ▼
        Attribution (importance × drift, SHAP shift) → Severity 0–100
                                   ▼
             Alert rules → persist → Slack / email → recommender
                                   ▼
                          Streamlit dashboard
```

**Build order:** Phases 1–4 run without Kafka (API and CSV ingestion). Kafka replaces the data source in Phase 5; the downstream pipeline does not change.

**Service responsibilities (one modular FastAPI app, not microservices):**

| Service | Responsibility |
|---|---|
| prediction_service | Load the approved model; return and persist predictions |
| validation_service | Validate rows; quarantine invalid ones with a reason |
| drift_service | Compare reference vs current window; return structured metrics |
| severity_service | Convert signals to an explainable 0–100 score |
| explanation_service | SHAP on samples/on demand; attribution ranking |
| alert_service | Create persistent alerts; dispatch notifications; record delivery result |
| retraining_service | Train candidate; evaluate vs champion; request approval |
| simulator | Inject controlled drift; write ground-truth manifest |

---

## 5. Final technology decisions

| Area | Choice | Notes |
|---|---|---|
| API | FastAPI, Pydantic schemas | |
| Streaming | **Kafka** (single broker, Docker, KRaft) | Pub/Sub is in your proposal; implement the consumer behind an interface and document Pub/Sub as an alternative adapter rather than building both |
| Database | **PostgreSQL + TimescaleDB** (use the `timescale/timescaledb` image from day 1) | It is a Postgres extension; adopting it early avoids a late migration |
| Feature store | **Feast, local provider** (Parquet offline, SQLite online) | For the model's features (training/serving consistency). Rolling stats come from Timescale continuous aggregates |
| Drift | numpy/scipy implementations (PSI, KS, χ², JS, Wasserstein) **plus** Evidently for HTML reports | Pin the Evidently version (its API changed between releases); unit-test your own PSI/KS on synthetic cases |
| Streaming detectors | `river` (ADWIN, DDM) on the error stream when labels exist | |
| Explanations | SHAP (TreeExplainer), sampled/on demand | |
| Scheduler | APScheduler | Celery+Redis only if you outgrow it |
| Dashboard | **Streamlit** (reuse Faheem's MIT dashboard patterns) | React only if the team has strong frontend skills |
| Alerts | Slack incoming webhook + SMTP email | Secrets in `.env` |
| Validation | Pandera (or Great Expectations if you prefer) | Pick one |
| Model tracking | MLflow (local file store) — optional | Keep a `model_versions` table either way |
| Ops metrics | Prometheus/Grafana — optional | Only after everything else works |
| Migrations | Alembic | |

Do not install everything at once. Add one component per phase, when its purpose is clear.

---

## 6. Dataset (decide in Week 1)

Your motivation is fraud and financial risk, so use a dataset that supports that story and has a **real time axis**.

| Option | Pros | Cons |
|---|---|---|
| PaySim (synthetic transactions with a time step) | Interpretable columns (type, amount, balances); fraud labels; imbalanced like real fraud | Synthetic; drift must be injected |
| IEEE-CIS Fraud Detection | Real, has `TransactionDT` | Many anonymised features, large, more preprocessing |
| ULB credit-card fraud | Very popular | Features V1–V28 are PCA components, so "which feature drove the drift" is not interpretable; covers only ~2 days |
| NYC Taxi (Faheem's choice) | Natural 2019→2020 drift, clean time axis | Regression, not fraud; weaker link to your proposal |

**Recommendation:** PaySim (or IEEE-CIS for real data) as the main dataset, plus synthetic injected drift for controlled tests. Fraud labels give you a realistic **delayed-label** scenario for concept-drift monitoring.

**Split by time, not randomly:** earliest period for training, next for validation/reference, remainder replayed as "production". Fit preprocessing on the training split only (avoid leakage). For imbalanced fraud use recall, precision, F1 and PR-AUC rather than accuracy.

---

## 7. Repository structure and environment setup

### 7.1 Target structure

```text
modelpulse/
├── backend/
│   ├── app/
│   │   ├── main.py
│   │   ├── api/            # health, models, predictions, monitoring, alerts, labels, simulate
│   │   ├── core/           # config.py, logging.py
│   │   ├── schemas/
│   │   ├── services/       # prediction, validation, drift, severity, explanation, alert, retraining
│   │   ├── streaming/      # producer.py, consumer.py, schema.py, dlq.py
│   │   ├── db/             # models.py, session.py, migrations/ (Alembic)
│   │   └── jobs/           # hourly_monitor.py, fast_path.py
│   ├── tests/
│   ├── requirements.txt
│   └── .env.example
├── dashboard/              # Streamlit app
├── ml/
│   ├── train.py
│   ├── evaluate.py
│   ├── feast_repo/
│   └── artifacts/          # git-ignored; model, reference set, baseline metrics
├── simulator/              # drift scenarios + ground-truth manifests
├── data/                   # git-ignored; raw/ and reference/
├── docs/                   # architecture, severity formula, evaluation results
├── docker-compose.yml      # timescaledb, kafka, backend, dashboard
├── .gitignore
└── README.md
```

This is your target layout, not a claim about either upstream repo's paths.

### 7.2 Prerequisites

Git, Python 3.10+ (match the versions your pinned libraries need), Docker Desktop, VS Code. Node.js is only needed if you later choose a React frontend.

```powershell
git --version
python --version
docker --version
docker compose version
```

### 7.3 Study the references first (Week 1)

```powershell
mkdir ModelPulse-work
cd ModelPulse-work
git clone https://github.com/faheemahmad02042019/mlops-drift-detection-platform.git reference-mlops
git clone https://github.com/Debddj/Sentinel.git reference-sentinel
```

Do not copy files between them. Follow each README exactly, run its tests, note what works, and fill the feature inventory in Section 2. Then create your own `modelpulse/` repository.

---

## 8. Phase-by-phase implementation

Each phase lists tasks and an acceptance milestone. Write tests as you go.

### Phase 0 — Setup and study (Week 1)

- Run Faheem's pipeline end-to-end; read Sentinel's drift, SHAP and simulator code.
- Confirm both licenses; decide what (if anything) you will reuse.
- Choose the dataset; agree the **DB schema and drift-result JSON contract** (Sections 11–12) so four people can work in parallel.
- Create the single ModelPulse repo, `.gitignore`, `.env.example`.
- **Milestone:** feature inventory filled from code; schema and contracts agreed.

### Phase 1 — Baseline model

- Load and validate data; time-based split; build a preprocessing + model pipeline (e.g. a gradient-boosted tree model).
- Record baseline metrics (PR-AUC, recall, precision, F1) in `baseline_metrics.json`.
- Save the model, preprocessing pipeline and the **reference feature set** (from the training/validation distribution) with its schema and window.
- **Milestone:** training is reproducible; the saved model predicts.

### Phase 2 — Prediction ingestion and storage

- Endpoints `POST /predict` and `POST /predict/batch` (Section 12); CSV replay script.
- Persist each inference: UTC timestamp, model name/version, features (or a privacy-safe subset), prediction and score, request/event ID, validation status. Do not store secrets or unnecessary personal data.
- Create hypertables and migrations.
- **Milestone:** predictions come from the real saved model and survive an application restart.

### Phase 3 — Data-quality validation

Minimum checks: required columns; types match schema; missing-value limits; plausible numeric ranges; allowed or explicitly "unseen" categories; duplicate event IDs handled idempotently; valid UTC timestamps. Invalid rows are rejected or quarantined **with a reason**. Store a validation result per batch. A quality failure must never be reported as statistical drift.
- **Milestone:** invalid data is caught before it can distort drift results.

### Phase 4 — Drift core (Section 9)

- Reference/current windows; ensemble tests per feature; prediction drift; run history and per-feature results persisted.
- Unit-test PSI/KS/χ²/JS/Wasserstein on known synthetic cases.
- Evidently HTML report as a secondary artifact.
- Build the first version of the simulator (sudden shift) so you can test immediately.
- **Milestone:** deliberately shifting a test stream is detected, and the per-feature evidence is stored.

### Phase 5 — Kafka streaming

- Docker Compose: single-broker Kafka. Topics: `events`, `events.dlq`.
- Producer: replays the dataset at an accelerated clock (and applies simulator transforms).
- Consumer: schema validation → idempotent write (unique `event_id`) → batch into 5-minute micro-batches.
- Retries with back-off; invalid events go to the DLQ with a reason; track consumer lag and error counts.
- Document whether windows use event time or processing time.
- **Milestone:** streaming gives the same monitoring results as batch mode, with event IDs and model versions intact.

### Phase 6 — Severity scoring and alerts (Section 10)

- Implement and unit-test the deterministic severity function with stored components.
- Alert rules (examples): quality failure above limit → data-quality alert; important features drifted with persistence → drift alert; performance below floor → performance alert; critical for several windows → escalation; insufficient samples/missing labels → "insufficient evidence".
- Keep alert **generation** separate from **delivery**: an alert is stored even if Slack/email fails; record the delivery result.
- Lifecycle: run → evaluate rules → persist → dispatch → record delivery → acknowledge/resolve.
- **Milestone:** a triggered drift event creates a stored alert and a test notification.

### Phase 7 — Attribution and SHAP

- Rank features by `importance × drift`; report cumulative share of the top 5.
- SHAP on sampled reference vs current batches; compare mean-absolute attributions.
- Optional domain classifier (reference vs current) for an independent view.
- Dashboard page: feature, statistic vs threshold, reference/current summaries, missing/unseen changes, SHAP summary, notes labelled "investigation hint".
- **Milestone:** a user can go from an alert to the features and evidence behind it.

### Phase 8 — Labels and concept drift

- `POST /labels` attaches ground truth to earlier predictions; compute windowed recall/precision/F1/PR-AUC; run ADWIN/DDM on the error stream.
- When labels are absent, show "performance unknown"; if you provide an estimate, label it clearly as an estimate.
- Simulator scenario: concept drift with delayed labels.
- **Milestone:** performance degradation is measured only when labels exist, and covariate drift alone never claims it.

### Phase 9 — Recommender and safe retraining

- Rule-based recommender (Section 10.4).
- Workflow: alert recommends retraining → confirm data validity and enough recent labelled data → train candidate in a separate run → evaluate on a time-based holdout vs the champion → check thresholds → store metrics/artifact → **human approval** → promote; keep the previous version for rollback.
- Never auto-replace production because drift was detected.
- **Milestone:** a candidate is trained and evaluated without silently replacing the approved model.

### Phase 10 — Feast (light)

- Define the model's features in Feast (local provider), materialise offline→online, and serve through the same definitions used in training.
- Document which features are computed, how they are timestamped, and how training/serving consistency is ensured. If this cannot be justified in your final design, say so explicitly and update the proposal instead of adding it for show.
- **Milestone:** training and serving use identical feature definitions.

### Phase 11 — Evaluation (Section 14)

Run the experiments, produce the results table and charts for the report and slides.

### Phase 12 — Polish

README with exact run commands, architecture and severity docs, CI (lint + tests), demo script rehearsal, optional Grafana/MLflow.

---

## 9. Drift detection design

### 9.1 Windows and timing

- Ingestion makes **5-minute micro-batches**; the **hourly job** compares a trailing window (default 1 h; also compute a 24 h window for stability) against the fixed reference.
- Record sample counts. If `n < min_samples`, the run is **"insufficient evidence"**, never "healthy".
- An hourly cycle cannot deliver "< 5 minute" detection. Report two latencies: **early-warning** (fast path per micro-batch: EWMA/PSI against the reference) and **confirmation** (hourly ensemble). Measure both.

### 9.2 Data drift per feature

| Feature type | Tests in the ensemble |
|---|---|
| Numeric | PSI, KS, Wasserstein (normalised by reference standard deviation) |
| Categorical | PSI on category proportions, χ², Jensen–Shannon distance; track unseen categories separately |

**Ensemble rule (configurable):** a feature is "drifted" when at least *k* of *m* tests exceed their thresholds (default 2 of 3). Persist every statistic, threshold and vote.

Statistical cautions to document: PSI cut-offs (0.1 / 0.25) are heuristics; KS p-values shrink with sample size, so also report an effect size; many features × many hourly runs inflate false positives, so require *k-of-m* voting plus persistence (e.g. 2 consecutive windows) before critical alerts.

### 9.3 Prediction drift

Monitor the score/class distribution (PSI/KS on predicted probability, change in predicted-positive rate). Needs no labels, but is **not proof of degradation**.

### 9.4 Concept drift / performance

- **With labels (even delayed):** join labels to stored predictions; compute windowed PR-AUC/recall/F1; ADWIN/DDM on the error stream; compare with baseline.
- **Without labels:** "performance unknown" (optionally a labelled estimate).

### 9.5 Drift simulator (Objective 04)

| Scenario | Implementation |
|---|---|
| Sudden covariate shift | Shift mean/scale of chosen features at time *t* |
| Gradual shift | Ramp the shift linearly over *T* hours |
| Seasonal / cyclic | Sinusoidal modulation (should not be flagged as permanent drift) |
| Category shift | Change category proportions; add unseen values |
| Concept drift | Change the feature→label relationship for a segment |
| Quality faults | Inject nulls, out-of-range values, duplicates, schema changes |

Every run writes a **ground-truth manifest** (which features, when, how large). This is what enables honest evaluation.

---

## 10. Severity score, attribution and recommender

### 10.1 Components (each 0–100, or unavailable)

- **D — data drift:** `100 × Σ w_f · clip(PSI_f / PSI_crit, 0, 1) / Σ w_f`, where `w_f` is normalised model feature importance (uniform fallback). Features that fail the ensemble vote contribute 0.
- **P — prediction drift:** `100 × clip(PSI_score / PSI_crit, 0, 1)`.
- **Q — data quality:** `100 × clip(failure_rate / q_max, 0, 1)`.
- **M — performance/concept:** `100 × clip((baseline − current) / (baseline − critical_floor), 0, 1)`; **unavailable** without labels.

### 10.2 Combination

`Severity = Σ_available W_c · C / Σ_available W_c`

Default weights `D 0.40, P 0.20, Q 0.15, M 0.25` (configurable). Weights renormalise when M is unavailable, and the UI shows "performance: not available", not 0.

Bands (tune with simulator runs): 0–24 Low · 25–49 Moderate · 50–74 High · 75–100 Critical.
Each score is stored with components, weights, thresholds, sample counts and a **confidence flag**. It is a transparent heuristic, not a calibrated probability of failure.

### 10.3 Attribution ("root-cause analysis")

1. Rank features by `w_f × normalised_drift_f`.
2. Report cumulative share ("top-5 account for X% of the drift score") — this makes the proposal's top-5 claim measurable.
3. Compare SHAP mean-absolute values on reference vs current samples.
4. Optional domain classifier for an independent view.
5. Wording: "leading drift contributors / investigation hints", never "proven cause". SHAP explains model output; it does not prove why data changed.

### 10.4 Recommender (rule-based, auditable)

| Condition | Recommendation |
|---|---|
| Quality failures high | Fix upstream data before any retraining |
| Data drift high, performance unknown | Collect labels and investigate top contributors; do not retrain blindly |
| Data drift + confirmed performance drop + enough recent labelled data | Suggest retraining, with a window and feature focus derived from the drifted features |
| Seasonal pattern detected | Suggest seasonal features or scheduled retraining |
| Insufficient samples | Wait; show "insufficient evidence" |

---

## 11. Data model (PostgreSQL + TimescaleDB)

| Table | Notes |
|---|---|
| `models`, `model_versions` | Name, version, artifact path, training window, baseline metrics, status |
| `reference_sets` | Model version, window, schema, location/hash |
| `prediction_events` (**hypertable**) | `ts`, `event_id` (unique), model version, features, prediction, score, validation status |
| `labels` | `event_id`, label, label timestamp |
| `batch_stats` (**continuous aggregate**) | 5-min and hourly per-feature rolling statistics |
| `monitoring_runs` | Windows, sample counts, status, severity, components, config snapshot |
| `feature_drift_results` | Run, feature, test, statistic, threshold, vote, drifted flag |
| `alerts` | Run, severity, status, created/acknowledged/resolved, notification result |
| `retraining_runs` | Trigger, candidate version, champion vs challenger metrics, decision |
| `simulation_runs` | Scenario manifest (ground truth) |

Use Alembic migrations; index `(model_version, ts)` and run IDs; set a retention policy for raw events; keep raw events and aggregated results separate.

---

## 12. API and event contracts

These are recommended ModelPulse contracts, not the routes of either reference repo.

### 12.1 Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Service health |
| `GET /models` | Registered models/versions |
| `POST /predict` | Single prediction, persisted |
| `POST /predict/batch` | Batch of rows |
| `POST /labels` | Attach ground truth to earlier predictions |
| `POST /monitoring/run` | Trigger a monitoring run now |
| `GET /monitoring/runs` | List runs |
| `GET /monitoring/runs/{run_id}` | One run's full report |
| `GET /alerts`, `POST /alerts/{id}/ack` | List / acknowledge alerts |
| `POST /retrain` | Request a candidate (human approval required to promote) |
| `POST /simulate` | Start a simulator scenario |

### 12.2 Kafka event (topic `events`)

```json
{
  "event_id": "uuid",
  "event_ts": "2026-02-01T00:04:59Z",
  "model_name": "fraud-model",
  "model_version": "1",
  "features": { "amount": 120.5, "type": "TRANSFER" },
  "prediction": 0,
  "score": 0.03,
  "label": null
}
```

Invalid events go to `events.dlq` with `{event, reason, received_ts}`.

### 12.3 Monitoring-run result (illustrative schema)

```json
{
  "run_id": "generated-id",
  "model_name": "fraud-model",
  "model_version": "1",
  "reference_window": { "start": "2026-01-01T00:00:00Z", "end": "2026-01-31T23:59:59Z" },
  "current_window":   { "start": "2026-02-01T00:00:00Z", "end": "2026-02-01T01:00:00Z" },
  "sample_count": 4800,
  "status": "ok",
  "dataset_drift_detected": true,
  "prediction_drift": { "metric": "psi", "value": 0.18, "threshold": 0.25 },
  "severity": {
    "score": 64,
    "band": "High",
    "confidence": "normal",
    "components": { "D": 71, "P": 38, "Q": 5, "M": null },
    "weights_used": { "D": 0.53, "P": 0.27, "Q": 0.20 }
  },
  "features": [
    {
      "feature_name": "amount",
      "tests": [
        { "metric": "psi", "value": 0.31, "threshold": 0.25, "exceeded": true },
        { "metric": "ks", "value": 0.22, "p_value": 0.0001, "exceeded": true },
        { "metric": "wasserstein", "value": 0.4, "threshold": 0.3, "exceeded": true }
      ],
      "votes": "3/3",
      "drift_detected": true,
      "drift_contribution": 0.34
    }
  ]
}
```

Use proper numeric types, UTC timestamps and database IDs in the real implementation.

---

## 13. Configuration (`config.yaml` / environment)

```yaml
windows:
  microbatch_minutes: 5
  monitor_cycle_minutes: 60
  trailing_window_hours: [1, 24]
  min_samples: 500
drift:
  numeric_tests: [psi, ks, wasserstein]
  categorical_tests: [psi, chi2, js]
  vote_k_of_m: [2, 3]
  persistence_windows: 2
  thresholds:
    psi: { warn: 0.10, crit: 0.25 }
severity:
  weights: { D: 0.40, P: 0.20, Q: 0.15, M: 0.25 }
  bands: { low: 0, moderate: 25, high: 50, critical: 75 }
  q_max: 0.10
  performance_critical_floor: 0.50   # tune per model
alerts:
  channels: [slack, email]
  escalate_after_windows: 3
retraining:
  require_approval: true
  min_labelled_rows: 5000
```

Secrets (Slack webhook, SMTP credentials, DB password) come only from environment variables; `.env.example` contains placeholders. Never commit tokens, webhook URLs, credentials or datasets.

---

## 14. Evaluation plan (how to get honest numbers)

All experiments use the simulator's ground-truth manifest. Use fixed random seeds and report mean ± spread over repeated runs.

| Metric (from your Conclusion slide) | How to measure |
|---|---|
| Detection rate: ensemble vs single tests | Inject known shifts (small/medium/large) over many trials; report true-positive rate **at the same false-positive rate** for each single test and the ensemble |
| False alert rate | Run many no-drift windows resampled from the reference period (≥ 200 hourly windows); report alerts per window, with and without the persistence rule |
| Detection latency | Time from injection to (a) early-warning and (b) confirmed alert; median and worst case |
| Root-cause precision | Precision@k / recall@k of ranked contributors vs the known shifted features; cumulative drift-score share of the top 5 |
| Retraining lead time | In delayed-label concept-drift scenarios: time between recommendation and the actual metric crossing the critical floor (may be negative; report honestly) |
| Severity validity | Severity should rise monotonically with injected magnitude; no-drift runs should stay Low |

Fraud-loss reduction and operational-cost savings cannot be measured without a real deployment. Either remove them, cite a source, or present them as a clearly assumption-labelled simulation (e.g. using the dataset's transaction amounts).

---

## 15. Testing strategy

**Unit:** PSI/KS/χ²/JS/Wasserstein on known cases; severity boundaries and missing components; alert-rule evaluation; label join; idempotent writes; model-version selection.

**Integration:** event → Kafka → validation → DB → monitoring run → alert → notification attempt; candidate training → evaluation → approval state; dashboard API responses match expected schemas. A failed notification must not erase the stored alert.

**Scenario tests (from the simulator):** no drift; sudden shift; gradual shift; seasonal pattern; quality faults; concept drift with delayed labels; tiny-sample window.

**Assertions to keep:** covariate drift alone never claims performance degradation; missing labels display as "unknown"; the same inputs always give the same severity; the dashboard never substitutes zero for unavailable data.

---

## 16. Corrections to the proposal deck

Fix these before the next review.

1. **Conclusion slide states unmeasured results** (latency < 5 min, 94%+ ensemble accuracy, top-5 > 85%, false alerts < 3%, 1–2 h lead time, 36% fraud-loss reduction, 24% cost savings). Reword as **targets**, then replace with measured results from Section 14.
2. **"< 5 minutes" latency conflicts with the hourly detection cycle** on the same slide. Use early-warning vs confirmation latency.
3. **"94%+ accuracy"** — accuracy is the wrong metric for a detector; use detection rate (recall) at a fixed false-positive rate.
4. **Problem-statement chart:** 0.92 → 0.33 is a drop of 0.59 points, about 64% relative; "dropped by 59%" is inconsistent. Label the chart as illustrative.
5. **Literature review slide covers only explainability papers**, while the reference list is mostly drift papers. Add a drift-focused review (concept-drift surveys, detectors, dataset shift).
6. **Verify every reference against its DOI/publisher page.** Entries that look wrong: **[8]** "G. E. Hinton et al., Deep learning, Nature 2015" is by LeCun, Bengio and Hinton; **[3]** author list/title/volume do not match any paper I can identify; **[7]** differs from the paper on the literature-review slide (Hassija et al., "Interpreting Black-Box Models"). Also double-check [5] and [6].
7. **Suggested additions** (real works; verify details when citing): Gama et al., "A survey on concept drift adaptation," ACM Computing Surveys, 2014; Lu et al., "Learning under concept drift: A review," IEEE TKDE, 2019; Rabanser et al., "Failing loudly: An empirical study of methods for detecting dataset shift," NeurIPS 2019; Bifet & Gavaldà, "Learning from time-changing data with adaptive windowing," SDM 2007; Quiñonero-Candela et al., *Dataset Shift in Machine Learning*, MIT Press, 2009.
8. **Terminology:** describe root-cause results as "drift attribution / leading contributors".
9. **Methodology slide:** add one line explaining that 5-minute ingestion batches are aggregated into hourly detection windows.

---



| Block | Target |
|---|---|
| Weeks 1–2 | Phases 0–3: reference study, dataset, baseline, ingestion, validation |
| Weeks 3–4 | Phases 4 and 6: drift core, severity, alerts, first dashboard — **first complete batch demo** |
| Weeks 5–6 | Phases 5 and 7: Kafka path, attribution, SHAP |
| Weeks 7–8 | Phases 8–9: labels, concept drift, retraining + recommender |
| Weeks 9–10 | Phases 10–12: Feast, evaluation, polish, rehearsal |

If your review date is sooner, finish the Weeks 3–4 batch demo first; it is the minimum credible product. Adjust the timeline to your actual dates.

---

## 18. Risks, cut-lines and hygiene

**Cut in this order if behind:** Grafana → MLflow → Feast → domain classifier → Pub/Sub adapter → seasonal/STL handling.
**Do not cut:** simulator, evaluation, severity explainability, persistence, the Kafka path (it is in your proposal).

**Common mistakes to avoid**

1. Copying both repositories wholesale (duplicate APIs, schemas, conflicting config).
2. Installing all infrastructure on day one.
3. Treating data drift as model failure.
4. Using SHAP as causal proof.
5. Hard-coded thresholds without tests or configuration.
6. Demo numbers in the dashboard instead of persisted results.
7. Retraining on unlabelled or poor-quality data.
8. Skipping license checks (keep the MIT notice for any reused Faheem code; confirm Sentinel's license first).
9. Ignoring version compatibility (Python, Evidently, Feast, SHAP, Kafka client, Postgres/Timescale) — pin versions.
10. Committing credentials or datasets; copying default credentials from reference repos.

---

## 19. Definition of done and demo script

1. Start everything with documented commands (Docker Compose).
2. Replay the dataset through Kafka; show a healthy baseline (Low severity).
3. Inject a sudden shift with the simulator; show the early-warning, then the confirmed alert.
4. Open the alert: per-feature statistics, ensemble votes, severity components, top contributors, SHAP shift.
5. Show the Slack/email notification and the persisted alert history after a restart.
6. Show a delayed-label concept-drift scenario: performance "unknown" first, then a measured drop.
7. Show the recommendation, train a candidate, compare champion vs challenger, require approval before promotion.
8. Show the evaluation table (Section 14) with measured results.

---

## 20. Master checklist

**Foundation**
- [ ] Run Faheem's repo; study Sentinel; feature inventory filled from code
- [ ] Licenses verified; reuse decisions recorded
- [ ] One repo, one schema, one backend; `.env.example` created
- [ ] Dataset chosen; time-based split defined

**Core product**
- [ ] Baseline model, reference set, baseline metrics saved
- [ ] Prediction API; predictions persisted with version and timestamp
- [ ] Validation with quarantine reasons
- [ ] Ensemble drift engine + prediction drift; run history persisted
- [ ] Simulator with ground-truth manifests
- [ ] Severity score tested and explained
- [ ] Alerts persisted; Slack/email delivery with recorded status
- [ ] Streamlit dashboard reading real persisted data

**Proposal commitments**
- [ ] Kafka ingestion with DLQ and 5-min micro-batches
- [ ] TimescaleDB hypertables and continuous aggregates
- [ ] Feast for model features (or documented decision otherwise)
- [ ] Hourly detection cycle + early-warning fast path

**Advanced**
- [ ] SHAP and attribution; domain-classifier check (optional)
- [ ] Label ingestion, performance metrics, ADWIN/DDM
- [ ] Recommender; candidate training; champion/challenger; approval and rollback

**Final**
- [ ] Evaluation results table and charts
- [ ] Deck corrections applied (Section 16)
- [ ] Demo rehearsed; README run commands verified on a clean machine
- [ ] No secrets or datasets in Git
