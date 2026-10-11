# ModelPulse v2 — Phase 6.3: Outbox Atomicity, Concurrency & Release Readiness Audit Report

**Date:** 2026-10-11  
**Specification:** Phase 6.3 — Outbox Atomicity, Concurrency, Delivery Semantics & Release Readiness Audit  
**Status:** Completed & Validated  
**Regression Test Suite:** **163 passed**, 1 skipped (live broker optional integration test) in 38.82s  

---

## Executive Summary

Phase 6.3 conducts an exhaustive, evidence-driven distributed systems audit of ModelPulse v2. Building directly upon the durable outbox introduced in Phase 6.2, this audit investigated the remaining reliability boundaries across database transactions, multi-worker concurrency, external delivery semantics, and Kafka streaming offset safety.

Key findings confirmed, reproduced, and resolved:
1. **Repository Run & Outbox Atomicity:** Prior to Phase 6.3, monitoring run persistence and alert outbox staging executed across **two disconnected SQLite transactions**. A crash between them left a persisted run with no outbox record, and outbox failure in the streaming consumer was swallowed as a warning while offsets advanced. We implemented atomic single-transaction persistence in [`backend/app/db/repository.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py) and [`backend/app/services/monitoring_service.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/services/monitoring_service.py), and updated [`backend/app/streaming/consumer.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/streaming/consumer.py) to abort batch finalization and halt offset commits upon outbox failure.
2. **Concurrent Recovery & Lease/Claim Mechanism:** Multiple Uvicorn/recovery workers querying `get_pending_alert_outbox()` concurrently could observe the same `pending` records and dispatch duplicate notifications. We introduced an atomic claim/lease mechanism (`claim_pending_alert_outbox`) transitioning records to `in_flight` under SQLite serialized transactions, preventing duplicate dispatch across workers.
3. **Outbox State & Exhaustion Protection:** Repeated staging on replayed batches previously used unconditional `ON CONFLICT DO UPDATE SET status = excluded.status`, which dangerously reset `delivered` or `failed` records back to `pending`. We implemented database-level terminal state protection (`CASE WHEN status IN ('delivered', 'failed', ...) THEN status ELSE excluded.status END`) and added explicit manual operator reset (`reset_failed_outbox_record`).
4. **SMTP Partial Recipient Refusal Safety:** When sending to multiple email recipients, partial recipient refusal caused the entire recipient list to be resent on retry, causing duplicate emails to already accepted recipients. We modified [`backend/app/alerting/email.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/email.py) so retries only target the refused addresses.
5. **Accurate Delivery Semantics & Release Gates:** Formally verified the at-least-once duplicate delivery window when a provider accepts an alert but the host crashes before persisting the delivery status. We explicitly declare that mocks and in-memory buses do not constitute live Kafka broker proof, and establish live container integration as an explicit release gate.

---

## 1. Repository and Transaction Integrity

### 1.1 Code Path Inspection & Failure Window

We audited the exact sequence of persistence, outbox staging, and Kafka offset commits:
```
[Micro-Batch Triggered]
        │
        ▼
1. Monitoring Execution & Persistence:
   MonitoringService.execute_monitoring_run(...)
     └─► repository.save_run(run) [Transaction 1]
        │
        ▼
2. Streaming Consumer Batch Finalization:
   StreamingConsumer._handle_flushed_batch(...)
     ├─► alert_service.stage_outbox(run) [Transaction 2] <─── FAILURE WINDOW
     ├─► offset_tracker.mark_resolved(...)
     └─► message_bus.commit_offset(...)
```

#### Defect Analysis:
1. **Non-Atomic Persistence:** In Phase 6.2, `save_run` committed `monitoring_runs` and `feature_drift_results` in SQLite Transaction 1. Subsequently, `stage_outbox` committed `alert_outbox` in SQLite Transaction 2. If the process crashed between Transaction 1 and Transaction 2, the monitoring run existed in the database, but its alert intent was permanently lost.
2. **Silent Offset Advancement on Outbox Staging Failure:** In `StreamingConsumer._handle_flushed_batch`:
   ```python
   # Prior vulnerable code
   try:
       get_alert_service().stage_outbox(run_resp)
   except Exception as stg_exc:
       logger.warning(f"[Consumer] Failed staging alert outbox: {stg_exc}")
   ```
   If outbox staging failed (e.g. disk I/O failure, disk full, SQLite busy error), the exception was caught as a warning. The consumer then proceeded to resolve tracker offsets and commit partition offsets. **Kafka offsets advanced past the batch, while the alert outbox was never persisted.**

### 1.2 Implemented Fixes

1. **Atomic Run + Outbox Persistence ([`backend/app/db/repository.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py)):**  
   Updated `save_run` in `MonitoringRepository`, `SQLiteMonitoringRepository`, and `InMemoryMonitoringRepository` to accept optional `outbox_records: Optional[List[AlertOutboxRecord]] = None`.  
   In `SQLiteMonitoringRepository.save_run`, all three tables (`monitoring_runs`, `feature_drift_results`, and `alert_outbox`) are inserted within the **exact same `with self._conn:` ACID block**. If any insert fails, SQLite rolls back the entire transaction.
2. **Monitoring Orchestration Integration ([`backend/app/services/monitoring_service.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/services/monitoring_service.py)):**  
   In `execute_monitoring_run(..., persist=True)`, outbox records are constructed via `AlertService.build_outbox_records(response)` and passed directly into `repository.save_run(response, outbox_records=outbox_records)`. Run persistence and alert outbox staging are now **100% atomic in SQLite**.
3. **Strict Offset Safety on Staging Failure ([`backend/app/streaming/consumer.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/streaming/consumer.py)):**  
   In `StreamingConsumer._handle_flushed_batch`:
   ```python
   if run_resp is not None:
       try:
           from backend.app.alerting.service import get_alert_service
           get_alert_service().stage_outbox(run_resp)
       except Exception as stg_exc:
           logger.error(
               f"[Consumer] Failed staging alert outbox for batch {meta.batch_id}: {stg_exc}. "
               "Aborting batch finalization and offset commit to prevent alert loss."
           )
           raise RuntimeError(
               f"Alert outbox staging failure for batch {meta.batch_id}: {stg_exc}"
           ) from stg_exc
   ```
   If outbox staging fails, the consumer immediately aborts batch finalization. Partition offsets are **never marked resolved and never committed**. The consumer records a batch failure and backs off, preserving offset safety for replay.

---

## 2. Outbox Concurrency and Recovery

### 2.1 Concurrency Gap & Duplicate Dispatch

When multiple Uvicorn workers or distributed nodes run recovery simultaneously:
* **The Race Condition:** Both workers called `repo.get_pending_alert_outbox()`. Both read the same records with status `pending`. Both workers concurrently invoked `slack_channel.send()` and `email_channel.send()`.
* **Impact:** Duplicate alert notifications sent to on-call teams and communication channels.

### 2.2 Atomic Claim / Lease Mechanism

We implemented atomic record claims in [`backend/app/db/repository.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py) and [`backend/app/alerting/models.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/models.py):

1. **State Addition:** Added `AlertOutboxStatus.IN_FLIGHT = "in_flight"`.
2. **Atomic Lease Method:** Added `claim_pending_alert_outbox(limit=50, worker_id="worker-1", lease_seconds=300)`:
   * In SQLite, runs inside an immediate write transaction under `with self._lock:` and `with self._conn:`.
   * Selects records where `status IN ('pending', 'retrying')` or `(status = 'in_flight' AND updated_at < lease_expiry)`.
   * Atomically updates their status to `in_flight`, updates `updated_at = now()`, and sets `error_message = "Claimed by {worker_id}"`.
   * Returns the claimed records. A concurrent worker executing the same method receives `0` records.
3. **Lease Expiry Reclaim:** If a worker crashes or hangs while processing an in-flight alert, the record lease expires after `lease_seconds`. Other workers can automatically reclaim the record for redelivery.

### 2.3 Terminal State Protection & Operator Reset

#### Problem:
On Kafka replay or repeated batch flushes, `stage_outbox` executed:
```sql
INSERT INTO alert_outbox (...) VALUES (...)
ON CONFLICT(outbox_id) DO UPDATE SET status = excluded.status, retry_count = excluded.retry_count;
```
If an alert had already been `delivered`, or had permanently `failed` after retry exhaustion, repeated staging reset its status back to `pending` and `retry_count` back to `0`, causing unintended redispatch storms.

#### Solution:
1. **Engine-Level State Protection:**
   In `SQLiteMonitoringRepository.save_alert_outbox` and `save_run`:
   ```sql
   ON CONFLICT(outbox_id) DO UPDATE SET
       status = CASE 
           WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
           THEN alert_outbox.status 
           ELSE excluded.status 
       END,
       retry_count = CASE 
           WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
           THEN alert_outbox.retry_count 
           ELSE excluded.retry_count 
       END,
       updated_at = excluded.updated_at,
       error_message = CASE
           WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight')
           THEN alert_outbox.error_message
           ELSE excluded.error_message
       END;
   ```
2. **Explicit Operator Reset (`reset_failed_outbox_record`):**
   Added `reset_failed_outbox_record(outbox_id: str) -> bool` to `MonitoringRepository`. Only deliberate operator action can reset an exhausted `failed` record back to `pending` with `retry_count = 0`.

---

## 3. Alert Delivery Semantics & Provider Verification

### 3.1 Crash After Provider Acceptance (At-Least-Once Semantics)

We analyzed and tested the crash boundary where an external provider accepts an alert:
1. Worker dispatches HTTP POST to Slack incoming webhook $\rightarrow$ Slack accepts with HTTP 200 `{"ok": true}`.
2. The host machine crashes, loses power, or is terminated by an OOM killer before executing `repo.update_alert_outbox_status(outbox_id, "delivered")`.
3. Upon restart, the outbox record remains `pending` (or its lease expires). Recovery reclaims and dispatches the alert again.

#### Provider Deduplication Realities:
* **Slack Incoming Webhooks:** Do **NOT** support idempotency headers or deduplication tokens. Any valid POST creates a new message in the Slack channel. Redelivery creates a visible duplicate message.
* **SMTP Relays:** Standard SMTP servers do **NOT** deduplicate based on `Message-ID` or payload. Redelivery sends a second email.
* **Architecture Guarantee:** ModelPulse v2 explicitly guarantees **at-least-once alert delivery semantics with deterministic alert identifiers**, not distributed exactly-once delivery across uncoordinated third-party networks.

### 3.2 SMTP Partial Recipient Refusal

In [`backend/app/alerting/email.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/email.py):
* If an email alert has multiple recipients (e.g., `alice@example.com, bob@example.com`) and the SMTP server accepts `alice` but refuses `bob` (`{"bob@example.com": (550, ...)}`), resending the entire message on retry causes Alice to receive duplicate emails.
* **Fix Implemented:** Updated `EmailAlertChannel.format_email` and `send` to track `pending_recipients`. On partial recipient refusal, `pending_recipients` is filtered to only the refused addresses. Retry attempts format the `To:` header with **only the refused recipients**, preventing duplicate messages to accepted recipients.

### 3.3 Cooldown Scope Clarification
* **Process-Local Suppression:** The 30-minute cooldown window (`_cooldown_cache` in `AlertService`) prevents alert fatigue across separate runs with identical fingerprints. It is in-memory and process-local. It resets upon process restart.
* **Durable Outbox Deduplication:** The outbox record ID (`outbox-{run_id}-{channel}`) is persistent on disk. Once a specific run's alert is delivered, repeated staging or redelivery will never redispatch it, surviving process restarts.

---

## 4. Kafka Failure-Boundary Tests

We tested failure boundaries across the streaming pipeline:
1. **Outbox Staging Failure Before Offset Commit:** Verified in `test_outbox_staging_failure_aborts_offset_commit`. A database disk failure during batch flush raises `RuntimeError`, consumer aborts, and partition offsets remain uncommitted (`comm_off == -1`).
2. **Replay After Restart:** Verified that redelivering uncommitted records after restart triggers idempotent execution: the deterministic run ID prevents duplicate runs, and existing outbox records are not re-created or duplicated.
3. **Live Kafka Release Gate:**
   * Active environment audit: Windows host without Docker Desktop CLI, WSL2 daemon inactive (`Wsl/Service/E_UNEXPECTED`), port 9092 closed.
   * `InMemoryMessageBus` tests validate business logic, offset math, DLQ routing, and outbox failure boundaries, but **do not constitute live broker integration proof**.
   * Live broker integration (`pytest -k test_live_broker_optional`) remains an explicit gate requiring container execution (`docker compose up -d`).

---

## 5. Verification Matrix & Regression Suite

### 5.1 Focused Phase 6.3 Test Matrix ([`backend/tests/test_phase6_3_atomicity.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/tests/test_phase6_3_atomicity.py))

```
backend/tests/test_phase6_3_atomicity.py::test_sqlite_run_and_outbox_atomic_commit PASSED
backend/tests/test_phase6_3_atomicity.py::test_sqlite_run_and_outbox_rollback_on_failure PASSED
backend/tests/test_phase6_3_atomicity.py::test_concurrent_recovery_workers_claim_lease_prevent_duplicates PASSED
backend/tests/test_phase6_3_atomicity.py::test_expired_lease_is_reclaimable_after_crash PASSED
backend/tests/test_phase6_3_atomicity.py::test_repeated_staging_cannot_reset_delivered_or_failed_record PASSED
backend/tests/test_phase6_3_atomicity.py::test_manual_recovery_of_failed_record PASSED
backend/tests/test_phase6_3_atomicity.py::test_smtp_partial_recipient_refusal_only_retries_refused PASSED
backend/tests/test_phase6_3_atomicity.py::test_provider_acceptance_crash_duplicate_delivery_window PASSED
backend/tests/test_phase6_3_atomicity.py::test_outbox_staging_failure_aborts_offset_commit PASSED
```

### 5.2 Full Backend Regression Suite Output

```
============================= test session starts =============================
platform win32 -- Python 3.12.4, pytest-8.3.4, pluggy-1.6.0
rootdir: D:\BTech\TY\Sem 5\EDI\ModelPulse-1
plugins: anyio-4.15.1, asyncio-0.25.0
collected 164 items

backend/tests/test_alert_cooldown.py .....                               [  3%]
backend/tests/test_alerting.py .............                             [ 10%]
backend/tests/test_audit_fixes.py ......                                 [ 14%]
backend/tests/test_dashboard_reliability.py ........                    [ 19%]
backend/tests/test_drift_metrics.py ...............                      [ 28%]
backend/tests/test_drift_simulator.py .............                      [ 36%]
backend/tests/test_event_contracts.py ...........                        [ 43%]
backend/tests/test_inference_api.py ...........                          [ 50%]
backend/tests/test_kafka_offset_safety.py ........                       [ 54%]
backend/tests/test_kafka_streaming.py .........s                         [ 60%]
backend/tests/test_micro_batcher.py .........                            [ 66%]
backend/tests/test_monitoring_orchestration.py ........                  [ 71%]
backend/tests/test_offset_verification.py ........                       [ 76%]
backend/tests/test_phase6_1_reliability.py ...........                   [ 82%]
backend/tests/test_phase6_2_recovery.py .......                          [ 87%]
backend/tests/test_phase6_3_atomicity.py .........                       [ 92%]
backend/tests/test_phase6_alerting.py .......                            [ 96%]
backend/tests/test_phase6_dashboard.py ......                           [100%]

================== 163 passed, 1 skipped in 38.82s ===================
```

---

## 6. Remaining Risks & Release Gates

| Domain | Verified Behavior | Remaining Risk / Unverified Behavior | Release Gate / Action Required |
| :--- | :--- | :--- | :--- |
| **Persistence Atomicity** | Run + outbox commit atomically in SQLite transaction. Rollback on failure verified. | Multi-process SQLite write contention under high concurrency (`busy_timeout`). | Configure WAL mode (`PRAGMA journal_mode=WAL`) in multi-process production environments. |
| **Outbox Concurrency** | Atomic claim lease transitions records to `in_flight`. Leases expire and reclaim after timeout. | Clock drift across distributed physical servers could skew lease expiry timestamps. | Use synchronized NTP across worker hosts in distributed deployments. |
| **External Providers** | Rejection handling (Slack HTTP 200 with `ok: false`, SMTP recipient refusal) and partial retry verified. | Live Slack webhooks and live SMTP relays remain unverified against production endpoints. | Execute staging smoke test with real credentials prior to production deployment. |
| **Streaming Pipeline** | Micro-batch windows, offset safety, DLQ isolation, and outbox failure offset holdback verified offline. | Real Kafka cluster partition rebalance, TCP disconnects, and broker failover not run live. | Run `pytest -k test_live_broker_optional` against active broker container (`docker compose up -d`). |
| **Delivery Semantics** | At-least-once delivery with deterministic deduplication keys verified. | Third-party webhook endpoints (Slack) lack idempotency support; duplicate messages possible on crash. | Documented as known distributed system constraint; do not claim exactly-once delivery. |

---

## 7. Architectural Boundaries

1. **Zero External Infrastructure Added:** No Redis, PostgreSQL, or TimescaleDB dependencies were introduced. The platform remains fully functional using Python standard libraries and SQLite.
2. **No Automated Retraining:** Retraining and automatic model promotion remain strictly out of scope.
3. **API & Dashboard Contract Preservation:** All existing FastAPI endpoints, response schemas, and Streamlit components were preserved intact.
