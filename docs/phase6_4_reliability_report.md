# ModelPulse v2 — Phase 6.4: Lease Fencing, Recovery Stress Tests & Live Kafka Release Gate Report

**Date:** 2026-10-11  
**Specification:** Phase 6.4 — Lease Fencing, Recovery Stress Tests & Live Kafka Release Gate  
**Status:** Completed & Validated  
**Regression Test Suite:** **171 passed**, 1 skipped (live broker optional integration test) in 49.20s  

---

## Executive Summary

Phase 6.4 performs a targeted distributed-systems audit of ModelPulse v2 focusing on concurrency safety, lease fencing, state-machine integrity, and streaming failure recovery. Building upon the Phase 6.3 transactional outbox foundation, this phase addresses subtle multi-worker races, guarantees monotonic fencing tokens across recovery cycles, protects terminal outbox records from clobbering, and rigorously enforces the live Kafka release gate.

Key findings confirmed, reproduced, and resolved:
1. **Lease Expiry & Stale-Worker Race Condition:** Prior to Phase 6.4, `claim_pending_alert_outbox` updated expired records to `in_flight` without recording a monotonic generation/fencing token. If Worker A's lease expired and Worker B reclaimed the record, a resuming Worker A calling `update_alert_outbox_status(outbox_id, ...)` executed an unconditional `WHERE outbox_id = ?`, overwriting Worker B's active processing or delivery status. We introduced monotonically increasing `claim_token` fencing tokens and `claimed_by` lease metadata, enforcing atomic fencing checks (`AND claim_token = ?`) in both SQLite and In-Memory repositories.
2. **Terminal State Protection & Audit:** We audited all outbox state transitions (`pending`, `in_flight`, `delivered`, `suppressed_cooldown`, `skipped_disabled`, and `failed`). Standard status updates are now prohibited from mutating terminal records (`delivered`, `suppressed_cooldown`, `skipped_disabled`, `failed`). Replay or duplicate batch staging cannot reset completed or terminal records back to pending.
3. **Restricted Operator Reset:** `reset_failed_outbox_record` was hardened with a strict `WHERE status = 'failed'` guard, preventing accidental operator resets of already-delivered alerts.
4. **Failure Injection & Offset Safety:** Verified that SQLite database persistence failure rolls back the atomic monitoring-run and outbox transaction, raises a `RuntimeError` in `StreamingConsumer`, and halts Kafka offset commits so unpersisted batches can be safely replayed.
5. **Live Kafka Release Gate:** Directly probed the Windows host environment. Docker and Podman were absent on the host PATH, WSL2 daemon returned `Wsl/Service/E_UNEXPECTED`, and TCP port 9092 was unreachable. In strict adherence to our reliability guidelines, mocked and in-memory tests are explicitly separated from live Kafka integration, and live Kafka verification remains an active release gate.

---

## 1. Lease Ownership and Fencing Token Audit

### 1.1 Reproducing the Stale-Worker Race Condition

We analyzed the distributed recovery sequence between two workers:
```
Time  Worker A                           Worker B                           Database (alert_outbox)
───   ─────────────────────────────      ─────────────────────────────      ───────────────────────
t0    Claims record (lease: 1s)                                             status=in_flight, claim_token=1, claimed_by=A
t1    Stalls (GC pause / slow HTTP)
t2    (Lease expires at t0 + 1s)
t3                                       Reclaims record (lease: 10s)       status=in_flight, claim_token=2, claimed_by=B
t4    Worker A wakes up!
      Attempts update:
      WHERE outbox_id = ?                [WITHOUT FENCING TOKEN]            status=failed (Worker A clobbers B!)
```

#### Defect Analysis:
In Phase 6.3:
```python
# Vulnerable Phase 6.3 implementation
UPDATE alert_outbox
SET status = ?, error_message = ?, updated_at = ?
WHERE outbox_id = ?;
```
Because the `WHERE` clause lacked both a fencing token check and lease ownership verification:
- Worker A had no awareness that its lease had expired.
- Worker A's late status write overwrote Worker B's active lease, resetting Worker B's ownership and overwriting newer diagnostic error messages or delivery states.
- If Worker B had already successfully delivered the alert, Worker A could overwrite `status='delivered'` with `status='failed'`, creating corrupt state and spurious duplicate alerts on operator reset.

### 1.2 Implemented Fencing Token Mechanism

We introduced a monotonically increasing fencing token pattern ([`backend/app/alerting/models.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/models.py) and [`backend/app/db/repository.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py)):

1. **Schema & Model Extension:**
   - Added `claim_token: int = 0` (monotonic generation counter) and `claimed_by: Optional[str] = None` (worker identifier) to `AlertOutboxRecord`.
   - Added `claim_token INTEGER NOT NULL DEFAULT 0` and `claimed_by TEXT` to the SQLite `alert_outbox` table with dynamic schema migration in `_init_db()`.

2. **Atomic Lease Claiming:**
   Each time a record is claimed (either initially from `pending` or upon reclaiming an expired `in_flight` record), SQLite atomically increments the token and records the worker identity:
   ```sql
   UPDATE alert_outbox
   SET status = 'in_flight',
       claim_token = claim_token + 1,
       claimed_by = ?,
       updated_at = ?,
       error_message = ?
   WHERE outbox_id IN (...);
   ```

3. **Fencing Token Validation on Status Update:**
   `update_alert_outbox_status` now accepts `claim_token: Optional[int] = None`:
   ```sql
   UPDATE alert_outbox
   SET status = ?, error_message = ?, retry_count = ?, updated_at = ?
   WHERE outbox_id = ?
     AND status NOT IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed')
     AND (? IS NULL OR claim_token = ?);
   ```
   If Worker A attempts to commit with stale `claim_token=1` while the database has `claim_token=2`:
   - `claim_token = ?` evaluates to `1 = 2` (False).
   - Zero rows are updated (`cursor.rowcount == 0`).
   - The method returns `False`, rejecting Worker A's stale update.
   - Worker B's active ownership and state are preserved intact.

4. **AlertService Recovery Integration:**
   In [`AlertService.recover_and_dispatch_pending`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/service.py#L250-L315), workers pass `claim_token=outbox_rec.claim_token`. If the update returns `False`:
   ```python
   logger.warning(
       f"[AlertService] Fencing token rejection for {outbox_rec.outbox_id} (token={outbox_rec.claim_token}). "
       "Lease expired and record was reclaimed by another worker."
   )
   ```
   The stale worker does not record cooldown or modify alert history.

---

## 2. Outbox State-Machine Audit

### 2.1 State Transition Specification

The complete audited state machine for `alert_outbox` records:

| Current Status | Event / Trigger | Target Status | Guard / Validation |
| :--- | :--- | :--- | :--- |
| `pending` | Worker claim (`claim_pending_alert_outbox`) | `in_flight` | `retry_count < max_retries`, atomic `claim_token = claim_token + 1` |
| `pending` | Inline evaluation delivered (`evaluate_and_dispatch`) | `delivered` | Severity >= threshold, provider 200 OK |
| `pending` | Inline evaluation suppressed (`evaluate_and_dispatch`) | `suppressed_cooldown` | Active cooldown window (< 300s) |
| `pending` | Inline evaluation skipped (`evaluate_and_dispatch`) | `skipped_disabled` | Channel unconfigured (missing token / host) |
| `in_flight` | Successful dispatch (`recover_and_dispatch_pending`) | `delivered` | Provider 200 OK, `claim_token` matches current lease |
| `in_flight` | Cooldown detected on recovery | `suppressed_cooldown` | Cooldown active, `claim_token` matches |
| `in_flight` | Channel unconfigured on recovery | `skipped_disabled` | Missing channel config, `claim_token` matches |
| `in_flight` | Transient dispatch error (< max_retries) | `pending` | Increments `retry_count`, released for next retry |
| `in_flight` | Delivery failure (>= max_retries) | `failed` | `retry_count >= max_retries`, terminal failure |
| `in_flight` (expired) | Lease expiry (`updated_at < now - lease_seconds`) | `in_flight` | Reclaimed by new worker, `claim_token = claim_token + 1` |
| `failed` | Operator manual reset (`reset_failed_outbox_record`) | `pending` | Operator API call, `retry_count = 0`, status was strictly `failed` |
| `delivered` | Any update or replay | `delivered` (NO-OP) | **Immutable terminal state.** Updates rejected; replay preserved |
| `suppressed_cooldown` | Any update or replay | `suppressed_cooldown` (NO-OP) | **Immutable terminal state.** Updates rejected |
| `skipped_disabled` | Any update or replay | `skipped_disabled` (NO-OP) | **Immutable terminal state.** Updates rejected |

### 2.2 Bounded Retries & Process Restarts
- Retries are tracked via `retry_count` and bounded by `max_retries` (default: 3) persisted directly in SQLite.
- Across worker restarts and host reboots, `get_pending_alert_outbox` and `claim_pending_alert_outbox` explicitly filter `WHERE retry_count < max_retries`.
- Once `retry_count >= max_retries`, the record transitions to `failed` and will never be retried automatically.

### 2.3 Terminal State Immutability
- In SQLite `update_alert_outbox_status`, the update query includes:
  `WHERE status NOT IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed')`.
  Once a record reaches a terminal state, subsequent update attempts return `False` and make no modifications.
- In `save_run` and `save_alert_outbox`, the `ON CONFLICT` clause strictly preserves terminal states:
  ```sql
  status = CASE 
      WHEN alert_outbox.status IN ('delivered', 'suppressed_cooldown', 'skipped_disabled', 'failed', 'in_flight') 
      THEN alert_outbox.status 
      ELSE excluded.status 
  END
  ```
  Replayed batches or duplicate staging cannot reset completed or terminal records back to `pending`.

### 2.4 Operator Reset Hardening
- `reset_failed_outbox_record(outbox_id)` was hardened with `WHERE outbox_id = ? AND status = 'failed'`.
- Attempting to call `reset_failed_outbox_record` on a `delivered` record matches 0 rows and returns `False`. The delivered record remains intact.

---

## 3. Persistence and Streaming Failure Injection

### 3.1 Failure Injection Scenarios Tested

We tested persistence and streaming failure injection in [`backend/tests/test_phase6_4_fencing.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/tests/test_phase6_4_fencing.py):

1. **SQLite Database Failure Injection (`test_database_failure_injection_prevents_offset_commit`):**
   - Simulated database disk I/O failure (`repo.simulate_failure = True`) during micro-batch flush.
   - **Result:** `save_run` raised `RuntimeError("Simulated SQLite database failure")`.
   - **Verification:**
     - Neither the monitoring run nor the outbox records were committed in SQLite (`repo.count_runs() == 0`).
     - In `StreamingConsumer`, batch finalization was aborted.
     - Kafka partition offset was **NOT committed** (remained `-1`).
     - Offset safety guaranteed: uncommitted messages remain available for redelivery upon consumer restart.

2. **Idempotent Replay After Restart:**
   - Simulated re-ingestion of the identical batch following a consumer restart.
   - **Result:** `save_run` executed with `ON CONFLICT(outbox_id) DO UPDATE` maintaining existing terminal records without generating duplicate outbox entries or duplicate alert notifications.

3. **DLQ Isolation on Malformed Payloads:**
   - Malformed JSON and schema validation failures were immediately routed to the Dead Letter Queue (`test-dlq`) with full diagnostic headers.
   - Valid records in the same topic partition were processed into micro-batches without interference.

---

## 4. Live Kafka Release Gate

### 4.1 Broker Environment Probing

In compliance with Phase 6.4 instructions, we inspected the existing Docker Compose configuration and probed the local host environment for a running Kafka broker:

1. **Docker Compose Inspection ([`docker-compose.yml`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/docker-compose.yml)):**
   - Configured service: `confluentinc/cp-kafka:7.7.0` running in KRaft mode (ZooKeeper not required).
   - Port mapping: `9092:9092` (host PLAINTEXT listener) and `29092` (internal broker listener).
   - Cluster ID: `MkU3OEVBNTcwNTJENDM2Qk`.

2. **Host Diagnostic Probes:**
   - **TCP Port 9092 Probe:**
     ```powershell
     Test-NetConnection -ComputerName localhost -Port 9092
     # Result: TcpTestSucceeded: False (Port 9092 closed)
     ```
   - **Container Engine Probe:**
     ```powershell
     Get-Command docker -ErrorAction SilentlyContinue
     Get-Command docker-compose -ErrorAction SilentlyContinue
     Get-Command podman -ErrorAction SilentlyContinue
     # Result: Neither Docker, Docker Compose, nor Podman is installed on Windows PATH.
     ```
   - **WSL2 Subsystem Probe:**
     ```powershell
     wsl -e which docker
     # Result: Catastrophic failure. Error code: Wsl/Service/E_UNEXPECTED
     ```

### 4.2 Exact Blocker & Release Gate Status

| Component | Status | Blocker Description | Required Action to Clear Gate |
| :--- | :--- | :--- | :--- |
| **Docker Desktop / Engine** | **Unavailable** | Docker CLI not installed on host PATH; WSL2 service in `E_UNEXPECTED` state. | Install Docker Desktop for Windows or repair WSL2 daemon. |
| **Local Kafka Broker** | **Offline** | Port 9092 connection refused. | Run `docker-compose up -d kafka` to start KRaft broker. |
| **Live Integration Test** | **Skipped (Clean)** | `backend/tests/test_phase5.py::test_live_kafka_broker_integration` skipped gracefully. | Run `pytest backend/tests/test_phase5.py -k test_live_kafka_broker_integration`. |

### 4.3 Explicit Categorization of Test Evidence

To maintain complete architectural integrity:
- **VERIFIED (Unit, Concurrency & ACID Persistence):**
  - Fencing token monotonicity and stale-worker rejection (`test_stale_worker_fencing_token_rejection_sqlite`, `test_stale_worker_fencing_token_rejection_in_memory`).
  - Outbox state machine transitions, bounded retries, and terminal record protection.
  - Operator reset restriction to failed records.
  - Streaming consumer offset holdback on database failure injection.
  - In-memory event bus batching, windowing, and offset tracker mechanics.
- **MOCKED (Network Boundary):**
  - Slack webhook delivery via `httpx.MockTransport`.
  - SMTP delivery via `unittest.mock.patch("aiosmtplib.send")`.
- **UNVERIFIED & HELD AT RELEASE GATE (Live Broker):**
  - Real TCP socket communication against an Apache Kafka broker process.
  - **Live Kafka integration remains an active RELEASE GATE and will not be reported as passed until a live broker environment is brought online and verified.**

---

## 5. Verification Commands for Live Kafka

When Docker or a real Kafka broker is provisioned on the host, run the following commands to clear the release gate:

```bash
# 1. Start KRaft Kafka broker
docker-compose up -d kafka

# 2. Verify broker health (wait ~10s for KRaft initialization)
docker-compose ps
powershell -Command "Test-NetConnection -ComputerName localhost -Port 9092"

# 3. Run the live Kafka integration test
.\.venv\Scripts\python.exe -m pytest backend/tests/test_phase5.py -k test_live_kafka_broker_integration -v

# 4. Run the complete test suite with live broker active
.\.venv\Scripts\python.exe -m pytest backend/tests -v
```

---

## 6. Regression Results and Modified Files

### 6.1 Test Suite Summary

- **Total Tests:** 172
- **Passed:** **171**
- **Skipped:** **1** (`test_live_kafka_broker_integration` — intentional live broker release gate)
- **Failed:** **0**
- **Execution Time:** 49.20s

```
============================= test session starts =============================
platform win32 -- Python 3.12.4, pytest-8.3.4, pluggy-1.6.0
rootdir: D:\BTech\TY\Sem 5\EDI\ModelPulse-1
plugins: anyio-4.15.1, asyncio-0.25.0
171 passed, 1 skipped in 49.20s
================================================================================
```

### 6.2 Modified & Created Files

| File | Change Summary |
| :--- | :--- |
| [`backend/app/alerting/models.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/models.py) | Added `claim_token: int = 0` and `claimed_by: Optional[str] = None` to `AlertOutboxRecord`. |
| [`backend/app/db/repository.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/db/repository.py) | Added schema migration for `claim_token` and `claimed_by`; atomic `claim_token = claim_token + 1` in `claim_pending_alert_outbox`; fencing token check (`AND claim_token = ?`) and terminal state protection in `update_alert_outbox_status`; hardened `reset_failed_outbox_record` with `AND status = 'failed'`; updated `InMemoryMonitoringRepository` with full feature parity. |
| [`backend/app/alerting/service.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/app/alerting/service.py) | Updated `recover_and_dispatch_pending` to pass `claim_token` on updates, log fencing rejection warnings without corrupting cooldown/history, and handle channel delivery status cleanly. |
| [`backend/tests/test_phase6_4_fencing.py`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/backend/tests/test_phase6_4_fencing.py) | Created 8 focused tests covering stale-worker fencing token rejection (SQLite & In-Memory), successive lease expirations, terminal record immutability, bounded retry exhaustion, operator reset restrictions, streaming failure injection, and alert recovery fencing. |
| [`docs/phase6_4_reliability_report.md`](file:///d:/BTech/TY/Sem%205/EDI/ModelPulse-1/docs/phase6_4_reliability_report.md) | Exhaustive reliability report documenting findings, fixes, state machine transitions, failure injection, and live Kafka release gate status. |

---

## 7. Conclusion

ModelPulse v2 Phase 6.4 successfully resolves the stale-worker lease race condition through monotonically increasing fencing tokens, strictly bounds retries across worker restarts, locks down terminal outbox states, and confirms persistence failure safety. The live Kafka release gate is explicitly documented with clear prerequisites, preserving engineering rigor and distributed systems correctness.
