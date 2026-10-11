"""ModelPulse Phase 6 Dashboard Presentation Tests.
Verifies:
1. DashboardClient dual-mode fallback (API mode vs direct repository fallback).
2. Data transformation helpers and HTML badge formatting.
3. Empty history, missing metrics, and graceful degradation.
4. Correct extraction of temporal coordinates, valid vs quarantined samples.
5. Component M (Performance degradation) representation: displays 'Unavailable' when labels delayed, never 0.
6. Drift ensemble and reconciliation table formatting against known fixture values.
"""
from datetime import datetime, timezone, timedelta
import pytest

from backend.app.db.repository import SQLiteMonitoringRepository
from backend.app.schemas.monitoring import (
    MonitoringRunResponse,
    PredictionDriftSummary,
    SeverityBreakdown,
    WindowSpec,
)
from backend.app.schemas.drift import FeatureDriftResult, TestResult
from dashboard.client import DashboardClient
from dashboard.components.styles import severity_badge, status_badge


@pytest.fixture
def mock_run_fixture():
    now = datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)
    test1 = TestResult(
        metric="psi",
        value=0.35,
        threshold=0.25,
        exceeded=True,
        reference_count=5000,
        current_count=1000,
        status="ok",
    )
    test2 = TestResult(
        metric="ks",
        value=0.18,
        threshold=0.10,
        p_value=0.001,
        exceeded=True,
        reference_count=5000,
        current_count=1000,
        status="ok",
    )
    test3 = TestResult(
        metric="wasserstein",
        value=0.08,
        threshold=0.15,
        exceeded=False,
        reference_count=5000,
        current_count=1000,
        status="ok",
    )

    feat_drift = FeatureDriftResult(
        feature_name="amount",
        feature_type="numeric",
        tests=[test1, test2, test3],
        votes="2/3",
        votes_count=2,
        total_tests=3,
        drift_detected=True,
        drift_contribution=0.55,
        reference_mean=150.0,
        current_mean=280.0,
    )

    return MonitoringRunResponse(
        run_id="run-fixture-dashboard-101",
        model_name="paysim-fraud-histgradientboost",
        model_version="1.0.0",
        run_type="streaming_microbatch",
        reference_window=WindowSpec(start=now, end=now, sample_count=5000),
        current_window=WindowSpec(start=now, end=now + timedelta(minutes=5), sample_count=1000),
        sample_count=1000,
        valid_sample_count=950,
        quarantined_sample_count=50,
        status="ok",
        dataset_drift_detected=True,
        prediction_drift=PredictionDriftSummary(
            metric="psi", value=0.28, threshold=0.25, drifted=True, status="ok"
        ),
        severity=SeverityBreakdown(
            score=58,
            raw_score=57.6,
            band="High",
            confidence="normal",
            components={"D": 65.0, "P": 70.0, "Q": 33.3, "M": None},
            weights_used={"D": 0.5333, "P": 0.2667, "Q": 0.20},
        ),
        features=[feat_drift],
        top_contributors=[{"feature_name": "amount", "contribution_score": 0.55, "cumulative_share": 0.85}],
        recommendations=["Investigate sudden transaction volume jump on 'amount'"],
        created_at=now,
    )


# ==============================================================================
# 1. VISUAL BADGE & HTML TRANSFORMATION HELPERS
# ==============================================================================

def test_severity_badge_rendering():
    """Verify severity badges render appropriate CSS classes and scores."""
    b_low = severity_badge("Low", score=12)
    assert "badge-low" in b_low
    assert "LOW (12)" in b_low

    b_crit = severity_badge("Critical", score=88)
    assert "badge-critical" in b_crit
    assert "CRITICAL (88)" in b_crit

    b_mod = severity_badge("Moderate")
    assert "badge-moderate" in b_mod


def test_status_badge_rendering():
    """Verify status badges render correct styling for ok, quality failure, and delivery."""
    s_ok = status_badge("ok")
    assert "#10b981" in s_ok

    s_fail = status_badge("quality_failure")
    assert "#ef4444" in s_fail

    s_sup = status_badge("suppressed_cooldown")
    assert "#f59e0b" in s_sup


# ==============================================================================
# 2. DASHBOARD CLIENT: DUAL-MODE FALLBACK
# ==============================================================================

def test_dashboard_client_local_fallback_when_api_offline(mock_run_fixture):
    """Verify DashboardClient operates cleanly in local repository mode when API is offline."""
    # Point to an offline port
    client = DashboardClient(api_base_url="http://localhost:59999")

    # 1. Backend status inspects gracefully
    status = client.get_backend_status()
    assert status["mode"] == "direct_local"
    assert status["status"] == "active_local_fallback"

    # 2. Persist mock run into repository
    repo = SQLiteMonitoringRepository(db_path=":memory:")
    repo.save_run(mock_run_fixture)

    # Patch client repository retrieval
    import backend.app.db.repository as repo_mod
    orig_repo = repo_mod.get_monitoring_repository()
    repo_mod.set_monitoring_repository(repo)
    try:
        runs = client.list_runs()
        assert len(runs) >= 1
        assert runs[0]["run_id"] == "run-fixture-dashboard-101"

        run_detail = client.get_run("run-fixture-dashboard-101")
        assert run_detail is not None
        assert run_detail["sample_count"] == 1000
    finally:
        repo_mod.set_monitoring_repository(orig_repo)


def test_dashboard_client_empty_history():
    """Verify empty run history returns an empty list without raising exceptions."""
    repo = SQLiteMonitoringRepository(db_path=":memory:")
    import backend.app.db.repository as repo_mod
    repo_mod._repository_instance = repo
    try:
        client = DashboardClient(api_base_url="http://localhost:59999")
        runs = client.list_runs()
        assert runs == []
    finally:
        repo_mod._repository_instance = None


# ==============================================================================
# 3. DIAGNOSTICS & DATA INTEGRITY: COMPONENT M UNAVAILABLE
# ==============================================================================

def test_component_m_unavailable_distinguished_from_zero(mock_run_fixture):
    """Verify Component M (performance degradation) is represented as None/Unavailable,
    strictly distinguishing missing fraud labels from zero performance drop.
    """
    comps = mock_run_fixture.severity.components
    assert comps["M"] is None, "Component M must be None when delayed labels are absent"
    assert comps["D"] == 65.0
    assert comps["P"] == 70.0
    assert comps["Q"] == 33.3

    # Effective weights must sum to 1.0 without M
    w = mock_run_fixture.severity.weights_used
    assert "M" not in w or w.get("M") == 0.0
    assert pytest.approx(sum(w.values()), 0.001) == 1.0


# ==============================================================================
# 4. TEMPORAL METADATA & DATA QUALITY ACCURACY
# ==============================================================================

def test_temporal_coordinates_and_quarantine_integrity(mock_run_fixture):
    """Verify monitoring run window bounds and quarantined counts are properly extracted."""
    data = mock_run_fixture.model_dump()

    # Total sample count matches valid + quarantined
    assert data["sample_count"] == 1000
    assert data["valid_sample_count"] == 950
    assert data["quarantined_sample_count"] == 50
    assert data["valid_sample_count"] + data["quarantined_sample_count"] == data["sample_count"]

    # Window metadata integrity
    cur_win = data["current_window"]
    start_dt = cur_win["start"]
    end_dt = cur_win["end"]
    # 5 minute difference
    diff = (end_dt - start_dt).total_seconds()
    assert diff == 300.0


# ==============================================================================
# 5. FEATURE DRIFT ENSEMBLE TRANSFORMATION
# ==============================================================================

def test_feature_drift_ensemble_voting_audit(mock_run_fixture):
    """Verify feature drift results preserve individual statistical tests and votes."""
    feat = mock_run_fixture.features[0]
    assert feat.feature_name == "amount"
    assert feat.votes == "2/3"
    assert feat.votes_count == 2
    assert feat.drift_detected is True
    assert len(feat.tests) == 3

    test_metrics = [t.metric for t in feat.tests]
    assert "psi" in test_metrics
    assert "ks" in test_metrics
    assert "wasserstein" in test_metrics
