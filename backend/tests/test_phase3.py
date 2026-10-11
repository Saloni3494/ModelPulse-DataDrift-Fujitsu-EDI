"""Phase 3 Test Suite: Controlled Drift Simulation & Severity Engine.
Verifies:
1. All 7 simulator scenarios, ground-truth manifests, and injected fault verification.
2. Strict reference immutability (unmutated reference DataFrames).
3. Reproducibility across random seeds.
4. Realistic class imbalance and synthetic disclaimers.
5. Severity scoring components (D, P, Q, M) normalization and saturation.
6. Dynamic weight renormalisation when components (like delayed labels) are absent.
7. Independent mathematical recalculation of severity scores.
8. Boundedness in [0, 100] and exact band boundary mapping.
9. Monotonicity: worsening any component cannot lower severity.
10. Edge cases: zero denominators, missing components, invalid inputs.
"""
import math
import numpy as np
import pandas as pd
import pytest

from simulator import (
    ScenarioManifest,
    SYNTHETIC_DISCLAIMER,
    load_or_create_reference,
    scenario_no_drift,
    scenario_numeric_mean_variance_shift,
    scenario_categorical_proportion_shift,
    scenario_unseen_categories,
    scenario_sudden_drift,
    scenario_gradual_drift,
    scenario_data_quality_failures,
    generate_all_scenarios,
)
from backend.app.schemas.drift import FeatureDriftResult, TestResult
from backend.app.schemas.monitoring import SeverityResult, SeverityBreakdown
from backend.app.services.severity_service import (
    SeverityConfig,
    compute_component_d,
    compute_component_p,
    compute_component_q,
    compute_component_m,
    compute_severity_score,
    evaluate_severity,
    HEURISTIC_DISCLAIMER,
)


@pytest.fixture
def synthetic_reference_df() -> pd.DataFrame:
    """Fixture providing clean reference dataset for testing."""
    return load_or_create_reference(reference_path="ml/artifacts/reference_features.csv", seed=42)


# ──────────────────────────────────────────────────────────────────────────────
# PART A: SIMULATOR TESTS
# ──────────────────────────────────────────────────────────────────────────────

def test_simulator_reference_immutability(synthetic_reference_df: pd.DataFrame):
    """Verify that none of the 7 scenarios mutate the input reference DataFrame."""
    ref_before = synthetic_reference_df.copy(deep=True)

    # Execute all scenarios
    scenarios = generate_all_scenarios(synthetic_reference_df, seed=100, n_samples=200)

    # Strictly assert that synthetic_reference_df has not been modified in any way
    pd.testing.assert_frame_equal(synthetic_reference_df, ref_before)
    assert len(scenarios) == 7


def test_simulator_reproducibility(synthetic_reference_df: pd.DataFrame):
    """Verify scenarios run with the same seed generate bit-for-bit identical outputs."""
    df1, m1 = scenario_numeric_mean_variance_shift(synthetic_reference_df, seed=42, n_samples=300)
    df2, m2 = scenario_numeric_mean_variance_shift(synthetic_reference_df, seed=42, n_samples=300)

    pd.testing.assert_frame_equal(df1, df2)
    assert m1.model_dump(exclude={"created_at"}) == m2.model_dump(exclude={"created_at"})

    # Different seed produces different data
    df3, m3 = scenario_numeric_mean_variance_shift(synthetic_reference_df, seed=99, n_samples=300)
    assert not df1["amount"].equals(df3["amount"])
    assert m1.seed != m3.seed


def test_simulator_scenario_no_drift(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 1 generates clean no-drift window and valid manifest."""
    cur_df, manifest = scenario_no_drift(synthetic_reference_df, seed=42, n_samples=500)

    assert len(cur_df) == 500
    assert manifest.scenario_id == "no_drift"
    assert manifest.drift_type == "none"
    assert manifest.changed_features == []
    assert manifest.injected_faults == []
    assert manifest.sample_counts["current"] == 500
    assert manifest.sample_counts["reference"] == len(synthetic_reference_df)
    assert manifest.synthetic_notice == SYNTHETIC_DISCLAIMER


def test_simulator_scenario_numeric_shift(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 2 shifts mean/variance of designated numeric features."""
    cur_df, manifest = scenario_numeric_mean_variance_shift(
        synthetic_reference_df,
        seed=42,
        n_samples=500,
        features=("amount", "oldbalanceOrg"),
        mean_multiplier=3.0,
        var_multiplier=2.0,
    )

    assert set(manifest.changed_features) == {"amount", "oldbalanceOrg"}
    assert manifest.drift_type == "numeric_mean_variance"

    # Injected changes verification: amount mean should be significantly larger
    ref_amount_mean = synthetic_reference_df["amount"].mean()
    cur_amount_mean = cur_df["amount"].mean()
    assert cur_amount_mean > ref_amount_mean * 1.5

    # Non-negativity constraint preserved
    assert (cur_df["amount"] >= 0).all()
    assert (cur_df["oldbalanceOrg"] >= 0).all()


def test_simulator_scenario_categorical_shift(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 3 alters category proportion distribution."""
    targets = {
        "TRANSFER": 0.60,
        "CASH_OUT": 0.30,
        "PAYMENT": 0.05,
        "CASH_IN": 0.03,
        "DEBIT": 0.02,
    }
    cur_df, manifest = scenario_categorical_proportion_shift(
        synthetic_reference_df,
        seed=42,
        n_samples=1000,
        feature="type",
        target_proportions=targets,
    )

    assert manifest.changed_features == ["type"]
    assert manifest.drift_type == "categorical_proportion"

    counts = cur_df["type"].value_counts(normalize=True)
    assert abs(counts.get("TRANSFER", 0.0) - 0.60) < 0.05
    assert abs(counts.get("PAYMENT", 0.0) - 0.05) < 0.03


def test_simulator_scenario_unseen_categories(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 4 injects novel unseen category levels."""
    cur_df, manifest = scenario_unseen_categories(
        synthetic_reference_df,
        seed=42,
        n_samples=500,
        feature="type",
        novel_categories=("CRYPTO", "WIRE_INTL"),
        injection_rate=0.20,
    )

    assert manifest.changed_features == ["type"]
    assert manifest.drift_type == "unseen_categories"
    assert len(manifest.injected_faults) == 1

    unique_cur = set(cur_df["type"].unique())
    unique_ref = set(synthetic_reference_df["type"].unique())
    unseen = unique_cur - unique_ref

    assert "CRYPTO" in unseen or "WIRE_INTL" in unseen
    # Injected rate verification
    injected_count = cur_df["type"].isin(["CRYPTO", "WIRE_INTL"]).sum()
    assert injected_count == int(500 * 0.20)


def test_simulator_scenario_sudden_drift(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 5 applies sudden multiplier jump at split index."""
    cur_df, manifest = scenario_sudden_drift(
        synthetic_reference_df,
        seed=42,
        n_samples=400,
        feature="amount",
        step_split_ratio=0.5,
        magnitude_multiplier=4.0,
    )

    assert manifest.changed_features == ["amount"]
    assert manifest.drift_type == "sudden"
    assert manifest.start_window == 200

    first_half = cur_df.iloc[:200]["amount"].mean()
    second_half = cur_df.iloc[200:]["amount"].mean()
    assert second_half > first_half * 2.0


def test_simulator_scenario_gradual_drift(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 6 applies monotonic linear ramp."""
    cur_df, manifest = scenario_gradual_drift(
        synthetic_reference_df,
        seed=42,
        n_samples=500,
        feature="amount",
        start_multiplier=1.0,
        end_multiplier=5.0,
    )

    assert manifest.changed_features == ["amount"]
    assert manifest.drift_type == "gradual"

    # Start samples should have lower values than end samples on average
    early_mean = cur_df.iloc[:50]["amount"].mean()
    late_mean = cur_df.iloc[-50:]["amount"].mean()
    assert late_mean > early_mean * 2.0


def test_simulator_scenario_data_quality_failures(synthetic_reference_df: pd.DataFrame):
    """Verify scenario 7 injects NaNs, negative values, and schema modifications."""
    cur_df, manifest = scenario_data_quality_failures(
        synthetic_reference_df,
        seed=42,
        n_samples=500,
        null_rate=0.10,
        invalid_range_rate=0.08,
        dropped_column="oldbalanceDest",
        added_column="unexpected_crypto_token",
    )

    assert "amount" in manifest.changed_features
    assert "oldbalanceDest" in manifest.changed_features
    assert "unexpected_crypto_token" in manifest.changed_features
    assert manifest.drift_type == "data_quality"
    assert len(manifest.injected_faults) == 4

    # Check nulls in amount
    null_count = cur_df["amount"].isna().sum()
    assert null_count == int(500 * 0.10)

    # Check negative amounts
    neg_count = (cur_df["amount"] < 0).sum()
    assert neg_count == int(500 * 0.08)

    # Check schema changes
    assert "oldbalanceDest" not in cur_df.columns
    assert "unexpected_crypto_token" in cur_df.columns


def test_simulator_realistic_class_imbalance(synthetic_reference_df: pd.DataFrame):
    """Verify realistic class imbalance is captured in manifests when labels are present."""
    cur_df, manifest = scenario_no_drift(synthetic_reference_df, seed=42, n_samples=1000)

    if "isFraud" in cur_df.columns:
        assert manifest.class_distribution is not None
        fraud_rate = manifest.class_distribution["fraud_rate"]
        # Realistic fraud rate in financial data is < 0.05 (typically 0.001 - 0.01)
        assert 0.0 <= fraud_rate <= 0.05
        assert manifest.class_distribution["total"] == 1000


# ──────────────────────────────────────────────────────────────────────────────
# PART B: SEVERITY ENGINE TESTS & INDEPENDENT AUDIT
# ──────────────────────────────────────────────────────────────────────────────

def test_severity_no_components_available():
    """Verify engine handles case when zero components are available."""
    res = compute_severity_score(
        component_d=None,
        component_p=None,
        component_q=None,
        component_m=None,
    )
    assert res.score == 0
    assert res.raw_score == 0.0
    assert res.band == "Low"
    assert res.confidence == "unavailable"
    assert res.status == "no_evidence"
    assert res.weights_used == {"D": 0.0, "P": 0.0, "Q": 0.0, "M": 0.0}
    assert "No monitoring components" in res.explanation


def test_severity_all_components_independent_audit():
    """Independent audit calculation of composite severity with all 4 components."""
    # Given inputs
    comp_d = 60.0
    comp_p = 40.0
    comp_q = 20.0
    comp_m = 80.0
    cfg = SeverityConfig()  # D=0.40, P=0.20, Q=0.15, M=0.25

    # Independent mathematical computation
    expected_raw = (
        0.40 * comp_d +
        0.20 * comp_p +
        0.15 * comp_q +
        0.25 * comp_m
    )
    # 24.0 + 8.0 + 3.0 + 20.0 = 55.0
    assert expected_raw == 55.0
    expected_int = 55

    res = compute_severity_score(
        component_d=comp_d,
        component_p=comp_p,
        component_q=comp_q,
        component_m=comp_m,
        config=cfg,
    )

    assert res.score == expected_int
    assert abs(res.raw_score - expected_raw) < 1e-4
    assert res.band == "High"  # 50-74 is High
    assert res.confidence == "normal"
    assert res.status == "ok"
    assert sum(res.weights_used.values()) == pytest.approx(1.0, abs=1e-5)


def test_severity_missing_labels_dynamic_renormalisation():
    """Verify dynamic weight renormalisation when ground-truth labels are absent (M is None)."""
    comp_d = 70.0
    comp_p = 50.0
    comp_q = 10.0
    comp_m = None  # Labels delayed

    cfg = SeverityConfig()
    # Available base weights: D=0.40, P=0.20, Q=0.15; sum = 0.75
    w_d_eff = 0.40 / 0.75  # ~0.533333
    w_p_eff = 0.20 / 0.75  # ~0.266667
    w_q_eff = 0.15 / 0.75  # ~0.200000

    expected_raw = w_d_eff * comp_d + w_p_eff * comp_p + w_q_eff * comp_q
    # 37.33333 + 13.33333 + 2.0 = 52.66667 -> 53
    expected_int = int(round(expected_raw))
    assert expected_int == 53

    res = compute_severity_score(
        component_d=comp_d,
        component_p=comp_p,
        component_q=comp_q,
        component_m=comp_m,
        config=cfg,
    )

    assert res.score == expected_int
    assert abs(res.raw_score - expected_raw) < 1e-3
    assert res.band == "High"
    assert res.confidence == "degraded"
    assert res.status == "degraded"
    assert res.weights_used["M"] == 0.0
    assert sum(res.weights_used.values()) == pytest.approx(1.0, abs=1e-5)
    assert "Labels absent: M omitted" in res.explanation


def test_severity_single_component_available():
    """Verify behavior when only one component (Q) is available."""
    res = compute_severity_score(
        component_d=None,
        component_p=None,
        component_q=85.0,
        component_m=None,
    )
    assert res.score == 85
    assert res.raw_score == 85.0
    assert res.band == "Critical"
    assert res.status == "quality_failure"
    assert res.weights_used["Q"] == 1.0
    assert res.weights_used["D"] == 0.0


def test_severity_boundaries_and_clipping():
    """Verify score is strictly bounded in [0, 100] even with extreme values."""
    res_min = compute_severity_score(component_d=0.0, component_p=0.0, component_q=0.0, component_m=0.0)
    assert res_min.score == 0
    assert res_min.band == "Low"

    res_max = compute_severity_score(component_d=100.0, component_p=100.0, component_q=100.0, component_m=100.0)
    assert res_max.score == 100
    assert res_max.band == "Critical"

    # Extreme out-of-bound inputs clipped to [0, 100]
    res_overflow = compute_severity_score(component_d=150.0, component_p=200.0)
    assert res_overflow.score == 100
    assert res_overflow.raw_score == 100.0


def test_severity_exact_band_boundaries():
    """Verify exact boundary mapping: Low (0-24), Moderate (25-49), High (50-74), Critical (75-100)."""
    cases = [
        (0.0, "Low"),
        (24.0, "Low"),
        (24.4, "Low"),
        (24.6, "Moderate"),  # rounds to 25
        (25.0, "Moderate"),
        (49.0, "Moderate"),
        (49.4, "Moderate"),
        (49.6, "High"),      # rounds to 50
        (50.0, "High"),
        (74.0, "High"),
        (74.4, "High"),
        (74.6, "Critical"),  # rounds to 75
        (75.0, "Critical"),
        (100.0, "Critical"),
    ]
    for val, expected_band in cases:
        res = compute_severity_score(component_d=val)
        assert res.band == expected_band, f"Failed for val={val}: got {res.band}, expected {expected_band}"


def test_severity_monotonicity():
    """Verify that worsening any component strictly never decreases severity score."""
    base_inputs = {"component_d": 40.0, "component_p": 30.0, "component_q": 20.0, "component_m": 50.0}
    base_res = compute_severity_score(**base_inputs)

    # Test increasing each component individually
    for comp in ["component_d", "component_p", "component_q", "component_m"]:
        for increase in [5.0, 15.0, 30.0]:
            mutated = dict(base_inputs)
            mutated[comp] = min(100.0, mutated[comp] + increase)
            higher_res = compute_severity_score(**mutated)
            assert higher_res.raw_score >= base_res.raw_score
            assert higher_res.score >= base_res.score


def test_component_d_importance_and_voting():
    """Verify Component D calculation with feature importances and voting filters."""
    # Feature 1: Drift detected, PSI = 0.50 (saturated >= 0.25 -> 1.0), Importance = 3.0
    f1 = FeatureDriftResult(
        feature_name="amount",
        feature_type="numeric",
        tests=[TestResult(metric="psi", value=0.50, threshold=0.25, exceeded=True)],
        votes="2/3",
        votes_count=2,
        total_tests=3,
        drift_detected=True,
        drift_contribution=3.0,
    )
    # Feature 2: Drift NOT detected, PSI = 0.15, Importance = 1.0 -> contributes 0
    f2 = FeatureDriftResult(
        feature_name="oldbalanceOrg",
        feature_type="numeric",
        tests=[TestResult(metric="psi", value=0.15, threshold=0.25, exceeded=False)],
        votes="1/3",
        votes_count=1,
        total_tests=3,
        drift_detected=False,
        drift_contribution=0.0,
    )

    importances = {"amount": 3.0, "oldbalanceOrg": 1.0}
    # Expected: 100 * (3.0 * 1.0 + 1.0 * 0.0) / (3.0 + 1.0) = 100 * 3.0 / 4.0 = 75.0
    d_score = compute_component_d([f1, f2], feature_importances=importances, psi_crit=0.25)
    assert d_score == 75.0

    # If empty feature results
    assert compute_component_d([]) is None
    assert compute_component_d(None) is None


def test_component_p_score_psi_and_ppr_max():
    """Verify Component P combines score PSI and predicted positive rate shift via max."""
    # Score PSI: 0.125 / 0.25 = 0.50 -> 50.0
    # PPR shift: |0.15 - 0.05| / 0.10 = 0.10 / 0.10 = 1.0 -> 100.0 (saturates)
    p_score = compute_component_p(
        pred_score_psi=0.125,
        ppr_reference=0.05,
        ppr_current=0.15,
        psi_crit=0.25,
        delta_ppr_crit=0.10,
    )
    assert p_score == 100.0

    # Only score PSI available
    p_score_only = compute_component_p(pred_score_psi=0.10, psi_crit=0.25)
    # 0.10 / 0.25 = 0.40 -> 40.0
    assert p_score_only == 40.0

    # Neither available
    assert compute_component_p(None, None, None) is None


def test_component_q_saturation_and_counts():
    """Verify Component Q calculation and saturation at q_max."""
    # 5% failure rate, q_max=0.10 -> 50.0
    assert compute_component_q(quality_failure_rate=0.05, q_max=0.10) == 50.0

    # 10% failure rate -> 100.0
    assert compute_component_q(quality_failure_rate=0.10, q_max=0.10) == 100.0

    # 25% failure rate -> saturated at 100.0
    assert compute_component_q(quality_failure_rate=0.25, q_max=0.10) == 100.0

    # Record count inputs
    q_from_counts = compute_component_q(invalid_record_count=15, total_record_count=300, q_max=0.10)
    # 15 / 300 = 0.05 -> 50.0
    assert q_from_counts == 50.0

    # Missing inputs
    assert compute_component_q(None) is None


def test_component_m_performance_degradation():
    """Verify Component M calculation relative to baseline and critical floor."""
    baseline = 0.75
    floor = 0.25
    # Denominator = 0.75 - 0.25 = 0.50

    # Case 1: Model improves (current = 0.80) -> M = 0.0
    assert compute_component_m(baseline, 0.80, critical_floor=floor) == 0.0

    # Case 2: Model drops halfway (current = 0.50) -> drop = 0.25 / 0.50 = 50.0
    assert compute_component_m(baseline, 0.50, critical_floor=floor) == 50.0

    # Case 3: Model drops to critical floor (current = 0.25) -> 100.0
    assert compute_component_m(baseline, 0.25, critical_floor=floor) == 100.0

    # Case 4: Model drops below floor (current = 0.10) -> saturated at 100.0
    assert compute_component_m(baseline, 0.10, critical_floor=floor) == 100.0

    # Case 5: Missing inputs
    assert compute_component_m(baseline, None) is None
    assert compute_component_m(None, 0.50) is None

    # Invalid config: floor >= baseline
    with pytest.raises(ValueError):
        compute_component_m(baseline_metric=0.50, current_metric=0.40, critical_floor=0.60)


def test_severity_config_validation():
    """Verify configuration validation rejects invalid weights and thresholds."""
    # Negative weight
    with pytest.raises(ValueError):
        SeverityConfig(weights={"D": -0.1, "P": 0.2, "Q": 0.15, "M": 0.25})

    # Zero total weight
    with pytest.raises(ValueError):
        SeverityConfig(weights={"D": 0.0, "P": 0.0, "Q": 0.0, "M": 0.0})

    # Missing component in weights
    with pytest.raises(ValueError):
        SeverityConfig(weights={"D": 0.5, "P": 0.5})

    # Non-positive psi_crit
    with pytest.raises(ValueError):
        SeverityConfig(psi_crit=0.0)


def test_severity_to_breakdown_schema_compatibility():
    """Verify SeverityResult converts seamlessly into monitoring SeverityBreakdown."""
    res = compute_severity_score(component_d=50.0, component_p=30.0)
    breakdown = res.to_breakdown()

    assert isinstance(breakdown, SeverityBreakdown)
    assert breakdown.score == res.score
    assert breakdown.band == res.band
    assert breakdown.components["D"] == 50.0
    assert breakdown.components["P"] == 30.0
    assert breakdown.weights_used["D"] + breakdown.weights_used["P"] == pytest.approx(1.0, abs=1e-5)
