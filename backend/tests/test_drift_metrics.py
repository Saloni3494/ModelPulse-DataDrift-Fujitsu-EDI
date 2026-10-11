"""ModelPulse Automated Test Suite for Standalone Statistical Drift Metrics.
Tests:
- Identical and shifted numeric distributions (PSI, KS, Normalized Wasserstein).
- Comparison against independent SciPy calculations and toy examples.
- Categorical drift (PSI, Chi-Square with Cochran condition, Jensen-Shannon distance).
- Unseen categories reporting and zero variance fallbacks.
- Edge cases: Empty inputs, insufficient samples, NaNs, infinities, constant features.
- Ensemble k-of-m voting logic in evaluate_feature_drift.
"""
import numpy as np
import pandas as pd
import pytest
from scipy import stats
from scipy.spatial.distance import jensenshannon

from backend.app.services.drift_metrics import (
    compute_numeric_psi,
    compute_ks_test,
    compute_normalized_wasserstein,
    compute_categorical_psi,
    compute_chi2_test,
    compute_jensen_shannon_distance,
    evaluate_feature_drift,
)


# ──────────────────────────────────────────────────────────────────────────────
# 1. NUMERIC DRIFT TESTS
# ──────────────────────────────────────────────────────────────────────────────

def test_numeric_psi_identical_distributions():
    """Verify PSI on identical distributions evaluates to near-zero (< 0.02) and no drift."""
    rng = np.random.default_rng(42)
    ref = rng.normal(loc=0.0, scale=1.0, size=1000)
    cur = ref.copy()

    res = compute_numeric_psi(ref, cur, threshold=0.25)
    assert res.metric == "psi"
    assert res.value < 0.02
    assert res.exceeded is False
    assert res.status == "ok"
    assert res.reference_count == 1000
    assert res.current_count == 1000


def test_numeric_psi_shifted_distribution():
    """Verify PSI correctly detects significant distribution shift (mean shifted by 3 sigma)."""
    rng = np.random.default_rng(42)
    ref = rng.normal(loc=0.0, scale=1.0, size=1000)
    cur = rng.normal(loc=3.0, scale=1.0, size=1000)

    res = compute_numeric_psi(ref, cur, threshold=0.25)
    assert res.value > 0.25
    assert res.exceeded is True
    assert res.status == "ok"


def test_ks_test_scipy_independent_comparison():
    """Compare KS test results directly against independent SciPy output."""
    rng = np.random.default_rng(42)
    ref = rng.normal(loc=0.0, scale=1.0, size=500)
    cur = rng.normal(loc=0.5, scale=1.0, size=500)

    # Scipy independent ground truth
    scipy_res = stats.ks_2samp(ref, cur)

    res = compute_ks_test(ref, cur, threshold=0.10)
    assert res.metric == "ks"
    assert res.value == round(float(scipy_res.statistic), 4)
    assert res.p_value == round(float(scipy_res.pvalue), 6)
    assert 0.0 <= res.value <= 1.0
    assert 0.0 <= res.p_value <= 1.0
    assert res.status == "ok"


def test_ks_test_identical_distributions():
    """Verify KS statistic on identical samples is 0.0 with p-value 1.0."""
    ref = np.linspace(-5, 5, 200)
    cur = ref.copy()

    res = compute_ks_test(ref, cur, threshold=0.10)
    assert res.value == 0.0
    assert res.p_value == 1.0
    assert res.exceeded is False


def test_normalized_wasserstein_scipy_comparison():
    """Compare normalized Wasserstein distance directly against SciPy W1 / std."""
    rng = np.random.default_rng(42)
    ref = rng.normal(loc=10.0, scale=2.0, size=600)
    cur = rng.normal(loc=13.0, scale=2.0, size=600)

    # Scipy independent ground truth
    raw_w1 = stats.wasserstein_distance(ref, cur)
    ref_std = np.std(ref)
    expected_norm = round(float(raw_w1 / ref_std), 4)

    res = compute_normalized_wasserstein(ref, cur, threshold=0.30)
    assert res.metric == "wasserstein"
    assert res.value == expected_norm
    assert res.exceeded is True
    assert res.status == "ok"


def test_normalized_wasserstein_zero_variance_fallback():
    """Verify documented fallback behavior when reference standard deviation is zero."""
    # Constant reference: zero standard deviation
    ref = np.array([5.0] * 50)
    cur_same = np.array([5.0] * 50)
    cur_diff = np.array([8.0] * 50)

    # Case A: Both constant and identical
    res_same = compute_normalized_wasserstein(ref, cur_same)
    assert res_same.value == 0.0
    assert res_same.exceeded is False
    assert res_same.status == "constant_feature"

    # Case B: Reference constant, current shifted by 3.0
    res_diff = compute_normalized_wasserstein(ref, cur_diff, threshold=0.30)
    assert res_diff.value == 3.0  # Raw distance fallback
    assert res_diff.exceeded is True
    assert res_diff.status == "zero_reference_variance"
    assert res_diff.details["fallback"] == "unnormalized_distance"
    assert res_diff.details["reference_std"] == 0.0


# ──────────────────────────────────────────────────────────────────────────────
# 2. CATEGORICAL DRIFT TESTS
# ──────────────────────────────────────────────────────────────────────────────

def test_categorical_psi_and_unseen_categories():
    """Verify categorical PSI and explicit reporting of unseen categories."""
    ref = ["CASH_OUT"] * 60 + ["TRANSFER"] * 40
    # Current has shifted proportions and an unseen category 'CRYPTO'
    cur = ["CASH_OUT"] * 20 + ["TRANSFER"] * 30 + ["CRYPTO"] * 50

    res, unseen = compute_categorical_psi(ref, cur, threshold=0.25)
    assert res.metric == "psi"
    assert res.exceeded is True
    assert unseen == ["CRYPTO"]
    assert res.details["unseen_categories"] == ["CRYPTO"]
    assert res.details["unseen_count"] == 1
    assert res.status == "ok"


def test_chi2_test_scipy_comparison():
    """Compare Chi-square test of homogeneity directly against SciPy chi2_contingency."""
    ref = ["A"] * 200 + ["B"] * 300
    cur = ["A"] * 100 + ["B"] * 400

    # Scipy ground truth contingency
    contingency = [[200, 300], [100, 400]]
    scipy_chi2, scipy_p, _, _ = stats.chi2_contingency(contingency)

    res, unseen = compute_chi2_test(ref, cur, threshold=0.05)
    assert res.metric == "chi2"
    assert res.value == round(float(scipy_chi2), 4)
    assert res.p_value == round(float(scipy_p), 6)
    assert res.exceeded is True  # p_value <= 0.05
    assert unseen == []
    assert res.status == "ok"


def test_chi2_cochran_criterion_flagging():
    """Verify Cochran condition flags small expected cell counts as unreliable."""
    # Very small sample counts producing expected frequencies < 5
    ref = ["A"] * 3 + ["B"] * 3
    cur = ["A"] * 2 + ["B"] * 4

    res, _ = compute_chi2_test(ref, cur, min_samples=5)
    assert res.status == "ok"
    assert res.details["unreliable_approximation"] is True
    assert res.details["min_expected_frequency"] < 5.0


def test_jensen_shannon_identical_and_disjoint():
    """Verify Jensen-Shannon distance on identical (0.0) and completely disjoint (1.0) distributions."""
    # Case A: Identical
    ref = ["TRANSFER"] * 100 + ["PAYMENT"] * 100
    cur = ["TRANSFER"] * 100 + ["PAYMENT"] * 100
    res_same, unseen_same = compute_jensen_shannon_distance(ref, cur)
    assert res_same.value == 0.0
    assert res_same.exceeded is False
    assert unseen_same == []

    # Case B: Completely disjoint categories
    ref_disjoint = ["TRANSFER"] * 100
    cur_disjoint = ["CRYPTO"] * 100
    res_disjoint, unseen_disjoint = compute_jensen_shannon_distance(ref_disjoint, cur_disjoint)
    assert res_disjoint.value == 1.0  # Max possible JS distance with base 2
    assert res_disjoint.exceeded is True
    assert unseen_disjoint == ["CRYPTO"]


# ──────────────────────────────────────────────────────────────────────────────
# 3. EDGE CASES & ROBUSTNESS TESTS
# ──────────────────────────────────────────────────────────────────────────────

def test_insufficient_and_empty_samples():
    """Verify empty and tiny inputs do not crash and report 'insufficient_samples' status."""
    empty = np.array([])
    valid = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
    tiny = np.array([1.0, 2.0])

    # PSI with empty input
    res_empty = compute_numeric_psi(empty, valid)
    assert res_empty.status == "insufficient_samples"
    assert res_empty.value == 0.0
    assert res_empty.exceeded is False

    # KS with tiny sample
    res_tiny = compute_ks_test(valid, tiny, min_samples=10)
    assert res_tiny.status == "insufficient_samples"
    assert res_tiny.value == 0.0
    assert res_tiny.exceeded is False

    # Categorical JS with empty input
    res_cat, _ = compute_jensen_shannon_distance([], ["A"] * 20)
    assert res_cat.status == "insufficient_samples"
    assert res_cat.value == 0.0


def test_nan_and_inf_handling():
    """Verify NaNs and infinities are dropped and recorded without corrupting metric computation."""
    ref_with_nan = [1.0, 2.0, np.nan, 3.0, np.inf, 4.0, -np.inf] + [2.5] * 30
    cur_clean = [2.5] * 35

    res = compute_numeric_psi(ref_with_nan, cur_clean, min_samples=10)
    assert res.status == "ok"
    assert not np.isnan(res.value)
    assert res.details["nan_dropped_ref"] == 3  # nan, inf, -inf dropped
    assert res.details["nan_dropped_cur"] == 0


def test_repeated_calls_reproducibility():
    """Verify deterministic metrics yield identical values across repeated calls."""
    rng = np.random.default_rng(123)
    ref = rng.normal(0, 1, 500)
    cur = rng.normal(0.5, 1.2, 500)

    res_1 = compute_numeric_psi(ref, cur)
    res_2 = compute_numeric_psi(ref, cur)
    assert res_1.value == res_2.value

    ks_1 = compute_ks_test(ref, cur)
    ks_2 = compute_ks_test(ref, cur)
    assert ks_1.value == ks_2.value
    assert ks_1.p_value == ks_2.p_value


# ──────────────────────────────────────────────────────────────────────────────
# 4. MULTI-METHOD ENSEMBLE EVALUATION ENGINE
# ──────────────────────────────────────────────────────────────────────────────

def test_evaluate_feature_drift_numeric_ensemble():
    """Verify k-of-m ensemble voting rule for numeric features."""
    rng = np.random.default_rng(42)
    ref = rng.normal(0.0, 1.0, 1000)

    # 1. No drift scenario
    cur_no_drift = rng.normal(0.0, 1.0, 1000)
    res_clean = evaluate_feature_drift(
        reference_values=ref,
        current_values=cur_no_drift,
        feature_name="amount",
        feature_type="numeric",
        k_of_m=(2, 3),
    )
    assert res_clean.feature_name == "amount"
    assert res_clean.feature_type == "numeric"
    assert res_clean.drift_detected is False
    assert res_clean.votes_count < 2
    assert res_clean.drift_contribution == 0.0

    # 2. Confirmed drift scenario (mean shifted by 1.5 sigma)
    cur_drift = rng.normal(1.5, 1.0, 1000)
    res_drift = evaluate_feature_drift(
        reference_values=ref,
        current_values=cur_drift,
        feature_name="amount",
        feature_type="numeric",
        importance=0.85,
        k_of_m=(2, 3),
    )
    assert res_drift.drift_detected is True
    assert res_drift.votes_count >= 2
    assert res_drift.drift_contribution > 0.0
    assert len(res_drift.tests) == 3


def test_evaluate_feature_drift_categorical_ensemble():
    """Verify k-of-m ensemble voting rule for categorical features."""
    ref = ["CASH_OUT"] * 500 + ["TRANSFER"] * 500
    cur = ["CASH_OUT"] * 100 + ["TRANSFER"] * 400 + ["NEW_PAYMENT"] * 500

    res = evaluate_feature_drift(
        reference_values=ref,
        current_values=cur,
        feature_name="type",
        feature_type="categorical",
        importance=0.75,
        k_of_m=(2, 3),
    )
    assert res.feature_name == "type"
    assert res.feature_type == "categorical"
    assert res.drift_detected is True
    assert res.votes_count >= 2
    assert "NEW_PAYMENT" in res.unseen_categories
    assert res.drift_contribution > 0.0
