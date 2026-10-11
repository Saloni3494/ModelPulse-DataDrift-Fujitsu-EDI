"""ModelPulse Statistical Drift Metrics Library.
Provides standalone, mathematically verified numeric and categorical drift detection algorithms.
Implements:
- Numeric: Population Stability Index (PSI), Kolmogorov-Smirnov (KS), Normalized Wasserstein distance.
- Categorical: Categorical PSI, Chi-Square homogeneity, Jensen-Shannon (JS) distance.
- Robust edge-case handling for NaNs, infinites, zero variance, and small samples.
"""
import math
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import jensenshannon

from backend.app.schemas.drift import TestResult, FeatureDriftResult

# Default statistical decision thresholds (per ModelPulse Approach v2)
DEFAULT_NUMERIC_THRESHOLDS = {
    "psi": 0.25,        # 0.10: moderate / warning, 0.25: significant / drift
    "ks": 0.10,         # Maximum distance D between empirical CDFs
    "ks_alpha": 0.05,   # p-value significance ceiling
    "wasserstein": 0.30,# Normalized earth mover's distance (sigma units)
}

DEFAULT_CATEGORICAL_THRESHOLDS = {
    "psi": 0.25,        # Categorical population stability index
    "chi2_alpha": 0.05, # p-value significance threshold for Chi-square
    "js": 0.10,         # Jensen-Shannon distance [0.0, 1.0]
}


def _clean_numeric_array(
    arr: Union[np.ndarray, pd.Series, List[float]]
) -> Tuple[np.ndarray, int]:
    """Clean numeric array: convert to 1D float array, drop NaNs and infinities.
    Returns (cleaned_array, dropped_count).
    """
    if arr is None:
        return np.array([], dtype=float), 0

    flat = np.asarray(arr, dtype=float).ravel()
    valid_mask = np.isfinite(flat)
    dropped = int(np.sum(~valid_mask))
    return flat[valid_mask], dropped


def _clean_categorical_array(
    arr: Union[np.ndarray, pd.Series, List[Any]]
) -> Tuple[np.ndarray, int]:
    """Clean categorical array: convert to 1D string array, drop nulls.
    Returns (cleaned_array, dropped_count).
    """
    if arr is None:
        return np.array([], dtype=object), 0

    s = pd.Series(arr).dropna()
    dropped = len(arr) - len(s)
    return s.astype(str).values, dropped


# ──────────────────────────────────────────────────────────────────────────────
# 1. NUMERIC DRIFT METRICS
# ──────────────────────────────────────────────────────────────────────────────

def compute_numeric_psi(
    reference: Union[np.ndarray, pd.Series, List[float]],
    current: Union[np.ndarray, pd.Series, List[float]],
    n_bins: int = 10,
    threshold: float = DEFAULT_NUMERIC_THRESHOLDS["psi"],
    min_samples: int = 10,
    epsilon: float = 1e-4,
) -> TestResult:
    """Compute Population Stability Index (PSI) between two numeric distributions.

    Bins are derived from reference distribution quantiles. Laplace smoothing (epsilon)
    prevents division by zero and undefined log transforms.
    """
    ref_clean, ref_dropped = _clean_numeric_array(reference)
    cur_clean, cur_dropped = _clean_numeric_array(current)

    ref_n, cur_n = len(ref_clean), len(cur_clean)

    # Edge Case: Insufficient samples
    if ref_n < min_samples or cur_n < min_samples:
        return TestResult(
            metric="psi",
            value=0.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="insufficient_samples",
            details={
                "reason": f"Sample count below min_samples ({min_samples})",
                "nan_dropped_ref": ref_dropped,
                "nan_dropped_cur": cur_dropped,
            },
        )

    # Edge Case: Zero variance (constant feature in reference)
    if np.allclose(ref_clean, ref_clean[0]):
        if np.allclose(cur_clean, ref_clean[0]):
            return TestResult(
                metric="psi",
                value=0.0,
                threshold=threshold,
                exceeded=False,
                reference_count=ref_n,
                current_count=cur_n,
                status="constant_feature",
                details={"reason": "Both reference and current are identical constants"},
            )
        else:
            return TestResult(
                metric="psi",
                value=1.0,
                threshold=threshold,
                exceeded=True,
                reference_count=ref_n,
                current_count=cur_n,
                status="constant_feature_shifted",
                details={"reason": "Reference is constant but current values shifted"},
            )

    # Quantile-based bin edges from reference distribution
    percentiles = np.linspace(0, 100, n_bins + 1)
    bin_edges = np.percentile(ref_clean, percentiles)
    bin_edges = np.unique(bin_edges)

    # If quantiles collapse due to repeated values, fallback to linear spacing
    if len(bin_edges) < 2:
        bin_edges = np.linspace(np.min(ref_clean), np.max(ref_clean), n_bins + 1)

    # Expand outer edges to capture out-of-range current values
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf

    ref_counts, _ = np.histogram(ref_clean, bins=bin_edges)
    cur_counts, _ = np.histogram(cur_clean, bins=bin_edges)

    b = len(ref_counts)
    ref_pct = (ref_counts + epsilon) / (ref_n + b * epsilon)
    cur_pct = (cur_counts + epsilon) / (cur_n + b * epsilon)

    psi_val = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    psi_val = float(max(0.0, psi_val))

    if math.isnan(psi_val) or math.isinf(psi_val):
        psi_val = 0.0

    return TestResult(
        metric="psi",
        value=round(psi_val, 4),
        threshold=threshold,
        exceeded=bool(psi_val >= threshold),
        reference_count=ref_n,
        current_count=cur_n,
        status="ok",
        details={
            "n_bins_used": b,
            "nan_dropped_ref": ref_dropped,
            "nan_dropped_cur": cur_dropped,
        },
    )


def compute_ks_test(
    reference: Union[np.ndarray, pd.Series, List[float]],
    current: Union[np.ndarray, pd.Series, List[float]],
    threshold: float = DEFAULT_NUMERIC_THRESHOLDS["ks"],
    alpha: float = DEFAULT_NUMERIC_THRESHOLDS["ks_alpha"],
    min_samples: int = 10,
) -> TestResult:
    """Compute two-sample Kolmogorov-Smirnov test.

    Returns the supremum distance statistic D and asymptotic p-value.
    Drift is flagged when effect size D >= threshold.
    """
    ref_clean, ref_dropped = _clean_numeric_array(reference)
    cur_clean, cur_dropped = _clean_numeric_array(current)

    ref_n, cur_n = len(ref_clean), len(cur_clean)

    if ref_n < min_samples or cur_n < min_samples:
        return TestResult(
            metric="ks",
            value=0.0,
            p_value=1.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="insufficient_samples",
            details={
                "reason": f"Sample count below min_samples ({min_samples})",
                "nan_dropped_ref": ref_dropped,
                "nan_dropped_cur": cur_dropped,
            },
        )

    # Scipy 2-sample KS test
    ks_res = stats.ks_2samp(ref_clean, cur_clean)
    stat = float(ks_res.statistic)
    p_val = float(ks_res.pvalue)

    exceeded = bool(stat >= threshold)

    return TestResult(
        metric="ks",
        value=round(stat, 4),
        p_value=round(p_val, 6),
        threshold=threshold,
        exceeded=exceeded,
        reference_count=ref_n,
        current_count=cur_n,
        status="ok",
        details={
            "p_value_alpha": alpha,
            "statistically_significant": bool(p_val <= alpha),
            "nan_dropped_ref": ref_dropped,
            "nan_dropped_cur": cur_dropped,
        },
    )


def compute_normalized_wasserstein(
    reference: Union[np.ndarray, pd.Series, List[float]],
    current: Union[np.ndarray, pd.Series, List[float]],
    threshold: float = DEFAULT_NUMERIC_THRESHOLDS["wasserstein"],
    min_samples: int = 10,
) -> TestResult:
    """Compute Wasserstein-1 distance normalized by reference standard deviation.

    Normalization: W_norm = W_1 / std(reference).
    Fallback when std(reference) == 0: returns raw W_1 with status='zero_reference_variance'.
    """
    ref_clean, ref_dropped = _clean_numeric_array(reference)
    cur_clean, cur_dropped = _clean_numeric_array(current)

    ref_n, cur_n = len(ref_clean), len(cur_clean)

    if ref_n < min_samples or cur_n < min_samples:
        return TestResult(
            metric="wasserstein",
            value=0.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="insufficient_samples",
            details={
                "reason": f"Sample count below min_samples ({min_samples})",
                "nan_dropped_ref": ref_dropped,
                "nan_dropped_cur": cur_dropped,
            },
        )

    raw_dist = float(stats.wasserstein_distance(ref_clean, cur_clean))
    ref_std = float(np.std(ref_clean))

    # Zero variance fallback handling
    if ref_std < 1e-12:
        if raw_dist < 1e-12:
            return TestResult(
                metric="wasserstein",
                value=0.0,
                threshold=threshold,
                exceeded=False,
                reference_count=ref_n,
                current_count=cur_n,
                status="constant_feature",
                details={"reason": "Both distributions constant with zero variance"},
            )
        else:
            return TestResult(
                metric="wasserstein",
                value=round(raw_dist, 4),
                threshold=threshold,
                exceeded=bool(raw_dist >= threshold),
                reference_count=ref_n,
                current_count=cur_n,
                status="zero_reference_variance",
                details={
                    "fallback": "unnormalized_distance",
                    "reason": "Reference standard deviation is zero; reported raw Wasserstein distance",
                    "reference_std": 0.0,
                },
            )

    norm_dist = float(raw_dist / ref_std)

    return TestResult(
        metric="wasserstein",
        value=round(norm_dist, 4),
        threshold=threshold,
        exceeded=bool(norm_dist >= threshold),
        reference_count=ref_n,
        current_count=cur_n,
        status="ok",
        details={
            "raw_wasserstein": round(raw_dist, 4),
            "reference_std": round(ref_std, 4),
            "nan_dropped_ref": ref_dropped,
            "nan_dropped_cur": cur_dropped,
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# 2. CATEGORICAL DRIFT METRICS
# ──────────────────────────────────────────────────────────────────────────────

def compute_categorical_psi(
    reference: Union[np.ndarray, pd.Series, List[Any]],
    current: Union[np.ndarray, pd.Series, List[Any]],
    threshold: float = DEFAULT_CATEGORICAL_THRESHOLDS["psi"],
    min_samples: int = 10,
    epsilon: float = 1e-4,
) -> Tuple[TestResult, List[str]]:
    """Compute PSI across category proportions, explicitly tracking unseen categories."""
    ref_clean, ref_dropped = _clean_categorical_array(reference)
    cur_clean, cur_dropped = _clean_categorical_array(current)

    ref_n, cur_n = len(ref_clean), len(cur_clean)

    if ref_n < min_samples or cur_n < min_samples:
        res = TestResult(
            metric="psi",
            value=0.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="insufficient_samples",
            details={"reason": f"Sample count below min_samples ({min_samples})"},
        )
        return res, []

    ref_counts = pd.Series(ref_clean).value_counts()
    cur_counts = pd.Series(cur_clean).value_counts()

    ref_categories = set(ref_counts.index)
    cur_categories = set(cur_counts.index)
    all_categories = sorted(list(ref_categories.union(cur_categories)))
    unseen_categories = sorted(list(cur_categories - ref_categories))

    k = len(all_categories)
    ref_aligned = np.array([ref_counts.get(cat, 0) for cat in all_categories], dtype=float)
    cur_aligned = np.array([cur_counts.get(cat, 0) for cat in all_categories], dtype=float)

    ref_pct = (ref_aligned + epsilon) / (ref_n + k * epsilon)
    cur_pct = (cur_aligned + epsilon) / (cur_n + k * epsilon)

    psi_val = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    psi_val = float(max(0.0, psi_val))

    if math.isnan(psi_val) or math.isinf(psi_val):
        psi_val = 0.0

    res = TestResult(
        metric="psi",
        value=round(psi_val, 4),
        threshold=threshold,
        exceeded=bool(psi_val >= threshold),
        reference_count=ref_n,
        current_count=cur_n,
        status="ok",
        details={
            "all_categories_count": k,
            "unseen_categories": unseen_categories,
            "unseen_count": len(unseen_categories),
            "nan_dropped_ref": ref_dropped,
            "nan_dropped_cur": cur_dropped,
        },
    )
    return res, unseen_categories


def compute_chi2_test(
    reference: Union[np.ndarray, pd.Series, List[Any]],
    current: Union[np.ndarray, pd.Series, List[Any]],
    threshold: float = DEFAULT_CATEGORICAL_THRESHOLDS["chi2_alpha"],
    min_samples: int = 10,
) -> Tuple[TestResult, List[str]]:
    """Compute Chi-Square test of homogeneity on aligned category frequencies.

    Includes Cochran criterion check: flags approximation as unreliable if expected
    frequencies < 5 in > 20% of cells or any expected frequency < 1.
    """
    ref_clean, ref_dropped = _clean_categorical_array(reference)
    cur_clean, cur_dropped = _clean_categorical_array(current)

    ref_n, cur_n = len(ref_clean), len(cur_clean)

    if ref_n < min_samples or cur_n < min_samples:
        res = TestResult(
            metric="chi2",
            value=0.0,
            p_value=1.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="insufficient_samples",
            details={"reason": f"Sample count below min_samples ({min_samples})"},
        )
        return res, []

    ref_counts = pd.Series(ref_clean).value_counts()
    cur_counts = pd.Series(cur_clean).value_counts()

    ref_categories = set(ref_counts.index)
    cur_categories = set(cur_counts.index)
    all_categories = sorted(list(ref_categories.union(cur_categories)))
    unseen_categories = sorted(list(cur_categories - ref_categories))

    # Single category distribution
    if len(all_categories) < 2:
        res = TestResult(
            metric="chi2",
            value=0.0,
            p_value=1.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="constant_feature",
            details={"reason": "Only 1 category present across both datasets"},
        )
        return res, unseen_categories

    contingency = np.array([
        [ref_counts.get(cat, 0) for cat in all_categories],
        [cur_counts.get(cat, 0) for cat in all_categories],
    ], dtype=float)

    # Scipy chi2_contingency
    try:
        chi2_stat, p_val, dof, expected = stats.chi2_contingency(contingency)
    except Exception as exc:
        res = TestResult(
            metric="chi2",
            value=0.0,
            p_value=1.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="invalid_input",
            details={"error": str(exc)},
        )
        return res, unseen_categories

    # Cochran condition check
    min_expected = float(np.min(expected))
    low_cells_ratio = float(np.mean(expected < 5.0))
    unreliable = bool(min_expected < 1.0 or low_cells_ratio > 0.20)

    exceeded = bool(p_val <= threshold)

    res = TestResult(
        metric="chi2",
        value=round(float(chi2_stat), 4),
        p_value=round(float(p_val), 6),
        threshold=threshold,
        exceeded=exceeded,
        reference_count=ref_n,
        current_count=cur_n,
        status="ok",
        details={
            "dof": int(dof),
            "unreliable_approximation": unreliable,
            "min_expected_frequency": round(min_expected, 3),
            "low_expected_cells_ratio": round(low_cells_ratio, 3),
            "unseen_categories": unseen_categories,
        },
    )
    return res, unseen_categories


def compute_jensen_shannon_distance(
    reference: Union[np.ndarray, pd.Series, List[Any]],
    current: Union[np.ndarray, pd.Series, List[Any]],
    threshold: float = DEFAULT_CATEGORICAL_THRESHOLDS["js"],
    min_samples: int = 10,
) -> Tuple[TestResult, List[str]]:
    """Compute Jensen-Shannon distance between aligned categorical distributions.

    Bounded in [0.0, 1.0] with base 2 logarithm.
    """
    ref_clean, ref_dropped = _clean_categorical_array(reference)
    cur_clean, cur_dropped = _clean_categorical_array(current)

    ref_n, cur_n = len(ref_clean), len(cur_clean)

    if ref_n < min_samples or cur_n < min_samples:
        res = TestResult(
            metric="js",
            value=0.0,
            threshold=threshold,
            exceeded=False,
            reference_count=ref_n,
            current_count=cur_n,
            status="insufficient_samples",
            details={"reason": f"Sample count below min_samples ({min_samples})"},
        )
        return res, []

    ref_counts = pd.Series(ref_clean).value_counts()
    cur_counts = pd.Series(cur_clean).value_counts()

    ref_categories = set(ref_counts.index)
    cur_categories = set(cur_counts.index)
    all_categories = sorted(list(ref_categories.union(cur_categories)))
    unseen_categories = sorted(list(cur_categories - ref_categories))

    # Normalized probability vectors
    p = np.array([ref_counts.get(cat, 0) / ref_n for cat in all_categories], dtype=float)
    q = np.array([cur_counts.get(cat, 0) / cur_n for cat in all_categories], dtype=float)

    js_dist = float(jensenshannon(p, q, base=2))
    js_dist = max(0.0, min(1.0, js_dist))

    if math.isnan(js_dist):
        js_dist = 0.0

    res = TestResult(
        metric="js",
        value=round(js_dist, 4),
        threshold=threshold,
        exceeded=bool(js_dist >= threshold),
        reference_count=ref_n,
        current_count=cur_n,
        status="ok",
        details={
            "all_categories_count": len(all_categories),
            "unseen_categories": unseen_categories,
        },
    )
    return res, unseen_categories


# ──────────────────────────────────────────────────────────────────────────────
# 3. UNIFIED PER-FEATURE EVALUATION ENGINE
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_feature_drift(
    reference_values: Any,
    current_values: Any,
    feature_name: str,
    feature_type: str,
    importance: float = 1.0,
    k_of_m: Tuple[int, int] = (2, 3),
    thresholds: Optional[Dict[str, float]] = None,
) -> FeatureDriftResult:
    """Evaluate multi-method statistical drift for a single numeric or categorical feature.

    Applies k-of-m ensemble voting rule to determine overall feature drift flag.
    Computes importance-weighted drift contribution.
    """
    k_needed, m_total = k_of_m
    unseen_cats: Optional[List[str]] = None
    ref_mean: Optional[float] = None
    cur_mean: Optional[float] = None

    if feature_type == "numeric":
        t = {**DEFAULT_NUMERIC_THRESHOLDS, **(thresholds or {})}
        test_psi = compute_numeric_psi(reference_values, current_values, threshold=t["psi"])
        test_ks = compute_ks_test(reference_values, current_values, threshold=t["ks"], alpha=t["ks_alpha"])
        test_wass = compute_normalized_wasserstein(reference_values, current_values, threshold=t["wasserstein"])
        tests = [test_psi, test_ks, test_wass]

        ref_c, _ = _clean_numeric_array(reference_values)
        cur_c, _ = _clean_numeric_array(current_values)
        if len(ref_c) > 0:
            ref_mean = round(float(np.mean(ref_c)), 4)
        if len(cur_c) > 0:
            cur_mean = round(float(np.mean(cur_c)), 4)

    elif feature_type == "categorical":
        t = {**DEFAULT_CATEGORICAL_THRESHOLDS, **(thresholds or {})}
        test_psi, unseen_1 = compute_categorical_psi(reference_values, current_values, threshold=t["psi"])
        test_chi2, unseen_2 = compute_chi2_test(reference_values, current_values, threshold=t["chi2_alpha"])
        test_js, unseen_3 = compute_jensen_shannon_distance(reference_values, current_values, threshold=t["js"])
        tests = [test_psi, test_chi2, test_js]
        unseen_cats = sorted(list(set(unseen_1 + unseen_2 + unseen_3)))

    else:
        raise ValueError(f"Unsupported feature_type: '{feature_type}'. Must be 'numeric' or 'categorical'.")

    # Ensemble Voting
    votes_count = sum(1 for test in tests if test.exceeded)
    total_tests = len(tests)
    drift_detected = bool(votes_count >= k_needed)

    # Primary metric for normalized contribution: PSI
    psi_test = next((test for test in tests if test.metric == "psi"), None)
    psi_val = psi_test.value if psi_test else 0.0
    psi_crit = (thresholds or {}).get("psi", 0.25)

    # Normalized drift clipped to [0, 1]
    norm_drift = min(1.0, psi_val / psi_crit) if psi_crit > 0 else 0.0
    drift_contribution = round(float(importance * norm_drift if drift_detected else 0.0), 4)

    return FeatureDriftResult(
        feature_name=feature_name,
        feature_type=feature_type,
        tests=tests,
        votes=f"{votes_count}/{total_tests}",
        votes_count=votes_count,
        total_tests=total_tests,
        drift_detected=drift_detected,
        drift_contribution=drift_contribution,
        unseen_categories=unseen_cats,
        reference_mean=ref_mean,
        current_mean=cur_mean,
    )
