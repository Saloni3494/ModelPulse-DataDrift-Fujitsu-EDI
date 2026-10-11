"""ModelPulse Controlled Drift Simulator Scenarios.
Implements reproducible, seed-controlled drift scenarios and manifests:
1. No drift
2. Numeric mean/variance shift
3. Categorical proportion shift
4. Unseen categories injection
5. Sudden drift
6. Gradual drift
7. Data quality failures (missing values, invalid ranges, schema alterations)

Guarantees strict immutability of the reference dataset and clear distinction
between synthetic test fixtures and real PaySim observations.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd

from simulator.manifest import ScenarioManifest
from ml.dataset import generate_synthetic_paysim

SYNTHETIC_DISCLAIMER = "SYNTHETIC FIXTURE - NOT REAL PAYSIM OBSERVATIONS"


def load_or_create_reference(
    reference_path: Optional[str] = "ml/artifacts/reference_features.csv",
    seed: int = 42,
    n_samples: int = 5000,
) -> pd.DataFrame:
    """Load existing reference dataset or generate synthetic fixture.
    Always returns a clean, deep-copied DataFrame.
    """
    if reference_path and Path(reference_path).exists():
        df = pd.read_csv(reference_path)
    else:
        df = generate_synthetic_paysim(n_samples=n_samples, random_seed=seed)
    return df.copy(deep=True)


def _extract_class_distribution(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Extract class distribution statistics if label column is present."""
    if "isFraud" not in df.columns:
        return None
    valid_series = df["isFraud"].dropna()
    total = len(valid_series)
    if total == 0:
        return {"total": 0, "fraud_count": 0, "fraud_rate": 0.0}
    fraud_count = int((valid_series == 1).sum())
    return {
        "total": total,
        "fraud_count": fraud_count,
        "fraud_rate": round(float(fraud_count / total), 6),
    }


def _sample_reference(
    reference_df: pd.DataFrame,
    n_samples: int,
    rng: np.random.Generator,
    preserve_order: bool = False,
) -> pd.DataFrame:
    """Sample n_samples rows from reference_df without mutating reference."""
    ref_len = len(reference_df)
    replace = n_samples > ref_len
    indices = rng.choice(ref_len, size=n_samples, replace=replace)
    if preserve_order:
        indices = np.sort(indices)
    sampled = reference_df.iloc[indices].copy(deep=True).reset_index(drop=True)
    return sampled


# ──────────────────────────────────────────────────────────────────────────────
# 1. NO DRIFT SCENARIO
# ──────────────────────────────────────────────────────────────────────────────

def scenario_no_drift(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 1: No Drift.
    Generates a current window drawn from the exact same distribution as reference.
    """
    # Strict immutability: deep copy
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng)

    manifest = ScenarioManifest(
        scenario_id="no_drift",
        seed=seed,
        changed_features=[],
        drift_type="none",
        magnitude=0.0,
        start_window=0,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=[],
        class_distribution=_extract_class_distribution(current_df),
        description="Identical generating distribution. No covariate, concept, or quality drift injected.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# 2. NUMERIC MEAN AND VARIANCE SHIFT
# ──────────────────────────────────────────────────────────────────────────────

def scenario_numeric_mean_variance_shift(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
    features: Tuple[str, ...] = ("amount", "oldbalanceOrg"),
    mean_multiplier: float = 2.5,
    var_multiplier: float = 1.8,
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 2: Numeric Mean and Variance Shift.
    Applies controlled location (mean) and scale (variance) shifts to numeric features.
    """
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng)

    changed_features = []
    magnitude_details = {}

    for col in features:
        if col in current_df.columns:
            ref_col = pd.to_numeric(ref_copy[col], errors="coerce").dropna()
            ref_mean = float(ref_col.mean()) if len(ref_col) > 0 else 1000.0
            ref_std = float(ref_col.std()) if len(ref_col) > 0 and ref_col.std() > 0 else 100.0

            cur_vals = pd.to_numeric(current_df[col], errors="coerce").fillna(ref_mean).values
            z_scores = (cur_vals - ref_mean) / (ref_std + 1e-9)

            # Apply shift: scale variance and shift mean
            shifted = (ref_mean * mean_multiplier) + (z_scores * ref_std * var_multiplier)
            # Enforce non-negativity for financial amounts and balances
            shifted = np.maximum(0.0, np.round(shifted, 2))
            current_df[col] = shifted
            changed_features.append(col)
            magnitude_details[col] = {
                "mean_multiplier": mean_multiplier,
                "var_multiplier": var_multiplier,
                "orig_mean": round(ref_mean, 2),
                "new_mean": round(float(np.mean(shifted)), 2),
            }

    manifest = ScenarioManifest(
        scenario_id="numeric_mean_variance_shift",
        seed=seed,
        changed_features=changed_features,
        drift_type="numeric_mean_variance",
        magnitude=magnitude_details,
        start_window=0,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=[],
        class_distribution=_extract_class_distribution(current_df),
        description=f"Numeric shift injected on features {changed_features} with mean x{mean_multiplier} and var x{var_multiplier}.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# 3. CATEGORICAL PROPORTION SHIFT
# ──────────────────────────────────────────────────────────────────────────────

def scenario_categorical_proportion_shift(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
    feature: str = "type",
    target_proportions: Optional[Dict[str, float]] = None,
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 3: Categorical Proportion Shift.
    Shifts the category frequency distribution of a categorical column (e.g. surge in TRANSFER).
    """
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng)

    if target_proportions is None:
        target_proportions = {
            "TRANSFER": 0.55,
            "CASH_OUT": 0.35,
            "PAYMENT": 0.05,
            "CASH_IN": 0.03,
            "DEBIT": 0.02,
        }

    categories = list(target_proportions.keys())
    probs = np.array(list(target_proportions.values()), dtype=float)
    probs = probs / np.sum(probs)  # Normalize

    new_categories = rng.choice(categories, size=n_samples, p=probs)
    current_df[feature] = new_categories

    manifest = ScenarioManifest(
        scenario_id="categorical_proportion_shift",
        seed=seed,
        changed_features=[feature],
        drift_type="categorical_proportion",
        magnitude=target_proportions,
        start_window=0,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=[],
        class_distribution=_extract_class_distribution(current_df),
        description=f"Categorical proportion shift on '{feature}': target proportions {target_proportions}.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# 4. UNSEEN CATEGORIES INJECTION
# ──────────────────────────────────────────────────────────────────────────────

def scenario_unseen_categories(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
    feature: str = "type",
    novel_categories: Tuple[str, ...] = ("CRYPTO", "WIRE_INTL"),
    injection_rate: float = 0.15,
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 4: Unseen Categories Injection.
    Injects novel categories never observed in the reference set into a categorical feature.
    """
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng)

    n_inject = int(n_samples * injection_rate)
    inject_indices = rng.choice(n_samples, size=n_inject, replace=False)
    injected_vals = rng.choice(list(novel_categories), size=n_inject)

    current_types = current_df[feature].astype(str).values
    current_types[inject_indices] = injected_vals
    current_df[feature] = current_types

    manifest = ScenarioManifest(
        scenario_id="unseen_categories",
        seed=seed,
        changed_features=[feature],
        drift_type="unseen_categories",
        magnitude={"injection_rate": injection_rate, "novel_categories": list(novel_categories)},
        start_window=0,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=[f"Injected {n_inject} ({injection_rate*100:.1f}%) unseen category records"],
        class_distribution=_extract_class_distribution(current_df),
        description=f"Injected novel unseen categories {novel_categories} at rate {injection_rate} into '{feature}'.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# 5. SUDDEN DRIFT SCENARIO
# ──────────────────────────────────────────────────────────────────────────────

def scenario_sudden_drift(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
    feature: str = "amount",
    step_split_ratio: float = 0.5,
    magnitude_multiplier: float = 4.0,
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 5: Sudden Drift.
    Abrupt step change occurring at step_split_ratio through the current window.
    """
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng, preserve_order=True)

    split_idx = int(n_samples * step_split_ratio)
    vals = current_df[feature].values.astype(float)
    vals[split_idx:] = np.round(vals[split_idx:] * magnitude_multiplier, 2)
    current_df[feature] = vals

    manifest = ScenarioManifest(
        scenario_id="sudden_drift",
        seed=seed,
        changed_features=[feature],
        drift_type="sudden",
        magnitude={"multiplier": magnitude_multiplier, "step_split_ratio": step_split_ratio},
        start_window=split_idx,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=[f"Sudden {magnitude_multiplier}x step shift at row {split_idx}"],
        class_distribution=_extract_class_distribution(current_df),
        description=f"Abrupt {magnitude_multiplier}x multiplier shift applied to '{feature}' starting at index {split_idx}.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# 6. GRADUAL DRIFT SCENARIO
# ──────────────────────────────────────────────────────────────────────────────

def scenario_gradual_drift(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
    feature: str = "amount",
    start_multiplier: float = 1.0,
    end_multiplier: float = 4.0,
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 6: Gradual Drift.
    Continuous linear ramp in feature magnitude from start_multiplier to end_multiplier.
    """
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng, preserve_order=True)

    ramp = np.linspace(start_multiplier, end_multiplier, n_samples)
    current_df[feature] = np.round(current_df[feature].values.astype(float) * ramp, 2)

    manifest = ScenarioManifest(
        scenario_id="gradual_drift",
        seed=seed,
        changed_features=[feature],
        drift_type="gradual",
        magnitude={"start_multiplier": start_multiplier, "end_multiplier": end_multiplier, "curve": "linear"},
        start_window=0,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=[f"Gradual linear ramp from {start_multiplier}x to {end_multiplier}x"],
        class_distribution=_extract_class_distribution(current_df),
        description=f"Gradual linear ramp applied to '{feature}' scaling from {start_multiplier}x to {end_multiplier}x across {n_samples} samples.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# 7. DATA QUALITY FAILURES SCENARIO
# ──────────────────────────────────────────────────────────────────────────────

def scenario_data_quality_failures(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
    null_rate: float = 0.12,
    invalid_range_rate: float = 0.08,
    dropped_column: str = "oldbalanceDest",
    added_column: str = "unexpected_crypto_token",
) -> Tuple[pd.DataFrame, ScenarioManifest]:
    """Scenario 7: Data Quality Failures.
    Injects missing values (NaNs), invalid negative ranges, and schema alterations.
    """
    ref_copy = reference_df.copy(deep=True)
    rng = np.random.default_rng(seed)
    current_df = _sample_reference(ref_copy, n_samples=n_samples, rng=rng)

    injected_faults = []
    changed_features = []

    # 1. Missing values injection
    n_null = int(n_samples * null_rate)
    null_indices = rng.choice(n_samples, size=n_null, replace=False)
    current_df.loc[null_indices, "amount"] = np.nan
    injected_faults.append(f"Injected {n_null} missing (NaN) values in 'amount'")
    changed_features.append("amount")

    # 2. Invalid range injection (negative transaction amounts)
    n_invalid = int(n_samples * invalid_range_rate)
    # Pick indices not already null
    non_null_indices = [i for i in range(n_samples) if i not in set(null_indices)]
    invalid_indices = rng.choice(non_null_indices, size=n_invalid, replace=False)
    current_df.loc[invalid_indices, "amount"] = -999.0
    injected_faults.append(f"Injected {n_invalid} invalid negative values (-999.0) in 'amount'")

    # 3. Schema alterations: Drop expected column
    if dropped_column in current_df.columns:
        current_df.drop(columns=[dropped_column], inplace=True)
        injected_faults.append(f"Schema change: dropped expected column '{dropped_column}'")
        changed_features.append(dropped_column)

    # 4. Schema alterations: Add unexpected column
    token_vals = [f"TOKEN_{rng.integers(1000, 9999)}" for _ in range(n_samples)]
    current_df[added_column] = token_vals
    injected_faults.append(f"Schema change: added unexpected column '{added_column}'")
    changed_features.append(added_column)

    manifest = ScenarioManifest(
        scenario_id="data_quality_failures",
        seed=seed,
        changed_features=changed_features,
        drift_type="data_quality",
        magnitude={
            "null_rate": null_rate,
            "invalid_range_rate": invalid_range_rate,
            "dropped_column": dropped_column,
            "added_column": added_column,
        },
        start_window=0,
        end_window=n_samples,
        sample_counts={"reference": len(reference_df), "current": len(current_df)},
        injected_faults=injected_faults,
        class_distribution=_extract_class_distribution(current_df),
        description="Data quality failures: missing values, negative transaction amounts, and schema modification.",
        synthetic_notice=SYNTHETIC_DISCLAIMER,
    )
    return current_df, manifest


# ──────────────────────────────────────────────────────────────────────────────
# HIGH-LEVEL SCENARIO RUNNER
# ──────────────────────────────────────────────────────────────────────────────

def generate_all_scenarios(
    reference_df: pd.DataFrame,
    seed: int = 42,
    n_samples: int = 1000,
) -> Dict[str, Tuple[pd.DataFrame, ScenarioManifest]]:
    """Generate all 7 controlled drift scenarios sequentially with reproducible seed offsets."""
    scenarios = {
        "no_drift": scenario_no_drift(reference_df, seed=seed, n_samples=n_samples),
        "numeric_mean_variance_shift": scenario_numeric_mean_variance_shift(
            reference_df, seed=seed + 1, n_samples=n_samples
        ),
        "categorical_proportion_shift": scenario_categorical_proportion_shift(
            reference_df, seed=seed + 2, n_samples=n_samples
        ),
        "unseen_categories": scenario_unseen_categories(
            reference_df, seed=seed + 3, n_samples=n_samples
        ),
        "sudden_drift": scenario_sudden_drift(
            reference_df, seed=seed + 4, n_samples=n_samples
        ),
        "gradual_drift": scenario_gradual_drift(
            reference_df, seed=seed + 5, n_samples=n_samples
        ),
        "data_quality_failures": scenario_data_quality_failures(
            reference_df, seed=seed + 6, n_samples=n_samples
        ),
    }
    return scenarios
