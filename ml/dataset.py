"""ModelPulse Dataset Loading and Chronological Splitting.
Provides data loaders, splitters, and synthetic test fixtures for PaySim fraud detection.
"""
from pathlib import Path
from typing import Dict, Tuple
import numpy as np
import pandas as pd

EXPECTED_PAYSIM_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]


def load_paysim_data(file_path: str = "data/raw/paysim.csv") -> pd.DataFrame:
    """Load PaySim dataset from disk.

    Raises:
        FileNotFoundError: If the dataset file does not exist locally,
            providing the official source URL and setup instructions.
        ValueError: If required columns are missing from the dataset.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(
            f"PaySim dataset not found at '{file_path}'.\n"
            "Official Source: Kaggle PaySim1 (Synthetic Financial Datasets for Fraud Detection)\n"
            "URL: https://www.kaggle.com/datasets/ealaxi/paysim1\n"
            f"Please download the dataset CSV and place it at '{file_path}'."
        )

    df = pd.read_csv(file_path)
    missing = [c for c in EXPECTED_PAYSIM_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset at '{file_path}' is missing required PaySim columns: {missing}")

    return df


def split_chronological(
    df: pd.DataFrame,
    train_max_step: int = 500,
    val_max_step: int = 620,
    temporal_col: str = "step",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split dataframe into non-overlapping chronological periods.

    Args:
        df: Input dataframe containing temporal_col.
        train_max_step: Maximum step included in training split (inclusive).
        val_max_step: Maximum step included in validation split (inclusive).
        temporal_col: Name of the temporal step column.

    Returns:
        Tuple of (train_df, val_df, test_df)
    """
    if train_max_step >= val_max_step:
        raise ValueError(
            f"train_max_step ({train_max_step}) must be strictly less than val_max_step ({val_max_step})"
        )

    if temporal_col not in df.columns:
        raise ValueError(f"Temporal column '{temporal_col}' not found in dataframe")

    train_df = df[df[temporal_col] <= train_max_step].copy()
    val_df = df[(df[temporal_col] > train_max_step) & (df[temporal_col] <= val_max_step)].copy()
    test_df = df[df[temporal_col] > val_max_step].copy()

    if len(train_df) == 0:
        raise ValueError(f"Train split is empty for temporal threshold {train_max_step}")
    if len(val_df) == 0:
        raise ValueError(f"Validation split is empty for window ({train_max_step}, {val_max_step}]")
    if len(test_df) == 0:
        raise ValueError(f"Test split is empty for step > {val_max_step}")

    # Enforce chronological ordering assertion
    assert train_df[temporal_col].max() < val_df[temporal_col].min(), "Train and Val splits overlap"
    assert val_df[temporal_col].max() < test_df[temporal_col].min(), "Val and Test splits overlap"

    return train_df, val_df, test_df


def generate_synthetic_paysim(
    n_samples: int = 6000,
    random_seed: int = 42,
    fraud_rate: float = 0.005,
) -> pd.DataFrame:
    """Generate synthetic PaySim-structured dataset fixture for testing.

    Note: This fixture is strictly for automated unit and smoke testing
    when real PaySim data is not present on disk.
    """
    rng = np.random.default_rng(random_seed)

    # Generate chronological steps across full 744 hour span (31 days)
    steps = rng.integers(1, 745, size=n_samples)
    steps = np.sort(steps)

    transaction_types = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
    type_probs = [0.35, 0.15, 0.35, 0.12, 0.03]
    types = rng.choice(transaction_types, size=n_samples, p=type_probs)

    amounts = np.round(rng.exponential(scale=15000, size=n_samples) + 10.0, 2)
    oldbalance_org = np.round(rng.exponential(scale=25000, size=n_samples), 2)
    
    # In PaySim fraud occurs almost exclusively on TRANSFER and CASH_OUT
    is_fraud = np.zeros(n_samples, dtype=int)
    eligible_fraud_indices = [
        i for i, t in enumerate(types) if t in ("TRANSFER", "CASH_OUT")
    ]
    n_fraud = max(10, int(len(eligible_fraud_indices) * fraud_rate))
    fraud_indices = rng.choice(eligible_fraud_indices, size=n_fraud, replace=False)
    is_fraud[fraud_indices] = 1

    # Adjust balances for fraud vs non-fraud
    newbalance_orig = np.maximum(0.0, oldbalance_org - amounts)
    # Fraud transactions often empty the origin balance completely
    newbalance_orig[fraud_indices] = 0.0

    oldbalance_dest = np.round(rng.exponential(scale=30000, size=n_samples), 2)
    newbalance_dest = oldbalance_dest + amounts

    # Generate customer / merchant IDs
    dest_prefix = rng.choice(["C", "M"], size=n_samples, p=[0.75, 0.25])
    # Payment to merchant: destination balance remains 0 in PaySim
    merchant_mask = (dest_prefix == "M")
    oldbalance_dest[merchant_mask] = 0.0
    newbalance_dest[merchant_mask] = 0.0

    name_orig = [f"C{rng.integers(100000, 999999)}" for _ in range(n_samples)]
    name_dest = [f"{prefix}{rng.integers(100000, 999999)}" for prefix in dest_prefix]

    # Rule-based flag: amount > 200,000 on TRANSFER
    is_flagged_fraud = (
        (is_fraud == 1) & (types == "TRANSFER") & (amounts > 200000)
    ).astype(int)

    df = pd.DataFrame({
        "step": steps,
        "type": types,
        "amount": amounts,
        "nameOrig": name_orig,
        "oldbalanceOrg": oldbalance_org,
        "newbalanceOrig": newbalance_orig,
        "nameDest": name_dest,
        "oldbalanceDest": oldbalance_dest,
        "newbalanceDest": newbalance_dest,
        "isFraud": is_fraud,
        "isFlaggedFraud": is_flagged_fraud,
    })

    return df
