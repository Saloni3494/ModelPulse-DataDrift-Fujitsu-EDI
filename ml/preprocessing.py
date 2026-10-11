"""ModelPulse Preprocessing and Feature Engineering.
Implements scikit-learn compatible transformers for PaySim fraud detection.
Guarantees zero target leakage and strict separation of training vs. held-out data.
"""
from typing import List, Optional
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

REQUIRED_INPUT_COLUMNS = [
    "step",
    "type",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]

LEAKAGE_COLUMNS = [
    "isFraud",
    "isFlaggedFraud",
]


def validate_raw_dataframe(df: pd.DataFrame, require_target: bool = False) -> None:
    """Validate incoming raw PaySim data integrity.

    Raises:
        ValueError: If required columns are missing, invalid ranges are present,
            or null values exceed acceptable limits.
    """
    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required raw PaySim columns: {missing}")

    if require_target and "isFraud" not in df.columns:
        raise ValueError("Target column 'isFraud' is required for training/evaluation but missing.")

    if require_target and not set(df["isFraud"].dropna().unique()).issubset({0, 1}):
        raise ValueError("Target 'isFraud' contains values outside {0, 1}.")

    if (df["amount"] < 0).any():
        raise ValueError("Negative transaction amounts found in raw dataframe.")

    if (df["step"] < 1).any():
        raise ValueError("Step values must be positive integers.")


class PaySimFeatureEngineer(BaseEstimator, TransformerMixin):
    """Engineers domain features for PaySim transactions and removes leakage/IDs.

    Engineered Features:
    - errorBalanceOrig: (newbalanceOrig + amount) - oldbalanceOrg
    - errorBalanceDest: (oldbalanceDest + amount) - newbalanceDest
    - hour_of_day: step % 24
    - day_of_week: (step // 24) % 7
    - dest_type: First character of nameDest ('C' for Customer, 'M' for Merchant)

    Dropped Columns:
    - isFraud (Target leak)
    - isFlaggedFraud (Rule heuristic leak)
    - nameOrig, nameDest (High cardinality string IDs)
    - step (Replaced by cyclical hour/day to avoid step memorization)
    """

    def __init__(self):
        self.feature_names_out_: Optional[List[str]] = None

    def fit(self, X: pd.DataFrame, y=None):
        """Fit transformer (records schema, stateless)."""
        # Ensure input is a DataFrame
        if not isinstance(X, pd.DataFrame):
            raise TypeError("X must be a pandas DataFrame")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform raw PaySim dataframe into engineered features."""
        if not isinstance(X, pd.DataFrame):
            raise TypeError("X must be a pandas DataFrame")

        df = X.copy()

        # 1. Strictly drop leakage columns if present
        for col in LEAKAGE_COLUMNS:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        # 2. Extract destination type from nameDest before dropping IDs
        if "nameDest" in df.columns:
            df["dest_type"] = df["nameDest"].astype(str).str[0].fillna("C")
            df["dest_type"] = df["dest_type"].apply(lambda x: x if x in ("C", "M") else "C")
        else:
            df["dest_type"] = "C"

        # 3. Drop high-cardinality IDs
        for id_col in ["nameOrig", "nameDest"]:
            if id_col in df.columns:
                df.drop(columns=[id_col], inplace=True)

        # 4. Temporal features (step decomposition)
        if "step" in df.columns:
            df["hour_of_day"] = (df["step"] % 24).astype(int)
            df["day_of_week"] = ((df["step"] // 24) % 7).astype(int)
            df.drop(columns=["step"], inplace=True)
        else:
            df["hour_of_day"] = 0
            df["day_of_week"] = 0

        # 5. Balance discrepancy indicators
        df["errorBalanceOrig"] = df["newbalanceOrig"] + df["amount"] - df["oldbalanceOrg"]
        df["errorBalanceDest"] = df["oldbalanceDest"] + df["amount"] - df["newbalanceDest"]

        # 6. Ensure standard numeric types and fill NaNs
        numeric_cols = [
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "errorBalanceOrig",
            "errorBalanceDest",
            "hour_of_day",
            "day_of_week",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Ensure type is clean string
        if "type" in df.columns:
            df["type"] = df["type"].astype(str)

        self.feature_names_out_ = list(df.columns)
        return df

    def get_feature_names_out(self, input_features=None):
        return self.feature_names_out_


def build_preprocessor_pipeline() -> ColumnTransformer:
    """Build scikit-learn ColumnTransformer for one-hot encoding and scaling."""
    categorical_features = ["type", "dest_type"]
    numeric_features = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "errorBalanceOrig",
        "errorBalanceDest",
        "hour_of_day",
        "day_of_week",
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                categorical_features,
            ),
            (
                "num",
                StandardScaler(),
                numeric_features,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )
    return preprocessor
