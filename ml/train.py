"""ModelPulse Baseline Model Training Pipeline.
Runs chronological split, model training, validation-based threshold tuning,
held-out testing, independent metric audit, and artifact persistence.
"""
import argparse
import sys
from pathlib import Path

# Ensure workspace root is in sys.path for direct script execution
_ROOT_DIR = Path(__file__).resolve().parent.parent
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))

import pandas as pd

from ml.config import BaselineConfig
from ml.dataset import load_paysim_data, split_chronological, generate_synthetic_paysim
from ml.model import (
    build_model_pipeline,
    find_optimal_threshold,
    compute_classification_metrics,
    audit_metrics_independently,
    save_baseline_artifacts,
)


def run_training_pipeline(
    config: BaselineConfig,
    use_synthetic_demo: bool = False,
    n_synthetic_samples: int = 6000,
) -> dict:
    """Execute end-to-end baseline training, validation, testing, and auditing."""
    print("=" * 70)
    print("  MODELPULSE — PHASE 1: BASELINE MODEL TRAINING PIPELINE")
    print("=" * 70)

    # 1. Dataset Loading
    data_path = Path(config.data_path)
    is_synthetic = False

    if data_path.exists():
        print(f"[DATASET] Loading real PaySim dataset from: {data_path}")
        df = load_paysim_data(str(data_path))
    else:
        if not use_synthetic_demo:
            print(f"\n[ERROR] PaySim dataset not found at '{config.data_path}'.")
            print("Official Source: Kaggle PaySim1 (Synthetic Financial Datasets for Fraud Detection)")
            print(f"URL: {config.dataset_source_url}")
            print(f"Please download the dataset and place it at: '{config.data_path}'")
            print("\nTo test the pipeline end-to-end using a verified synthetic test fixture, run:")
            print("    python ml/train.py --synthetic-demo\n")
            sys.exit(1)

        print("\n" + "!" * 70)
        print("  [DEMO/TEST FIXTURE MODE] Real PaySim dataset not detected.")
        print(f"  Generating synthetic PaySim fixture (N={n_synthetic_samples}).")
        print("  NOTE: Metrics produced in this mode reflect the synthetic fixture only.")
        print("!" * 70 + "\n")
        df = generate_synthetic_paysim(
            n_samples=n_synthetic_samples,
            random_seed=config.random_seed,
        )
        is_synthetic = True

    print(f"  Total records loaded: {len(df):,}")
    print(f"  Temporal range (steps): {df['step'].min()} to {df['step'].max()}")
    print(f"  Overall Fraud Count: {df['isFraud'].sum():,} ({df['isFraud'].mean():.4%})")

    # 2. Chronological Splitting
    print(f"\n[SPLIT] Chronological splitting:")
    print(f"  Train: steps 1 to {config.train_max_step}")
    print(f"  Val/Ref: steps {config.train_max_step + 1} to {config.val_max_step}")
    print(f"  Test (Held-out): steps > {config.val_max_step}")

    train_df, val_df, test_df = split_chronological(
        df,
        train_max_step=config.train_max_step,
        val_max_step=config.val_max_step,
        temporal_col=config.temporal_column,
    )

    print(f"  Train samples: {len(train_df):,} | Fraud: {train_df['isFraud'].sum()} ({train_df['isFraud'].mean():.4%})")
    print(f"  Val samples:   {len(val_df):,} | Fraud: {val_df['isFraud'].sum()} ({val_df['isFraud'].mean():.4%})")
    print(f"  Test samples:  {len(test_df):,} | Fraud: {test_df['isFraud'].sum()} ({test_df['isFraud'].mean():.4%})")

    # 3. Features & Target Separation (Ensure zero leakage)
    X_train = train_df.drop(columns=[config.target_column])
    y_train = train_df[config.target_column].values

    X_val = val_df.drop(columns=[config.target_column])
    y_val = val_df[config.target_column].values

    X_test = test_df.drop(columns=[config.target_column])
    y_test = test_df[config.target_column].values

    # 4. Pipeline Construction & Training
    print(f"\n[TRAIN] Fitting baseline {config.model_name}...")
    pipeline = build_model_pipeline(config)
    pipeline.fit(X_train, y_train)
    print("  [OK] Training completed.")

    # 5. Validation-Based Threshold Selection
    print("\n[THRESHOLD] Selecting decision threshold on Validation set (tuning for F1)...")
    val_pred_proba = pipeline.predict_proba(X_val)[:, 1]
    optimal_threshold = find_optimal_threshold(y_val, val_pred_proba)
    print(f"  Selected optimal decision threshold: {optimal_threshold:.2f}")

    # Compute Validation metrics
    val_metrics = compute_classification_metrics(y_val, val_pred_proba, threshold=optimal_threshold)
    print(f"  Validation Precision: {val_metrics['precision']:.4f}")
    print(f"  Validation Recall:    {val_metrics['recall']:.4f}")
    print(f"  Validation F1:        {val_metrics['f1']:.4f}")
    print(f"  Validation PR-AUC:    {val_metrics['pr_auc']:.4f}")

    # Compute Train metrics (for diagnostic overfit check)
    train_pred_proba = pipeline.predict_proba(X_train)[:, 1]
    train_metrics = compute_classification_metrics(y_train, train_pred_proba, threshold=optimal_threshold)

    # 6. Evaluation on Held-Out Chronological Test Set
    print("\n[EVALUATION] Evaluating on Held-Out Test Set (strictly using selected threshold)...")
    test_pred_proba = pipeline.predict_proba(X_test)[:, 1]
    test_metrics = compute_classification_metrics(y_test, test_pred_proba, threshold=optimal_threshold)
    print(f"  Test Precision: {test_metrics['precision']:.4f}")
    print(f"  Test Recall:    {test_metrics['recall']:.4f}")
    print(f"  Test F1:        {test_metrics['f1']:.4f}")
    print(f"  Test PR-AUC:    {test_metrics['pr_auc']:.4f}")
    print(f"  Confusion Matrix: {test_metrics['confusion_matrix']}")

    # 7. Independent Metric Audit
    print("\n[AUDIT] Running independent verification of held-out test metrics...")
    audit_results = audit_metrics_independently(
        y_test,
        test_pred_proba,
        optimal_threshold,
        test_metrics,
    )
    print(f"  Matches reported metrics: {audit_results['matches_reported']}")
    print(f"  Continuous probabilities: {audit_results['is_continuous_proba']} (unique={audit_results['unique_prediction_scores']})")
    print(f"  Suspect leakage flag:     {audit_results['leakage_suspect']}")
    print(f"  Audit Passed:             {audit_results['audit_passed']}")

    # 8. Artifacts Persistence
    print(f"\n[ARTIFACTS] Saving versioned artifacts to: {config.artifacts_dir}")
    # Prepare reference features dataset (combines train + val features as reference baseline)
    ref_df = pd.concat([train_df, val_df], ignore_index=True)
    artifacts = save_baseline_artifacts(
        pipeline=pipeline,
        train_metrics=train_metrics,
        val_metrics=val_metrics,
        test_metrics=test_metrics,
        reference_df=ref_df,
        config=config,
    )
    for k, v in artifacts.items():
        print(f"  - {k}: {v}")

    print("\n" + "=" * 70)
    print("  [SUCCESS] Baseline training and independent audit finished successfully.")
    print("=" * 70 + "\n")

    return {
        "pipeline": pipeline,
        "train_metrics": train_metrics,
        "val_metrics": val_metrics,
        "test_metrics": test_metrics,
        "audit_results": audit_results,
        "is_synthetic": is_synthetic,
        "artifacts": artifacts,
    }


def main():
    parser = argparse.ArgumentParser(description="ModelPulse Baseline Model Training Pipeline")
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/raw/paysim.csv",
        help="Path to PaySim CSV file",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=str,
        default="ml/artifacts",
        help="Output directory for artifacts",
    )
    parser.add_argument(
        "--synthetic-demo",
        action="store_true",
        help="Execute using synthetic test fixture if real data is missing",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=6000,
        help="Sample count for synthetic fixture (default: 6000)",
    )
    args = parser.parse_args()

    config = BaselineConfig(
        data_path=args.data_path,
        artifacts_dir=args.artifacts_dir,
    )

    run_training_pipeline(
        config=config,
        use_synthetic_demo=args.synthetic_demo,
        n_synthetic_samples=args.samples,
    )


if __name__ == "__main__":
    main()
