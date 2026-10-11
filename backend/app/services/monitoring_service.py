"""ModelPulse Monitoring Orchestration Service.
Coordinates the end-to-end monitoring lifecycle:
1. Loads frozen reference dataset and metadata (immutable).
2. Ingests evaluation batches or synthesizes controlled simulator scenarios.
3. Pre-validates data quality and schema health.
4. Evaluates feature-level statistical drift across numeric and categorical ensembles (Phase 2).
5. Assesses prediction score distribution PSI and predicted-positive-rate shifts.
6. Evaluates ground-truth performance degradation when labels are available (delayed-label aware).
7. Computes explainable deterministic composite severity (Phase 3).
8. Persists monitoring run records into the configured repository.
"""
from datetime import datetime, timezone
import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

from backend.app.db.repository import MonitoringRepository, get_monitoring_repository
from backend.app.schemas.drift import FeatureDriftResult, TestResult
from backend.app.schemas.monitoring import (
    MonitoringRunRequest,
    MonitoringRunResponse,
    WindowSpec,
    PredictionDriftSummary,
    SeverityBreakdown,
    TopContributor,
)
from backend.app.services.drift_metrics import (
    compute_numeric_psi,
    evaluate_feature_drift,
)
from backend.app.services.model_service import ModelService
from backend.app.services.severity_service import (
    compute_component_d,
    compute_component_p,
    compute_component_q,
    compute_component_m,
    compute_severity_score,
    SeverityConfig,
)
from simulator.scenarios import (
    load_or_create_reference,
    scenario_no_drift,
    scenario_numeric_mean_variance_shift,
    scenario_categorical_proportion_shift,
    scenario_unseen_categories,
    scenario_sudden_drift,
    scenario_gradual_drift,
    scenario_data_quality_failures,
)

MONITORED_FEATURES = [
    ("amount", "numeric", 3.0),
    ("oldbalanceOrg", "numeric", 2.0),
    ("newbalanceOrig", "numeric", 2.0),
    ("oldbalanceDest", "numeric", 1.0),
    ("newbalanceDest", "numeric", 1.0),
    ("type", "categorical", 2.0),
]

REQUIRED_COLUMNS = [
    "step",
    "type",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]

STANDARD_COLUMNS = set(REQUIRED_COLUMNS) | {
    "isFraud",
    "isFlaggedFraud",
    "nameOrig",
    "nameDest",
}


class MonitoringService:
    """Orchestrator for end-to-end drift monitoring runs."""

    def __init__(
        self,
        model_service: Optional[ModelService] = None,
        repository: Optional[MonitoringRepository] = None,
        reference_path: str = "ml/artifacts/reference_features.csv",
    ):
        self.model_service = model_service or ModelService.get_instance()
        self.repository = repository or get_monitoring_repository()
        self.reference_path = reference_path
        self._reference_df: Optional[pd.DataFrame] = None
        self._reference_scores: Optional[np.ndarray] = None
        self._ref_ppr: Optional[float] = None

    def get_reference_data(self) -> pd.DataFrame:
        """Load and cache frozen reference dataset, strictly returning a deep copy."""
        if self._reference_df is None:
            self._reference_df = load_or_create_reference(
                reference_path=self.reference_path, seed=42
            )
        return self._reference_df.copy(deep=True)

    def _get_reference_scores(self) -> Tuple[np.ndarray, float]:
        """Compute or return cached model scores and predicted positive rate on reference data."""
        if self._reference_scores is None:
            ref_df = self.get_reference_data()
            if not self.model_service._loaded:
                self.model_service.load_artifacts()
            probas = self.model_service.pipeline.predict_proba(ref_df)
            self._reference_scores = probas[:, 1]
            tau = self.model_service.optimal_threshold
            self._ref_ppr = float((self._reference_scores >= tau).sum() / len(self._reference_scores))
        return self._reference_scores, self._ref_ppr

    def execute_monitoring_run(
        self, request: MonitoringRunRequest, persist: bool = True
    ) -> MonitoringRunResponse:
        """Execute a full monitoring run from request and optionally persist it."""
        run_id = request.run_id or str(uuid.uuid4())

        # Check repository-level idempotency for deterministic runs
        if request.run_id and persist:
            existing = self.repository.get_run(run_id)
            if existing is not None:
                logger.info(
                    f"[MonitoringService] Run '{run_id}' already exists in repository; "
                    "returning existing record idempotently without re-execution."
                )
                return existing

        now = datetime.now(timezone.utc)
        recommendations: List[str] = []

        # 1. Load frozen reference dataset
        ref_df = self.get_reference_data()
        ref_count = len(ref_df)

        # 2. Ingest evaluation batch or generate scenario
        cur_df, ground_truth_labels, scenario_desc = self._resolve_current_window(
            request, ref_df
        )
        cur_count = len(cur_df)

        # 3. Assess Data Quality & Schema Integrity
        (
            quality_failure_rate,
            invalid_count,
            quality_issues,
            valid_count,
            quarantined_count,
        ) = self._assess_data_quality(cur_df)
        comp_q = compute_component_q(
            quality_failure_rate=quality_failure_rate, q_max=0.10
        )
        if quality_issues:
            recommendations.append(
                f"Data quality warnings: {'; '.join(quality_issues)}."
            )

        # 4. Feature-Level Drift Detection (Phase 2)
        feature_results, feature_importances = self._evaluate_feature_drift_set(
            ref_df, cur_df
        )
        dataset_drift_detected = any(f.drift_detected for f in feature_results)

        # 5. Top Contributors & Contribution Ranking
        top_contributors = self._compute_top_contributors(feature_results)
        if dataset_drift_detected:
            drifted_names = [f.feature_name for f in feature_results if f.drift_detected]
            recommendations.append(
                f"Data drift confirmed on feature(s): {', '.join(drifted_names)}."
            )

        # 6. Component D (Importance-Weighted Data Drift)
        comp_d = compute_component_d(
            feature_results=feature_results,
            feature_importances=feature_importances,
            psi_crit=0.25,
        )

        # 7. Model Inference & Prediction Drift (Phase 1 & Phase 2 Integration)
        pred_drift_summary, comp_p = self._evaluate_prediction_drift(
            cur_df, recommendations
        )

        # 8. Model Performance Degradation (Component M)
        comp_m = self._evaluate_performance_degradation(
            cur_df, ground_truth_labels, recommendations
        )

        # 9. Composite Severity Scoring (Phase 3)
        severity_result = compute_severity_score(
            component_d=comp_d,
            component_p=comp_p,
            component_q=comp_q,
            component_m=comp_m,
            sample_count=cur_count,
        )

        # Determine overall run status
        if comp_q is not None and comp_q >= 75.0:
            status = "quality_failure"
        elif cur_count < 50:
            status = "insufficient_evidence"
        else:
            status = "ok"

        # 10. Construct Structured Response
        response = MonitoringRunResponse(
            run_id=run_id,
            model_name=self.model_service.model_name,
            model_version=self.model_service.model_version,
            run_type=request.run_type,
            reference_window=WindowSpec(
                start=now,
                end=now,
                sample_count=ref_count,
            ),
            current_window=WindowSpec(
                start=now,
                end=now,
                sample_count=cur_count,
            ),
            sample_count=cur_count,
            valid_sample_count=valid_count,
            quarantined_sample_count=quarantined_count,
            status=status,
            dataset_drift_detected=dataset_drift_detected,
            prediction_drift=pred_drift_summary,
            severity=severity_result.to_breakdown(),
            features=feature_results,
            top_contributors=top_contributors,
            recommendations=recommendations or ["All monitored indicators within normal parameters."],
            created_at=now,
        )

        # 11. Persist to repository if requested (atomic run + alert outbox transaction)
        if persist:
            outbox_records = None
            try:
                from backend.app.alerting.service import get_alert_service
                outbox_records = get_alert_service().build_outbox_records(response)
            except Exception:
                outbox_records = None
            self.repository.save_run(response, outbox_records=outbox_records)

        return response

    def _resolve_current_window(
        self, request: MonitoringRunRequest, ref_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Optional[np.ndarray], str]:
        """Resolve current window from simulator scenario or supplied data."""
        if request.scenario_id:
            sid = request.scenario_id.lower()
            seed = request.scenario_seed
            n_samples = request.scenario_samples

            if sid == "no_drift":
                cur_df, manifest = scenario_no_drift(ref_df, seed=seed, n_samples=n_samples)
            elif sid == "numeric_mean_variance_shift":
                cur_df, manifest = scenario_numeric_mean_variance_shift(
                    ref_df, seed=seed, n_samples=n_samples
                )
            elif sid == "categorical_proportion_shift":
                cur_df, manifest = scenario_categorical_proportion_shift(
                    ref_df, seed=seed, n_samples=n_samples
                )
            elif sid == "unseen_categories":
                cur_df, manifest = scenario_unseen_categories(
                    ref_df, seed=seed, n_samples=n_samples
                )
            elif sid == "sudden_drift":
                cur_df, manifest = scenario_sudden_drift(
                    ref_df, seed=seed, n_samples=n_samples
                )
            elif sid == "gradual_drift":
                cur_df, manifest = scenario_gradual_drift(
                    ref_df, seed=seed, n_samples=n_samples
                )
            elif sid == "data_quality_failures":
                cur_df, manifest = scenario_data_quality_failures(
                    ref_df, seed=seed, n_samples=n_samples
                )
            else:
                raise ValueError(
                    f"Unsupported simulator scenario '{request.scenario_id}'. "
                    "Supported: no_drift, numeric_mean_variance_shift, categorical_proportion_shift, "
                    "unseen_categories, sudden_drift, gradual_drift, data_quality_failures."
                )

            labels = None
            if request.ground_truth_labels is not None:
                labels = np.asarray(request.ground_truth_labels, dtype=int)
            return cur_df, labels, manifest.description

        elif request.current_data:
            cur_df = pd.DataFrame(request.current_data)
            labels = None
            if request.ground_truth_labels is not None:
                labels = np.asarray(request.ground_truth_labels, dtype=int)
            return cur_df, labels, "Supplied raw evaluation batch"

        else:
            raise ValueError(
                "Either 'scenario_id' or 'current_data' must be supplied to execute a monitoring run."
            )

    def _get_valid_mask(self, df: pd.DataFrame) -> pd.Series:
        """Return boolean Series indicating valid rows (not quarantined)."""
        if len(df) == 0:
            return pd.Series(dtype=bool)

        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                return pd.Series(False, index=df.index)

        valid_mask = ~df[REQUIRED_COLUMNS].isna().any(axis=1)

        amt = pd.to_numeric(df["amount"], errors="coerce")
        valid_mask &= amt.notna() & (amt >= 0)

        step = pd.to_numeric(df["step"], errors="coerce")
        valid_mask &= step.notna() & (step >= 1)

        o_org = pd.to_numeric(df["oldbalanceOrg"], errors="coerce")
        n_org = pd.to_numeric(df["newbalanceOrig"], errors="coerce")
        valid_mask &= o_org.notna() & (o_org >= 0) & n_org.notna() & (n_org >= 0)

        o_dest = pd.to_numeric(df["oldbalanceDest"], errors="coerce")
        n_dest = pd.to_numeric(df["newbalanceDest"], errors="coerce")
        valid_mask &= o_dest.notna() & (o_dest >= 0) & n_dest.notna() & (n_dest >= 0)

        valid_mask &= df["type"].notna() & (df["type"].astype(str).str.strip() != "")

        return valid_mask

    def _assess_data_quality(
        self, df: pd.DataFrame
    ) -> Tuple[float, int, List[str], int, int]:
        """Detect schema corruptions, missing columns, and null/range violations.

        Returns:
            Tuple of:
            - quality_failure_rate: float in [0.0, 1.0]
            - invalid_cells: int
            - issues: List[str]
            - valid_count: int
            - quarantined_count: int
        """
        issues: List[str] = []
        n_rows = len(df)
        if n_rows == 0:
            return 1.0, 0, ["Current window contains 0 records."], 0, 0

        invalid_cells = 0

        # Check missing expected columns
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            invalid_cells += len(missing_cols) * n_rows

        # Check unexpected columns
        unexpected_cols = [c for c in df.columns if c not in STANDARD_COLUMNS]
        if unexpected_cols:
            issues.append(f"Unexpected column(s) detected: {unexpected_cols}")

        if missing_cols:
            return 1.0, invalid_cells, issues, 0, n_rows

        present_cols = [c for c in REQUIRED_COLUMNS if c in df.columns]
        null_cells = int(df[present_cols].isna().sum().sum())
        if null_cells > 0:
            issues.append(f"Found {null_cells} missing / null cells")
            invalid_cells += null_cells

        if "amount" in df.columns:
            neg_amounts = int((pd.to_numeric(df["amount"], errors="coerce") < 0).sum())
            if neg_amounts > 0:
                issues.append(f"Found {neg_amounts} negative transaction amounts")
                invalid_cells += neg_amounts

        if "step" in df.columns:
            neg_steps = int((pd.to_numeric(df["step"], errors="coerce") < 1).sum())
            if neg_steps > 0:
                issues.append(f"Found {neg_steps} invalid step values (< 1)")
                invalid_cells += neg_steps

        valid_mask = self._get_valid_mask(df)
        quarantined_count = int((~valid_mask).sum())
        valid_count = n_rows - quarantined_count

        failure_rate = min(1.0, quarantined_count / n_rows) if n_rows > 0 else 1.0
        return round(failure_rate, 4), invalid_cells, issues, valid_count, quarantined_count

    def _evaluate_feature_drift_set(
        self, ref_df: pd.DataFrame, cur_df: pd.DataFrame
    ) -> Tuple[List[FeatureDriftResult], Dict[str, float]]:
        """Evaluate ensemble drift for monitored features."""
        results: List[FeatureDriftResult] = []
        importances: Dict[str, float] = {}

        for feat_name, feat_type, imp in MONITORED_FEATURES:
            importances[feat_name] = imp

            if feat_name not in cur_df.columns:
                # Column dropped in current window: record schema failure
                res = FeatureDriftResult(
                    feature_name=feat_name,
                    feature_type=feat_type,
                    tests=[
                        TestResult(
                            metric="psi",
                            value=1.0,
                            threshold=0.25,
                            exceeded=True,
                            status="missing_column",
                            details={"reason": "Feature dropped from current dataset"},
                        )
                    ],
                    votes="1/1",
                    votes_count=1,
                    total_tests=1,
                    drift_detected=True,
                    drift_contribution=imp,
                )
                results.append(res)
                continue

            ref_vals = ref_df[feat_name]
            cur_vals = cur_df[feat_name]

            res = evaluate_feature_drift(
                reference_values=ref_vals,
                current_values=cur_vals,
                feature_name=feat_name,
                feature_type=feat_type,
                importance=imp,
                k_of_m=(2, 3),
            )
            results.append(res)

        return results, importances

    def _compute_top_contributors(
        self, feature_results: List[FeatureDriftResult]
    ) -> List[TopContributor]:
        """Rank features by drift contribution score and cumulative share."""
        drifted = [f for f in feature_results if f.drift_detected]
        if not drifted:
            return []

        drifted_sorted = sorted(drifted, key=lambda f: f.drift_contribution, reverse=True)
        total_drift = sum(f.drift_contribution for f in drifted_sorted)

        contributors: List[TopContributor] = []
        cumulative = 0.0

        for f in drifted_sorted:
            share = (f.drift_contribution / total_drift) if total_drift > 0 else 0.0
            cumulative += share
            contributors.append(
                TopContributor(
                    feature_name=f.feature_name,
                    contribution_score=round(f.drift_contribution, 4),
                    cumulative_share=round(min(1.0, cumulative), 4),
                )
            )

        return contributors

    def _evaluate_prediction_drift(
        self, cur_df: pd.DataFrame, recommendations: List[str]
    ) -> Tuple[PredictionDriftSummary, Optional[float]]:
        """Score clean records through model pipeline and calculate prediction drift."""
        n_rows = len(cur_df)
        valid_mask = self._get_valid_mask(cur_df)
        clean_cur_df = cur_df[valid_mask] if len(cur_df) > 0 else cur_df
        valid_count = len(clean_cur_df)

        if n_rows > 0 and valid_count == 0:
            recommendations.append(
                "Prediction drift evaluation skipped: all records in current window were quarantined."
            )
            return (
                PredictionDriftSummary(
                    metric="psi",
                    value=None,
                    threshold=0.25,
                    drifted=False,
                    status="all_quarantined",
                ),
                None,
            )

        if valid_count < 10:
            recommendations.append(
                f"Prediction drift evaluation skipped: insufficient valid samples ({valid_count} < 10 required)."
            )
            return (
                PredictionDriftSummary(
                    metric="psi",
                    value=None,
                    threshold=0.25,
                    drifted=False,
                    status="insufficient_samples",
                ),
                None,
            )

        try:
            cur_probas = self.model_service.pipeline.predict_proba(clean_cur_df)
            cur_scores = cur_probas[:, 1]
            tau = self.model_service.optimal_threshold
            cur_ppr = float((cur_scores >= tau).sum() / len(cur_scores))

            ref_scores, ref_ppr = self._get_reference_scores()

            psi_result = compute_numeric_psi(
                reference=ref_scores, current=cur_scores, threshold=0.25
            )
            pred_psi = round(psi_result.value, 4)
            drifted = psi_result.exceeded

            if drifted:
                recommendations.append(
                    f"Prediction drift detected: model score distribution PSI={pred_psi:.3f} (threshold=0.250)."
                )

            comp_p = compute_component_p(
                pred_score_psi=pred_psi,
                ppr_reference=ref_ppr,
                ppr_current=cur_ppr,
                psi_crit=0.25,
                delta_ppr_crit=0.10,
            )

            return (
                PredictionDriftSummary(
                    metric="psi",
                    value=pred_psi,
                    threshold=0.25,
                    drifted=drifted,
                    status="ok",
                ),
                comp_p,
            )
        except Exception:
            return (
                PredictionDriftSummary(
                    metric="psi",
                    value=None,
                    threshold=0.25,
                    drifted=False,
                    status="unavailable",
                ),
                None,
            )

    def _evaluate_performance_degradation(
        self,
        cur_df: pd.DataFrame,
        labels: Optional[np.ndarray],
        recommendations: List[str],
    ) -> Optional[float]:
        """Compute Component M when labels are available, or None when delayed."""
        if labels is None or len(labels) == 0:
            recommendations.append(
                "Ground-truth labels unavailable (delayed feedback). Performance impact unobserved; "
                "collect recent labels before retraining."
            )
            return None

        # Filter clean rows matching labels
        try:
            valid_mask = self._get_valid_mask(cur_df)
            clean_cur_df = cur_df[valid_mask]
            clean_labels = np.asarray(labels)[valid_mask.values]

            if len(clean_cur_df) == 0:
                recommendations.append(
                    "Performance degradation evaluation skipped: all records quarantined."
                )
                return None

            baseline_val = float(
                self.model_service.metrics.get("val_metrics", {}).get("pr_auc", 0.7556)
            )
            # Predict scores on current records
            cur_probas = self.model_service.pipeline.predict_proba(clean_cur_df)
            cur_scores = cur_probas[:, 1]

            from sklearn.metrics import average_precision_score

            if len(np.unique(clean_labels)) > 1:
                cur_pr_auc = float(average_precision_score(clean_labels, cur_scores))
            else:
                cur_pr_auc = 0.0

            comp_m = compute_component_m(
                baseline_metric=baseline_val,
                current_metric=cur_pr_auc,
                critical_floor=0.30,
            )
            if comp_m is not None and comp_m >= 50.0:
                recommendations.append(
                    f"Performance degradation alert: PR-AUC dropped from {baseline_val:.3f} to {cur_pr_auc:.3f}."
                )
            return comp_m
        except Exception:
            return None
