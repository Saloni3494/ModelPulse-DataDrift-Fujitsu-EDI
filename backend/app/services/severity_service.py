"""ModelPulse Multi-Component Severity Scoring Engine.
Implements the ModelPulse v2 transparent 0-100 severity calculation across:
- D: Importance-weighted data drift from feature-level ensemble results.
- P: Prediction drift combining prediction score PSI and predicted positive rate (PPR) shifts.
- Q: Data quality failure severity relative to configured critical limits.
- M: Model performance degradation relative to baseline and critical floor (delayed-label aware).

Includes:
- Dynamic weight renormalisation when components (such as delayed labels for M) are unavailable.
- Configurable thresholds, weights, and severity bands.
- Monotonic guarantees and boundary clipping [0, 100].
- Transparent heuristic disclosures and correlation notices.
"""
from dataclasses import dataclass, field
import math
from typing import Any, Dict, List, Optional, Tuple, Union

from backend.app.schemas.drift import FeatureDriftResult
from backend.app.schemas.monitoring import SeverityResult, SeverityBreakdown

HEURISTIC_DISCLAIMER = (
    "ModelPulse severity score is an explainable composite heuristic for operational prioritization, "
    "not an empirical probability of model failure."
)

CORRELATION_NOTICE = (
    "Signal correlation note: Input data drift (D) and prediction drift (P) may share common underlying "
    "distribution shifts. Examine component breakdown rather than relying solely on the scalar score."
)


@dataclass
class SeverityConfig:
    """Configurable thresholds, weights, and bands for severity scoring."""

    # Base component weights (D, P, Q, M)
    weights: Dict[str, float] = field(
        default_factory=lambda: {
            "D": 0.40,
            "P": 0.20,
            "Q": 0.15,
            "M": 0.25,
        }
    )

    # Thresholds & saturation limits
    psi_crit: float = 0.25         # Critical PSI ceiling (features & predictions)
    delta_ppr_crit: float = 0.10   # Critical threshold for absolute shift in predicted positive rate
    q_max: float = 0.10            # Data quality critical saturation limit (10% failures = 100)
    m_critical_floor: float = 0.30 # Critical performance floor below which degradation saturates at 100
    min_samples_threshold: int = 50 # Minimum sample count for full confidence

    # Operational severity bands: Low (0-24), Moderate (25-49), High (50-74), Critical (75-100)
    bands: Dict[str, Tuple[int, int]] = field(
        default_factory=lambda: {
            "Low": (0, 24),
            "Moderate": (25, 49),
            "High": (50, 74),
            "Critical": (75, 100),
        }
    )

    def __post_init__(self):
        """Validate configuration parameters."""
        required_keys = {"D", "P", "Q", "M"}
        if not required_keys.issubset(self.weights.keys()):
            raise ValueError(f"Weights must contain all components {required_keys}, got {set(self.weights.keys())}")

        for k, v in self.weights.items():
            if v < 0:
                raise ValueError(f"Weight for component '{k}' cannot be negative: {v}")

        total_weight = sum(self.weights.values())
        if total_weight <= 0:
            raise ValueError("Sum of configured base weights must be strictly positive.")

        if self.psi_crit <= 0:
            raise ValueError(f"psi_crit must be strictly positive, got {self.psi_crit}")
        if self.delta_ppr_crit <= 0:
            raise ValueError(f"delta_ppr_crit must be strictly positive, got {self.delta_ppr_crit}")
        if self.q_max <= 0:
            raise ValueError(f"q_max must be strictly positive, got {self.q_max}")


# ──────────────────────────────────────────────────────────────────────────────
# COMPONENT CALCULATIONS (D, P, Q, M)
# ──────────────────────────────────────────────────────────────────────────────

def compute_component_d(
    feature_results: Optional[List[FeatureDriftResult]],
    feature_importances: Optional[Dict[str, float]] = None,
    psi_crit: float = 0.25,
) -> Optional[float]:
    """Compute Component D: Importance-weighted data drift in [0.0, 100.0].

    Formula:
        D = 100 * sum_f(w_f * norm_drift_f) / sum_f(w_f)
    where:
        norm_drift_f = clip(PSI_f / psi_crit, 0, 1) if drift_detected else 0.0
        w_f = feature_importances[f] (defaults to 1.0 if unspecified)

    Returns:
        float in [0.0, 100.0], or None if feature_results is empty or None.
    """
    if feature_results is None or len(feature_results) == 0:
        return None

    if psi_crit <= 0:
        raise ValueError(f"psi_crit must be positive, got {psi_crit}")

    importances = feature_importances or {}
    weighted_drift_sum = 0.0
    total_importance = 0.0

    for res in feature_results:
        w_f = float(importances.get(res.feature_name, 1.0))
        if w_f < 0:
            w_f = 0.0
        total_importance += w_f

        if res.drift_detected:
            # Locate PSI test from result
            psi_val = 0.0
            for t in res.tests:
                if t.metric == "psi":
                    psi_val = t.value
                    break
            else:
                # Fallback to drift_contribution / w_f if tests list does not explicitly include psi
                psi_val = res.drift_contribution / w_f if w_f > 0 else 0.0

            norm_drift = min(1.0, max(0.0, psi_val / psi_crit))
            weighted_drift_sum += w_f * norm_drift
        else:
            # Undetected / passed ensemble tests contribute 0
            weighted_drift_sum += 0.0

    if total_importance == 0:
        return 0.0

    d_score = 100.0 * (weighted_drift_sum / total_importance)
    return round(float(min(100.0, max(0.0, d_score))), 2)


def compute_component_p(
    pred_score_psi: Optional[float] = None,
    ppr_reference: Optional[float] = None,
    ppr_current: Optional[float] = None,
    psi_crit: float = 0.25,
    delta_ppr_crit: float = 0.10,
) -> Optional[float]:
    """Compute Component P: Prediction drift in [0.0, 100.0].

    Combines:
    1. Score distribution drift: norm_score = clip(PSI_score / psi_crit, 0, 1)
    2. Predicted positive rate shift: norm_ppr = clip(|PPR_cur - PPR_ref| / delta_ppr_crit, 0, 1)
    P = 100 * max(norm_score, norm_ppr) (or available signal)

    Returns:
        float in [0.0, 100.0], or None if neither signal is provided.
    """
    has_psi = pred_score_psi is not None and not math.isnan(pred_score_psi)
    has_ppr = (
        ppr_reference is not None
        and ppr_current is not None
        and not math.isnan(ppr_reference)
        and not math.isnan(ppr_current)
    )

    if not has_psi and not has_ppr:
        return None

    norm_signals: List[float] = []

    if has_psi:
        if psi_crit <= 0:
            raise ValueError(f"psi_crit must be positive, got {psi_crit}")
        norm_score = min(1.0, max(0.0, pred_score_psi / psi_crit))
        norm_signals.append(norm_score)

    if has_ppr:
        if delta_ppr_crit <= 0:
            raise ValueError(f"delta_ppr_crit must be positive, got {delta_ppr_crit}")
        delta_ppr = abs(ppr_current - ppr_reference)
        norm_ppr = min(1.0, max(0.0, delta_ppr / delta_ppr_crit))
        norm_signals.append(norm_ppr)

    # Max across available prediction drift signals ensures either score distribution
    # shape shifts or classification volume threshold jumps trigger severity.
    norm_p = max(norm_signals)
    p_score = 100.0 * norm_p
    return round(float(min(100.0, max(0.0, p_score))), 2)


def compute_component_q(
    quality_failure_rate: Optional[float] = None,
    invalid_record_count: Optional[int] = None,
    total_record_count: Optional[int] = None,
    q_max: float = 0.10,
) -> Optional[float]:
    """Compute Component Q: Data quality failure severity in [0.0, 100.0].

    Formula:
        Q = 100 * clip(failure_rate / q_max, 0, 1)
    where failure_rate can be passed directly or computed as invalid_records / total_records.
    Saturation point: failure_rate >= q_max (default 0.10 = 10%) yields Q = 100.

    Returns:
        float in [0.0, 100.0], or None if quality metrics are unavailable.
    """
    if q_max <= 0:
        raise ValueError(f"q_max must be positive, got {q_max}")

    rate: Optional[float] = None
    if quality_failure_rate is not None and not math.isnan(quality_failure_rate):
        rate = quality_failure_rate
    elif (
        invalid_record_count is not None
        and total_record_count is not None
        and total_record_count > 0
    ):
        rate = invalid_record_count / total_record_count

    if rate is None:
        return None

    norm_q = min(1.0, max(0.0, rate / q_max))
    q_score = 100.0 * norm_q
    return round(float(min(100.0, max(0.0, q_score))), 2)


def compute_component_m(
    baseline_metric: Optional[float] = None,
    current_metric: Optional[float] = None,
    critical_floor: float = 0.30,
    higher_is_better: bool = True,
) -> Optional[float]:
    """Compute Component M: Model performance degradation in [0.0, 100.0].

    Formula (when higher_is_better):
        M = 100 * clip((baseline - current) / (baseline - critical_floor), 0, 1)

    When current >= baseline, M = 0.0.
    When current <= critical_floor, M = 100.0 (saturation).
    When ground-truth labels are delayed or absent, returns None.

    Returns:
        float in [0.0, 100.0], or None if labels/metrics are missing.
    """
    if baseline_metric is None or current_metric is None:
        return None

    if math.isnan(baseline_metric) or math.isnan(current_metric):
        return None

    if higher_is_better:
        denom = baseline_metric - critical_floor
        if denom <= 0:
            raise ValueError(
                f"critical_floor ({critical_floor}) must be strictly lower than baseline_metric ({baseline_metric})"
            )
        drop = baseline_metric - current_metric
        norm_drop = min(1.0, max(0.0, drop / denom))
    else:
        denom = critical_floor - baseline_metric
        if denom <= 0:
            raise ValueError(
                f"critical_floor ({critical_floor}) must be strictly higher than baseline_metric ({baseline_metric})"
            )
        drop = current_metric - baseline_metric
        norm_drop = min(1.0, max(0.0, drop / denom))

    m_score = 100.0 * norm_drop
    return round(float(min(100.0, max(0.0, m_score))), 2)


# ──────────────────────────────────────────────────────────────────────────────
# COMPOSITE SEVERITY ENGINE
# ──────────────────────────────────────────────────────────────────────────────

def compute_severity_score(
    component_d: Optional[float] = None,
    component_p: Optional[float] = None,
    component_q: Optional[float] = None,
    component_m: Optional[float] = None,
    config: Optional[SeverityConfig] = None,
    sample_count: Optional[int] = None,
    evidence_counts: Optional[Dict[str, int]] = None,
) -> SeverityResult:
    """Compute overall ModelPulse deterministic composite severity score [0, 100].

    Aggregates available components using dynamic weight renormalisation:
        Severity = sum_{c in available}(W_c_eff * C_c)
        where W_c_eff = W_c / sum_{k in available}(W_k)

    Strict constraints:
    - Never substitutes missing components with zero.
    - Effective weights sum to 1.0 when any component is available.
    - Deterministic and monotonic: increasing any component value never decreases score.
    - Maps into explainable bands: Low (0-24), Moderate (25-49), High (50-74), Critical (75-100).
    """
    cfg = config or SeverityConfig()
    raw_components: Dict[str, Optional[float]] = {
        "D": component_d,
        "P": component_p,
        "Q": component_q,
        "M": component_m,
    }

    # Filter available (finite, non-null) components
    available: Dict[str, float] = {}
    for k, v in raw_components.items():
        if v is not None and not math.isnan(v):
            available[k] = float(v)

    # Base thresholds snapshot
    thresholds_dict: Dict[str, float] = {
        "psi_crit": cfg.psi_crit,
        "delta_ppr_crit": cfg.delta_ppr_crit,
        "q_max": cfg.q_max,
        "m_critical_floor": cfg.m_critical_floor,
    }

    counts_dict: Dict[str, int] = {
        "sample_count": sample_count if sample_count is not None else 0,
        "available_components": len(available),
        **(evidence_counts or {}),
    }

    # CASE 1: No components available
    if len(available) == 0:
        return SeverityResult(
            score=0,
            raw_score=0.0,
            band="Low",
            confidence="unavailable",
            components={"D": None, "P": None, "Q": None, "M": None},
            weights_used={"D": 0.0, "P": 0.0, "Q": 0.0, "M": 0.0},
            original_weights=dict(cfg.weights),
            thresholds=thresholds_dict,
            evidence_counts=counts_dict,
            status="no_evidence",
            explanation="No monitoring components (D, P, Q, M) are currently available. Severity cannot be evaluated.",
            is_heuristic=True,
        )

    # CASE 2: At least one component available -> Dynamic Weight Renormalisation
    w_avail_sum = sum(cfg.weights[k] for k in available.keys())

    weights_used: Dict[str, float] = {}
    if w_avail_sum == 0:
        # Edge case: All available components had 0 configured weight; apply uniform weighting
        uniform_weight = 1.0 / len(available)
        for k in ["D", "P", "Q", "M"]:
            weights_used[k] = uniform_weight if k in available else 0.0
    else:
        # Renormalize active weights
        for k in ["D", "P", "Q", "M"]:
            if k in available:
                weights_used[k] = cfg.weights[k] / w_avail_sum
            else:
                weights_used[k] = 0.0

    # Ensure effective active weights sum exactly to 1.0
    active_weight_sum = sum(weights_used[k] for k in available.keys())
    if active_weight_sum > 0:
        for k in available.keys():
            weights_used[k] = round(weights_used[k] / active_weight_sum, 6)
        # Fix rounding epsilon to guarantee exact 1.0
        first_k = next(iter(available.keys()))
        weights_used[first_k] += round(1.0 - sum(weights_used.values()), 6)

    # Deterministic Composite Score Calculation
    raw_composite = sum(weights_used[k] * available[k] for k in available.keys())
    raw_composite = min(100.0, max(0.0, raw_composite))
    integer_score = int(round(raw_composite))
    integer_score = min(100, max(0, integer_score))

    # Determine Severity Band
    band = "Low"
    for b_name, (b_low, b_high) in cfg.bands.items():
        if b_low <= integer_score <= b_high:
            band = b_name
            break
    else:
        if integer_score >= 75:
            band = "Critical"

    # Determine Confidence and Status
    is_low_sample = sample_count is not None and sample_count < cfg.min_samples_threshold
    m_missing = "M" not in available

    if is_low_sample:
        confidence = "low_sample"
    elif m_missing:
        confidence = "degraded"
    else:
        confidence = "normal"

    if available.get("Q", 0.0) >= 75.0:
        status = "quality_failure"
    elif m_missing:
        status = "degraded"
    else:
        status = "ok"

    # Human-Readable Explainability Breakdown
    active_comp_str = ", ".join(
        f"{k}={available[k]:.1f} (eff_weight={weights_used[k]:.3f})" for k in sorted(available.keys())
    )
    m_note = "Labels absent: M omitted with automatic weight renormalisation. " if m_missing else ""
    explanation = (
        f"Composite severity score {integer_score}/100 ({band}). {m_note}"
        f"Available components: [{active_comp_str}]. {HEURISTIC_DISCLAIMER} {CORRELATION_NOTICE}"
    )

    return SeverityResult(
        score=integer_score,
        raw_score=round(raw_composite, 4),
        band=band,
        confidence=confidence,
        components=raw_components,
        weights_used=weights_used,
        original_weights=dict(cfg.weights),
        thresholds=thresholds_dict,
        evidence_counts=counts_dict,
        status=status,
        explanation=explanation,
        is_heuristic=True,
    )


def evaluate_severity(
    feature_results: Optional[List[FeatureDriftResult]] = None,
    feature_importances: Optional[Dict[str, float]] = None,
    pred_score_psi: Optional[float] = None,
    ppr_reference: Optional[float] = None,
    ppr_current: Optional[float] = None,
    quality_failure_rate: Optional[float] = None,
    invalid_record_count: Optional[int] = None,
    total_record_count: Optional[int] = None,
    baseline_metric: Optional[float] = None,
    current_metric: Optional[float] = None,
    config: Optional[SeverityConfig] = None,
    sample_count: Optional[int] = None,
) -> SeverityResult:
    """Convenience pipeline to compute individual components and composite severity."""
    cfg = config or SeverityConfig()

    comp_d = compute_component_d(
        feature_results=feature_results,
        feature_importances=feature_importances,
        psi_crit=cfg.psi_crit,
    )
    comp_p = compute_component_p(
        pred_score_psi=pred_score_psi,
        ppr_reference=ppr_reference,
        ppr_current=ppr_current,
        psi_crit=cfg.psi_crit,
        delta_ppr_crit=cfg.delta_ppr_crit,
    )
    comp_q = compute_component_q(
        quality_failure_rate=quality_failure_rate,
        invalid_record_count=invalid_record_count,
        total_record_count=total_record_count,
        q_max=cfg.q_max,
    )
    comp_m = compute_component_m(
        baseline_metric=baseline_metric,
        current_metric=current_metric,
        critical_floor=cfg.m_critical_floor,
    )

    return compute_severity_score(
        component_d=comp_d,
        component_p=comp_p,
        component_q=comp_q,
        component_m=comp_m,
        config=cfg,
        sample_count=sample_count,
    )
