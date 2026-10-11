# Reference Repositories

This directory contains the upstream reference codebases studied and evaluated for the **ModelPulse** project during Phase 0:

1. **`references/Sentinel/`**
   - Source: [Debddj/Sentinel](https://github.com/Debddj/Sentinel)
   - License: MIT License (see `references/Sentinel/LICENSE`)
   - Purpose: Design reference for SHAP attribution, $\Delta\text{SHAP}$ top-movers, EWMA adaptive thresholds, and drift type classification.
   - Note: Retains its full independent Git history.

2. **`references/mlops-drift-detection-platform/`**
   - Source: [faheemahmad02042019/mlops-drift-detection-platform](https://github.com/faheemahmad02042019/mlops-drift-detection-platform)
   - License: MIT License (see `references/mlops-drift-detection-platform/LICENSE`)
   - Purpose: Design reference for statistical drift tests (PSI, KS, Wasserstein), Streamlit dashboard patterns, and champion/challenger retraining flows.
   - Note: Retains its full independent Git history.

Both reference repositories are kept strictly isolated and must not be modified or directly imported by ModelPulse core production code. Any algorithms adapted from these repositories adhere to the MIT license terms.
