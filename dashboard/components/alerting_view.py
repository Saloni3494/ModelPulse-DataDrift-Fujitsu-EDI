"""ModelPulse Alerting & Notifications View (Phase 6).
Displays alert channel readiness, cooldown status, and recent delivery audit log.
"""
from typing import Any, Dict, List
import pandas as pd
import streamlit as st

from dashboard.components.styles import severity_badge, status_badge, render_df


def render_alerting_view(
    alert_status: Dict[str, Any],
    recent_alerts: List[Dict[str, Any]],
):
    """Render the alerting configuration and delivery history tab."""
    st.subheader("Notification Channels & Alert Dispatch")

    # 1. Channel Configuration Status Cards
    slack_cfg = alert_status.get("slack", {})
    email_cfg = alert_status.get("email", {})
    min_sev = alert_status.get("min_severity", "High")
    cooldown = alert_status.get("cooldown_seconds", 1800)

    c1, c2, c3 = st.columns(3)
    with c1:
        slack_ok = slack_cfg.get("configured", False)
        status_str = "Configured" if slack_ok else "Not Configured"
        st.metric(
            "Slack Webhook Channel",
            status_str,
            help="Configured via SLACK_WEBHOOK_URL environment variable.",
        )
        if slack_ok:
            st.caption(f"Webhook: `{slack_cfg.get('webhook_masked')}`")
        else:
            st.caption("Provide `SLACK_WEBHOOK_URL` in `.env` to enable.")

    with c2:
        email_ok = email_cfg.get("configured", False)
        status_str = "Configured" if email_ok else "Not Configured"
        st.metric(
            "Email (SMTP) Channel",
            status_str,
            help="Configured via SMTP_HOST and ALERT_RECIPIENT_EMAIL environment variables.",
        )
        if email_ok:
            st.caption(f"Host: `{email_cfg.get('smtp_host')}` | To: `{email_cfg.get('recipient')}`")
        else:
            st.caption("Provide `SMTP_HOST` & `ALERT_RECIPIENT_EMAIL` to enable.")

    with c3:
        st.metric("Alert Dispatch Policy", f"{min_sev}+ Severity")
        st.caption(f"Cooldown window: {cooldown // 60} minutes to suppress storms.")

    # 1.1 Aggregated Delivery Status Counters
    stats = alert_status.get("stats", {})
    if stats:
        st.markdown("##### Delivery Status Summary")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("Delivered", stats.get("delivered", 0), help="Confirmed successful delivery by provider.")
        with k2:
            st.metric("Suppressed (Cooldown)", stats.get("suppressed_cooldown", 0), help="Suppressed to prevent duplicate alert storms.")
        with k3:
            st.metric("Skipped (Disabled)", stats.get("skipped_disabled", 0), help="Channel credentials not configured.")
        with k4:
            st.metric("Failed", stats.get("failed", 0), help="Provider rejected or all retries exhausted.")

    st.markdown("---")

    # 2. Alert Dispatch History Table
    st.markdown("#### Recent Alert Dispatch Audit History")
    st.caption("ModelPulse records all alert delivery attempts, delivery confirmations, retries, and cooldown suppressions.")

    if recent_alerts:
        rows = []
        for a in recent_alerts:
            rows.append({
                "Alert ID": a.get("alert_id"),
                "Run ID": a.get("run_id"),
                "Channel": a.get("channel", "").upper(),
                "Severity": a.get("severity_band"),
                "Delivery Status": a.get("status"),
                "Timestamp (UTC)": str(a.get("sent_at")),
                "Retries": a.get("retry_count", 0),
                "Error / Diagnosis": a.get("error_message") or "Delivery confirmed (HTTP 200 / SMTP OK)",
            })
        render_df(pd.DataFrame(rows))
    else:
        st.info("No alert dispatch events recorded in this session.")

    # 3. Setup & Secrets Guidance
    st.markdown("---")
    st.markdown("#### Alert Configuration Runbook")
    with st.expander("How to configure live Slack & Email alerting"):
        st.markdown(
            """
            To activate live multi-channel alerting, define the following variables in `.env`:

            ```bash
            # Slack Incoming Webhook
            SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T00/B00/XXXXX"

            # SMTP Email Notifications
            SMTP_HOST="smtp.mailgun.org"
            SMTP_PORT="587"
            SMTP_USER="postmaster@yourdomain.com"
            SMTP_PASSWORD="your-smtp-password"
            ALERT_RECIPIENT_EMAIL="mlops-oncall@yourdomain.com"

            # Alert Policy
            ALERT_MIN_SEVERITY="High"        # 'Low', 'Moderate', 'High', 'Critical'
            ALERT_COOLDOWN_SECONDS="1800"   # 30-minute anti-storm deduplication
            ```

            *Security Note: Never commit secrets or webhooks to version control. ModelPulse reads credentials strictly from environment variables.*
            """
        )
