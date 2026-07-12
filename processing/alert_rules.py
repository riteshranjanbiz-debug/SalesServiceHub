"""
Alert rule evaluators for each Guidewire domain.

Each function receives a windowed aggregate row (dict) and returns
a list of alert dicts (empty list = no alerts).

Called from both the Beam pipeline (streaming) and tests.
"""

import uuid
from datetime import datetime, timezone
from typing import Any

# ── Thresholds ─────────────────────────────────────────────────────────────────

PC_CANCELLATION_RATE_THRESHOLD = 0.20   # 20% cancellations in a 5-min window
PC_LAPSE_COUNT_THRESHOLD       = 5      # 5+ lapses in one window

CC_CAT_EVENT_THRESHOLD         = 1      # any CAT event triggers alert
CC_HIGH_RESERVE_THRESHOLD      = 50_000 # total reserves > $50k in one window
CC_PAID_RATIO_THRESHOLD        = 0.80   # paid-to-reserve > 80% — possible leakage

BC_FAILURE_RATE_THRESHOLD      = 0.25   # 25% payment failure rate in window
BC_DELINQUENCY_COUNT_THRESHOLD = 10     # 10+ delinquencies in one window


def _alert(alert_type: str, domain: str, severity: str,
           agg: dict[str, Any], metric_name: str,
           metric_value: float, threshold: float) -> dict[str, Any]:
    return {
        "alert_id":     str(uuid.uuid4()),
        "alert_type":   alert_type,
        "domain":       domain,
        "severity":     severity,
        "window_start": agg.get("window_start"),
        "window_end":   agg.get("window_end"),
        "metric_name":  metric_name,
        "metric_value": metric_value,
        "threshold":    threshold,
        "context": {
            "event_type":       agg.get("event_type"),
            "line_of_business": agg.get("line_of_business"),
            "state":            agg.get("state"),
            "event_count":      agg.get("event_count"),
        },
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }


# ── PolicyCenter alerts ────────────────────────────────────────────────────────

def evaluate_policy_alerts(agg: dict[str, Any]) -> list[dict[str, Any]]:
    alerts = []
    event_count        = agg.get("event_count") or 0
    cancellation_count = agg.get("cancellation_count") or 0
    event_type         = agg.get("event_type", "")

    if event_count > 0:
        cancel_rate = cancellation_count / event_count
        if cancel_rate >= PC_CANCELLATION_RATE_THRESHOLD:
            alerts.append(_alert(
                alert_type="high_cancellation_rate",
                domain="policy",
                severity="high" if cancel_rate >= 0.30 else "medium",
                agg=agg,
                metric_name="cancellation_rate",
                metric_value=round(cancel_rate, 4),
                threshold=PC_CANCELLATION_RATE_THRESHOLD,
            ))

    if event_type == "policy.lapsed" and event_count >= PC_LAPSE_COUNT_THRESHOLD:
        alerts.append(_alert(
            alert_type="lapse_spike",
            domain="policy",
            severity="medium",
            agg=agg,
            metric_name="lapse_count",
            metric_value=float(event_count),
            threshold=float(PC_LAPSE_COUNT_THRESHOLD),
        ))

    return alerts


# ── ClaimCenter alerts ─────────────────────────────────────────────────────────

def evaluate_claim_alerts(agg: dict[str, Any]) -> list[dict[str, Any]]:
    alerts = []
    cat_count     = agg.get("cat_event_count") or 0
    total_reserves = agg.get("total_reserves") or 0.0
    total_paid     = agg.get("total_paid") or 0.0
    event_count    = agg.get("event_count") or 0

    if cat_count >= CC_CAT_EVENT_THRESHOLD:
        alerts.append(_alert(
            alert_type="cat_event_spike",
            domain="claim",
            severity="critical",
            agg=agg,
            metric_name="cat_event_count",
            metric_value=float(cat_count),
            threshold=float(CC_CAT_EVENT_THRESHOLD),
        ))

    if total_reserves >= CC_HIGH_RESERVE_THRESHOLD:
        alerts.append(_alert(
            alert_type="high_reserve_window",
            domain="claim",
            severity="high",
            agg=agg,
            metric_name="total_reserves",
            metric_value=total_reserves,
            threshold=float(CC_HIGH_RESERVE_THRESHOLD),
        ))

    if total_reserves > 0:
        paid_ratio = total_paid / total_reserves
        if paid_ratio >= CC_PAID_RATIO_THRESHOLD and event_count >= 5:
            alerts.append(_alert(
                alert_type="high_paid_to_reserve_ratio",
                domain="claim",
                severity="medium",
                agg=agg,
                metric_name="paid_to_reserve_ratio",
                metric_value=round(paid_ratio, 4),
                threshold=CC_PAID_RATIO_THRESHOLD,
            ))

    return alerts


# ── BillingCenter alerts ───────────────────────────────────────────────────────

def evaluate_billing_alerts(agg: dict[str, Any]) -> list[dict[str, Any]]:
    alerts = []
    event_count     = agg.get("event_count") or 0
    failure_count   = agg.get("payment_failure_count") or 0
    delinq_count    = agg.get("delinquency_count") or 0

    if event_count > 0:
        failure_rate = failure_count / event_count
        if failure_rate >= BC_FAILURE_RATE_THRESHOLD:
            alerts.append(_alert(
                alert_type="payment_failure_spike",
                domain="billing",
                severity="high" if failure_rate >= 0.40 else "medium",
                agg=agg,
                metric_name="payment_failure_rate",
                metric_value=round(failure_rate, 4),
                threshold=BC_FAILURE_RATE_THRESHOLD,
            ))

    if delinq_count >= BC_DELINQUENCY_COUNT_THRESHOLD:
        alerts.append(_alert(
            alert_type="delinquency_spike",
            domain="billing",
            severity="high",
            agg=agg,
            metric_name="delinquency_count",
            metric_value=float(delinq_count),
            threshold=float(BC_DELINQUENCY_COUNT_THRESHOLD),
        ))

    return alerts
