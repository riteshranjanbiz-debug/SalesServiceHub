-- Data Product: Delinquency Watchlist
-- Billing accounts currently delinquent or with recent payment failures.
-- Consumers: collections team, cancellation workflow triggers, billing ops

WITH ranked AS (
  SELECT
    payload.billing_account_number,
    payload.policy_number,
    payload.days_past_due,
    payload.outstanding_balance,
    payload.delinquency_reason,
    payload.delinquency_workflow,
    payload.payment_plan,
    payload.payment_method,
    payload.failure_reason,
    event_type,
    timestamp                     AS latest_event_time,
    ROW_NUMBER() OVER (
      PARTITION BY payload.billing_account_number
      ORDER BY timestamp DESC
    )                             AS rn
  FROM `${project_id}.raw.billing_events`
  WHERE event_type IN (
      'delinquency.opened',
      'payment.failed',
      'payment.reversed',
      'policy.written_off'
    )
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND payload.billing_account_number IS NOT NULL
),
active_delinquencies AS (
  SELECT * EXCEPT (rn)
  FROM ranked
  WHERE rn = 1
    AND event_type != 'delinquency.closed'
)
SELECT
  *,
  CASE
    WHEN days_past_due >= 30 THEN 'CriticalRisk'
    WHEN days_past_due >= 15 THEN 'HighRisk'
    WHEN days_past_due >= 7  THEN 'MediumRisk'
    ELSE                          'LowRisk'
  END AS risk_tier
FROM active_delinquencies
ORDER BY days_past_due DESC, outstanding_balance DESC
