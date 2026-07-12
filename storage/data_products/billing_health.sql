-- Data Product: Billing Health (rolling 24h, 5-min granularity)
-- Source: enriched.billing_summary_5min
-- Consumers: billing ops dashboard, collections team, finance

SELECT
  window_start,
  window_end,
  event_type,
  event_count,
  total_amount_collected,
  total_amount_due,
  delinquency_count,
  payment_failure_count,
  SAFE_DIVIDE(total_amount_collected, NULLIF(total_amount_due, 0)) AS collection_rate,
  SAFE_DIVIDE(payment_failure_count,  NULLIF(event_count, 0))      AS failure_rate,
  SAFE_DIVIDE(delinquency_count,      NULLIF(event_count, 0))      AS delinquency_rate,
  SUM(total_amount_collected)  OVER (ORDER BY window_start
                                     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_collected,
  SUM(payment_failure_count)   OVER (ORDER BY window_start
                                     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_failures,
  SUM(delinquency_count)       OVER (ORDER BY window_start
                                     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_delinquencies,
  computed_at
FROM `${project_id}.enriched.billing_summary_5min`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
