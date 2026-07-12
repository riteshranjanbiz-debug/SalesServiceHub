-- Data Product: Policy Performance (rolling 24h, 5-min granularity)
-- Source: enriched.policy_summary_5min
-- Consumers: underwriting dashboards, producer scorecards

SELECT
  window_start,
  window_end,
  event_type,
  line_of_business,
  state,
  event_count,
  total_written_premium,
  cancellation_count,
  SAFE_DIVIDE(cancellation_count, event_count)            AS cancellation_rate,
  SUM(event_count)        OVER (PARTITION BY event_type, line_of_business
                                ORDER BY window_start
                                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_count,
  SUM(total_written_premium) OVER (PARTITION BY line_of_business
                                   ORDER BY window_start
                                   ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_premium,
  computed_at
FROM `${project_id}.enriched.policy_summary_5min`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
