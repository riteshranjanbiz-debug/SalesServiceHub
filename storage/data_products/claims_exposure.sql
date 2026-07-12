-- Data Product: Claims Exposure (rolling 24h, 5-min granularity)
-- Source: enriched.claim_activity_5min
-- Consumers: claims management dashboard, CAT response team, actuarial

SELECT
  window_start,
  window_end,
  event_type,
  line_of_business,
  event_count,
  total_reserves,
  total_paid,
  cat_event_count,
  SAFE_DIVIDE(total_paid, NULLIF(total_reserves, 0))      AS paid_to_reserve_ratio,
  total_reserves - total_paid                              AS net_reserve_exposure,
  SUM(event_count)    OVER (PARTITION BY line_of_business
                            ORDER BY window_start
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_claims,
  SUM(total_reserves) OVER (PARTITION BY line_of_business
                            ORDER BY window_start
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_reserves,
  SUM(cat_event_count) OVER (ORDER BY window_start
                             ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_1h_cat_events,
  computed_at
FROM `${project_id}.enriched.claim_activity_5min`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
