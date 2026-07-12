-- Data Product: Active Alerts (last 1 hour)
-- Consumers: ops dashboard, on-call engineers, Slack integrations

SELECT
  alert_id,
  alert_type,
  domain,
  severity,
  window_start,
  window_end,
  metric_name,
  metric_value,
  threshold,
  SAFE_DIVIDE(metric_value, NULLIF(threshold, 0)) - 1.0 AS pct_over_threshold,
  context.event_type,
  context.line_of_business,
  context.state,
  context.event_count,
  triggered_at,
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'high'     THEN 2
    WHEN 'medium'   THEN 3
    ELSE                 4
  END AS severity_rank
FROM `${project_id}.raw.alerts`
WHERE triggered_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY severity_rank, triggered_at DESC
