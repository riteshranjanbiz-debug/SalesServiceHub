-- Data Product: Open Claims Summary
-- Latest state per claim from raw ClaimCenter events.
-- Consumers: adjuster workqueue, SLA monitoring, litigation watch

WITH ranked AS (
  SELECT
    payload.claim_number,
    payload.policy_number,
    payload.line_of_business,
    payload.loss_type,
    payload.coverage_type,
    payload.assigned_adjuster,
    payload.adjuster_team,
    payload.reserve_amount,
    payload.paid_to_date,
    payload.reserve_amount - payload.paid_to_date  AS open_reserve,
    payload.status,
    payload.catastrophe_code,
    payload.subrogation_flag,
    payload.litigation_flag,
    payload.loss_location.state                    AS loss_state,
    payload.loss_location.zip                      AS loss_zip,
    payload.loss_date,
    payload.reported_date,
    event_type                                     AS latest_event_type,
    timestamp                                      AS latest_event_time,
    ROW_NUMBER() OVER (
      PARTITION BY payload.claim_number
      ORDER BY timestamp DESC
    )                                              AS rn
  FROM `${project_id}.raw.claim_events`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    AND payload.claim_number IS NOT NULL
)
SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1
  AND status IN ('New', 'Open', 'Reopened')
