-- Data Product: Active Policy Snapshot
-- Latest known status per policy number from raw PolicyCenter events.
-- Consumers: policy search, endorsement workflows, billing joins

WITH ranked AS (
  SELECT
    payload.policy_number,
    payload.policy_type,
    payload.line_of_business,
    payload.account_number,
    payload.state,
    payload.status,
    payload.written_premium,
    payload.currency,
    payload.term_type,
    payload.effective_date,
    payload.expiration_date,
    payload.producer_code,
    payload.underwriting_company,
    payload.insured.name          AS insured_name,
    payload.insured.contact_public_id,
    event_type                    AS latest_event_type,
    timestamp                     AS latest_event_time,
    source,
    ROW_NUMBER() OVER (
      PARTITION BY payload.policy_number
      ORDER BY timestamp DESC
    )                             AS rn
  FROM `${project_id}.raw.policy_events`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND payload.policy_number IS NOT NULL
)
SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1
  AND status IN ('InForce', 'Bound', 'Quoted', 'Reinstated')
