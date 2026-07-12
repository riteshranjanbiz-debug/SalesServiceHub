# ── Pub/Sub ───────────────────────────────────────────────────────────────────
output "pc_topic_id" { value = google_pubsub_topic.pc_policy_events.id }
output "cc_topic_id" { value = google_pubsub_topic.cc_claim_events.id }
output "bc_topic_id" { value = google_pubsub_topic.bc_billing_events.id }

output "pc_dlq_topic_id" { value = google_pubsub_topic.pc_policy_events_dlq.id }
output "cc_dlq_topic_id" { value = google_pubsub_topic.cc_claim_events_dlq.id }
output "bc_dlq_topic_id" { value = google_pubsub_topic.bc_billing_events_dlq.id }

# ── BigQuery ──────────────────────────────────────────────────────────────────
output "bq_raw_dataset" { value = google_bigquery_dataset.raw.dataset_id }
output "bq_enriched_dataset" { value = google_bigquery_dataset.enriched.dataset_id }
output "bq_data_products_dataset" { value = google_bigquery_dataset.data_products.dataset_id }

output "bq_raw_policy_table" { value = google_bigquery_table.raw_policy_events.table_id }
output "bq_raw_claim_table" { value = google_bigquery_table.raw_claim_events.table_id }
output "bq_raw_billing_table" { value = google_bigquery_table.raw_billing_events.table_id }

# ── Data Products ─────────────────────────────────────────────────────────────
output "dp_policy_performance" { value = google_bigquery_table.dp_policy_performance.table_id }
output "dp_active_policy_snapshot" { value = google_bigquery_table.dp_active_policy_snapshot.table_id }
output "dp_claims_exposure" { value = google_bigquery_table.dp_claims_exposure.table_id }
output "dp_open_claims_summary" { value = google_bigquery_table.dp_open_claims_summary.table_id }
output "dp_billing_health" { value = google_bigquery_table.dp_billing_health.table_id }
output "dp_delinquency_watchlist" { value = google_bigquery_table.dp_delinquency_watchlist.table_id }

# ── Infrastructure ────────────────────────────────────────────────────────────
output "dataflow_temp_bucket" { value = google_storage_bucket.dataflow_temp.name }
output "dataflow_sa_email" { value = google_service_account.dataflow_sa.email }
