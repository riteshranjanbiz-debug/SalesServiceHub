# ── Pub/Sub ───────────────────────────────────────────────────────────────────
output "pc_topic_id" { value = module.salesservicehub.pc_topic_id }
output "cc_topic_id" { value = module.salesservicehub.cc_topic_id }
output "bc_topic_id" { value = module.salesservicehub.bc_topic_id }

output "pc_dlq_topic_id" { value = module.salesservicehub.pc_dlq_topic_id }
output "cc_dlq_topic_id" { value = module.salesservicehub.cc_dlq_topic_id }
output "bc_dlq_topic_id" { value = module.salesservicehub.bc_dlq_topic_id }

# ── BigQuery ──────────────────────────────────────────────────────────────────
output "bq_raw_dataset" { value = module.salesservicehub.bq_raw_dataset }
output "bq_enriched_dataset" { value = module.salesservicehub.bq_enriched_dataset }
output "bq_data_products_dataset" { value = module.salesservicehub.bq_data_products_dataset }

output "bq_raw_policy_table" { value = module.salesservicehub.bq_raw_policy_table }
output "bq_raw_claim_table" { value = module.salesservicehub.bq_raw_claim_table }
output "bq_raw_billing_table" { value = module.salesservicehub.bq_raw_billing_table }

# ── Data Products ─────────────────────────────────────────────────────────────
output "dp_policy_performance" { value = module.salesservicehub.dp_policy_performance }
output "dp_active_policy_snapshot" { value = module.salesservicehub.dp_active_policy_snapshot }
output "dp_claims_exposure" { value = module.salesservicehub.dp_claims_exposure }
output "dp_open_claims_summary" { value = module.salesservicehub.dp_open_claims_summary }
output "dp_billing_health" { value = module.salesservicehub.dp_billing_health }
output "dp_delinquency_watchlist" { value = module.salesservicehub.dp_delinquency_watchlist }

# ── Infrastructure ────────────────────────────────────────────────────────────
output "dataflow_temp_bucket" { value = module.salesservicehub.dataflow_temp_bucket }
output "dataflow_sa_email" { value = module.salesservicehub.dataflow_sa_email }
output "api_url" { value = module.salesservicehub.api_url }
