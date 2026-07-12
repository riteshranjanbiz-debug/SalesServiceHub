# ═══════════════════════════════════════════════════════════════════════════════
# Data Products — PolicyCenter
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "dp_policy_performance" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "policy_performance"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/policy_performance.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, domain = "policy", layer = "product" }

  depends_on = [google_bigquery_table.enriched_policy_summary_5min]
}

resource "google_bigquery_table" "dp_active_policy_snapshot" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "active_policy_snapshot"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/active_policy_snapshot.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, domain = "policy", layer = "product" }

  depends_on = [google_bigquery_table.raw_policy_events]
}

# ═══════════════════════════════════════════════════════════════════════════════
# Data Products — ClaimCenter
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "dp_claims_exposure" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "claims_exposure"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/claims_exposure.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, domain = "claim", layer = "product" }

  depends_on = [google_bigquery_table.enriched_claim_activity_5min]
}

resource "google_bigquery_table" "dp_open_claims_summary" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "open_claims_summary"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/open_claims_summary.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, domain = "claim", layer = "product" }

  depends_on = [google_bigquery_table.raw_claim_events]
}

# ═══════════════════════════════════════════════════════════════════════════════
# Data Products — BillingCenter
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "dp_billing_health" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "billing_health"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/billing_health.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, domain = "billing", layer = "product" }

  depends_on = [google_bigquery_table.enriched_billing_summary_5min]
}

resource "google_bigquery_table" "dp_delinquency_watchlist" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "delinquency_watchlist"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/delinquency_watchlist.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, domain = "billing", layer = "product" }

  depends_on = [google_bigquery_table.raw_billing_events]
}
