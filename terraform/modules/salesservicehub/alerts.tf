# ── Alerts Pub/Sub topic ───────────────────────────────────────────────────────

resource "google_pubsub_topic" "insurance_alerts" {
  name                       = "insurance-alerts"
  project                    = var.project_id
  message_retention_duration = "86400s"
  labels                     = { env = var.env, purpose = "alerts" }
}

resource "google_pubsub_subscription" "insurance_alerts_push" {
  name    = "insurance-alerts-sub"
  topic   = google_pubsub_topic.insurance_alerts.name
  project = var.project_id

  ack_deadline_seconds       = 30
  message_retention_duration = "86400s"

  labels = { env = var.env, purpose = "alerts" }
}

# ── Alerts BigQuery table ──────────────────────────────────────────────────────

resource "google_bigquery_table" "raw_alerts" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "alerts"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../../../storage/raw/alerts_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "triggered_at"
  }

  clustering = ["domain", "severity", "alert_type"]

  labels = { env = var.env, purpose = "alerts" }
}

# ── Active alerts data product view ───────────────────────────────────────────

resource "google_bigquery_table" "dp_active_alerts" {
  dataset_id          = google_bigquery_dataset.data_products.dataset_id
  table_id            = "active_alerts"
  project             = var.project_id
  deletion_protection = false

  view {
    query = templatefile(
      "${path.module}/../../../storage/data_products/active_alerts.sql",
      { project_id = var.project_id }
    )
    use_legacy_sql = false
  }

  labels = { env = var.env, purpose = "alerts", layer = "product" }

  depends_on = [google_bigquery_table.raw_alerts]
}
