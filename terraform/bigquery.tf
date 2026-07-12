# ═══════════════════════════════════════════════════════════════════════════════
# Datasets
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_dataset" "raw" {
  dataset_id    = "raw"
  friendly_name = "Raw Events — Guidewire Landing Layer"
  description   = "1:1 events from PolicyCenter, ClaimCenter, BillingCenter as received"
  location      = var.region
  project       = var.project_id
  labels        = { env = var.env, layer = "raw" }
}

resource "google_bigquery_dataset" "enriched" {
  dataset_id    = "enriched"
  friendly_name = "Enriched — 5-min Windowed Aggregates"
  description   = "Stream-processed 5-minute window aggregates per Guidewire domain"
  location      = var.region
  project       = var.project_id
  labels        = { env = var.env, layer = "enriched" }
}

resource "google_bigquery_dataset" "data_products" {
  dataset_id    = "data_products"
  friendly_name = "Data Products — Insurance Intelligence"
  description   = "Business-facing views: policy performance, claims exposure, billing health"
  location      = var.region
  project       = var.project_id
  labels        = { env = var.env, layer = "product" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# Raw tables — PolicyCenter
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "raw_policy_events" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "policy_events"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../storage/raw/policy_events_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["event_type", "payload.line_of_business", "payload.state"]

  labels = { env = var.env, domain = "policy", source_system = "guidewire_pc" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# Raw tables — ClaimCenter
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "raw_claim_events" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "claim_events"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../storage/raw/claim_events_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["event_type", "payload.line_of_business", "payload.status"]

  labels = { env = var.env, domain = "claim", source_system = "guidewire_cc" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# Raw tables — BillingCenter
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "raw_billing_events" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "billing_events"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../storage/raw/billing_events_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["event_type", "payload.payment_plan", "payload.delinquency_reason"]

  labels = { env = var.env, domain = "billing", source_system = "guidewire_bc" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# Enriched tables — 5-min windowed aggregates
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_bigquery_table" "enriched_policy_summary_5min" {
  dataset_id          = google_bigquery_dataset.enriched.dataset_id
  table_id            = "policy_summary_5min"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../storage/enriched/policy_summary_5min_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  labels = { env = var.env, domain = "policy", layer = "enriched" }
}

resource "google_bigquery_table" "enriched_claim_activity_5min" {
  dataset_id          = google_bigquery_dataset.enriched.dataset_id
  table_id            = "claim_activity_5min"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../storage/enriched/claim_activity_5min_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  labels = { env = var.env, domain = "claim", layer = "enriched" }
}

resource "google_bigquery_table" "enriched_billing_summary_5min" {
  dataset_id          = google_bigquery_dataset.enriched.dataset_id
  table_id            = "billing_summary_5min"
  project             = var.project_id
  deletion_protection = false

  schema = file("${path.module}/../storage/enriched/billing_summary_5min_schema.json")

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  labels = { env = var.env, domain = "billing", layer = "enriched" }
}
