# Service account for the Dataflow pipeline
resource "google_service_account" "dataflow_sa" {
  account_id   = "ssh-dataflow-pipeline"
  display_name = "SalesServiceHub Dataflow Pipeline SA"
  project      = var.project_id
}

# Dataflow worker role
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Read from Pub/Sub subscriptions
resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Publish to DLQ topics
resource "google_project_iam_member" "pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Write to BigQuery
resource "google_project_iam_member" "bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Read/write GCS temp bucket
resource "google_storage_bucket_iam_member" "dataflow_temp_rw" {
  bucket = google_storage_bucket.dataflow_temp.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}
