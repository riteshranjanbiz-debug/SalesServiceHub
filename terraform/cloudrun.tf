resource "google_cloud_run_v2_service" "api" {
  name     = "ssh-insurance-api"
  location = var.region
  project  = var.project_id

  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.api_sa.email

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }

    containers {
      image = "gcr.io/${var.project_id}/ssh-insurance-api:latest"

      ports {
        container_port = 8080
      }

      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }

      env {
        name  = "BQ_LOCATION"
        value = var.region
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
  }

  labels = { env = var.env, component = "api" }

  depends_on = [google_project_iam_member.api_bq_reader]
}

# ── API Service Account ────────────────────────────────────────────────────────

resource "google_service_account" "api_sa" {
  account_id   = "ssh-insurance-api"
  display_name = "SalesServiceHub API Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "api_bq_reader" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.api_sa.email}"
}

resource "google_project_iam_member" "api_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.api_sa.email}"
}

# ── Public invoker (remove for internal-only deployment) ──────────────────────

resource "google_cloud_run_v2_service_iam_member" "api_public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# ── Output ─────────────────────────────────────────────────────────────────────

output "api_url" {
  value       = google_cloud_run_v2_service.api.uri
  description = "Insurance Data Products API URL"
}
