resource "google_storage_bucket" "dataflow_temp" {
  name          = var.dataflow_temp_bucket
  location      = var.region
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 3 }
    action { type = "Delete" }
  }

  labels = { env = var.env, purpose = "dataflow-temp" }
}
