variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "env" {
  description = "Environment: dev | staging | prod"
  type        = string
  default     = "dev"
}

variable "dataflow_temp_bucket" {
  description = "GCS bucket name for Dataflow temp files"
  type        = string
}

variable "allow_public_api_access" {
  description = "If true, grants roles/run.invoker to allUsers on the API service. Leave false in prod; authenticate callers via IAM instead."
  type        = bool
  default     = false
}

variable "api_invokers" {
  description = "IAM members (e.g. \"user:x@y.com\", \"serviceAccount:x@y.iam.gserviceaccount.com\", \"group:x@y.com\") granted roles/run.invoker on the API service."
  type        = list(string)
  default     = []
}

variable "api_image" {
  description = "Full image ref for the API Cloud Run service. Pass a digest/SHA tag (not :latest) so Terraform detects the change and deploys a new revision."
  type        = string
  default     = null
}

locals {
  api_image = coalesce(var.api_image, "gcr.io/${var.project_id}/ssh-insurance-api:latest")
}
