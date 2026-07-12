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
