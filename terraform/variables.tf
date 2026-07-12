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
