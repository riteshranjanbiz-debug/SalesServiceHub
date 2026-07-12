terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    # Bucket/prefix are supplied at init time via -backend-config=backend.hcl
    # (see backend.hcl.example).
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "salesservicehub" {
  source = "../../modules/salesservicehub"

  project_id              = var.project_id
  region                  = var.region
  env                     = var.env
  dataflow_temp_bucket    = var.dataflow_temp_bucket
  allow_public_api_access = var.allow_public_api_access
  api_invokers            = var.api_invokers
}
