# SalesServiceHub

A near real-time data pipeline for Guidewire insurance systems (PolicyCenter, ClaimCenter, BillingCenter) on Google Cloud. It simulates policy, claim, and billing events, streams them through validation and enrichment, lands them in BigQuery, computes windowed aggregates and alerts, and serves the results via a REST API.

## Architecture

```
Guidewire (PolicyCenter / ClaimCenter / BillingCenter)
        │  simulated by ingestion/event_generator
        ▼
   Pub/Sub topics (pc-policy-events, cc-claim-events, bc-billing-events)
        │
        ▼
   Apache Beam pipeline (Dataflow or DirectRunner)
   parse → validate → enrich → strip internal fields
        │                              │
        ▼                              ▼
   BigQuery raw.*_events        5-min tumbling windows
                                        │
                                        ├─► BigQuery enriched.*_5min
                                        └─► alert rules → raw.alerts
        │
        ▼
   BigQuery data_products views (policy performance, claims exposure,
   billing health, delinquency watchlist, active alerts, ...)
        │
        ▼
   FastAPI serving layer (Cloud Run) — REST endpoints over the data products
```

## Repository layout

| Path | Purpose |
|---|---|
| `ingestion/event_generator/` | Simulates Guidewire events and publishes them to Pub/Sub (or stdout via `--dry-run`) |
| `ingestion/schemas/` | JSON Schema (draft-07) contracts for policy/claim/billing event payloads |
| `processing/pipeline.py` | Apache Beam streaming pipeline: parse, validate, enrich, DLQ handling, windowed aggregation, alerting |
| `processing/alert_rules.py` | Threshold-based alert evaluators (cancellation rate, lapse spikes, CAT events, delinquency, etc.) |
| `serving/api.py` | FastAPI REST API exposing BigQuery-backed data products |
| `storage/raw/`, `storage/enriched/` | BigQuery table schemas for raw events and windowed aggregates |
| `storage/data_products/` | SQL view definitions consumed by Terraform |
| `terraform/` | GCP infrastructure: Pub/Sub, BigQuery, Cloud Run, GCS, IAM |
| `docker/` | Dockerfiles for the API (`Dockerfile.api`) and event generator (`Dockerfile.generator`) |
| `tests/` | Unit tests for the generator and pipeline |

## Tech stack

- **Apache Beam** (Dataflow-compatible) for stream processing
- **Google Cloud Pub/Sub** for event ingestion
- **Google BigQuery** for raw storage, aggregates, and data product views
- **FastAPI** + **Uvicorn** for the serving API, deployed on **Cloud Run**
- **Terraform** for infrastructure provisioning

## Getting started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Provision infrastructure

```bash
cd terraform
# edit terraform.tfvars and set your project_id, region, env
terraform init
terraform plan
terraform apply
```

### 3. Run the event generator

```bash
# Local dry run, no GCP required
python ingestion/event_generator/generator.py --dry-run

# Publish to Pub/Sub
python ingestion/event_generator/generator.py --project <gcp-project> --rate 10 --duration 60
```

### 4. Run the pipeline

```bash
# Local, no Dataflow
python processing/pipeline.py --project dummy --runner DirectRunner --dry-run

# On Dataflow
python processing/pipeline.py \
  --project <gcp-project> \
  --region us-central1 \
  --runner DataflowRunner \
  --temp-location gs://<dataflow-temp-bucket>/tmp \
  --service-account-email ssh-dataflow-pipeline@<gcp-project>.iam.gserviceaccount.com
```

### 5. Run the API

```bash
export GCP_PROJECT=<gcp-project>
uvicorn serving.api:app --reload --port 8080
```

Endpoints: `/health`, `/policies/active`, `/policies/performance`, `/claims/open`, `/claims/exposure`, `/billing/health`, `/billing/delinquencies`, `/alerts`.

### 6. Run tests

```bash
pytest
```

## Notes

- `terraform/terraform.tfvars` is gitignored — it holds your local GCP project ID and is not committed.
