# SalesServiceHub

A near real-time data pipeline for Guidewire insurance systems (PolicyCenter, ClaimCenter, BillingCenter) on Google Cloud. It simulates policy, claim, and billing events, streams them through validation and enrichment, lands them in BigQuery, computes windowed aggregates and alerts, and serves the results via a REST API and an operations dashboard.

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
   FastAPI serving layer (Cloud Run) — REST endpoints + /dashboard
```

## Deployed environment

| | |
|---|---|
| GCP project | `salesservicehub` (account `ritesh.ranjan.biz@gmail.com`) |
| API / dashboard | Cloud Run service `ssh-insurance-api` — IAM-authenticated, no public access |
| Dashboard | `/dashboard` on the API — see [Viewing the dashboard](#viewing-the-dashboard) |
| CI/CD | GitHub Actions, deploys `terraform/envs/dev` on every push to `main` — see [CI/CD](#cicd) |
| A second GCP project, `salesservicehub-dev` (different account), was created early on and later deleted — `salesservicehub` is the only live one. |

### Viewing the dashboard

The API has no public/anonymous access (`allow_public_api_access = false`). To view `/dashboard` locally:

```bash
gcloud run services proxy ssh-insurance-api --region=us-central1 --project=salesservicehub --port=8080
```

Then open `http://localhost:8080/dashboard`. The proxy attaches your `gcloud` identity token to every request automatically — no manual token handling needed, as long as your account is listed in `api_invokers` (see `terraform/envs/dev/terraform.tfvars`).

To call the JSON endpoints directly instead:

```bash
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" https://ssh-insurance-api-psf7xy4caq-uc.a.run.app/alerts
```

## Repository layout

| Path | Purpose |
|---|---|
| `ingestion/event_generator/` | Simulates Guidewire events and publishes them to Pub/Sub (or stdout via `--dry-run`) |
| `ingestion/schemas/` | JSON Schema (draft-07) contracts for policy/claim/billing event payloads |
| `processing/pipeline.py` | Apache Beam streaming pipeline: parse, validate, enrich, DLQ handling, windowed aggregation, alerting |
| `processing/alert_rules.py` | Threshold-based alert evaluators (cancellation rate, lapse spikes, CAT events, delinquency, etc.) |
| `serving/api.py` | FastAPI REST API exposing BigQuery-backed data products, plus `/dashboard` |
| `serving/dashboard.html` | Self-contained (no external deps) operations dashboard, served at `/dashboard` |
| `storage/raw/`, `storage/enriched/` | BigQuery table schemas for raw events and windowed aggregates |
| `storage/data_products/` | SQL view definitions consumed by Terraform |
| `terraform/modules/salesservicehub/` | Reusable Terraform module: Pub/Sub, BigQuery, Cloud Run, GCS, IAM |
| `terraform/envs/dev/`, `terraform/envs/prod/` | Per-environment wiring — own state backend, own tfvars, isolated from each other |
| `docker/` | Dockerfiles for the API (`Dockerfile.api`) and event generator (`Dockerfile.generator`) |
| `.github/workflows/ci.yml` | Tests + `terraform fmt`/`validate` on every push/PR |
| `.github/workflows/cd.yml` | Builds/pushes the API image and deploys `envs/dev` on every push to `main` |
| `tests/` | Unit tests for the generator and pipeline |

## Tech stack

- **Apache Beam** (Dataflow-compatible) for stream processing
- **Google Cloud Pub/Sub** for event ingestion
- **Google BigQuery** for raw storage, aggregates, and data product views
- **FastAPI** + **Uvicorn** for the serving API and dashboard, deployed on **Cloud Run**
- **Terraform** for infrastructure provisioning (module + per-env state)
- **GitHub Actions + Workload Identity Federation** for CI/CD (no long-lived GCP keys)

## CI/CD

Two workflows, both triggered on push:

- **`ci.yml`** — runs `pytest` and `terraform fmt -check` / `validate` (no cloud credentials needed) on every push and PR.
- **`cd.yml`** — on push to `main`: authenticates to GCP via **Workload Identity Federation** (repo-scoped, no stored keys), builds and pushes the API image tagged with the commit SHA (not just `:latest` — Terraform needs a changing value to detect the image update and roll a new Cloud Run revision), then runs `terraform apply` against `envs/dev`.

Non-secret deploy config (project ID, region, state bucket, WIF provider, deployer service account, invoker) lives in **GitHub repo variables**, not secrets — none of it is sensitive on its own. `terraform.tfvars` / `backend.hcl` are gitignored and generated at CD runtime from those variables; for local work, copy the pattern from `terraform/envs/*/backend.hcl.example` and `envs/prod/terraform.tfvars.example`.

The deploy identity (`github-actions-deployer@salesservicehub.iam.gserviceaccount.com`) holds only the roles Terraform's resources actually need: `pubsub.admin`, `bigquery.admin`, `storage.admin`, `run.admin`, `iam.serviceAccountAdmin`, `iam.serviceAccountUser`, `resourcemanager.projectIamAdmin`, `artifactregistry.writer`, `artifactregistry.createOnPushWriter` — not `editor`/`owner`.

## Getting started (local)

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

`apache-beam` needs an older `setuptools` to build on some machines/Python versions:
```bash
pip install "setuptools<81" wheel
pip install --no-build-isolation -r requirements.txt
```

### 2. Provision infrastructure

```bash
cd terraform/envs/dev   # or envs/prod
# create backend.hcl and terraform.tfvars from the *.example files in this dir
terraform init -backend-config=backend.hcl
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

### 3. Run the event generator

```bash
# Local dry run, no GCP required
python ingestion/event_generator/generator.py --dry-run

# Publish to Pub/Sub
python ingestion/event_generator/generator.py --project <gcp-project> --rate 10 --duration 60
```

### 4. Run the pipeline

Run as a package module from the repo root (not `python processing/pipeline.py` directly — it uses absolute imports like `from processing.alert_rules import ...`):

```bash
# Local (DirectRunner) — still reads real Pub/Sub and writes real BigQuery
python -m processing.pipeline --project <gcp-project> --runner DirectRunner

# On Dataflow
python -m processing.pipeline \
  --project <gcp-project> \
  --region us-central1 \
  --runner DataflowRunner \
  --temp-location gs://<dataflow-temp-bucket>/tmp \
  --service-account-email ssh-dataflow-pipeline@<gcp-project>.iam.gserviceaccount.com
```

It's a streaming pipeline — it runs indefinitely until you stop it (`Ctrl-C` or `pkill -f processing.pipeline`).

### 5. Run the API

```bash
export GCP_PROJECT=<gcp-project>
export BQ_LOCATION=us-central1   # must match the region datasets were created in, default "US" will 404
uvicorn serving.api:app --reload --port 8080
```

Endpoints: `/health`, `/dashboard`, `/policies/active`, `/policies/performance`, `/claims/open`, `/claims/exposure`, `/billing/health`, `/billing/delinquencies`, `/alerts`.

### 6. Run tests

```bash
pytest
```

## Testing end-to-end with real data

1. Start the pipeline (step 4 above) in the background.
2. Run the generator for ~20s: `python ingestion/event_generator/generator.py --project <gcp-project> --rate 5 --duration 20`.
3. Check `raw.*_events` tables in BigQuery for the published rows — should land within seconds.
4. Wait for a 5-minute tumbling window boundary to pass (real wall-clock time — the DirectRunner watermark won't advance without a boundary crossing), then check `enriched.*_5min` and `raw.alerts`.
5. Query `data_products` views, or hit the API/dashboard, to see the aggregates and alerts.
6. Stop the pipeline process when done.

## Notes

- `terraform/envs/*/terraform.tfvars` and `backend.hcl` are gitignored — they hold real project/bucket names and are local-only (or generated at CD runtime).
- The Cloud Run API is IAM-locked by default (`allow_public_api_access = false`). Add callers to `api_invokers` in your tfvars rather than making it public.
