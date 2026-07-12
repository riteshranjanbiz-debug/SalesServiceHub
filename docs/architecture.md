# Architecture decisions

## Data flow

Guidewire (PolicyCenter / ClaimCenter / BillingCenter) → Pub/Sub → Apache Beam
(parse → validate → enrich) → BigQuery raw tables, in parallel with a 5-minute
tumbling window → BigQuery enriched aggregates → threshold-based alert rules →
BigQuery `raw.alerts` → `data_products` views → FastAPI (Cloud Run) → dashboard.

Each domain (policy/claim/billing) has its own topic, DLQ topic, and raw table,
but shares the same pipeline shape. Bad messages (invalid JSON, missing
required envelope/payload fields) are routed to the domain's DLQ topic rather
than failing the pipeline or being silently dropped.

## Why these choices

**Apache Beam over a simpler consumer loop.** The windowed aggregation
(5-minute rollups per domain) and the DLQ branching are naturally expressed as
Beam transforms, and the same pipeline code runs unchanged on `DirectRunner`
(local testing) or `DataflowRunner` (production) — see
`processing/pipeline.py`'s `try/except` import guard, which also lets
`tests/test_pipeline.py` unit-test the `DoFn`s without a real Beam install.

**Terraform module + per-environment directories, not a single flat
`terraform/`.** `terraform/modules/salesservicehub/` holds every resource
definition; `terraform/envs/{dev,prod}/` each own their own state backend and
tfvars. A mistake in prod config can't touch dev's state, and standing up a
new environment is copying a directory, not restructuring resources. See
[deployment.md](deployment.md) for the mechanics.

**Cloud Run IAM lockdown by default, not a public endpoint.** The API/dashboard
serve real (simulated) insurance data from BigQuery. `allow_public_api_access`
defaults to `false`; callers are added explicitly via `api_invokers`. This was
a deliberate change from the initial scaffold, which had `allUsers` as
`roles/run.invoker` — see the "Prod-readiness" work in the project's history
for the reasoning about this class of decision.

**Workload Identity Federation over a service-account JSON key for CI/CD.**
GitHub Actions authenticates to GCP by exchanging its OIDC token for a
short-lived credential — no long-lived key sits in GitHub Secrets to leak or
rotate. The WIF provider's attribute condition
(`assertion.repository=='riteshranjanbiz-debug/SalesServiceHub'`) restricts
which repo can assume the deploy identity at all.

**The deployer service account has scoped roles, not `roles/editor`.** Each
granted role maps to a resource type Terraform actually manages:
`pubsub.admin`, `bigquery.admin`, `storage.admin`, `run.admin`,
`iam.serviceAccountAdmin`, `iam.serviceAccountUser`,
`resourcemanager.projectIamAdmin` (needed because Terraform creates
`google_project_iam_member` bindings), `artifactregistry.writer` +
`artifactregistry.createOnPushWriter` (needed because `gcr.io` pushes are now
backed by Artifact Registry — see [runbook.md](runbook.md)).

**The API image is tagged with the git SHA in Terraform, not `:latest`.**
Terraform diffs the `image` field as a string. If it's always `:latest`, a new
image push doesn't change that string, so Terraform sees no diff and never
rolls a new Cloud Run revision — the code silently never deploys. `api_image`
is passed in via `-var` at CD time as `gcr.io/<project>/ssh-insurance-api:<sha>`,
so every deploy is a real, detectable change.

## BigQuery clustering

Raw event tables are clustered on top-level envelope fields
(`event_type`, `domain`, `entity_type`) rather than nested `payload.*` fields —
BigQuery clustering only accepts top-level columns. See
[runbook.md](runbook.md) for how this surfaced.

## What's intentionally not built yet

- No prod GCP project exists — `terraform/envs/prod/` has only `.example`
  config files. Nothing is provisioned there.
- No manual-approval gate on `cd.yml` — every push to `main` deploys to dev
  automatically. Acceptable for a dev environment; would need a gate (or a
  separate protected `prod` workflow/environment) before treating any
  environment this pipeline touches as prod-facing.
- Dataflow is not continuously running — the pipeline is deployed by running
  `python -m processing.pipeline --runner DataflowRunner ...` manually; there's
  no Terraform resource or CD step that keeps a Dataflow job alive.
