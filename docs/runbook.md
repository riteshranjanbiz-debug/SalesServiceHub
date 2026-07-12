# Runbook

## Checking system health

```bash
# API + dashboard reachable
gcloud run services proxy ssh-insurance-api --region=us-central1 --project=salesservicehub --port=8080
curl -s http://localhost:8080/health

# Latest CI/CD run status
gh run list --limit 5

# Any messages stuck in a DLQ (should normally be empty)
for S in pc-policy-events-dlq-monitor-sub cc-claim-events-dlq-monitor-sub bc-billing-events-dlq-monitor-sub; do
  echo "=== $S ==="
  gcloud pubsub subscriptions pull "$S" --project=salesservicehub --auto-ack --limit=10
done

# Row counts, sanity check
bq query --project_id=salesservicehub --use_legacy_sql=false \
  "SELECT 'policy' d, COUNT(*) c FROM \`salesservicehub.raw.policy_events\`
   UNION ALL SELECT 'claim', COUNT(*) FROM \`salesservicehub.raw.claim_events\`
   UNION ALL SELECT 'billing', COUNT(*) FROM \`salesservicehub.raw.billing_events\`"
```

## Redeploying

Any push to `main` redeploys automatically (`.github/workflows/cd.yml`). To
force a redeploy without a code change (e.g. after an IAM fix), re-run the
workflow rather than pushing an empty commit:

```bash
gh run list --limit 1
gh run rerun <run-id>
```

## Rolling back

Cloud Run keeps prior revisions. To point traffic at a previous one:

```bash
gcloud run revisions list --service=ssh-insurance-api --region=us-central1 --project=salesservicehub
gcloud run services update-traffic ssh-insurance-api --region=us-central1 --project=salesservicehub \
  --to-revisions=<revision-name>=100
```

This is a traffic-routing change outside Terraform's view — the next
`terraform apply` will route traffic back to whatever revision matches the
current `api_image` var, so treat a manual rollback as temporary.

## Known issues we've hit (and their fixes)

These were all found by actually running the system for the first time, not
by inspection — useful context if something looks similar in the future.

| Symptom | Cause | Fix |
|---|---|---|
| `pytest` fails with `ImportError: cannot import name '_TaggedOutput'` only when `apache-beam` is actually installed | `_TaggedOutput` was only defined in the `except ImportError` fallback branch of `processing/pipeline.py`, never in the real-Beam branch | Alias `_TaggedOutput = beam.pvalue.TaggedOutput` in the `try` branch too |
| `docker push` to `gcr.io/...` fails: `denied: Permission 'artifactregistry.repositories.uploadArtifacts' denied` | `gcr.io` pushes on newer GCP projects are backed by Artifact Registry, not the legacy GCS-backed Container Registry | Grant `roles/artifactregistry.writer` to the pushing identity |
| Same push then fails: `gcr.io repo does not exist. Creating on push requires the artifactregistry.repositories.createOnPush permission` | The backing AR repo for `gcr.io` is created lazily on first push | Grant `roles/artifactregistry.createOnPushWriter` |
| `terraform apply` fails creating the Cloud Run service: `Permission 'iam.serviceaccounts.actAs' denied` | The deployer had `iam.serviceAccountAdmin` (manage SA objects) but not `iam.serviceAccountUser` (attach/impersonate an SA on a resource like Cloud Run) — these are separate permissions | Grant `roles/iam.serviceAccountUser` to the deployer |
| `terraform apply` fails on `google_bigquery_table`: `Fields specified for clustering can only be top-level fields. Invalid field: payload.line_of_business` | BigQuery clustering doesn't support nested `RECORD` fields | Cluster on top-level envelope fields (`event_type`, `domain`, `entity_type`) instead |
| A new image is pushed and CD reports success, but the API still serves old code | `google_cloud_run_v2_service.api`'s `image` field was hardcoded to `:latest` — Terraform diffs the string, sees no change, never rolls a new revision | Pass a SHA-tagged image via `-var="api_image=...:${{ github.sha }}"` at apply time |
| API returns `Internal Server Error` for any BigQuery-backed endpoint when run locally, but works fine on Cloud Run | Local run defaulted `BQ_LOCATION` to `US`, but the datasets live in `us-central1` — these are different BigQuery locations | Set `export BQ_LOCATION=us-central1` locally (Cloud Run already sets this correctly via Terraform) |
| `gh push` to `main` rejected: `refusing to allow an OAuth App to create or update workflow ... without workflow scope` | The `gh` CLI token was authenticated before `.github/workflows/` existed, without the `workflow` OAuth scope | `gh auth refresh -h github.com -s workflow`, then `gh auth setup-git` |
| `gcloud auth application-default login` repeatedly fails with `cloud-platform scope is required but not consented` | The browser had a different Google account already signed in and silently picked it, not the one intended — the consent screen wasn't actually shown for the right account | Retry with `--account=<intended-email>` explicitly, and verify with `curl` against the OAuth `userinfo` endpoint before assuming it worked |
| `pip install -r requirements.txt` fails building `apache-beam`: `ModuleNotFoundError: No module named 'pkg_resources'` | pip's build isolation fetches a fresh `setuptools` for the build step, and recent `setuptools` versions dropped `pkg_resources`; `apache-beam`'s legacy `setup.py` needs it | `pip install "setuptools<81" wheel`, then `pip install --no-build-isolation -r requirements.txt` |

## Testing the pipeline end-to-end with real data

See the README's "Testing end-to-end with real data" section. In short: run
`python -m processing.pipeline --runner DirectRunner` in the background, run
the generator for ~20s, check `raw.*` tables land within seconds, then wait
for a 5-minute window boundary before checking `enriched.*` and `raw.alerts`
— the DirectRunner's watermark won't advance past a window boundary without
either more incoming messages or enough wall-clock time passing.
