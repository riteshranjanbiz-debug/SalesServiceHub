# Deployment guide

How `salesservicehub` (dev) was actually stood up, written so the same steps
can be repeated for `prod` or any new environment. Every command shown was
run in order to build the current live system.

## 1. GCP project

```bash
gcloud projects create <project-id> --name="<display name>"
gcloud billing projects link <project-id> --billing-account=<billing-account-id>
gcloud services enable \
  pubsub.googleapis.com bigquery.googleapis.com dataflow.googleapis.com \
  run.googleapis.com iam.googleapis.com storage.googleapis.com \
  containerregistry.googleapis.com artifactregistry.googleapis.com \
  cloudresourcemanager.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  --project <project-id>
```

## 2. Terraform state bucket

```bash
gcloud storage buckets create gs://<project-id>-tfstate \
  --project=<project-id> --location=us-central1 --uniform-bucket-level-access
gcloud storage buckets update gs://<project-id>-tfstate --versioning
```

## 3. Workload Identity Federation (for GitHub Actions)

```bash
PROJECT_NUMBER=$(gcloud projects describe <project-id> --format="value(projectNumber)")

gcloud iam workload-identity-pools create "github-actions-pool" \
  --project=<project-id> --location=global --display-name="GitHub Actions Pool"

gcloud iam workload-identity-pools providers create-oidc "github-actions-provider" \
  --project=<project-id> --location=global --workload-identity-pool="github-actions-pool" \
  --display-name="GitHub Actions Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository=='riteshranjanbiz-debug/SalesServiceHub'" \
  --issuer-uri="https://token.actions.githubusercontent.com"

gcloud iam service-accounts create github-actions-deployer \
  --project=<project-id> --display-name="GitHub Actions Deployer"

SA="github-actions-deployer@<project-id>.iam.gserviceaccount.com"
for ROLE in roles/pubsub.admin roles/bigquery.admin roles/storage.admin roles/run.admin \
            roles/iam.serviceAccountAdmin roles/iam.serviceAccountUser \
            roles/resourcemanager.projectIamAdmin roles/artifactregistry.writer \
            roles/artifactregistry.createOnPushWriter; do
  gcloud projects add-iam-policy-binding <project-id> \
    --member="serviceAccount:${SA}" --role="${ROLE}" --condition=None
done

gcloud iam service-accounts add-iam-policy-binding "${SA}" \
  --project=<project-id> --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-actions-pool/attribute.repository/riteshranjanbiz-debug/SalesServiceHub"
```

The `attribute-condition` on the provider means **only this exact repo** can
ever assume this identity — that's the actual security boundary, not secrecy
of the provider name (which isn't secret; it's stored as a plain GitHub repo
variable, see below).

## 4. Local Terraform config

```bash
cd terraform/envs/<env>   # dev or prod
cp backend.hcl.example backend.hcl        # fill in bucket = "<project-id>-tfstate"
cp terraform.tfvars.example terraform.tfvars   # fill in project_id, region, dataflow_temp_bucket, api_invokers
terraform init -backend-config=backend.hcl
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

The first `apply` will fail creating the Cloud Run service — the image
doesn't exist in Artifact Registry yet. Build and push it first
(`docker build -f docker/Dockerfile.api -t gcr.io/<project-id>/ssh-insurance-api:latest .`
then `docker push`), then `apply` again. After that, CD takes over.

## 5. GitHub repo variables

Non-secret — visible to anyone with read access to the repo, which is fine
since none of it grants access on its own (the WIF attribute condition does):

```bash
gh variable set GCP_PROJECT_ID --body "<project-id>"
gh variable set GCP_REGION --body "us-central1"
gh variable set GCP_DATAFLOW_TEMP_BUCKET --body "<bucket-name>"
gh variable set GCP_TFSTATE_BUCKET --body "<project-id>-tfstate"
gh variable set GCP_API_INVOKER --body "user:<your-email>"
gh variable set GCP_DEPLOYER_SA --body "github-actions-deployer@<project-id>.iam.gserviceaccount.com"
gh variable set GCP_WORKLOAD_IDENTITY_PROVIDER \
  --body "projects/<project-number>/locations/global/workloadIdentityPools/github-actions-pool/providers/github-actions-provider"
```

## 6. Standing up prod specifically

`.github/workflows/cd.yml` only targets `terraform/envs/dev` today. To add
prod:

1. Repeat steps 1–5 above for a new `<prod-project-id>`.
2. Copy `cd.yml` to a `cd-prod.yml` (or parameterize the existing one) pointing
   at `terraform/envs/prod` and a `GCP_*_PROD` set of repo variables.
3. Strongly consider a manual-approval gate here (a GitHub Environment with
   required reviewers) — `cd.yml`'s dev deploy currently has none, which is a
   deliberate tradeoff for a dev environment that shouldn't be repeated for
   prod. See `docs/architecture.md`, "What's intentionally not built yet."
