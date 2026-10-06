# Deploying `acc-asset-file-metadata` to an AWS account

`.github/workflows/glue-acc-asset-file-metadata.yml` checks and deploys
`glue/acc_asset_file_metadata.py`:

| Event | What runs |
|---|---|
| Pull request touching the job | check: compile under Python 3.9, ruff, shellcheck |
| Push to `main` touching the job | check, then deploy to the **`dev`** Environment |
| *Actions → Run workflow* | check, then deploy to the Environment you pick |

**One GitHub Environment is one AWS account.** The workflow has no account
details in it. It reads them all from the Environment's variables, so adding an
account means adding an Environment. GitHub gets into AWS through OIDC, so
there are no AWS keys to store or rotate.

Each deploy uploads the script to
`s3://<CODE_BUCKET>/<CODE_PREFIX>/<JOB_NAME>/<git sha>/acc_asset_file_metadata.py`
and points the job at it. Uploads are never overwritten, so to roll back you
re-run the workflow at an older commit.

## Adding an account (once per account)

### 1. Prerequisites in the account

- A bucket for the script (`CODE_BUCKET`) and a bucket for the CSV (`DATA_BUCKET`).
  They can be the same bucket.
- The APS secret, in the same shape as the other accounts (see `make_secret.py`).
  The secret stays out of GitHub and out of this pipeline.

### 2. Create the two roles

```bash
aws cloudformation deploy \
  --stack-name acc-asset-file-metadata-deploy \
  --template-file glue/deploy/bootstrap-account.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      GitHubEnvironment=prod \
      CodeBucket=my-glue-scripts \
      DataBucket=my-acc-bucket \
      SecretName=aps/acc-extract

aws cloudformation describe-stacks --stack-name acc-asset-file-metadata-deploy \
  --query 'Stacks[0].Outputs' --output table
```

If the account already has a `token.actions.githubusercontent.com` OIDC provider,
add `CreateOidcProvider=false`. An account can hold only one.

The stack creates:

- **`github-deploy-acc-asset-file-metadata-<env>`**: the role GitHub assumes.
  Only workflow runs bound to that one GitHub Environment of this repo can
  assume it. It can upload the script, get/create/update this one job, and
  pass the job role to Glue.
- **`AWSGlueServiceRole-acc-asset-file-metadata`**: the role the job runs as.
  It can read the secret, write under `<prefix>/_metadata/`, and read its own
  script. This is narrower than the shared role in `IAM_SETUP.md`, because this
  job never reads data back.

If you already run the job under the role from `IAM_SETUP.md`, you can use that
role's ARN for `GLUE_ROLE_ARN` instead. The deploy role's `iam:PassRole` then has
to name that role.

### 3. Create the GitHub Environment

*Repo → Settings → Environments → New environment* (for example `prod`). Use the
same name you gave `GitHubEnvironment`. Then add **variables** (not secrets,
since none of these values are sensitive):

| Variable | Required | Value |
|---|---|---|
| `AWS_DEPLOY_ROLE_ARN` | yes | stack output `DeployRoleArn` |
| `AWS_REGION` | yes | for example `us-east-1` |
| `GLUE_ROLE_ARN` | yes | stack output `GlueJobRoleArn` |
| `CODE_BUCKET` | yes | script bucket |
| `DATA_BUCKET` | yes | CSV bucket, passed as `--s3_bucket` |
| `SECRET_NAME` | yes | passed as `--secret_name` |
| `JOB_NAME` | no | default `acc-asset-file-metadata` |
| `CODE_PREFIX` | no | default `glue-scripts` (must match the stack) |
| `S3_PREFIX` | no | default `acc/assets`, passed as `--s3_prefix` |
| `TIMEOUT_MINUTES` | no | default `180` |
| `EXTRA_ARGS_JSON` | no | extra job arguments for this account, for example `{"--hub_id":"b.xxx","--split_by_project":"true"}` |

### Loading into Snowflake

Every run also loads its rows into a Snowflake table. The credentials are read
from the Secrets Manager secret `snowflake/acc-loader`; the name is fixed in the
script (`SNOWFLAKE_SECRET_NAME`), not passed as a job parameter. Without that
secret the job fails before it starts the sweep. To set up an account:

1. Store the Snowflake credentials as a secret named `snowflake/acc-loader`.
   Key-pair auth is preferred; `password` works in place of `private_key`:
   ```json
   {"account": "xy12345.us-east-1", "user": "SVC_ACC_LOADER",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...", "role": "ACC_LOADER",
    "warehouse": "LOAD_WH", "database": "RAW", "schema": "ACC"}
   ```
   ```bash
   aws secretsmanager create-secret --name snowflake/acc-loader \
       --secret-string file://snowflake.json && rm snowflake.json
   ```
2. Re-deploy the bootstrap stack so the job role can read that secret.
3. Optionally, add overrides to the Environment's `EXTRA_ARGS_JSON`:
   `--snowflake_database`, `--snowflake_schema`, `--snowflake_table` and
   `--snowflake_mode`. For example, `{"--snowflake_mode":"history"}`:
   - `replace` (default): the table holds the latest sweep of each project.
   - `append`: every run's full set of rows, tagged by `RUN_ID`.
   - `history`: one row per version, written only when something changed, with
     `VALID_FROM`/`VALID_TO`/`IS_CURRENT`/`IS_DELETED` and a `<table>_CURRENT`
     view. Default table `ACC_ASSET_FILE_METADATA_HISTORY`.

On the Snowflake side, the role needs USAGE on the warehouse, database and
schema. It needs CREATE TABLE on the schema for the first run, or you can create
the table up front. After that it needs SELECT, INSERT and DELETE on the table.
`history` mode also needs UPDATE on the table, and CREATE TABLE on the schema
on every run, because each run stages its rows in a temporary table.
If the Snowflake account has a network policy, it must allow the job's egress
addresses. A Glue job with no VPC connection egresses from AWS's shared ranges,
so a strict policy needs the job on a VPC connection with a NAT gateway that
has a fixed IP.

To gate production, add **Required reviewers** and restrict **Deployment
branches** to `main` on that Environment. A deploy then waits for approval.

### 4. Deploy

*Actions → glue / acc-asset-file-metadata → Run workflow*, then pick the
Environment. The run fails unless the job ends up pointing at the new upload.

## Deploying from a laptop

The workflow calls a plain script, so you can deploy by hand with any credentials
for the target account:

```bash
export CODE_BUCKET=my-glue-scripts DATA_BUCKET=my-acc-bucket SECRET_NAME=aps/acc-extract \
       GLUE_ROLE_ARN=arn:aws:iam::123456789012:role/AWSGlueServiceRole-acc-asset-file-metadata
glue/deploy/deploy_glue_job.sh
```

## What this does not do

- **Run the job.** A deploy only changes the job definition, and nothing calls
  ACC. Start a run with `aws glue start-job-run --job-name acc-asset-file-metadata`.
- **Schedule it.** Add a Glue trigger separately if the job should run on a timer.
- **Deploy `acc_assets_to_s3.py`.** The deploy script is generic
  (`SCRIPT_PATH`, `JOB_NAME`), but the sibling job needs its own workflow and
  job role, because it needs `s3:GetObject`, `ListBucket` and multipart
  permissions.
