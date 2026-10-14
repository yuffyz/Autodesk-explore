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

- A bucket for the script (`AUTODESK_CODE_BUCKET`) and a bucket for the CSV (`AUTODESK_DATA_BUCKET`).
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
role's ARN for `AUTODESK_GLUE_ROLE_ARN` instead. The deploy role's `iam:PassRole` then has
to name that role.

### 3. Create the GitHub Environment

*Repo → Settings → Environments → New environment* (for example `prod`). Use the
same name you gave `GitHubEnvironment`. Then add **variables** (not secrets,
since none of these values are sensitive). Every name carries the `AUTODESK_`
prefix; the workflow passes each one to `deploy_glue_job.sh` without it, so
`AUTODESK_CODE_BUCKET` arrives as `CODE_BUCKET`:

| Variable | Required | Value |
|---|---|---|
| `AUTODESK_AWS_DEPLOY_ROLE_ARN` | yes | stack output `DeployRoleArn` |
| `AUTODESK_AWS_REGION` | yes | for example `us-east-1` |
| `AUTODESK_GLUE_ROLE_ARN` | yes | stack output `GlueJobRoleArn` |
| `AUTODESK_CODE_BUCKET` | yes | script bucket |
| `AUTODESK_DATA_BUCKET` | yes | CSV bucket, passed as `--s3_bucket` |
| `AUTODESK_SECRET_NAME` | yes | passed as `--secret_name` |
| `AUTODESK_JOB_NAME` | no | default `acc-asset-file-metadata` |
| `AUTODESK_CODE_PREFIX` | no | default `glue-scripts` (must match the stack) |
| `AUTODESK_S3_PREFIX` | no | default `acc/assets`, passed as `--s3_prefix` |
| `AUTODESK_TIMEOUT_MINUTES` | no | default `180` |
| `AUTODESK_EXTRA_ARGS_JSON` | no | extra job arguments for this account, for example `{"--hub_id":"b.xxx","--split_by_project":"true"}` |
| `AUTODESK_GLUE_CONNECTION` | no | Glue connection to run the job in, for a fixed egress IP: stack output `GlueConnectionName` (see below). Unset = no VPC; removing it detaches the job on the next deploy |

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
3. Optionally, add overrides to the Environment's `AUTODESK_EXTRA_ARGS_JSON`:
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
address. Without a VPC connection the job egresses from AWS's shared ranges,
which change, so put it on a fixed IP first (next section).

### Fixed egress IP for a Snowflake allowlist (optional, once per account)

`network-account.yaml` builds a small dedicated VPC for the job: a private
subnet whose only way out is a NAT gateway on an Elastic IP, plus the Glue
`NETWORK` connection the job attaches to. Everything the job calls on the
internet (Snowflake, APS, PyPI for `--additional-python-modules`, Secrets
Manager) then leaves from that one address. S3 goes through a gateway endpoint
instead, which is free, and S3 doesn't check source addresses.

1. Create the stack. The CloudFormation console works the same way as for step 2
   (*With new resources (standard)*). It needs no IAM acknowledgment.
   ```bash
   aws cloudformation deploy \
     --stack-name acc-asset-file-metadata-network \
     --template-file glue/deploy/network-account.yaml

   aws cloudformation describe-stacks --stack-name acc-asset-file-metadata-network \
     --query 'Stacks[0].Outputs' --output table
   ```
   The default VPC range is `10.250.0.0/24`. Override `VpcCidr`,
   `PublicSubnetCidr` and `PrivateSubnetCidr` if that clashes with a network you
   may peer with.
2. Re-deploy the bootstrap stack (step 2) so the deploy role can read the
   connection (`glue:GetConnection` on `<JobName>-*`).
3. Allowlist the `NatPublicIp` output in Snowflake, as `<ip>/32`. For example:
   ```sql
   CREATE NETWORK RULE acc_loader_egress MODE = INGRESS TYPE = IPV4
     VALUE_LIST = ('203.0.113.10/32');
   CREATE NETWORK POLICY acc_loader ALLOWED_NETWORK_RULE_LIST = ('acc_loader_egress');
   ALTER USER SVC_ACC_LOADER SET NETWORK_POLICY = acc_loader;
   ```
   A policy on the service user alone leaves everyone else's access unchanged.
   If an account-level policy already exists, add the IP to it instead.
4. Set `AUTODESK_GLUE_CONNECTION` on the Environment to the `GlueConnectionName`
   output, and deploy. The deploy fails before uploading anything if the
   connection doesn't exist, and afterwards checks that the job is attached.

Notes:

- **Cost:** a NAT gateway costs about $33 a month plus $0.045 per GB processed,
  even when the job isn't running. The Elastic IP is about $3.60 a month.
- **The IP survives the stack.** The Elastic IP is retained if the stack is
  deleted or replaced, so the Snowflake rule isn't silently broken. It then
  stays in the account (and on the bill) until released by hand. A *new*
  stack allocates a new IP.
- **Single AZ.** A Glue connection names one subnet, so the job runs in one
  AZ. If that AZ is down, the run fails and can be re-run later.
- **An existing VPC works too.** If the account already has a private subnet
  behind a NAT gateway with a fixed IP, create a Glue `NETWORK` connection in
  it instead. Its security group needs a self-referencing inbound rule for all
  TCP, and the subnet needs a route to S3. Name the connection `<JobName>-…`,
  or widen `glue:GetConnection` in the bootstrap stack. Then set
  `AUTODESK_GLUE_CONNECTION` to it.

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
