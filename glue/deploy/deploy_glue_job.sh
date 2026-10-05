#!/usr/bin/env bash
# Upload a Glue Python Shell script to S3 and create-or-update its job.
#
# Everything account-specific comes from the environment, so the same script
# deploys to any AWS account: GitHub Actions fills these from the target
# Environment's variables, and a laptop can export them by hand.
#
#   required  CODE_BUCKET      bucket Glue reads the script from
#             GLUE_ROLE_ARN    role the job runs as
#             DATA_BUCKET      --s3_bucket passed to the job
#             SECRET_NAME      --secret_name passed to the job
#   optional  JOB_NAME         default acc-asset-file-metadata
#             SCRIPT_PATH      default glue/acc_asset_file_metadata.py
#             CODE_PREFIX      default glue-scripts
#             S3_PREFIX        default acc/assets
#             PYTHON_MODULES   default PyJWT==2.10.1,cryptography==43.0.1,
#                              snowflake-connector-python==3.13.2
#             PYTHON_VERSION   default 3.9
#             TIMEOUT_MINUTES  default 180
#             EXTRA_ARGS_JSON  default {} - merged into the job's default
#                              arguments, e.g. {"--hub_id":"b.xxx"}
#             VERSION          default the git short sha (or "local")
#
# The script lands at a key that carries the version, never overwriting an
# earlier one: rolling back is re-running the deploy at an older commit, and the
# job definition always says exactly which upload it is running.
set -euo pipefail

: "${CODE_BUCKET:?CODE_BUCKET is required}"
: "${GLUE_ROLE_ARN:?GLUE_ROLE_ARN is required}"
: "${DATA_BUCKET:?DATA_BUCKET is required}"
: "${SECRET_NAME:?SECRET_NAME is required}"

JOB_NAME="${JOB_NAME:-acc-asset-file-metadata}"
SCRIPT_PATH="${SCRIPT_PATH:-glue/acc_asset_file_metadata.py}"
CODE_PREFIX="${CODE_PREFIX:-glue-scripts}"
S3_PREFIX="${S3_PREFIX:-acc/assets}"
PYTHON_MODULES="${PYTHON_MODULES:-PyJWT==2.10.1,cryptography==43.0.1,snowflake-connector-python==3.13.2}"
PYTHON_VERSION="${PYTHON_VERSION:-3.9}"
TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-180}"
EXTRA_ARGS_JSON="${EXTRA_ARGS_JSON:-}"
[ -n "$EXTRA_ARGS_JSON" ] || EXTRA_ARGS_JSON='{}'
VERSION="${VERSION:-$(git rev-parse --short HEAD 2>/dev/null || echo local)}"

[ -f "$SCRIPT_PATH" ] || { echo "no script at $SCRIPT_PATH" >&2; exit 1; }
echo "$EXTRA_ARGS_JSON" | jq -e 'type == "object"' >/dev/null \
  || { echo "EXTRA_ARGS_JSON must be a JSON object, got: $EXTRA_ARGS_JSON" >&2; exit 1; }

account=$(aws sts get-caller-identity --query Account --output text)
echo "deploying $JOB_NAME @ $VERSION to account $account"

script_name=$(basename "$SCRIPT_PATH")
key="${CODE_PREFIX%/}/${JOB_NAME}/${VERSION}/${script_name}"
aws s3 cp "$SCRIPT_PATH" "s3://${CODE_BUCKET}/${key}" --only-show-errors
echo "script -> s3://${CODE_BUCKET}/${key}"

# One body serves both create-job and update-job. update-job REPLACES the whole
# definition, so every field that matters is set here rather than inherited from
# whatever the job looked like before.
body=$(jq -n \
  --arg role    "$GLUE_ROLE_ARN" \
  --arg script  "s3://${CODE_BUCKET}/${key}" \
  --arg pyver   "$PYTHON_VERSION" \
  --arg mods    "$PYTHON_MODULES" \
  --arg secret  "$SECRET_NAME" \
  --arg bucket  "$DATA_BUCKET" \
  --arg prefix  "$S3_PREFIX" \
  --arg version "$VERSION" \
  --argjson timeout "$TIMEOUT_MINUTES" \
  --argjson extra   "$EXTRA_ARGS_JSON" \
  '{
     Description: ("ACC asset reference metadata -> CSV (deployed " + $version + ")"),
     Role: $role,
     Command: {Name: "pythonshell", PythonVersion: $pyver, ScriptLocation: $script},
     DefaultArguments: ({
       "--additional-python-modules": $mods,
       "--secret_name": $secret,
       "--s3_bucket": $bucket,
       "--s3_prefix": $prefix
     } + $extra),
     MaxCapacity: 1.0,
     Timeout: $timeout,
     ExecutionProperty: {MaxConcurrentRuns: 1}
   }')

if aws glue get-job --job-name "$JOB_NAME" >/dev/null 2>&1; then
  aws glue update-job --job-name "$JOB_NAME" --job-update "$body" >/dev/null
  echo "updated glue job $JOB_NAME"
else
  aws glue create-job --cli-input-json "$(jq --arg n "$JOB_NAME" '. + {Name: $n}' <<<"$body")" >/dev/null
  echo "created glue job $JOB_NAME"
fi

# Read the definition back: proves the job now points at this upload.
deployed=$(aws glue get-job --job-name "$JOB_NAME" --query 'Job.Command.ScriptLocation' --output text)
if [ "$deployed" != "s3://${CODE_BUCKET}/${key}" ]; then
  echo "job points at $deployed, expected s3://${CODE_BUCKET}/${key}" >&2
  exit 1
fi
echo "verified: $JOB_NAME -> $deployed"
