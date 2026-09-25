# IAM role for the ACC extraction Glue job

Fill these in once and the commands below paste straight through:

```bash
ACCOUNT=123456789012
REGION=us-east-1
ROLE=AWSGlueServiceRole-acc-extract     # the AWSGlueServiceRole- prefix keeps
                                        # console PassRole policies happy
DATA_BUCKET=my-acc-bucket               # where the PDFs land
CODE_BUCKET=my-glue-scripts             # where the .py script lives
SECRET=aps/acc-extract
PREFIX=acc/assets
```

## 1. Trust policy — let Glue assume the role

```bash
cat > trust.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
JSON

aws iam create-role --role-name "$ROLE" \
  --assume-role-policy-document file://trust.json \
  --description "Runs the ACC asset extraction Glue job"
```

## 2. Attach the AWS-managed Glue policy

Covers CloudWatch Logs and the `aws-glue-*` scratch buckets Glue uses itself.

```bash
aws iam attach-role-policy --role-name "$ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

## 3. Inline policy — what this job specifically touches

```bash
cat > policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadTheApsCredentials",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:${REGION}:${ACCOUNT}:secret:${SECRET}-*"
    },
    {
      "Sid": "WriteExtractedFiles",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::${DATA_BUCKET}/${PREFIX}/*"
    },
    {
      "Sid": "HeadObjectNeedsListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${DATA_BUCKET}",
      "Condition": { "StringLike": { "s3:prefix": "${PREFIX}/*" } }
    },
    {
      "Sid": "ReadTheJobScript",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::${CODE_BUCKET}/*"
    }
  ]
}
JSON

aws iam put-role-policy --role-name "$ROLE" \
  --policy-name acc-extract-access \
  --policy-document file://policy.json

rm trust.json policy.json
```

### Why each one is there

| Permission | Needed by |
|---|---|
| `secretsmanager:GetSecretValue` | `sm.get_secret_value()` — the APS credentials |
| `s3:PutObject` | `upload_fileobj` and `put_object` (files, state, manifest) |
| `s3:GetObject` | reading `state.json` back, and every `head_object` |
| `s3:AbortMultipartUpload` | **the 47MB MP4.** Anything over 8MB uploads multipart; without this a failed part leaves an un-abortable upload that you keep paying for |
| `s3:ListBucket` | `head_object` on a *missing* key returns 403 instead of 404 without it — the job still works, but every real permission error gets misreported as "file not present" |
| `s3:GetObject` on the code bucket | Glue reads the script itself from S3 |

The trailing `-*` on the secret ARN is required — Secrets Manager appends six
random characters to every secret's ARN.

## 4. Verify before running the job

```bash
aws iam get-role --role-name "$ROLE" --query 'Role.Arn' --output text
aws iam list-attached-role-policies --role-name "$ROLE"
aws iam list-role-policies --role-name "$ROLE"
```

## 5. Create the job with the role

```bash
aws glue create-job --name acc-assets-to-s3 \
  --role "arn:aws:iam::${ACCOUNT}:role/${ROLE}" \
  --command "Name=pythonshell,PythonVersion=3.9,ScriptLocation=s3://${CODE_BUCKET}/acc_assets_to_s3.py" \
  --default-arguments "{
    \"--additional-python-modules\":\"PyJWT==2.10.1,cryptography==43.0.1\",
    \"--secret_name\":\"${SECRET}\",
    \"--project_id\":\"22222222-2222-2222-2222-222222222222\",
    \"--s3_bucket\":\"${DATA_BUCKET}\",
    \"--s3_prefix\":\"${PREFIX}\"
  }" \
  --max-capacity 1.0
```

## 6. The metadata job — same role, second job

`acc_asset_file_metadata.py` writes a CSV under `${PREFIX}/_metadata/` and reads
the same secret, so the role above already covers it: no new permissions. It
never downloads a file and never reads S3 back, so `s3:GetObject`,
`s3:ListBucket` and `s3:AbortMultipartUpload` go unused by this job — they are
there for the extraction job.

Note there is **no `--project_id`**: the job sweeps every project the service
account can reach. The AWS side does not change when a project is added to the
SSA in ACC.

```bash
aws glue create-job --name acc-asset-file-metadata \
  --role "arn:aws:iam::${ACCOUNT}:role/${ROLE}" \
  --command "Name=pythonshell,PythonVersion=3.9,ScriptLocation=s3://${CODE_BUCKET}/acc_asset_file_metadata.py" \
  --default-arguments "{
    \"--additional-python-modules\":\"PyJWT==2.10.1,cryptography==43.0.1\",
    \"--secret_name\":\"${SECRET}\",
    \"--s3_bucket\":\"${DATA_BUCKET}\",
    \"--s3_prefix\":\"${PREFIX}\"
  }" \
  --max-capacity 1.0
```

Sweeping every project makes this job longer than the extraction job, not
heavier — it transfers nothing. If it ever approaches Glue's timeout, `--hub_id`
or a comma-separated `--project_id` splits it across runs.

If you point `--csv_key` somewhere outside `${PREFIX}/`, widen the
`WriteExtractedFiles` resource to match — otherwise the run does all its work
and 403s on the final `put_object`.

## Two extras, only if they apply

**Bucket encrypted with SSE-KMS** — add to the inline policy, or every write 403s:

```json
{
  "Effect": "Allow",
  "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
  "Resource": "arn:aws:kms:REGION:ACCOUNT:key/KEY-ID"
}
```

**Job attached to a VPC connection** — Python Shell installs
`--additional-python-modules` from PyPI at startup, so the subnet needs a NAT
gateway or the job fails before it reaches any of your code. With no connection
configured (the default) Glue has internet access and this does not apply.
