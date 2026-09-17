# Autodesk ACC data extraction

Pull files off an Autodesk Construction Cloud (ACC) asset's **References** tab
programmatically — resolving an asset tag to the PDFs, drawings and videos
attached to it.

Two ways to run it:

- **`src/acc_asset_files.py`** — one asset, to local disk. Good for exploring.
- **`glue/acc_assets_to_s3.py`** — every asset in a project, to S3, incremental.
  The production path; see [AWS Glue job](#aws-glue-job).

> **This is not Forma.** `Build > Assets > ... > References > Files` is the ACC
> Build **Assets** module. Autodesk Forma is a separate site-design product with
> no Assets register; its API (`/forma/integrate/v1alpha/*`) deals in proposals
> and geometry and will not return these documents.

## Layout

```
.
├── src/         scripts (all importable from each other)
├── glue/        AWS Glue job
├── secrets/     SSA private keys        — gitignored
├── data/        downloaded ACC content  — gitignored
└── archive/     original aps_auth.py before refactoring
```

## Quick start

```bash
# list hubs the app can see (2-legged, no browser)
python3 src/aps_hubs.py --app

# projects in a hub
python3 src/aps_projects.py

# list an asset's referenced files without downloading
python3 src/acc_asset_files.py --ssa --dry-run

# download them
python3 src/acc_asset_files.py --ssa --out ./data/asset_files_ssa
```

Whole project into S3, pulling only what changed since the last run:

```bash
aws glue start-job-run --job-name acc-assets-to-s3
```

## How the extraction works

"References > Files" is not one API call. It takes three hops:

| # | API | Does |
|---|-----|------|
| 1 | Assets | `clientAssetId` (`EX-ASSET-001`) → internal asset GUID |
| 2 | Relationships | asset GUID → linked document lineage urns |
| 3 | Data Management | lineage → tip version → storage urn → signed S3 URL → file |

## Authentication — three kinds, and they behave differently

| Mode | How | Sees |
|------|-----|------|
| **2-legged** | `client_credentials` | Hubs/projects where the **app** is provisioned. Cannot call the Assets API at all — it returns `401 "The user ID could not be determined from secure headers"`. |
| **3-legged** | browser login | Only what **that user** is a member of. |
| **SSA** | signed JWT | A robot identity. Unattended, and what you want for a pipeline. |

Two lessons that cost real debugging time:

- `/project/v1/hubs` **silently omits** a hub if either the user isn't a member
  *or* the app isn't provisioned. An empty list does not mean "no access" — use
  `aps_hub_probe.py` to fetch a hub by id and get a distinguishing status code.
- The Assets API needs **project-level** membership *plus* the Assets product
  enabled. Account/hub admin is not enough, and the 403 covers both cases.

### Service account (SSA)

Autodesk **Secure Service Accounts** are robot identities that authenticate with
a signed JWT instead of a browser. Verified working token exchange:

```
POST /authentication/v2/token
grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
assertion=<RS256-signed JWT>          # Basic auth = client id/secret
```

Note this is **not** the `client_credentials` + `client_assertion` form that
some third-party docs describe — that shape is rejected.

```bash
python3 src/aps_service_account.py list
python3 src/aps_service_account.py create "Name" --confirm
python3 src/aps_service_account.py key <serviceAccountId> --confirm
python3 src/aps_service_account.py token <saId> <keyId> <key.pem>
```

An SSA is a **new identity and inherits nothing** — it needs its own ACC project
invite with the Assets module enabled, same as a person.
SSAs also cannot call `/userprofile/v1/users/@me` (returns `410`).

## Scripts

| File | Purpose |
|------|---------|
| `aps_auth.py` | 3-legged + 2-legged tokens, disk cache, refresh, `whoami()` |
| `aps_hubs.py` | list hubs (`--app`, `--projects`, `--relogin`) |
| `aps_projects.py` | list projects in a hub (`--all` includes archived) |
| `aps_hub_probe.py` | fetch one hub by id to diagnose a missing-hub problem |
| `aps_service_account.py` | manage SSAs and mint JWT tokens |
| `acc_asset_files.py` | the extraction (`--ssa`, `--dry-run`, `--out`) |
| `../glue/acc_assets_to_s3.py` | Glue Python Shell job: all assets in a project → S3 |

Config is read from env vars where set, falling back to in-file defaults:
`APS_CLIENT_ID`, `APS_CLIENT_SECRET`, `APS_SSA_ID`, `APS_SSA_KID`, `APS_SSA_PEM`.

## AWS Glue job

`glue/acc_assets_to_s3.py` extracts **every** asset's References in a project to
S3, rather than the one asset `acc_asset_files.py` handles.

```bash
aws glue create-job --name acc-assets-to-s3 \
  --role <GlueRole> \
  --command Name=pythonshell,PythonVersion=3.9,ScriptLocation=s3://<code>/acc_assets_to_s3.py \
  --default-arguments '{
    "--additional-python-modules":"PyJWT==2.10.1,cryptography==43.0.1",
    "--secret_name":"aps/acc-extract",
    "--project_id":"22222222-2222-2222-2222-222222222222",
    "--s3_bucket":"my-bucket",
    "--s3_prefix":"acc/assets"
  }' --max-capacity 1.0
```

**Python Shell, not Spark** — the work is API calls and transfers, not compute
(3 assets / 32 documents here). If this grows to thousands of assets, shard on
asset id or move to Spark and map over the asset list.

Credentials come from **Secrets Manager**, never the script:

```json
{"client_id":"...","client_secret":"...","ssa_id":"EXAMPLESSAID0001",
 "ssa_kid":"33333333-...","ssa_private_key":"-----BEGIN RSA PRIVATE KEY-----\n..."}
```

IAM needs `secretsmanager:GetSecretValue` on that secret and
`s3:PutObject/GetObject/ListBucket` on the destination prefix.

Output layout:

```
s3://<bucket>/<prefix>/project=<id>/asset=<clientAssetId>/<filename>
s3://<bucket>/<prefix>/_manifest/project=<id>/run=<timestamp>.jsonl
```

The Hive-style `project=` / `asset=` partitioning means Athena or a Glue crawler
can read the manifest directly.

### Incremental runs

The job pulls **only what is new or changed**. It keeps state in S3:

```
<prefix>/_state/project=<id>/state.json
```

mapping each `(assetId, lineageUrn)` pair to the version it last extracted. Each
run enumerates assets and references (metadata only, cheap), resolves each
reference's *tip* version, and compares:

| Verdict | When | Action |
|---------|------|--------|
| `NEW` | pair not in state | download |
| `UPDATED` | tip moved to a later version | download |
| `UNCHANGED` | same version urn, same destination key | skip, no transfer |

Two details that are easy to get wrong, and were:

- **Compare version urns, not S3 keys.** ACC documents are versioned and a
  re-upload keeps the filename, so a key-existence check would see the old
  object and skip the new revision forever.
- **Key state on asset+lineage, not lineage.** The same document is commonly
  referenced by several assets and written once per asset; a lineage-only key
  marks the second asset's copy as done and never writes it. In this project 3
  assets share references — 32 pairs across 30 distinct documents.

Extra parameters:

| Flag | Effect |
|------|--------|
| `--full_refresh true` | ignore state, re-pull everything |
| `--keep_versions true` | write to `v<N>/<filename>` so old versions survive |
| `--verify_s3 true` | HEAD S3 before trusting state; re-pulls anything deleted |

State is saved even on a partial run, so successful transfers are not redone;
failed references stay absent from state and are retried next run. Changing the
destination layout (`--keep_versions`, `--s3_prefix`) is detected and re-pulls.

Also: files are **streamed** APS→S3 (some are 40MB+, never buffered whole); a
failure on one asset or file is logged and the run continues, but the job
**exits non-zero** if anything failed rather than reporting a green run; the
access token is re-minted when it nears expiry, so long transfers do not die at
the one-hour mark.

**Verified:** auth, asset listing, reference lookup and download-URL resolution
run against the live tenant (3 assets, 32 references). The incremental logic was
exercised end-to-end against live APS with an in-memory S3 stand-in: cold run 32
transfers, unchanged run 0, single bumped version 1, `--full_refresh` 32,
deleted-object recovery under `--verify_s3` 1, `--keep_versions` relayout 32.
**Unverified:** the AWS side — Secrets Manager wiring, IAM, the real `boto3`
upload and the `--additional-python-modules` install have never run in Glue.

## Security

**This repo has a GitHub remote.** `.gitignore` excludes `secrets/`, `*.pem`,
`data/` and the token cache. Before committing, check `git status` — do not
`git add -A` blindly.

- **The `.pem` is the whole credential.** Autodesk issues it once and will never
  show it again. Anything that can read it can act as the service account.
  It belongs in a secret manager; point `APS_SSA_PEM` at the retrieved path.
- **`CLIENT_ID` / `CLIENT_SECRET` are currently hardcoded** in `src/aps_auth.py`.
  The env-var lines are still there, commented out. Switch to those and rotate
  the secret before this goes anywhere shared.
- **Two expiries** will silently kill the pipeline: the service account
  (2026-09-16) and the key. Check `expiresAt` on startup.

## Known issues

- `filter[clientAssetId]` is **silently ignored** by the API — passing a nonsense
  tag still returns every asset in the project. `acc_asset_files.py` now filters
  client-side; before that fix it took `results[0]` and was correct only by luck.
- The References tab mixes media types (27 PDFs + 3 MP4s here) and extension
  case varies (`.mp4` / `.MP4`) — filter case-insensitively.
- `src/acc_asset_files.py` re-downloads everything on every run — it has no
  state. The Glue job is the incremental one; the local script is for poking
  around, not for repeated pulls.
- Downloads are sequential in both. Fine at this size (32 files); the unit of
  parallelism, if it is ever needed, is the asset.

## Not done yet

- **Run the Glue job for real.** Everything APS-side is verified against the live
  tenant, but Secrets Manager, IAM, the `boto3` upload and the
  `--additional-python-modules` install have never executed in Glue.
  `cryptography` installing cleanly in the Python Shell runtime is the likeliest
  friction.
- **Decide on deduplication.** 32 (asset, document) pairs span 30 distinct
  files — documents are shared between assets and stored once per asset. Good
  for browsing by asset, wasteful in bytes. Worth settling before the data grows.
- **Scheduling and alerting.** The job exits non-zero on any failure, so a Glue
  schedule plus an EventBridge rule on failure is enough; neither is set up.
- **Only one project is wired up.** `Example Project` is
  hardcoded as the default; the hub has 29 active projects. `--project_id` takes
  any of them, but nothing iterates over projects yet.
- **Forma proper** is still untouched. `/forma/integrate/v1alpha/*` is a real
  route (403s rather than 404s) but has never been called with a working token.
