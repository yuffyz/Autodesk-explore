# Autodesk ACC data extraction

Pull files off an Autodesk Construction Cloud (ACC) asset's **References** tab
programmatically — resolving an asset tag to the PDFs, drawings and videos
attached to it.

Two ways to run it:

- **`src/acc_asset_files.py`** — one asset, to local disk. Good for exploring.
- **`glue/acc_assets_to_s3.py`** — every asset in a project, to S3, incremental.
  The production path; see [AWS Glue job](#aws-glue-job).
- **`glue/acc_asset_file_metadata.py`** — every project the service account can
  reach, writing a CSV *about* the files instead of the files themselves; see
  [Metadata report](#metadata-report-csv).

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

Every reference in every project the service account can see, as one CSV:

```bash
aws glue start-job-run --job-name acc-asset-file-metadata
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
| `../glue/acc_asset_file_metadata.py` | Glue Python Shell job: every reachable project's reference metadata → one CSV |

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

## Metadata report (CSV)

`glue/acc_asset_file_metadata.py` answers a different question from the
extraction job: not "where are the files" but "what are they". It writes one
CSV with a row per **(project, asset, referenced file)**.

```bash
aws glue start-job-run --job-name acc-asset-file-metadata
```

| Asset | Category | Create date | Project | File Name | … |
|---|---|---|---|---|---|
| `EX-ASSET-001` | Equipment Tag | 2013-10-01T00:00:00Z | Example Project | WPS 8-3-3 Rev2 (10-1-13).pdf | … |

### No project id — the token defines the scope

Unlike the extraction job, this one takes **no `--project_id`**. It asks APS
what the service account can reach and sweeps all of it:

```
/project/v1/hubs                 → every hub the SSA belongs to
/project/v1/hubs/<hub>/projects  → every project inside each one
```

That listing *is* "what this identity has access to": a hub appears only when
the SSA is a member **and** the app is provisioned in the account, and a project
appears only when the SSA is a member of it. Adding the SSA to a new project is
all it takes to include it in the next run — nothing to redeploy. The listing is
also where project **names** come from, so there is no separate name lookup.

Two things it deliberately skips rather than fails on:

- **Non-ACC hubs.** Fusion Team and A360 personal hubs have no Assets register,
  so every call against them would 403. Only `hubs:autodesk.bim360:Account` is
  swept; the rest get one log line.
- **Projects without Assets.** Membership in a project does not mean the Assets
  module is on for the SSA there — the hub has 29 active projects and only some
  are wired for it. A 401/403/404 from the asset listing is counted as a
  *skipped* project, not an error. Treating it as an error would mean the job
  never goes green. Anything else that fails is a real error and the run exits
  non-zero.

If the sweep finds no projects at all the job exits non-zero: an empty report
from an SSA that cannot see anything is a configuration problem, not a result.

### Columns and output

After the four named columns come the rest of the file's metadata (extension,
type, created by, last modified + by, version, size, MIME type, folder path,
whether it has downloadable content), the rest of the asset's (category path,
description, status, barcode, created/updated), the hub, and the urns needed to
trace a row back to ACC. Any **custom attributes** on the asset are appended as
`Asset: <name>` columns — the header is the union across *every* swept project,
so two projects with different attributes produce one CSV holding both sets,
blank where a project has none.

```
s3://<bucket>/<prefix>/_metadata/run=<timestamp>.csv   every project, one file
s3://<bucket>/<prefix>/_metadata/latest.csv            same content
```

One combined CSV rather than a file per project, because `Project` and
`Project Id` are columns: the combined file answers both "what is in this
project" and "where else does this document appear", while a per-project file
answers only the first. `--split_by_project true` writes the per-project files
alongside it.

The timestamped file is the audit trail; `latest.csv` is what to point a
dashboard or a person at. It opens straight in Excel — written UTF-8 **with a
BOM**, without which Excel reads a UTF-8 CSV in the local codepage and mangles
non-ASCII names. Excel will still reformat anything resembling a number or a
date on open (leading zeros in a tag are the usual casualty); import as text, or
read the CSV with pandas or Athena, if that matters.

Why a separate job rather than a flag on the extractor: the extractor is
incremental and only knows about the files it touched on that run, whereas a
report has to describe *every* reference every time. It downloads nothing — two
metadata calls per reference — so the full sweep is cheap.

| Flag | Effect |
|---|---|
| `--project_id` | comma-separated ids to restrict the sweep to; omit for everything reachable |
| `--hub_id` | restrict to one hub, skipping hub discovery |
| `--csv_key` | write at exactly this key (suppresses `latest.csv`) |
| `--include_folder_path true` | resolve each document's ACC folder; one call per *new* folder, cached |
| `--split_by_project true` | also write a per-project CSV each run |
| `--write_latest false` | timestamped file only |

Ambiguity worth knowing about: **`Create date` is the file's**, since each row
is a file. The asset's own is `Asset Created At`. And a document referenced by
three assets produces three rows — `Asset` is a column, and a shared file
genuinely belongs to each of them; deduplicate downstream if that is not wanted.

The category, custom attribute and status lookups each degrade to a logged
warning and a blank column instead of failing the run, because none of them is
the report itself.

**Verified:** the asset, relationship and tip-version calls are the same ones
the extraction job makes against the live tenant, and hub/project listing is the
same call `src/aps_hubs.py --projects` makes (though not yet as the SSA). The
discovery, sweep, CSV assembly and S3 writes were exercised end-to-end against a
mocked APS and an in-memory S3: two hubs with the Fusion one skipped, a paginated
project listing followed via `links.next`, a 403 project skipped without failing
the run, custom attribute columns unioned across two projects, per-project folder
caches, unicode filenames, embedded quotes, missing storage, a dead reference, a
dead asset, `--project_id` / `--hub_id` filtering, and the no-hubs exit.
**Unverified against live APS:** the category, custom attribute and status
endpoints and `--include_folder_path` — the ones that degrade gracefully, so a
wrong guess there costs a column, not the report.

## Security

**This repo has a GitHub remote.** `.gitignore` excludes `secrets/`, `*.pem`,
`data/` and the token cache. Before committing, check `git status` — do not
`git add -A` blindly.

- **The `.pem` is the whole credential.** Autodesk issues it once and will never
  show it again. Anything that can read it can act as the service account.
  It belongs in a secret manager; point `APS_SSA_PEM` at the retrieved path.
- **`CLIENT_ID` / `CLIENT_SECRET` come from the environment only** —
  `APS_CLIENT_ID` and `APS_CLIENT_SECRET`, read in `src/aps_auth.py`. Nothing
  authenticating belongs in a tracked file.
- **The old client secret was committed and must be treated as burned.** It was
  hardcoded in `src/aps_auth.py` and is gone from the working tree, but it
  remains in this repo's git history and therefore in anything cloned or forked
  from the remote. **Rotate it in <https://aps.autodesk.com/myapps/>** — removing
  the line does not un-publish it.
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
- **Only the extraction job is single-project.** `Example Project` is hardcoded as its default and `--project_id` takes any one of the
  hub's 29 active projects, but it still sweeps one at a time. The metadata job
  no longer works this way — it discovers every project the SSA can reach — and
  the same discovery could be lifted into the extractor.
- **Forma proper** is still untouched. `/forma/integrate/v1alpha/*` is a real
  route (403s rather than 404s) but has never been called with a working token.
