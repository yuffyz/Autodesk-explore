"""AWS Glue PYTHON SHELL job: ACC asset References -> S3.

Walks every asset in an ACC project, follows each asset's References tab to the
documents attached to it, and streams those files into S3 alongside a JSONL
manifest describing what landed where.

WHY PYTHON SHELL, NOT SPARK
  The work is API calls and file transfers, not computation. The reference
  project holds 3 assets / 32 documents. Spark would add cluster startup cost
  and buy nothing. If this ever grows to thousands of assets, the unit of
  parallelism is the asset - shard on asset id across several job runs, or move
  to Spark and map over the asset list.

JOB PARAMETERS (--key value)
  --secret_name    AWS Secrets Manager secret holding the APS credentials
  --project_id     ACC project id, no "b." prefix
  --s3_bucket      destination bucket
  --s3_prefix      key prefix (default: acc/assets)
  --scopes         APS scopes (default: data:read account:read)
  --full_refresh   "true" to ignore saved state and re-pull everything
  --keep_versions  "true" to keep old versions at v<N>/ instead of overwriting
  --verify_s3      "true" to also HEAD S3 before trusting state (slower, safer)

INCREMENTAL EXTRACTION
  The job keeps a state file in S3 mapping each (asset, document lineage) pair
  to the version it last pulled:

    <prefix>/_state/project=<id>/state.json

  On each run it enumerates assets and references (metadata only, cheap), then
  resolves each reference's *tip* version and compares that version urn to the
  state. Only NEW (never seen) and UPDATED (tip moved to a later version) files
  are downloaded; UNCHANGED files cost one metadata call and no transfer.

  Keying on the version urn rather than the S3 key matters: ACC documents are
  versioned, and re-uploading a file keeps its name. A key-existence check would
  see the old object and skip the new revision forever.

  The state key is asset+lineage, not lineage alone: one document is commonly
  referenced by several assets and is written once per asset, so a lineage-only
  key would mark the second asset's copy as already done and never write it.

SECRET SHAPE (JSON)
  {
    "client_id":       "...",
    "client_secret":   "...",
    "ssa_id":          "EXAMPLESSAID0001",
    "ssa_kid":         "33333333-...",
    "ssa_private_key": "-----BEGIN RSA PRIVATE KEY-----\\n..."
  }
  The private key is issued once by Autodesk and cannot be re-downloaded.

JOB SETUP
  Type            Python Shell (Python 3.9), 1 DPU
  --additional-python-modules   PyJWT==2.10.1,cryptography==43.0.1
  IAM             secretsmanager:GetSecretValue on the secret,
                  s3:PutObject/GetObject/ListBucket on the destination prefix

OUTPUT
  s3://<bucket>/<prefix>/project=<id>/asset=<clientAssetId>/<filename>
  s3://<bucket>/<prefix>/_manifest/project=<id>/run=<timestamp>.jsonl

STATUS: written against API contracts verified interactively against this ACC
tenant (auth, asset listing, relationships, download chain all confirmed). It
has NOT been executed as a Glue job - the AWS side (Secrets Manager wiring, IAM,
module install) is unverified.
"""
import json
import logging
import mimetypes
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions

SCRIPT_VERSION = "2026-09-24-b"   # logged at startup: proves which code S3 is serving

LOG = logging.getLogger("acc_extract")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

APS = "https://developer.api.autodesk.com"
TOKEN_URL = f"{APS}/authentication/v2/token"


# --------------------------------------------------------------------------
# auth
# --------------------------------------------------------------------------
def normalise_key(creds):
    """Validate the secret and say precisely what is wrong with it.

    PyJWT reports every unloadable key as "Could not parse the provided public
    key", which is the same message for a double-escaped PEM, a truncated one, a
    base64-wrapped one and an empty string. That message alone cannot tell you
    which, so check here and name the actual fault.
    """
    required = ("client_id", "client_secret", "ssa_id", "ssa_kid", "ssa_private_key")
    missing = [k for k in required if k not in creds]
    empty   = [k for k in required if k in creds and not str(creds[k]).strip()]
    if missing:
        raise RuntimeError(
            f"secret is missing key(s) {missing}. Present: {sorted(creds)}. "
            f"Expected exactly: {list(required)}")
    if empty:
        raise RuntimeError(f"secret has empty value(s) for {empty}")

    pem = creds["ssa_private_key"]
    LOG.info("key diagnostics: chars=%d real_newlines=%d literal_backslash_n=%d "
             "starts=%r ends=%r",
             len(pem), pem.count("\n"), pem.count("\\n"),
             pem[:28], pem.strip()[-26:])

    if "\\n" in pem and "\n" not in pem:
        LOG.warning("private key is double-escaped; un-escaping. "
                    "Rewrite the secret with --secret-string file://<json>.")
        pem = pem.replace("\\n", "\n")
        creds = dict(creds, ssa_private_key=pem)

    stripped = pem.strip()
    if not stripped.startswith("-----BEGIN"):
        raise RuntimeError(
            "ssa_private_key does not begin with -----BEGIN. "
            f"It starts with {stripped[:40]!r}. If that looks like base64, the PEM "
            "was encoded again before being stored; store the PEM text itself.")
    if "-----END" not in stripped:
        raise RuntimeError(
            f"ssa_private_key has no -----END line (length {len(stripped)}). "
            "The value is truncated — Secrets Manager holds only part of the key.")
    if stripped.count("\n") < 3:
        raise RuntimeError(
            f"ssa_private_key has {stripped.count(chr(10))} line breaks; a PEM needs "
            "many. The newlines were lost when the secret was written.")

    # Final proof: load it here, so failure is reported against the key itself
    # rather than surfacing later as PyJWT's generic message.
    try:
        from cryptography.hazmat.primitives import serialization
        serialization.load_pem_private_key(stripped.encode(), password=None)
    except Exception as e:
        raise RuntimeError(
            f"ssa_private_key is a PEM but will not load: {type(e).__name__}: {e}. "
            "Re-issue the key with aps_service_account.py if it cannot be recovered.")
    LOG.info("private key loaded and validated")
    return creds


class ApsToken:
    """Mints and transparently re-mints an SSA access token.

    APS tokens last an hour; a run that transfers large files can outlive one,
    so every caller goes through .value and gets a fresh token when needed.
    """

    def __init__(self, creds, scopes):
        self._c = creds
        self._scopes = scopes
        self._token = None
        self._expires_at = 0

    @property
    def value(self):
        if self._token and time.time() < self._expires_at - 300:
            return self._token
        return self._mint()

    def _mint(self):
        import jwt  # PyJWT, supplied via --additional-python-modules

        now = int(time.time())
        assertion = jwt.encode(
            {
                "iss": self._c["client_id"],
                "sub": self._c["ssa_id"],
                "aud": TOKEN_URL,
                "exp": now + 300,
                "iat": now,
                "jti": str(uuid.uuid4()),
                "scope": self._scopes.split(),
            },
            self._c["ssa_private_key"],
            algorithm="RS256",
            headers={"kid": self._c["ssa_kid"]},
        )

        import base64

        basic = base64.b64encode(
            f"{self._c['client_id']}:{self._c['client_secret']}".encode()
        ).decode()
        # Verified grant shape. NOT client_credentials + client_assertion, which
        # several third-party write-ups describe and which APS rejects.
        form = urllib.parse.urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
                "scope": self._scopes,
            }
        ).encode()
        req = urllib.request.Request(
            TOKEN_URL,
            data=form,
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.load(r)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"SSA token failed {e.code}: {e.read().decode()[:300]}")

        self._token = d["access_token"]
        self._expires_at = time.time() + d.get("expires_in", 3600)
        LOG.info("minted SSA token, expires_in=%s", d.get("expires_in"))
        return self._token


def api(path, token, retries=4):
    """GET an APS JSON endpoint, retrying throttles and transient 5xx."""
    url = path if path.startswith("http") else APS + path
    for attempt in range(retries):
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token.value}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:300]
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = 2 ** attempt
                LOG.warning("%s on %s, retrying in %ss", e.code, url, wait)
                time.sleep(wait)
                continue
            raise RuntimeError(f"GET {url} -> {e.code}: {body}")
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"GET {url} failed: {e}")


# --------------------------------------------------------------------------
# hop 1-2: assets and their references
# --------------------------------------------------------------------------
def list_assets(project, token):
    """Every asset in the project. Paginates on limit/offset."""
    out, offset, limit = [], 0, 200
    while True:
        d = api(
            f"/construction/assets/v2/projects/{project}/assets"
            f"?limit={limit}&offset={offset}",
            token,
        )
        page = d.get("results", [])
        out += page
        if len(page) < limit:
            LOG.info("found %d asset(s) in project %s", len(out), project)
            return out
        offset += limit


def asset_references(asset_id, project, token):
    """Document lineages on an asset's References tab."""
    q = urllib.parse.urlencode(
        {"domain": "autodesk-bim360-asset", "type": "asset", "id": asset_id}
    )
    d = api(f"/bim360/relationship/v2/containers/{project}/relationships:search?{q}", token)

    lineages = []
    for rel in d.get("relationships", []):
        for ent in rel.get("entities", []):
            # keep the document end of the relationship, drop the asset end
            if ent.get("domain", "").endswith("documentmanagement") or "lineage" in str(
                ent.get("type", "")
            ):
                if ent.get("id"):
                    lineages.append(ent["id"])
    return list(dict.fromkeys(lineages))


# --------------------------------------------------------------------------
# hop 3: lineage -> signed url -> S3
# --------------------------------------------------------------------------
STORAGE_RE = re.compile(r"urn:adsk\.objects:os\.object:([^/]+)/(.+)$")


def resolve_file(lineage_urn, project, token):
    """Lineage -> metadata about its current tip version."""
    d = api(
        f"/data/v1/projects/b.{project}/items/"
        f"{urllib.parse.quote(lineage_urn, safe='')}/tip",
        token,
    )
    ver = d["data"]
    attrs = ver.get("attributes", {})
    info = {
        "name": attrs.get("displayName") or attrs.get("name") or lineage_urn,
        "version_urn": ver["id"],
        "version_number": attrs.get("versionNumber"),
        "storage_size": attrs.get("storageSize"),
        "last_modified": attrs.get("lastModifiedTime"),
        "oss_bucket": None,
        "oss_object": None,
    }
    storage = (ver.get("relationships", {}).get("storage", {}).get("data") or {}).get("id")
    if not storage:
        return info
    m = STORAGE_RE.match(storage)
    if not m:
        LOG.warning("%s: unrecognised storage urn %s", info["name"], storage)
        return info
    info["oss_bucket"], info["oss_object"] = m.group(1), m.group(2)
    return info


def stream_to_s3(bucket, obj, s3, dest_bucket, dest_key, token):
    """Stream APS -> S3 without buffering the whole file (some are 40MB+)."""
    signed = api(
        f"/oss/v2/buckets/{bucket}/objects/"
        f"{urllib.parse.quote(obj, safe='')}/signeds3download",
        token,
    )
    url = signed.get("url")
    if not url:
        raise RuntimeError(f"no signed url in response: {json.dumps(signed)[:200]}")

    ctype = mimetypes.guess_type(dest_key)[0] or "application/octet-stream"
    with urllib.request.urlopen(url, timeout=300) as body:
        s3.upload_fileobj(
            body, dest_bucket, dest_key, ExtraArgs={"ContentType": ctype}
        )
    return s3.head_object(Bucket=dest_bucket, Key=dest_key)["ContentLength"]


def s3_object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def load_state(s3, bucket, key):
    """"<assetId>|<lineageUrn>" -> what we last extracted. Empty on first run."""
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except s3.exceptions.ClientError:
        LOG.info("no prior state at s3://%s/%s - treating as first run", bucket, key)
        return {}
    try:
        state = json.loads(body)
    except ValueError:
        # Never silently start from scratch: that would re-download everything
        # and overwrite a state file that may just be truncated.
        raise RuntimeError(f"state at s3://{bucket}/{key} is not valid JSON")
    LOG.info("loaded state: %d known (asset, lineage) pair(s)", len(state))
    return state


def save_state(s3, bucket, key, state):
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(state, indent=2, sort_keys=True).encode(),
        ContentType="application/json",
    )
    LOG.info("state saved: %d pair(s) -> s3://%s/%s", len(state), bucket, key)


def classify(info, prior, s3, bucket, key, verify_s3):
    """NEW / UPDATED / UNCHANGED for one reference."""
    if prior is None:
        return "NEW"
    if prior.get("version_urn") != info["version_urn"]:
        return "UPDATED"
    if prior.get("s3_key") != key:
        # Destination moved (e.g. --keep_versions toggled, or --s3_prefix changed),
        # so the current version is not actually at the key we would read it from.
        LOG.info("%s: destination key changed, re-pulling", info["name"])
        return "NEW"
    if verify_s3 and not s3_object_exists(s3, bucket, key):
        # state says we have it but the object is gone - re-pull rather than
        # report success for a file that is not actually there.
        LOG.warning("%s: in state but missing from S3, re-pulling", info["name"])
        return "NEW"
    return "UNCHANGED"


# --------------------------------------------------------------------------
def main():
    args = getResolvedOptions(
        sys.argv,
        ["secret_name", "project_id", "s3_bucket"],
    )
    # optional params, resolved separately so their absence is not fatal
    optional = {
        "s3_prefix": "acc/assets",
        "scopes": "data:read account:read",
        "full_refresh": "false",
        "keep_versions": "false",
        "verify_s3": "false",
    }
    for key, default in optional.items():
        flag = f"--{key}"
        optional[key] = sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default
    full_refresh = optional["full_refresh"].lower() == "true"
    keep_versions = optional["keep_versions"].lower() == "true"
    verify_s3 = optional["verify_s3"].lower() == "true"

    project = args["project_id"].removeprefix("b.")
    bucket = args["s3_bucket"]
    prefix = optional["s3_prefix"].strip("/")

    LOG.info("acc_assets_to_s3 version %s", SCRIPT_VERSION)
    sm = boto3.client("secretsmanager")
    creds = json.loads(sm.get_secret_value(SecretId=args["secret_name"])["SecretString"])
    creds = normalise_key(creds)
    token = ApsToken(creds, optional["scopes"])
    s3 = boto3.client("s3")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    state_key = f"{prefix}/_state/project={project}/state.json"
    state = {} if full_refresh else load_state(s3, bucket, state_key)
    if full_refresh:
        LOG.info("full_refresh=true - ignoring prior state, re-pulling everything")

    manifest = []
    stats = {"assets": 0, "new": 0, "updated": 0, "unchanged": 0,
             "no_storage": 0, "errors": 0, "bytes": 0}

    for asset in list_assets(project, token):
        tag = asset.get("clientAssetId") or asset["id"]
        stats["assets"] += 1
        try:
            lineages = asset_references(asset["id"], project, token)
        except Exception as e:                      # one bad asset must not kill the run
            LOG.error("asset %s: reference lookup failed: %s", tag, e)
            stats["errors"] += 1
            continue

        LOG.info("asset %s: %d reference(s)", tag, len(lineages))
        for urn in lineages:
            try:
                info = resolve_file(urn, project, token)
                name = info["name"]
                if not info["oss_bucket"]:
                    LOG.info("  %s: no downloadable storage, skipping", name)
                    stats["no_storage"] += 1
                    continue

                # Old versions are only preserved when asked for; by default the
                # stable key always holds the current tip.
                vdir = f"v{info['version_number']}/" if keep_versions else ""
                key = f"{prefix}/project={project}/asset={tag}/{vdir}{name}"

                # asset-scoped: the same lineage can hang off several assets and
                # is stored once per asset, so each pair tracks its own state.
                skey = f"{asset['id']}|{urn}"
                verdict = classify(info, state.get(skey), s3, bucket, key, verify_s3)
                if verdict == "UNCHANGED":
                    stats["unchanged"] += 1
                    continue

                size = stream_to_s3(
                    info["oss_bucket"], info["oss_object"], s3, bucket, key, token
                )
                stats["new" if verdict == "NEW" else "updated"] += 1
                stats["bytes"] += size

                record = {
                    "run_id": run_id,
                    "change": verdict,
                    "project_id": project,
                    "asset_id": asset["id"],
                    "client_asset_id": tag,
                    "file_name": name,
                    "lineage_urn": urn,
                    "version_urn": info["version_urn"],
                    "version_number": info["version_number"],
                    "last_modified": info["last_modified"],
                    "s3_key": key,
                    "size_bytes": size,
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                }
                manifest.append(record)
                state[skey] = {
                    "version_urn": info["version_urn"],
                    "version_number": info["version_number"],
                    "s3_key": key,
                    "size_bytes": size,
                    "client_asset_id": tag,
                    "file_name": name,
                    "extracted_at": record["extracted_at"],
                }
                LOG.info("  %s %s -> s3://%s/%s (%s bytes)",
                         verdict, name, bucket, key, f"{size:,}")
            except Exception as e:
                LOG.error("  reference %s failed: %s", urn, e)
                stats["errors"] += 1

    # Save state even on a partial run so the next run does not redo successful
    # transfers; failed references simply stay absent from state and get retried.
    save_state(s3, bucket, state_key, state)

    if manifest:
        mkey = f"{prefix}/_manifest/project={project}/run={run_id}.jsonl"
        s3.put_object(
            Bucket=bucket,
            Key=mkey,
            Body="\n".join(json.dumps(m) for m in manifest).encode(),
            ContentType="application/x-ndjson",
        )
        LOG.info("manifest -> s3://%s/%s", bucket, mkey)

    LOG.info("done: %s", json.dumps(stats))
    if not manifest:
        LOG.info("nothing new - every reference already extracted at its current version")
    if stats["errors"]:
        # surface partial failure to Glue rather than reporting a green run
        raise SystemExit(f"completed with {stats['errors']} error(s)")


if __name__ == "__main__":
    main()
