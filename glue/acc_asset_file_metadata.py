"""AWS Glue PYTHON SHELL job: ACC asset References -> metadata CSV.

The sibling job (acc_assets_to_s3.py) moves the *bytes* of every referenced
document into S3. This one moves the *facts about* those documents into a single
CSV: one row per (asset, referenced file) pair, with the asset's tag and
category alongside the file's own metadata.

    Asset             Category       Create date           Project                        File Name
    EX-ASSET-001  Equipment Tag  2013-10-01T00:00:00Z  Example Project  WPS 8-3-3 Rev2 (10-1-13).pdf

WHY A SEPARATE JOB, NOT A FLAG ON THE OTHER ONE
  The extraction job is incremental and deliberately skips anything unchanged -
  it only knows about the files it touched on that run. A metadata report has
  to describe *every* reference every time, so it always does the full sweep.
  Bolting that onto the extractor would mean two conflicting notions of "done"
  in one script. This job downloads nothing: it is metadata calls only, so a
  full sweep is cheap (~2 calls per reference).

ONE ROW PER (ASSET, FILE) PAIR
  Documents are commonly referenced by several assets - in the reference project
  32 pairs span 30 distinct files. Each pair gets its own row, because 'Asset' is
  a column and a shared file genuinely belongs to each of its assets. A file
  referenced three times appears three times, differing only in the asset
  columns. Deduplicate downstream if that is not what you want.

JOB PARAMETERS (--key value)
  --secret_name          AWS Secrets Manager secret holding the APS credentials
  --project_id           ACC project id, no "b." prefix
  --s3_bucket            destination bucket
  --s3_prefix            key prefix (default: acc/assets)
  --scopes               APS scopes (default: data:read account:read)
  --project_name         skip the project-name lookup and use this literally
  --csv_key              write the CSV at exactly this key instead of the default
  --include_folder_path  "true" to resolve each document's ACC folder (2 extra
                         calls per *new* folder, cached; off by default)
  --write_latest         "false" to skip the stable latest.csv copy

OUTPUT
  s3://<bucket>/<prefix>/_metadata/project=<id>/run=<timestamp>.csv
  s3://<bucket>/<prefix>/_metadata/project=<id>/latest.csv   (same content)

  The timestamped file is the audit trail; latest.csv is the one to point a
  dashboard, a Glue crawler or a human at.

  The CSV is written UTF-8 **with a BOM** (utf-8-sig): without it Excel opens a
  plain UTF-8 CSV in the local codepage and mangles any non-ASCII name. Be aware
  Excel still reformats anything that *looks* like a number or a date on open -
  it will eat leading zeros in a tag. Import as text, or read the CSV with
  pandas/Athena, if that matters.

COLUMNS
  Asset, Category, Create date, Project come first, named as asked for. After
  them: the rest of the asset's fields, the file's own metadata, and the urns
  needed to trace a row back to ACC. Any custom attributes defined on the asset
  are appended as "Asset: <attribute name>" columns - the header is built from
  the union of what the assets actually carry, so a project with no custom
  attributes simply gets no such columns.

  'Create date' is the *file's* creation time, since each row is a file. The
  asset's own creation time is kept separately as 'Asset Created At'.

SECRET SHAPE (JSON)
  Identical to acc_assets_to_s3.py - the same secret serves both jobs:
  {"client_id","client_secret","ssa_id","ssa_kid","ssa_private_key"}

JOB SETUP
  Type            Python Shell (Python 3.9), 1 DPU
  --additional-python-modules   PyJWT==2.10.1,cryptography==43.0.1
  IAM             secretsmanager:GetSecretValue on the secret,
                  s3:PutObject on <prefix>/_metadata/*
                  (no s3:GetObject needed - this job never reads S3 back)

  The auth and traversal helpers are duplicated from acc_assets_to_s3.py rather
  than imported: a Python Shell job is one file unless you wire up
  --extra-py-files, and one self-contained script is worth the repetition.

STATUS: the asset, relationship and tip-version calls are the same ones the
extraction job makes and are verified against this ACC tenant. NOT verified:
the project-name lookup (/construction/admin/v1), the category and custom
attribute definition endpoints, and the optional folder-path resolution. All
four degrade to a logged warning and an empty column rather than failing the
run, so a wrong guess costs a column, not the report.
"""
import csv
import io
import json
import logging
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

SCRIPT_VERSION = "2026-09-25-a"   # logged at startup: proves which code S3 is serving

LOG = logging.getLogger("acc_metadata")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

APS = "https://developer.api.autodesk.com"
TOKEN_URL = f"{APS}/authentication/v2/token"


# --------------------------------------------------------------------------
# auth  (identical contract to acc_assets_to_s3.py)
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
            "The value is truncated - Secrets Manager holds only part of the key.")
    if stripped.count("\n") < 3:
        raise RuntimeError(
            f"ssa_private_key has {stripped.count(chr(10))} line breaks; a PEM needs "
            "many. The newlines were lost when the secret was written.")
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

    This job makes no large transfers, but a project with thousands of
    references can still outlive an hour of metadata calls, so every caller goes
    through .value and gets a fresh token when the old one nears expiry.
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
        import base64
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


def paginate(path, token, limit=200):
    """Collect every page of a limit/offset Assets API endpoint."""
    out, offset = [], 0
    sep = "&" if "?" in path else "?"
    while True:
        d = api(f"{path}{sep}limit={limit}&offset={offset}", token)
        page = d.get("results", []) if isinstance(d, dict) else d
        out += page
        if len(page) < limit:
            return out
        offset += limit


# --------------------------------------------------------------------------
# project / category / custom attribute lookups
#
# Every one of these is decoration on the report rather than the report itself,
# so each returns a usable empty value on failure. A project where the Assets
# admin endpoints are not reachable still gets a CSV of real file metadata,
# with a warning in the log saying which column went blank and why.
# --------------------------------------------------------------------------
def project_display_name(project, token, override):
    """Human-readable project name, e.g. 'Example Project'."""
    if override:
        return override
    try:
        d = api(f"/construction/admin/v1/projects/{project}", token)
        name = d.get("name") or d.get("data", {}).get("attributes", {}).get("name")
        if name:
            LOG.info("project %s -> %r", project, name)
            return name
        LOG.warning("project lookup returned no name; falling back to the id")
    except Exception as e:
        LOG.warning("project name lookup failed (%s) - using the id instead. "
                    "Pass --project_name to set it explicitly.", e)
    return project


def category_index(project, token):
    """categoryId -> {'name', 'parentId'} for the project's asset categories."""
    try:
        rows = paginate(f"/construction/assets/v2/projects/{project}/categories", token)
    except Exception as e:
        LOG.warning("category lookup failed (%s) - Category column will be blank", e)
        return {}
    index = {c["id"]: {"name": c.get("name") or "", "parentId": c.get("parentId")}
             for c in rows if c.get("id")}
    LOG.info("loaded %d asset categor(ies)", len(index))
    return index


def category_path(cat_id, index):
    """('Equipment Tag', 'Mechanical > Valves > Equipment Tag') for a category.

    Categories are a tree and an asset points at a leaf. The leaf name alone is
    what people call "the category", but it is not always unique across
    branches, so the full path is carried too - and the walk is depth-capped
    because a malformed parent chain would otherwise loop forever.
    """
    if not cat_id or cat_id not in index:
        return "", ""
    names, seen, node = [], set(), cat_id
    while node and node in index and node not in seen and len(names) < 20:
        seen.add(node)
        names.append(index[node]["name"])
        node = index[node].get("parentId")
    return names[0], " > ".join(reversed(names))


def custom_attribute_names(project, token):
    """custom attribute id/name -> display name, for column headers.

    Assets return custom attributes keyed by the definition's name in some
    shapes and by its id in others; this map covers both so the column header
    reads as a human wrote it rather than as a GUID.
    """
    try:
        rows = paginate(f"/construction/assets/v2/projects/{project}/custom-attributes", token)
    except Exception as e:
        LOG.warning("custom attribute definitions unavailable (%s) - "
                    "custom attribute columns will use their raw keys", e)
        return {}
    names = {}
    for r in rows:
        label = r.get("displayName") or r.get("name") or r.get("id")
        for key in (r.get("id"), r.get("name")):
            if key:
                names[key] = label
    LOG.info("loaded %d custom attribute definition(s)", len(rows))
    return names


def status_index(project, token):
    """statusId -> status label, if the project exposes its status steps."""
    for path in (f"/construction/assets/v2/projects/{project}/status-step-sets",
                 f"/construction/assets/v2/projects/{project}/statuses"):
        try:
            rows = paginate(path, token)
        except Exception:
            continue
        index = {}
        for row in rows:
            # a step set nests its steps; a flat status list does not
            for step in row.get("values", row.get("steps", [row])):
                if isinstance(step, dict) and step.get("id"):
                    index[step["id"]] = step.get("label") or step.get("name") or ""
        if index:
            LOG.info("loaded %d asset status(es)", len(index))
            return index
    LOG.warning("asset status labels unavailable - Asset Status will show the raw id")
    return {}


# --------------------------------------------------------------------------
# hop 1-2: assets and their references  (same calls as the extraction job)
# --------------------------------------------------------------------------
def list_assets(project, token):
    """Every asset in the project. Paginates on limit/offset."""
    assets = paginate(f"/construction/assets/v2/projects/{project}/assets", token)
    LOG.info("found %d asset(s) in project %s", len(assets), project)
    return assets


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
# hop 3: lineage -> tip version metadata  (metadata only - nothing is downloaded)
# --------------------------------------------------------------------------
STORAGE_RE = re.compile(r"urn:adsk\.objects:os\.object:([^/]+)/(.+)$")


def resolve_version(lineage_urn, project, token):
    """Everything the tip version knows about itself.

    The extraction job reads this same endpoint but keeps only what it needs to
    fetch bytes. Here the response *is* the product, so the whole attribute set
    is carried across.
    """
    d = api(
        f"/data/v1/projects/b.{project}/items/"
        f"{urllib.parse.quote(lineage_urn, safe='')}/tip",
        token,
    )
    ver = d["data"]
    attrs = ver.get("attributes", {})
    ext_data = (attrs.get("extension") or {}).get("data") or {}
    name = attrs.get("displayName") or attrs.get("name") or lineage_urn

    info = {
        "name": name,
        "extension": name.rsplit(".", 1)[-1].lower() if "." in name else "",
        "create_time": attrs.get("createTime"),
        "create_user": attrs.get("createUserName") or attrs.get("createUserId"),
        "last_modified": attrs.get("lastModifiedTime"),
        "last_modified_user": (attrs.get("lastModifiedUserName")
                               or attrs.get("lastModifiedUserId")),
        "version_number": attrs.get("versionNumber"),
        "storage_size": attrs.get("storageSize"),
        "mime_type": attrs.get("mimeType"),
        "file_type": attrs.get("fileType") or ext_data.get("sourceFileName", "").rsplit(".", 1)[-1],
        "source_file_name": ext_data.get("sourceFileName"),
        "process_state": ext_data.get("processState"),
        "version_urn": ver["id"],
        "item_urn": (ver.get("relationships", {}).get("item", {}).get("data") or {}).get("id"),
        "storage_urn": (ver.get("relationships", {}).get("storage", {}).get("data") or {}).get("id"),
    }
    # A row for an item with no storage is still worth having - it says the
    # reference exists and is not a downloadable file - so this is recorded,
    # not skipped.
    info["has_file"] = bool(info["storage_urn"] and STORAGE_RE.match(info["storage_urn"] or ""))
    return info


def folder_path(item_urn, project, token, cache):
    """'Project Files/Welding/WPS' for a document, or '' if it cannot be read.

    Two calls per document (item -> parent folder -> that folder's own parents),
    which is why it is opt-in. Folders are shared by many documents, so the
    cache makes the cost per *folder* rather than per file.
    """
    if not item_urn:
        return ""
    if item_urn in cache:
        return cache[item_urn]
    try:
        item = api(f"/data/v1/projects/b.{project}/items/"
                   f"{urllib.parse.quote(item_urn, safe='')}", token)
        folder = (item["data"].get("relationships", {}).get("parent", {}).get("data") or {}).get("id")
        names = []
        while folder and len(names) < 20:
            if folder in cache:                      # rest of the path already known
                names.append(cache[folder])
                break
            d = api(f"/data/v1/projects/b.{project}/folders/"
                    f"{urllib.parse.quote(folder, safe='')}", token)
            names.append(d["data"]["attributes"].get("displayName") or "")
            folder = (d["data"].get("relationships", {}).get("parent", {}).get("data") or {}).get("id")
        path = "/".join(reversed([n for n in names if n]))
    except Exception as e:
        LOG.warning("folder path for %s unavailable: %s", item_urn, e)
        path = ""
    cache[item_urn] = path
    return path


# --------------------------------------------------------------------------
# the report
# --------------------------------------------------------------------------
# The four the request named, in the order it named them, then the rest.
LEAD_COLUMNS = ["Asset", "Category", "Create date", "Project"]

TAIL_COLUMNS = [
    "File Name", "File Extension", "File Type", "Source File Name",
    "Created By", "Last Modified", "Last Modified By",
    "Version", "Size (bytes)", "MIME Type", "Has File", "Process State",
    "Folder Path",
    "Category Path", "Asset Description", "Asset Status", "Asset Barcode",
    "Asset Created At", "Asset Updated At",
    "Project Id", "Asset Id", "Lineage URN", "Version URN", "Storage URN",
    "Extracted At",
]


def asset_custom_attributes(asset, attr_names):
    """Flatten an asset's custom attributes to {'Asset: <label>': value}.

    The API has returned these as a dict in some projects and a list of
    {name, value} records in others, so both shapes are accepted rather than
    assuming one and silently dropping the columns in the other.
    """
    raw = asset.get("customAttributes")
    out = {}
    if isinstance(raw, dict):
        items = raw.items()
    elif isinstance(raw, list):
        items = [(r.get("name") or r.get("attributeDefinitionId") or r.get("id"),
                  r.get("value")) for r in raw if isinstance(r, dict)]
    else:
        return out
    for key, value in items:
        if key is None:
            continue
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        out[f"Asset: {attr_names.get(key, key)}"] = value
    return out


def build_row(asset, info, ctx):
    """One (asset, file) pair as a flat record."""
    leaf, path = category_path(asset.get("categoryId"), ctx["categories"])
    row = {
        "Asset": asset.get("clientAssetId") or asset.get("id"),
        "Category": leaf,
        "Create date": info["create_time"],
        "Project": ctx["project_name"],

        "File Name": info["name"],
        "File Extension": info["extension"],
        "File Type": info["file_type"],
        "Source File Name": info["source_file_name"],
        "Created By": info["create_user"],
        "Last Modified": info["last_modified"],
        "Last Modified By": info["last_modified_user"],
        "Version": info["version_number"],
        "Size (bytes)": info["storage_size"],
        "MIME Type": info["mime_type"],
        "Has File": "yes" if info["has_file"] else "no",
        "Process State": info["process_state"],
        "Folder Path": info.get("folder_path", ""),

        "Category Path": path,
        "Asset Description": asset.get("description"),
        "Asset Status": ctx["statuses"].get(asset.get("statusId"), asset.get("statusId")),
        "Asset Barcode": asset.get("barcode"),
        "Asset Created At": asset.get("createdAt"),
        "Asset Updated At": asset.get("updatedAt"),

        "Project Id": ctx["project"],
        "Asset Id": asset.get("id"),
        "Lineage URN": info["lineage_urn"],
        "Version URN": info["version_urn"],
        "Storage URN": info["storage_urn"],
        "Extracted At": ctx["extracted_at"],
    }
    row.update(asset_custom_attributes(asset, ctx["attr_names"]))
    return row


def to_csv(rows):
    """Rows -> CSV text, with the header built from the union of their keys.

    Custom attributes differ per project and sometimes per asset, so the header
    cannot be a fixed list: a DictWriter over a short header would raise on the
    first asset carrying an attribute the header does not mention.
    """
    extra = sorted({k for r in rows for k in r} - set(LEAD_COLUMNS) - set(TAIL_COLUMNS))
    header = LEAD_COLUMNS + TAIL_COLUMNS + extra
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=header, restval="", extrasaction="raise",
                       lineterminator="\r\n")   # CRLF: Excel's expectation
    w.writeheader()
    for row in sorted(rows, key=lambda r: (str(r["Asset"]), str(r["File Name"]))):
        w.writerow({k: ("" if v is None else v) for k, v in row.items()})
    return buf.getvalue()


def put_csv(s3, bucket, key, text):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        # utf-8-sig: the BOM is what makes Excel read the file as UTF-8.
        Body=text.encode("utf-8-sig"),
        ContentType="text/csv; charset=utf-8",
    )
    LOG.info("csv -> s3://%s/%s", bucket, key)


# --------------------------------------------------------------------------
def main():
    args = getResolvedOptions(sys.argv, ["secret_name", "project_id", "s3_bucket"])
    # optional params, resolved separately so their absence is not fatal
    optional = {
        "s3_prefix": "acc/assets",
        "scopes": "data:read account:read",
        "project_name": "",
        "csv_key": "",
        "include_folder_path": "false",
        "write_latest": "true",
    }
    for key, default in optional.items():
        flag = f"--{key}"
        optional[key] = sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default
    include_folders = optional["include_folder_path"].lower() == "true"
    write_latest = optional["write_latest"].lower() != "false"

    project = args["project_id"].removeprefix("b.")
    bucket = args["s3_bucket"]
    prefix = optional["s3_prefix"].strip("/")

    LOG.info("acc_asset_file_metadata version %s", SCRIPT_VERSION)
    sm = boto3.client("secretsmanager")
    creds = json.loads(sm.get_secret_value(SecretId=args["secret_name"])["SecretString"])
    creds = normalise_key(creds)
    token = ApsToken(creds, optional["scopes"])
    s3 = boto3.client("s3")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ctx = {
        "project": project,
        "project_name": project_display_name(project, token, optional["project_name"]),
        "categories": category_index(project, token),
        "attr_names": custom_attribute_names(project, token),
        "statuses": status_index(project, token),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }

    rows, folders = [], {}
    stats = {"assets": 0, "references": 0, "rows": 0, "no_file": 0, "errors": 0}

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
        stats["references"] += len(lineages)
        for urn in lineages:
            try:
                info = resolve_version(urn, project, token)
                info["lineage_urn"] = urn
                if include_folders:
                    info["folder_path"] = folder_path(info["item_urn"], project, token, folders)
                if not info["has_file"]:
                    stats["no_file"] += 1
                rows.append(build_row(asset, info, ctx))
                stats["rows"] += 1
            except Exception as e:
                LOG.error("  reference %s failed: %s", urn, e)
                stats["errors"] += 1

    if not rows:
        # No CSV at all is a worse outcome than an empty one: a downstream
        # reader cannot tell "job never ran" from "project has no references".
        LOG.warning("no references found in project %s - writing a header-only csv", project)

    text = to_csv(rows)
    key = optional["csv_key"] or f"{prefix}/_metadata/project={project}/run={run_id}.csv"
    put_csv(s3, bucket, key, text)
    if write_latest and not optional["csv_key"]:
        put_csv(s3, bucket, f"{prefix}/_metadata/project={project}/latest.csv", text)

    LOG.info("done: %s", json.dumps(stats))
    if stats["errors"]:
        # surface partial failure to Glue rather than reporting a green run
        raise SystemExit(f"completed with {stats['errors']} error(s); "
                         f"csv written with {stats['rows']} row(s)")


if __name__ == "__main__":
    main()
