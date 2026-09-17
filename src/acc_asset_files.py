#!/usr/bin/env python3
"""Download the PDFs on an ACC asset's References > Files tab.

    python3 acc_asset_files.py                      # defaults below
    python3 acc_asset_files.py --ssa                # run as the service account
    python3 acc_asset_files.py <clientAssetId> <projectId> [--out DIR] [--dry-run]

Three hops, because "References > Files" is not one API:

  1. Assets API         clientAssetId ("EX-ASSET-001") -> internal asset GUID
  2. Relationships API  asset GUID -> linked document lineage urns  (the References tab)
  3. Data Management    lineage -> tip version -> storage -> signed S3 url -> file

Needs a 3-LEGGED token: the Assets API rejects app-only tokens with
"The user ID could not be determined from secure headers", and the signed-in
user must be a member of the project WITH the Assets product enabled.

STATUS: hops 2 and 3 are written from the API contracts but have NOT been run
against live data - the account used for development lacks project access, so
every call 403s before reaching them. Response shapes at those hops (especially
which entity in a relationship is the document, and the storage urn layout) may
need a tweak on first real run. Hop 1 and the auth path are verified.
"""
import json, os, re, sys, urllib.error, urllib.parse, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent      # repo root, whatever the cwd

from aps_auth import get_token, whoami

# Service account (SSA) - unattended identity, no browser, no human membership.
SSA_ID  = os.environ.get("APS_SSA_ID",  "EXAMPLESSAID0001")
SSA_KID = os.environ.get("APS_SSA_KID", "33333333-3333-3333-3333-333333333333")
SSA_PEM = os.environ.get("APS_SSA_PEM", str(
    ROOT / "secrets" / "ssa_EXAMPLESSAID0001_33333333-3333-3333-3333-333333333333.pem"))

BASE = "https://developer.api.autodesk.com"
DEFAULT_TAG  = "EX-ASSET-001"
DEFAULT_PROJ = "22222222-2222-2222-2222-222222222222"   # Example Project


def api(path, token, method="GET", body=None):
    h = {"Authorization": f"Bearer {token}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode(); h["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, headers=h, data=data, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:    return e.code, json.loads(raw)
        except Exception: return e.code, raw[:300]


def die_on_access(status, payload, what):
    if status in (401, 403):
        detail = payload.get("detail") if isinstance(payload, dict) else payload
        sys.exit(
            f"\n{status} fetching {what}: {detail}\n\n"
            "This is an ACC permissions problem, not a code problem. The signed-in\n"
            "user must be a member of the project AND have the Assets product enabled.\n"
            "Ask an ACC project admin to add them under Project Admin > Members."
        )


# ---------- hop 1: clientAssetId -> asset GUID ----------
def find_asset(tag, project, token):
    q = urllib.parse.urlencode({"filter[clientAssetId]": tag})
    s, d = api(f"/construction/assets/v2/projects/{project}/assets?{q}&limit=5", token)
    die_on_access(s, d, "assets")
    if s != 200:
        sys.exit(f"assets lookup failed {s}: {d}")
    # The API ignores filter[clientAssetId] entirely (a nonsense tag still returns
    # every asset), so the match has to be made here or we silently grab the wrong one.
    results = [a for a in d.get("results", []) if a.get("clientAssetId") == tag]
    if not results:
        sys.exit(f"No asset with clientAssetId {tag!r} in project {project}.")
    return results[0]


# ---------- hop 2: asset -> referenced document lineages ----------
def find_references(asset_id, project, token):
    """The References tab is relationship records in the relationship service."""
    q = urllib.parse.urlencode({
        "domain": "autodesk-bim360-asset",
        "type": "asset",
        "id": asset_id,
    })
    s, d = api(f"/bim360/relationship/v2/containers/{project}/relationships:search?{q}", token)
    die_on_access(s, d, "relationships")
    if s != 200:
        sys.exit(f"relationship search failed {s}: {d}")

    lineages = []
    for rel in d.get("relationships", []):
        for ent in rel.get("entities", []):
            # skip the asset end of the relationship; keep the document end
            if ent.get("domain", "").endswith("documentmanagement") or "lineage" in str(ent.get("type", "")):
                lineages.append(ent.get("id"))
    return [l for l in dict.fromkeys(lineages) if l]


# ---------- hop 3: lineage -> tip version -> storage -> bytes ----------
def download_lineage(lineage_urn, project, token, out_dir, dry_run):
    dm_proj = project if project.startswith("b.") else "b." + project
    s, d = api(f"/data/v1/projects/{dm_proj}/items/{urllib.parse.quote(lineage_urn, safe='')}/tip", token)
    if s != 200:
        print(f"    tip lookup failed {s}: {str(d)[:120]}"); return None

    ver = d["data"]
    name = ver["attributes"].get("displayName") or lineage_urn
    storage = (ver.get("relationships", {}).get("storage", {}).get("data") or {}).get("id")
    if not storage:
        print(f"    {name}: no storage on tip version (may be a non-file item)"); return None

    # urn:adsk.objects:os.object:<bucket>/<object>
    m = re.match(r"urn:adsk\.objects:os\.object:([^/]+)/(.+)$", storage)
    if not m:
        print(f"    {name}: unrecognised storage urn {storage}"); return None
    bucket, obj = m.group(1), m.group(2)

    if dry_run:
        print(f"    {name}  [dry-run, not downloaded]"); return name

    s, d = api(f"/oss/v2/buckets/{bucket}/objects/{urllib.parse.quote(obj, safe='')}"
               f"/signeds3download", token)
    if s != 200:
        print(f"    {name}: signed url failed {s}: {str(d)[:120]}"); return None

    os.makedirs(out_dir, exist_ok=True)
    dest = os.path.join(out_dir, name)
    urllib.request.urlretrieve(d["url"], dest)
    print(f"    saved {dest} ({os.path.getsize(dest):,} bytes)")
    return dest


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    tag     = args[0] if args else DEFAULT_TAG
    project = args[1] if len(args) > 1 else DEFAULT_PROJ
    project = project[2:] if project.startswith("b.") else project
    out_dir = str(ROOT / "data" / "asset_files")
    if "--out" in sys.argv:
        out_dir = sys.argv[sys.argv.index("--out") + 1]
    dry_run = "--dry-run" in sys.argv

    if "--ssa" in sys.argv:
        from aps_service_account import mint_token
        token = mint_token(SSA_ID, SSA_KID, SSA_PEM, "data:read data:write account:read")
        print(f"acting as service account {SSA_ID}")
    else:
        token = get_token()
        me = whoami(token)
        print(f"signed in as: {me.get('name')} <{me.get('email')}>")
    print(f"project     : {project}")
    print(f"asset tag   : {tag}\n")

    asset = find_asset(tag, project, token)
    print(f"[1/3] asset {asset.get('clientAssetId')} -> {asset.get('id')}")

    lineages = find_references(asset["id"], project, token)
    print(f"[2/3] {len(lineages)} referenced document(s)")
    if not lineages:
        print("      (nothing on the References tab, or references are of another kind)")
        return

    print("[3/3] downloading:")
    for urn in lineages:
        download_lineage(urn, project, token, out_dir, dry_run)


if __name__ == "__main__":
    main()
