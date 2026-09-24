#!/usr/bin/env python3
"""Build the Secrets Manager payload for the Glue job from local files.

Reads the client credentials out of src/aps_auth.py and the SSA private key out
of secrets/, and emits the exact JSON the job expects. Nothing is printed to the
terminal unless you ask for it.

    python3 glue/make_secret.py --check          # verify the values mint a token
    python3 glue/make_secret.py --out secret.json
    python3 glue/make_secret.py --stdout | pbcopy

Then, to store it (the CLI reads the file, so the secret never lands in shell history):

    aws secretsmanager create-secret --name aps/acc-extract \
        --description "Autodesk APS service account for ACC asset extraction" \
        --secret-string file://secret.json
    rm secret.json            # the PEM is a permanent credential - do not leave copies
"""
import argparse, glob, json, os, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))


def build():
    import aps_auth
    from acc_asset_files import SSA_ID, SSA_KID, SSA_PEM

    pem = Path(SSA_PEM)
    if not pem.exists():
        found = sorted(glob.glob(str(ROOT / "secrets" / "ssa_*.pem")))
        if not found:
            sys.exit(f"No SSA private key found. Expected {SSA_PEM}")
        pem = Path(found[0])

    if not aps_auth.CLIENT_ID or not aps_auth.CLIENT_SECRET:
        sys.exit("CLIENT_ID / CLIENT_SECRET are empty in src/aps_auth.py")

    return {
        "client_id":       aps_auth.CLIENT_ID,
        "client_secret":   aps_auth.CLIENT_SECRET,
        "ssa_id":          SSA_ID,
        "ssa_kid":         SSA_KID,
        "ssa_private_key": pem.read_text(),
    }


def check(creds):
    """Prove these five values actually mint a token before they are stored."""
    import base64, time, urllib.error, urllib.parse, urllib.request, uuid
    import jwt

    now = int(time.time())
    assertion = jwt.encode(
        {"iss": creds["client_id"], "sub": creds["ssa_id"],
         "aud": "https://developer.api.autodesk.com/authentication/v2/token",
         "exp": now + 300, "iat": now, "jti": str(uuid.uuid4()),
         "scope": ["data:read", "account:read"]},
        creds["ssa_private_key"], algorithm="RS256", headers={"kid": creds["ssa_kid"]})
    basic = base64.b64encode(
        f"{creds['client_id']}:{creds['client_secret']}".encode()).decode()
    req = urllib.request.Request(
        "https://developer.api.autodesk.com/authentication/v2/token",
        data=urllib.parse.urlencode({
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion, "scope": "data:read account:read"}).encode(),
        headers={"Authorization": f"Basic {basic}",
                 "Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req) as r:
            d = json.load(r)
        print(f"OK - token minted, expires_in={d.get('expires_in')}")
        return True
    except urllib.error.HTTPError as e:
        print(f"FAILED {e.code}: {e.read().decode()[:200]}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", help="write the JSON to this file")
    ap.add_argument("--stdout", action="store_true", help="print the JSON (contains secrets)")
    ap.add_argument("--check", action="store_true", help="verify the values mint a token")
    a = ap.parse_args()

    creds = build()

    # Always show the shape with values redacted, so a plain run is safe to screenshot.
    preview = {k: (v[:6] + "…" if k != "ssa_private_key" else
                   v.splitlines()[0] + " …redacted…") for k, v in creds.items()}
    print(json.dumps(preview, indent=2))
    print(f"\n5 keys, {len(json.dumps(creds))} bytes "
          f"(Secrets Manager limit is 65536)\n")

    if a.check:
        if not check(creds):
            sys.exit(1)
    if a.out:
        Path(a.out).write_text(json.dumps(creds, indent=2))
        os.chmod(a.out, 0o600)
        print(f"written to {a.out} (0600) - delete it once the secret is stored")
    if a.stdout:
        print(json.dumps(creds, indent=2))


if __name__ == "__main__":
    main()
