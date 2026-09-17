#!/usr/bin/env python3
"""Manage APS Secure Service Accounts (SSA) - robot identities for unattended
extraction, so a pipeline stops depending on a real person's ACC membership.

    python3 aps_service_account.py list
    python3 aps_service_account.py create "Data Extraction Bot" --confirm
    python3 aps_service_account.py key <serviceAccountId> --confirm
    python3 aps_service_account.py token <serviceAccountId> <keyId> <key.pem> \
            --scope "data:read account:read"

create/key make real changes to your Autodesk tenant and need --confirm.

VERIFIED: the list endpoint and the SSA scopes, probed against the live API.
UNVERIFIED: create, key, and the JWT token exchange have not been run - doing so
creates a real identity. The JWT exchange in particular has two plausible shapes
(assertion vs client_assertion); mint_token tries both and reports which worked.
"""
import base64, json, os, stat, sys, time, urllib.error, urllib.parse, urllib.request, uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

import aps_auth

BASE = "https://developer.api.autodesk.com"
SA   = f"{BASE}/authentication/v2/service-accounts"

READ_SCOPES  = "application:service_account:read application:service_account_key:read"
WRITE_SCOPES = ("application:service_account:read application:service_account:write "
                "application:service_account_key:read application:service_account_key:write")


def app_token(scope):
    req = urllib.request.Request(
        aps_auth.TOKEN,
        data=urllib.parse.urlencode({"grant_type": "client_credentials",
                                     "scope": scope}).encode(),
        headers={"Authorization": aps_auth._basic_auth(),
                 "Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)["access_token"]
    except urllib.error.HTTPError as e:
        sys.exit(f"token failed {e.code}: {e.read().decode()[:250]}")


def call(url, token, method="GET", body=None):
    h = {"Authorization": f"Bearer {token}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode(); h["Content-Type"] = "application/json"
    try:
        with urllib.request.urlopen(
                urllib.request.Request(url, headers=h, data=data, method=method)) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:    return e.code, json.loads(raw)
        except Exception: return e.code, raw[:300]


def cmd_list():
    s, d = call(SA, app_token(READ_SCOPES))
    if s != 200:
        sys.exit(f"list failed {s}: {d}")
    accounts = d.get("serviceAccounts", [])
    if not accounts:
        print("No service accounts yet on this app.")
        return
    for a in accounts:
        print(f"  {a.get('firstName','')} {a.get('lastName','')}".rstrip())
        for k in ("serviceAccountId", "email", "status", "createdAt"):
            if a.get(k):
                print(f"     {k}: {a[k]}")


def cmd_create(name):
    first, _, last = name.partition(" ")
    s, d = call(SA, app_token(WRITE_SCOPES), "POST",
                {"name": name, "firstName": first, "lastName": last or "Bot"})
    print(f"{s}: {json.dumps(d, indent=2)[:800]}")
    if s in (200, 201):
        print("\nNext: create a key, then have an ACC admin invite this email "
              "to the project(s).")


def cmd_key(sa_id):
    s, d = call(f"{SA}/{sa_id}/keys", app_token(WRITE_SCOPES), "POST", {})
    if s not in (200, 201):
        sys.exit(f"key creation failed {s}: {d}")
    kid = d.get("kid") or d.get("keyId") or d.get("key_id")
    pem = d.get("privateKey") or d.get("private_key_pem")
    print(f"keyId: {kid}")
    if not pem:
        print(json.dumps(d, indent=2)[:600]); return
    secrets_dir = ROOT / "secrets"
    secrets_dir.mkdir(exist_ok=True)
    path = str(secrets_dir / f"ssa_{sa_id}_{kid}.pem")
    with open(path, "w") as f:
        f.write(pem)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)       # 0600
    print(f"private key written to {path} (0600)")
    print("Autodesk will not show this key again - back it up in a secret store now.")


def mint_token(sa_id, kid, pem_path, scope):
    """Exchange a signed JWT for an access token acting as the service account."""
    import jwt                                        # PyJWT
    key = open(pem_path).read()
    now = int(time.time())
    claims = {
        "iss": aps_auth.CLIENT_ID,
        "sub": sa_id,
        "aud": f"{BASE}/authentication/v2/token",
        "exp": now + 300,
        "iat": now,
        "jti": str(uuid.uuid4()),
        "scope": scope.split(),
    }
    assertion = jwt.encode(claims, key, algorithm="RS256", headers={"kid": kid})

    # Two documented-looking shapes; try both and report which the server accepts.
    attempts = [
        ("jwt-bearer / assertion", {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion, "scope": scope}),
        ("client_credentials / client_assertion", {
            "grant_type": "client_credentials",
            "client_assertion_type":
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": assertion, "scope": scope}),
    ]
    for label, form in attempts:
        req = urllib.request.Request(
            aps_auth.TOKEN, data=urllib.parse.urlencode(form).encode(),
            headers={"Authorization": aps_auth._basic_auth(),
                     "Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req) as r:
                d = json.load(r)
            print(f"OK via {label}")
            print(f"access_token: {d['access_token'][:24]}...  expires_in={d.get('expires_in')}")
            return d["access_token"]
        except urllib.error.HTTPError as e:
            print(f"  {label} -> {e.code} {e.read().decode()[:180]}")
    sys.exit("Both JWT exchange shapes failed - see errors above.")


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd = sys.argv[1]
    args = [a for a in sys.argv[2:] if not a.startswith("--")]

    if cmd == "list":
        cmd_list()
    elif cmd in ("create", "key"):
        if "--confirm" not in sys.argv:
            sys.exit(f"'{cmd}' changes your Autodesk tenant. Re-run with --confirm.")
        (cmd_create if cmd == "create" else cmd_key)(args[0])
    elif cmd == "token":
        scope = (sys.argv[sys.argv.index("--scope") + 1]
                 if "--scope" in sys.argv else "data:read account:read")
        mint_token(args[0], args[1], args[2], scope)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
