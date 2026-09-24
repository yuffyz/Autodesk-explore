#!/usr/bin/env python3
"""APS 3-legged OAuth against http://localhost:8080/

Import `get_token()` from other scripts; it caches the token on disk and
refreshes it silently, so the browser dance only happens once.
Run directly to force a fresh login and print the raw token response.
"""
import base64, http.server, json, os, secrets, stat, time
import urllib.error, urllib.parse, urllib.request, webbrowser
from pathlib import Path

# CLIENT_ID     = os.environ.get("APS_CLIENT_ID", "")
# CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET", "")
CLIENT_ID     = "***REMOVED-CLIENT-ID***"
CLIENT_SECRET = "***REMOVED-CLIENT-SECRET***"
REDIRECT_URI  = "http://localhost:8080/"          # must match the portal EXACTLY
# offline_access is what gets us a refresh_token, so we can skip the browser later.
SCOPES        = "data:read data:write account:read offline_access"

AUTH  = "https://developer.api.autodesk.com/authentication/v2/authorize"
TOKEN = "https://developer.api.autodesk.com/authentication/v2/token"

CACHE = Path.home() / ".aps_token.json"


def _basic_auth():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit(
            "APS_CLIENT_ID / APS_CLIENT_SECRET are not set.\n"
            "  export APS_CLIENT_ID=...\n"
            "  export APS_CLIENT_SECRET=...\n"
            "Get them from https://aps.autodesk.com/myapps/ and make sure the app's\n"
            f"Callback URL is exactly {REDIRECT_URI}"
        )
    return "Basic " + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()


def _post_token(form):
    req = urllib.request.Request(
        TOKEN,
        data=urllib.parse.urlencode(form).encode(),
        headers={
            "Authorization": _basic_auth(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"token request failed ({e.code}): {e.read().decode()}") from None


def _save(tok):
    tok = dict(tok)
    tok["expires_at"] = time.time() + tok.get("expires_in", 3600)
    CACHE.write_text(json.dumps(tok, indent=2))
    CACHE.chmod(stat.S_IRUSR | stat.S_IWUSR)          # 0600 - it is a credential
    return tok


def _load():
    try:
        return json.loads(CACHE.read_text())
    except (OSError, ValueError):
        return None


def login(scopes=SCOPES, prompt_login=False):
    """Full 3-legged browser flow. Returns the token response dict.

    prompt_login=True forces Autodesk to re-ask for credentials instead of
    silently reusing the SSO session already in the browser - which is how you
    end up authenticated as the wrong identity and seeing the wrong hubs.
    """
    _basic_auth()                                     # fail fast, before the browser opens
    state, got = secrets.token_urlsafe(16), {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            got.update({k: v[0] for k, v in
                        urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).items()})
            self.send_response(200); self.end_headers()
            self.wfile.write(b"Done - you can close this tab.")
        def log_message(self, *a): pass

    params = {
        "response_type": "code", "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI, "scope": scopes, "state": state,
    }
    if prompt_login:
        params["prompt"] = "login"
    url = AUTH + "?" + urllib.parse.urlencode(params)
    print("Opening:", url)

    srv = http.server.HTTPServer(("localhost", 8080), Handler)
    webbrowser.open(url)
    while "code" not in got and "error" not in got:   # skips favicon hits
        srv.handle_request()

    if "error" in got:
        raise SystemExit(f"authorize failed: {got}")
    if got.get("state") != state:
        raise SystemExit("state mismatch - discarding")

    return _save(_post_token({
        "grant_type": "authorization_code",
        "code": got["code"],
        "redirect_uri": REDIRECT_URI,                 # same string as above
    }))


def get_token(scopes=SCOPES, force=False, prompt_login=False):
    """Return a valid access token, reusing / refreshing the cached one."""
    if not force:
        tok = _load()
        if tok and tok.get("expires_at", 0) > time.time() + 60:
            return tok["access_token"]
        if tok and tok.get("refresh_token"):
            try:
                return _save(_post_token({
                    "grant_type": "refresh_token",
                    "refresh_token": tok["refresh_token"],
                    "scope": scopes,
                }))["access_token"]
            except SystemExit:
                pass                                   # refresh died - fall through to login
    return login(scopes, prompt_login=prompt_login or force)["access_token"]


def get_app_token(scopes="account:read data:read data:write"):
    """2-legged (client_credentials) token: the APP's identity, no user.

    Use this for unattended extraction. It does not depend on any human being a
    member of the account - only on the app's Client ID being provisioned in
    that ACC account - so it sees every hub the app is registered with.
    Not cached: these are cheap to mint and short-lived.
    """
    return _post_token({"grant_type": "client_credentials", "scope": scopes})["access_token"]


def whoami(token):
    """Return the Autodesk identity a token actually belongs to."""
    req = urllib.request.Request(
        "https://api.userprofile.autodesk.com/userinfo",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


if __name__ == "__main__":
    print(json.dumps(login(), indent=2))
