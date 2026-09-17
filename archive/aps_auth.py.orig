#!/usr/bin/env python3
"""APS 3-legged OAuth against http://localhost:8080/"""
import base64, http.server, json, os, secrets, urllib.parse, urllib.request, webbrowser

# CLIENT_ID     = os.environ["APS_CLIENT_ID"]
# CLIENT_SECRET = os.environ["APS_CLIENT_SECRET"]
CLIENT_ID     = ""
CLIENT_SECRET = ""
REDIRECT_URI  = "http://localhost:8080/"          # must match the portal EXACTLY
SCOPES        = "data:read data:write account:read"

AUTH  = "https://developer.api.autodesk.com/authentication/v2/authorize"
TOKEN = "https://developer.api.autodesk.com/authentication/v2/token"

state, got = secrets.token_urlsafe(16), {}

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        got.update({k: v[0] for k, v in
                    urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).items()})
        self.send_response(200); self.end_headers()
        self.wfile.write(b"Done - you can close this tab.")
    def log_message(self, *a): pass

url = AUTH + "?" + urllib.parse.urlencode({
    "response_type": "code", "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI, "scope": SCOPES, "state": state,
})
print("Opening:", url)

srv = http.server.HTTPServer(("localhost", 8080), Handler)
webbrowser.open(url)
while "code" not in got and "error" not in got:   # skips favicon hits
    srv.handle_request()

if "error" in got:
    raise SystemExit(f"authorize failed: {got}")
if got.get("state") != state:
    raise SystemExit("state mismatch - discarding")

req = urllib.request.Request(
    TOKEN,
    data=urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": got["code"],
        "redirect_uri": REDIRECT_URI,             # same string as above
    }).encode(),
    headers={
        "Authorization": "Basic " + base64.b64encode(
            f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode(),
        "Content-Type": "application/x-www-form-urlencoded",
    },
)
print(json.dumps(json.load(urllib.request.urlopen(req)), indent=2))