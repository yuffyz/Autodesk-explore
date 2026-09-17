#!/usr/bin/env python3
"""Ask the API directly about one hub, to tell 'not a member' apart from
'app not provisioned'.

/project/v1/hubs only lists a hub when BOTH are true:
  1. the signed-in user is a member of that account, and
  2. this app's Client ID is provisioned in that account.
The list endpoint silently omits the hub either way - but fetching the hub by
id distinguishes them:

    403  -> the hub exists and you are a member; the APP is not provisioned  <- fixable by an admin
    404  -> the hub does not exist, or you are not a member of it
    200  -> visible after all (so the problem was the list call, not access)

Usage:
    python3 aps_hub_probe.py <account-or-hub-id>

Grab the id from the ACC URL while you are inside the account, e.g.
https://acc.autodesk.com/.../accounts/<THIS-GUID>/...  - the leading "b." is
added for you if you leave it off.
"""
import json, sys, urllib.error, urllib.request

from aps_auth import get_token, whoami

BASE = "https://developer.api.autodesk.com"


def probe(hub_id, token):
    url = f"{BASE}/project/v1/hubs/{hub_id}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:400]


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)

    hub_id = sys.argv[1]
    if not hub_id.startswith("b."):
        hub_id = "b." + hub_id                        # ACC hub ids are "b." + account guid

    token = get_token()
    me = whoami(token)
    print(f"signed in as: {me.get('name')} <{me.get('email')}>")
    print(f"probing hub : {hub_id}\n")

    status, body = probe(hub_id, token)

    if status == 200:
        attrs = body["data"]["attributes"]
        print(f"200 - visible: {attrs.get('name')}  (region {attrs.get('region')})")
        print("     So it IS reachable; the list call was the wrong question.")
    elif status == 403:
        print("403 - You are a member, but this APP is not provisioned in the account.")
        print("     Fix: an ACC account admin opens")
        print("       Account Admin > Settings > Custom Integrations > Add Custom Integration")
        print("     and adds this Client ID. Then re-run aps_hubs.py.")
        print(f"\n     raw: {body}")
    elif status == 404:
        print("404 - No such hub for this user: either the id is wrong, or this")
        print("      Autodesk identity is not a member of that account.")
        print(f"\n     raw: {body}")
    else:
        print(f"{status} - unexpected\n     raw: {body}")


if __name__ == "__main__":
    main()
