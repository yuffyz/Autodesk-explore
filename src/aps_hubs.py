#!/usr/bin/env python3
"""Step 1 of the APS 'Download a file' tutorial: list the hubs I can reach.

    GET https://developer.api.autodesk.com/project/v1/hubs

Needs a 3-legged token with data:read - a 2-legged token returns an empty list,
because hubs are tied to a human's account, not to the app.

    python3 aps_hubs.py             # list hubs
    python3 aps_hubs.py --projects  # list hubs and the projects inside each one
    python3 aps_hubs.py --relogin   # re-auth as a different Autodesk user first
    python3 aps_hubs.py --app       # 2-legged: the APP's view, ignores user membership

A hub only shows up if BOTH are true: the signed-in user is a member of it, AND
the app's Client ID is provisioned in that account (ACC Account Admin > Settings
> Custom Integrations). A missing hub is nearly always one of those two.
"""
import json, sys, urllib.error, urllib.request

from aps_auth import get_app_token, get_token, whoami

BASE = "https://developer.api.autodesk.com"

# What the opaque extension.type strings actually mean, for the printout.
HUB_KIND = {
    "hubs:autodesk.core:Hub":          "Fusion Team / Teamhub",
    "hubs:autodesk.a360:PersonalHub":  "A360 personal hub",
    "hubs:autodesk.bim360:Account":    "ACC / BIM 360 account",
}


def get(path, token):
    req = urllib.request.Request(
        BASE + path, headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"GET {path} failed ({e.code}): {e.read().decode()}")


def main():
    if "--app" in sys.argv:
        token = get_app_token()
        print("using 2-legged app token (no user context)\n")
    else:
        token = get_token(force="--relogin" in sys.argv)
        me = whoami(token)
        print(f"signed in as: {me.get('name')} <{me.get('email')}>")
        print("(3-legged: only shows hubs this USER belongs to - try --app)\n")

    hubs = get("/project/v1/hubs", token).get("data", [])

    if not hubs:
        print(
            "No hubs returned.\n"
            "Usually this means the app has not been granted access to the account yet:\n"
            "an ACC/BIM 360 account admin has to add your Client ID under\n"
            "Account Admin > Settings > Custom Integrations (SaaS integrations).\n"
            "If you expected hubs here, first check the 'signed in as' line above."
        )
        return

    print(f"{len(hubs)} hub(s):\n")
    for h in hubs:
        attrs = h.get("attributes", {})
        ext_type = attrs.get("extension", {}).get("type", "")
        print(f"  {attrs.get('name', '<unnamed>')}")
        print(f"    id      : {h['id']}")
        print(f"    type    : {ext_type}  ({HUB_KIND.get(ext_type, 'unknown kind')})")
        region = attrs.get("region")
        if region:
            print(f"    region  : {region}")

        if "--projects" in sys.argv:
            projects = get(f"/project/v1/hubs/{h['id']}/projects", token).get("data", [])
            print(f"    projects: {len(projects)}")
            for p in projects:
                p_type = p.get("attributes", {}).get("extension", {}).get("type", "")
                print(f"      - {p.get('attributes', {}).get('name', '<unnamed>')}"
                      f"  [{p['id']}]  {p_type}")
        print()


if __name__ == "__main__":
    main()
