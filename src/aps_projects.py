#!/usr/bin/env python3
"""List the projects inside an ACC/BIM 360 hub.

Uses a 2-legged (app) token by default: unattended, and independent of whether
any particular person is a member of the account. Pass --user to use the
3-legged flow instead, which only sees hubs you personally belong to.

    python3 aps_projects.py                       # default hub below
    python3 aps_projects.py <hub-or-account-id>
    python3 aps_projects.py <id> --all            # include archived
    python3 aps_projects.py <id> --user           # 3-legged instead
"""
import json, sys, urllib.error, urllib.parse, urllib.request

from aps_auth import get_app_token, get_token

BASE = "https://developer.api.autodesk.com"
DEFAULT_HUB = "b.11111111-1111-1111-1111-111111111111"   # ACC account hub


def get(url, token):
    try:
        with urllib.request.urlopen(urllib.request.Request(
                url, headers={"Authorization": f"Bearer {token}"})) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{url}\n  failed {e.code}: {e.read().decode()[:300]}")


def hub_name(hub_id, token):
    d = get(f"{BASE}/project/v1/hubs/{hub_id}", token)
    return d["data"]["attributes"]["name"]


def projects(account_id, token):
    """Account Admin API - richer than /project/v1, gives status and type."""
    out, offset, limit = [], 0, 100
    while True:
        page = get(f"{BASE}/hq/v1/accounts/{account_id}"
                   f"/projects?limit={limit}&offset={offset}", token)
        out += page
        if len(page) < limit:
            return out
        offset += limit


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    hub_id = args[0] if args else DEFAULT_HUB
    if not hub_id.startswith("b."):
        hub_id = "b." + hub_id

    token = get_token() if "--user" in sys.argv else get_app_token()
    name = hub_name(hub_id, token)
    rows = projects(hub_id[2:], token)

    show_all = "--all" in sys.argv
    active = [p for p in rows if p.get("status") == "active"]
    shown = rows if show_all else active

    print(f"{name}  ({hub_id})")
    print(f"{len(rows)} project(s) total - {len(active)} active"
          f"{'' if show_all else '  [--all to include archived]'}\n")

    width = max((len(p.get("name") or "") for p in shown), default=0)
    for p in sorted(shown, key=lambda p: (p.get("name") or "").lower()):
        flag = "" if p.get("status") == "active" else f"  ({p.get('status')})"
        print(f"  {(p.get('name') or '<unnamed>'):<{width}}  {p.get('id')}"
              f"  {p.get('project_type') or '-'}{flag}")


if __name__ == "__main__":
    main()
