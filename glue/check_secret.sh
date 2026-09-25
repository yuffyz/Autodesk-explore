#!/usr/bin/env bash
# Diagnose the stored secret WITHOUT printing the key material.
#   ./glue/check_secret.sh aps/acc-extract [region]
set -euo pipefail
SECRET="${1:?usage: check_secret.sh <secret-name> [region]}"
REGION="${2:-${AWS_REGION:-us-east-1}}"

aws secretsmanager get-secret-value --secret-id "$SECRET" --region "$REGION" \
  --query SecretString --output text | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except ValueError as e:
    sys.exit(f"SecretString is not valid JSON: {e}")

required = ["client_id","client_secret","ssa_id","ssa_kid","ssa_private_key"]
missing = [k for k in required if k not in d]
print(f"keys present : {sorted(d)}")
if missing:
    print(f"MISSING      : {missing}")

pem = d.get("ssa_private_key","")
real = pem.count("\n")
lit  = pem.count("\\n")
print(f"PEM length   : {len(pem)} chars")
print(f"real newlines: {real}")
print(f"literal \\\\n   : {lit}")

if lit and not real:
    print("\nDIAGNOSIS: the PEM was escaped twice - it holds the two characters")
    print("  backslash + n where line breaks belong. This is the exact cause of")
    print("  \"Could not parse the provided public key\". Rewrite the secret with file://")
    sys.exit(2)
if real < 3:
    print("\nDIAGNOSIS: too few line breaks to be a PEM.")
    sys.exit(2)
if not pem.startswith("-----BEGIN"):
    print("\nDIAGNOSIS: does not start with -----BEGIN.")
    sys.exit(2)
print("\nPEM structure looks correct.")
'
