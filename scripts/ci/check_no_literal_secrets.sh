#!/usr/bin/env bash
# Fails when a deploy workflow writes a GitHub secret into a Deployment as a
# literal env value (audit ST-N07). `kubectl set env deployment/x KEY="${{
# secrets.KEY }}"` puts the value in the Deployment spec and every old
# ReplicaSet, where the default 'view' role can read it. Store it in
# app-secrets and reference it instead: see scripts/ci/secret_env.sh.
#
#   scripts/ci/check_no_literal_secrets.sh [workflow files...]
set -euo pipefail
cd "$(dirname "$0")/../.."
files=("$@")
[ ${#files[@]} -gt 0 ] || files=(.github/workflows/*.yml)

bad=$(awk '
  FNR == 1 { inset = 0 }
  /kubectl[[:space:]]+set[[:space:]]+env/ { inset = 1 }
  inset && /secrets\./ { printf "%s:%d: %s\n", FILENAME, FNR, $0 }
  inset && !/\\[[:space:]]*$/ { inset = 0 }
' "${files[@]}")

if [ -n "$bad" ]; then
  echo "Literal secret written into a Deployment (use secret_put + secret_ref from scripts/ci/secret_env.sh):"
  echo "$bad"
  exit 1
fi
echo "no literal secrets in kubectl set env (${#files[@]} workflow files)"
