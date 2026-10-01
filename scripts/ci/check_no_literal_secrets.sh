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

# `kubectl set env` with no -c writes into every container, init containers
# included, where values outlive the move to secretKeyRef (ST-N07, 1 Oct).
nocont=$(awk '
  FNR == 1 { cmd = ""; inset = 0 }
  /kubectl[[:space:]]+set[[:space:]]+env[[:space:]]+deployment/ { inset = 1; cmd = ""; start = FNR }
  inset { cmd = cmd " " $0 }
  inset && !/\\[[:space:]]*$/ {
    if (cmd !~ /[[:space:]]-c[[:space:]]/ && cmd !~ /--containers/) printf "%s:%d: set env names no container\n", FILENAME, start
    inset = 0
  }
' "${files[@]}")
if [ -n "$nocont" ]; then
  echo "kubectl set env must name the container with -c (it otherwise writes into init containers too):"
  echo "$nocont"
  exit 1
fi

if [ -n "$bad" ]; then
  echo "Literal secret written into a Deployment (use secret_put + secret_ref from scripts/ci/secret_env.sh):"
  echo "$bad"
  exit 1
fi
echo "no literal secrets in kubectl set env (${#files[@]} workflow files)"
