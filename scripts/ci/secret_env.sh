#!/usr/bin/env bash
# Keep secrets out of the Deployment spec (audit ST-N07).
#
# A literal `value:` in a Deployment env is readable by anyone who can read
# Deployments or old ReplicaSets, and the default 'view' role hides Secrets but
# not those. Every secret therefore lives in the app-secrets Secret and reaches
# the pod through valueFrom.secretKeyRef. Sourced by the deploy workflows:
#
#   NS=studojo DEP=frontend . scripts/ci/secret_env.sh
#   secret_put   meta-capi-token "$META_CAPI_TOKEN"   # store (skips empty)
#   secret_adopt APOLLO_API_KEY apollo-api-key         # move a hand-set literal
#   secret_ref   RAZORPAY_KEY_SECRET razorpay-key-prod-secret
#   secret_audit                                       # warn on any literal left
#
# Same helper as job-outreach-svc/scripts/ci/secret_env.sh. Values never pass
# through the command line of anything but kubectl's patch body, and GitHub
# masks secrets in the log.
: "${NS:?NS is required}" "${DEP:?DEP is required}"  # no set -u: this is sourced into the deploy step

_b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

_secret_get() {  # the decoded value of app-secrets/KEY ("" when absent)
  kubectl -n "$NS" get secret app-secrets -o jsonpath="{.data.${1//./\\.}}" | base64 -d 2>/dev/null
}

secret_has() {  # secret_has KEY
  [ -n "$(kubectl -n "$NS" get secret app-secrets -o jsonpath="{.data.${1//./\\.}}")" ]
}

secret_put() {  # secret_put KEY VALUE  (an empty VALUE changes nothing)
  [ -n "${2:-}" ] || return 0
  kubectl -n "$NS" patch secret app-secrets --type=merge \
    -p "{\"data\":{\"$1\":\"$(_b64 "$2")\"}}" >/dev/null
  echo "app-secrets/$1 set"
}

_literal() {  # the plain value an env var holds on the Deployment today, if any
  kubectl -n "$NS" get "deployment/$DEP" \
    -o jsonpath="{.spec.template.spec.containers[?(@.name=='$DEP')].env[?(@.name=='$1')].value}"
}

secret_ref() {  # secret_ref ENV KEY: point ENV at app-secrets/KEY (drops any literal)
  if ! secret_has "$2"; then
    echo "::warning::app-secrets/$2 missing in $NS; $1 left as it is"
    return 0
  fi
  kubectl -n "$NS" patch "deployment/$DEP" --type=strategic -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"$DEP\",\"env\":[{\"name\":\"$1\",\"value\":null,\"valueFrom\":{\"secretKeyRef\":{\"name\":\"app-secrets\",\"key\":\"$2\"}}}]}]}}}}" >/dev/null
  echo "$1 -> app-secrets/$2"
}

secret_adopt() {  # secret_adopt ENV KEY: move a hand-set literal into app-secrets, then ref it
  local lit
  lit=$(_literal "$1")
  if ! secret_has "$2"; then
    secret_put "$2" "$lit"
  elif [ -n "$lit" ] && [ "$lit" != "$(_secret_get "$2")" ]; then
    # Never swap a working value for a different one on the way through: the
    # pod would start with another credential than it runs with today.
    echo "::warning::$DEP $1 differs from app-secrets/$2 in $NS; literal left in place, reconcile by hand"
    return 0
  fi
  secret_ref "$1" "$2"
}

strip_init_env() {  # init containers only wait for postgres/rabbitmq and need no app env
  # A `kubectl set env` without -c writes into EVERY container, init containers
  # included, so secrets once set that way stayed behind there as literals
  # after the main container moved to secretKeyRef (audit ST-N07, 1 Oct).
  local n i
  n=$(kubectl -n "$NS" get "deployment/$DEP" -o jsonpath='{range .spec.template.spec.initContainers[*]}{.name}{"\n"}{end}' | grep -c . || true)
  for ((i = n - 1; i >= 0; i--)); do
    if [ -n "$(kubectl -n "$NS" get "deployment/$DEP" -o jsonpath="{.spec.template.spec.initContainers[$i].env}")" ]; then
      kubectl -n "$NS" patch "deployment/$DEP" --type=json \
        -p "[{\"op\":\"remove\",\"path\":\"/spec/template/spec/initContainers/$i/env\"}]" >/dev/null
      echo "$DEP in $NS: removed env from init container $i"
    fi
  done
}

secret_audit() {  # warn about any secret-looking env var still held as a literal
  local left
  strip_init_env
  left=$(kubectl -n "$NS" get "deployment/$DEP" \
    -o jsonpath="{range .spec.template.spec.containers[?(@.name=='$DEP')].env[?(@.value)]}{.name}{'\n'}{end}{range .spec.template.spec.initContainers[*].env[?(@.value)]}{.name}{'\n'}{end}" |
    grep -E 'SECRET|TOKEN|PASSWORD|PEPPER|API_KEY|CONNECTION_STRING' || true)
  if [ -n "$left" ]; then
    echo "::warning::$DEP in $NS still holds literal secrets: $(echo "$left" | paste -sd, -)"
  else
    echo "$DEP in $NS: no literal secrets in the spec"
  fi
}
