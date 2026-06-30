#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if GOWORK=off go list -m all | grep -E 'github\.com/winghv/SuperWHV-cloud|superwhv-cloud-platform|/platform($|/)' >/dev/null; then
  echo "AgentWharf must not depend on platform modules" >&2
  exit 1
fi

checked_files="$(git ls-files --cached --others --exclude-standard \
  ':!:scripts/check-boundary.sh' \
  ':!:.github/workflows/ci.yml')"

if [ -z "$checked_files" ]; then
  exit 0
fi

matches="$(grep -InE '\bVM\b|\bvm_[[:alnum:]_]*\b|\bagent_vms\b|\bvm_specs\b|\bbilling\b|\bentitlement\b|\btenant\b|\bcontrol[ -]?plane\b|\bnode[ -]?agent\b|计费|租户|套餐' $checked_files || true)"
violations="$(printf '%s\n' "$matches" | grep -vE '^(spec/v1\.md|hub/websocket_test\.go):.*(session\.idle_warning|\bVM\b|\bvm_[[:alnum:]_]*\b)' || true)"

if [ -n "$violations" ]; then
  printf '%s\n' "$violations"
  echo "AgentWharf must stay platform-neutral; remove platform-only concepts from tracked files" >&2
  exit 1
fi
