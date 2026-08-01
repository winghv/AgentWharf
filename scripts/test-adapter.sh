#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

profile=$(mktemp "${TMPDIR:-/tmp}/agentwharf-adapter-cover.XXXXXX")
trap 'rm -f "$profile"' EXIT

go test -race ./adapter/core -count=1 -coverprofile="$profile"
go vet ./adapter/core

coverage=$(go tool cover -func="$profile" | awk '/^total:/ {gsub("%", "", $3); print $3}')
if [[ -z "$coverage" ]]; then
	echo "adapter coverage total is unavailable" >&2
	exit 1
fi
awk -v coverage="$coverage" 'BEGIN { if (coverage < 80) { printf "adapter coverage %.1f%% is below 80%%\n", coverage > "/dev/stderr"; exit 1 } }'
printf 'PASS adapter gate coverage=%s%%\n' "$coverage"
