#!/bin/sh

set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
coverage_min=${AGENTWHARF_COVERAGE_MIN:-80}
dsn=${AGENTWHARF_POSTGRES_TEST_DATABASE_URL:-${SUPERWHV_TEST_DATABASE_URL:-${DATABASE_URL:-}}}

case "$coverage_min" in
  ''|*[!0-9.]*|*.*.*) printf '%s\n' "invalid AGENTWHARF_COVERAGE_MIN" >&2; exit 2 ;;
esac

if [ -z "$dsn" ]; then
  printf '%s\n' "test-postgres requires AGENTWHARF_POSTGRES_TEST_DATABASE_URL, SUPERWHV_TEST_DATABASE_URL, or DATABASE_URL" >&2
  exit 1
fi

export AGENTWHARF_POSTGRES_TEST_DATABASE_URL="$dsn"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-postgres.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

run_coverage_gate() {
  package=$1
  profile="$tmp_dir/$(printf '%s' "$package" | tr '/.' '__').cover"
  last_profile="$profile"
  printf '==> %s (race, PostgreSQL, coverage >= %s%%)\n' "$package" "$coverage_min"
  (
    cd "$repo_dir"
    go test -race -count=1 -covermode=atomic -coverprofile="$profile" "$package"
  )
  coverage=$(go tool cover -func="$profile" | awk '/^total:/ {gsub(/%/, "", $NF); print $NF}')
  if [ -z "$coverage" ]; then
    printf '%s\n' "coverage report missing for $package" >&2
    return 1
  fi
  if [ "$package" = "./hub" ]; then
    printf 'Hub package coverage (informational until T68A): %s%%\n' "$coverage"
    return 0
  fi
  if ! awk -v got="$coverage" -v want="$coverage_min" 'BEGIN { exit !(got + 0 >= want + 0) }'; then
    printf 'coverage gate failed for %s: %s%% < %s%%\n' "$package" "$coverage" "$coverage_min" >&2
    return 1
  fi
  printf 'coverage gate passed for %s: %s%%\n' "$package" "$coverage"
}

run_coverage_gate ./hub

hub_profile="$last_profile"
hub_core_coverage=$(awk '
  NR > 1 && $1 ~ /hub\/(activity_dispatcher|connection_dispatch|event_batcher|handshake|transport|warm_attach_credential)\.go:/ {
    total += $2
    if ($3 > 0) covered += $2
  }
  END {
    if (total == 0) exit 1
    printf "%.1f", (covered * 100) / total
  }
' "$hub_profile")
if ! awk -v got="$hub_core_coverage" -v want="$coverage_min" 'BEGIN { exit !(got + 0 >= want + 0) }'; then
  printf 'Hub core coverage gate failed: %s%% < %s%%\n' "$hub_core_coverage" "$coverage_min" >&2
  exit 1
fi
printf 'Hub core coverage gate passed: %s%% (history/attach/fence/dispatcher surfaces)\n' "$hub_core_coverage"

run_coverage_gate ./store/postgres
