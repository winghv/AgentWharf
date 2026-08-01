#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
output=$(mktemp "${TMPDIR:-/tmp}/agentwharf-postgres-static.XXXXXX")
trap 'rm -f "$output"' EXIT HUP INT TERM

if (
  unset AGENTWHARF_POSTGRES_TEST_DATABASE_URL SUPERWHV_TEST_DATABASE_URL DATABASE_URL
  "$script_dir/test-postgres.sh"
) >"$output" 2>&1; then
  printf '%s\n' 'test-postgres unexpectedly passed without a DSN' >&2
  exit 1
fi

grep -F 'test-postgres requires' "$output" >/dev/null
