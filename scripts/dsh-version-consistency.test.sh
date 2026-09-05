#!/bin/sh
set -eu

root=$(mktemp -d "${TMPDIR:-/tmp}/dsh-version-consistency.XXXXXX")
trap 'rm -rf "$root"' EXIT

packages="dsh-agent dsh-agent-loop dsh-tools dsh-code-runtime dsh-session dsh-llm"
for package in $packages; do
  directory="$root/node_modules/@deepseek-ai/$package"
  mkdir -p "$directory"
  printf '{"name":"@deepseek-ai/%s","version":"0.1.2-rc.1","main":"index.js"}\n' "$package" >"$directory/package.json"
  : >"$directory/index.js"
done

scripts/dsh-version-consistency.sh "$root" "0.1.2-rc.1" >/dev/null

printf '%s\n' '{"name":"@deepseek-ai/dsh-tools","version":"0.1.1-rc.2","main":"index.js"}' >"$root/node_modules/@deepseek-ai/dsh-tools/package.json"
if scripts/dsh-version-consistency.sh "$root" "0.1.2-rc.1" >/dev/null 2>&1; then
  echo "top-level mixed DSH package was accepted" >&2
  exit 1
fi

printf '%s\n' '{"name":"@deepseek-ai/dsh-tools","version":"0.1.2-rc.1","main":"index.js"}' >"$root/node_modules/@deepseek-ai/dsh-tools/package.json"
nested="$root/node_modules/@deepseek-ai/dsh-agent/node_modules/@deepseek-ai/dsh-stale"
mkdir -p "$nested"
printf '%s\n' '{"name":"@deepseek-ai/dsh-stale","version":"0.1.1-rc.2","main":"index.js"}' >"$nested/package.json"
: >"$nested/index.js"
if scripts/dsh-version-consistency.sh "$root" "0.1.2-rc.1" >/dev/null 2>&1; then
  echo "nested mixed DSH package was accepted" >&2
  exit 1
fi

printf 'dsh-version-consistency.test: ok\n'
