#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

test -x scripts/install.sh || {
  echo "scripts/install.sh must exist and be executable" >&2
  exit 1
}

dry_run_output=$(
  AGENTWHARF_INSTALL_DRY_RUN=1 \
  AGENTWHARF_VERSION=v0.1.2 \
  AGENTWHARF_OS=linux \
  AGENTWHARF_ARCH=amd64 \
  AGENTWHARF_INSTALL_DIR=/tmp/agentwharf-bin \
  scripts/install.sh
)

printf '%s\n' "$dry_run_output" | grep -F "https://github.com/winghv/agentwharf/releases/download/v0.1.2/agentwharf-linux-amd64.tar.gz" >/dev/null
printf '%s\n' "$dry_run_output" | grep -F "/tmp/agentwharf-bin/agentwharf" >/dev/null
printf '%s\n' "$dry_run_output" | grep -F "/tmp/agentwharf-bin/wharf" >/dev/null

test -f .github/workflows/release.yml || {
  echo ".github/workflows/release.yml must publish release assets" >&2
  exit 1
}

grep -F "agentwharf-linux-amd64.tar.gz" .github/workflows/release.yml >/dev/null
grep -F "scripts/install.sh" .github/workflows/release.yml >/dev/null
grep -F "curl -fsSL https://github.com/winghv/agentwharf/releases/latest/download/install.sh | sh" README.md >/dev/null

if grep -F "go install github.com/winghv/agentwharf/cmd/agentwharf" README.md >/dev/null; then
  echo "README quickstart must not require Go module installation" >&2
  exit 1
fi
