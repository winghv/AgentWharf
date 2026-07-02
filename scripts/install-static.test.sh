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
printf '%s\n' "$dry_run_output" | grep -F "install_wharf=/tmp/agentwharf-bin/wharf" >/dev/null
if printf '%s\n' "$dry_run_output" | grep -F "install_agentwharf=" >/dev/null; then
  echo "installer must expose only the wharf command" >&2
  exit 1
fi

existing_install_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-existing.XXXXXX")
canonical_existing_install_dir=$(CDPATH= cd -- "$existing_install_dir" && pwd -P)
touch "$existing_install_dir/agentwharf"
chmod 0755 "$existing_install_dir/agentwharf"
ln -sf agentwharf "$existing_install_dir/wharf"
upgrade_dry_run_output=$(
  PATH="$existing_install_dir:$PATH" \
  AGENTWHARF_INSTALL_DRY_RUN=1 \
  AGENTWHARF_VERSION=v0.1.2 \
  AGENTWHARF_OS=linux \
  AGENTWHARF_ARCH=amd64 \
  scripts/install.sh
)

printf '%s\n' "$upgrade_dry_run_output" | grep -F "install_mode=upgrade" >/dev/null
printf '%s\n' "$upgrade_dry_run_output" | grep -F "existing_wharf=$existing_install_dir/wharf" >/dev/null
printf '%s\n' "$upgrade_dry_run_output" | grep -F "install_wharf=$canonical_existing_install_dir/wharf" >/dev/null
printf '%s\n' "$upgrade_dry_run_output" | grep -F "cleanup_legacy_agentwharf=$canonical_existing_install_dir/agentwharf" >/dev/null

fake_release_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-release.XXXXXX")
fake_payload_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-payload.XXXXXX")
fake_install_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-install.XXXXXX")
printf '#!/bin/sh\nprintf upgraded-wharf\n' >"$fake_payload_dir/agentwharf"
chmod 0755 "$fake_payload_dir/agentwharf"
(cd "$fake_payload_dir" && tar -czf "$fake_release_dir/agentwharf-linux-amd64.tar.gz" agentwharf)
(cd "$fake_release_dir" && shasum -a 256 agentwharf-linux-amd64.tar.gz | awk '{print $1 "  agentwharf-linux-amd64.tar.gz"}' >checksums.txt)
printf '#!/bin/sh\nprintf legacy-agentwharf\n' >"$fake_install_dir/agentwharf"
chmod 0755 "$fake_install_dir/agentwharf"
ln -sf agentwharf "$fake_install_dir/wharf"
PATH="$fake_install_dir:$PATH" \
  AGENTWHARF_RELEASE_BASE="file://$fake_release_dir" \
  AGENTWHARF_VERSION=v0.1.2 \
  AGENTWHARF_OS=linux \
  AGENTWHARF_ARCH=amd64 \
  AGENTWHARF_SKIP_PROVIDER_BRIDGES=1 \
  scripts/install.sh >/dev/null

test -x "$fake_install_dir/wharf"
test ! -L "$fake_install_dir/wharf"
test ! -e "$fake_install_dir/agentwharf"
test "$("$fake_install_dir/wharf")" = "upgraded-wharf"

test -f .github/workflows/release.yml || {
  echo ".github/workflows/release.yml must publish release assets" >&2
  exit 1
}

grep -F "agentwharf-linux-amd64.tar.gz" .github/workflows/release.yml >/dev/null
grep -F "scripts/install.sh" .github/workflows/release.yml >/dev/null
grep -F "curl -fsSL https://github.com/winghv/agentwharf/releases/latest/download/install.sh | sh" README.md >/dev/null
grep -F "@agentclientprotocol/claude-agent-acp" scripts/install.sh >/dev/null
grep -F "@agentclientprotocol/codex-acp" scripts/install.sh >/dev/null
grep -F "Run the same install command again to upgrade" README.md >/dev/null

if grep -F "go install github.com/winghv/agentwharf/cmd/agentwharf" README.md >/dev/null; then
  echo "README quickstart must not require Go module installation" >&2
  exit 1
fi
