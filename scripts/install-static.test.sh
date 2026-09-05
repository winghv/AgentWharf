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
printf '%s\n' "$dry_run_output" | grep -F "provider_package=@agentclientprotocol/codex-acp@1.8.0" >/dev/null
printf '%s\n' "$dry_run_output" | grep -F "provider_package=@deepseek-ai/dsh@0.1.2-rc.1" >/dev/null
printf '%s\n' "$dry_run_output" | grep -F 'dsh_runtime_dir=' >/dev/null
printf '%s\n' "$dry_run_output" | grep -F "dsh_config_url=https://github.com/winghv/agentwharf/releases/download/v0.1.2/dsh-cordis.yml" >/dev/null
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

# Exercise the default DSH installation path with a fake npm registry/client.
dsh_release_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-dsh-release.XXXXXX")
dsh_install_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-dsh-install.XXXXXX")
dsh_provider_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-dsh-provider.XXXXXX")
dsh_fake_bin=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf-dsh-bin.XXXXXX")
printf '#!/bin/sh\nprintf dsh-enabled-wharf\n' >"$dsh_release_dir/agentwharf"
chmod 0755 "$dsh_release_dir/agentwharf"
(cd "$dsh_release_dir" && tar -czf "$dsh_release_dir/agentwharf-linux-amd64.tar.gz" agentwharf)
cp scripts/dsh/cordis.yml "$dsh_release_dir/dsh-cordis.yml"
printf '#!/bin/sh\nset -eu\nprefix=\nwhile [ "$#" -gt 0 ]; do\n  if [ "$1" = "--prefix" ]; then prefix=$2; shift 2; continue; fi\n  shift\ndone\nmkdir -p "$prefix/node_modules/.bin"\nfor bridge in claude-agent-acp codex-acp dsh; do\n  printf "#!/bin/sh\\nexit 0\\n" >"$prefix/node_modules/.bin/$bridge"\n  chmod 0755 "$prefix/node_modules/.bin/$bridge"\ndone\n' >"$dsh_fake_bin/npm"
chmod 0755 "$dsh_fake_bin/npm"
(cd "$dsh_release_dir" && shasum -a 256 agentwharf-linux-amd64.tar.gz dsh-cordis.yml | awk '{print $1 "  " $2}' >checksums.txt)
PATH="$dsh_fake_bin:$PATH" \
  AGENTWHARF_RELEASE_BASE="file://$dsh_release_dir" \
  AGENTWHARF_VERSION=v0.1.2 \
  AGENTWHARF_OS=linux \
  AGENTWHARF_ARCH=amd64 \
  AGENTWHARF_INSTALL_DIR="$dsh_install_dir" \
  AGENTWHARF_PROVIDER_DIR="$dsh_provider_dir" \
  scripts/install.sh >/dev/null
test -x "$dsh_install_dir/dsh"
test -f "$dsh_provider_dir/dsh/cordis.yml"
test "$("$dsh_install_dir/wharf")" = "dsh-enabled-wharf"
rm -rf "$dsh_release_dir" "$dsh_install_dir" "$dsh_provider_dir" "$dsh_fake_bin"

test -f .github/workflows/release.yml || {
  echo ".github/workflows/release.yml must publish release assets" >&2
  exit 1
}

grep -F "agentwharf-linux-amd64.tar.gz" .github/workflows/release.yml >/dev/null
grep -F "scripts/install.sh" .github/workflows/release.yml >/dev/null
grep -F "scripts/install.ps1" .github/workflows/release.yml >/dev/null
grep -F "dsh-cordis.yml" .github/workflows/release.yml >/dev/null
test -f scripts/dsh/cordis.yml
grep -F "curl -fsSL https://github.com/winghv/agentwharf/releases/latest/download/install.sh | sh" README.md >/dev/null
grep -F "irm https://github.com/winghv/agentwharf/releases/latest/download/install.ps1 | iex" README.md >/dev/null
grep -F "MINGW* | MSYS* | CYGWIN*" scripts/install.sh >/dev/null
grep -F "Invoke-WebRequest" scripts/install.ps1 >/dev/null
grep -F "Get-FileHash" scripts/install.ps1 >/dev/null
grep -F "Node.js 22 or newer is required" scripts/install.ps1 >/dev/null
grep -F "Get-Command npm.cmd" scripts/install.ps1 >/dev/null
grep -F 'npm provider bridge installation failed (exit code' scripts/install.ps1 >/dev/null
grep -F "Node.js 22 or newer" README.md >/dev/null
grep -F "@agentclientprotocol/claude-agent-acp" scripts/install.sh >/dev/null
grep -F "@agentclientprotocol/codex-acp@1.8.0" scripts/install.sh >/dev/null
grep -F '@agentclientprotocol/codex-acp@1.8.0' scripts/install.ps1 >/dev/null
grep -F '@deepseek-ai/dsh@$dsh_version' scripts/install.sh >/dev/null
grep -F 'dsh_runtime_dir' scripts/install.sh >/dev/null
grep -F '@deepseek-ai/dsh@$dshVersion' scripts/install.ps1 >/dev/null
grep -F 'dshRuntimeDir' scripts/install.ps1 >/dev/null
test -x scripts/dsh-version-consistency.sh
grep -F '@deepseek-ai/dsh-agent-loop' scripts/dsh-version-consistency.sh >/dev/null
grep -F '@deepseek-ai/dsh-tools' scripts/dsh-version-consistency.sh >/dev/null
if grep -F '@winghv/dsh-acp-activity' scripts/install.sh scripts/install.ps1 >/dev/null; then
  echo "the retired DSH fork must not be installed" >&2
  exit 1
fi
grep -F "AGENTWHARF_SKIP_DSH" scripts/install.sh >/dev/null
grep -F "dsh-cordis.yml" scripts/install.ps1 >/dev/null
grep -F '@deepseek-ai/dsh@$dshVersion' scripts/install.ps1 >/dev/null
grep -F "Run the same install command again to upgrade" README.md >/dev/null

if grep -F "go install github.com/winghv/agentwharf/cmd/agentwharf" README.md >/dev/null; then
  echo "README quickstart must not require Go module installation" >&2
  exit 1
fi
