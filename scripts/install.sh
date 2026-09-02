#!/bin/sh
set -eu

repo=${AGENTWHARF_REPO:-winghv/agentwharf}
version=${AGENTWHARF_VERSION:-latest}
provider_dir=${AGENTWHARF_PROVIDER_DIR:-${HOME:-}/.agentwharf/providers}
claude_acp_package=${AGENTWHARF_CLAUDE_ACP_PACKAGE:-@agentclientprotocol/claude-agent-acp@0.54.1}
codex_acp_package=${AGENTWHARF_CODEX_ACP_PACKAGE:-@agentclientprotocol/codex-acp@1.0.2}
dsh_version=${AGENTWHARF_DSH_VERSION:-0.1.1-rc.2}
dsh_acp_activity_package=${AGENTWHARF_DSH_ACP_ACTIVITY_PACKAGE:-@winghv/dsh-acp-activity@0.1.1-rc.2.1}
dsh_config_asset=dsh-cordis.yml

say() {
  printf 'wharf: %s\n' "$*" >&2
}

fail() {
  say "$*"
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

command_path() {
  path=$(command -v "$1" 2>/dev/null || true)
  case "$path" in
    */*) printf '%s\n' "$path" ;;
    *) ;;
  esac
}

command_dir() {
  path="$1"
  dir=$(dirname "$path")
  (CDPATH= cd -- "$dir" 2>/dev/null && pwd -P) || return 1
}

detect_existing_install_dir() {
  path=$(command_path wharf)
  if [ -z "$path" ]; then
    path=$(command_path agentwharf)
  fi
  [ -n "$path" ] || return 1
  command_dir "$path"
}

existing_wharf=$(command_path wharf)
existing_agentwharf=$(command_path agentwharf)
existing_install_dir=$(detect_existing_install_dir || true)
if [ "${AGENTWHARF_INSTALL_DIR+x}" = "x" ]; then
  install_dir=$AGENTWHARF_INSTALL_DIR
elif [ -n "$existing_install_dir" ]; then
  install_dir=$existing_install_dir
else
  install_dir=/usr/local/bin
fi

install_mode=install
if [ -n "$existing_install_dir" ] && [ "$install_dir" = "$existing_install_dir" ]; then
  install_mode=upgrade
fi

detect_os() {
  case "$(uname -s)" in
    Darwin) printf 'darwin' ;;
    Linux) printf 'linux' ;;
    MINGW* | MSYS* | CYGWIN*) fail "Windows detected in Git Bash; run PowerShell: irm https://github.com/$repo/releases/latest/download/install.ps1 | iex" ;;
    *) fail "unsupported OS: $(uname -s)" ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64 | amd64) printf 'amd64' ;;
    arm64 | aarch64) printf 'arm64' ;;
    *) fail "unsupported architecture: $(uname -m)" ;;
  esac
}

os=${AGENTWHARF_OS:-$(detect_os)}
arch=${AGENTWHARF_ARCH:-$(detect_arch)}

case "$os-$arch" in
  darwin-amd64 | darwin-arm64 | linux-amd64 | linux-arm64) ;;
  *) fail "unsupported target: $os-$arch" ;;
esac

asset="agentwharf-$os-$arch.tar.gz"
if [ "$version" = "latest" ]; then
  release_base="https://github.com/$repo/releases/latest/download"
else
  release_base="https://github.com/$repo/releases/download/$version"
fi
release_base=${AGENTWHARF_RELEASE_BASE:-$release_base}
asset_url="$release_base/$asset"
checksum_url="$release_base/checksums.txt"

if [ "${AGENTWHARF_INSTALL_DRY_RUN:-}" = "1" ]; then
  printf 'asset_url=%s\n' "$asset_url"
  printf 'checksum_url=%s\n' "$checksum_url"
  printf 'install_mode=%s\n' "$install_mode"
  [ -n "$existing_wharf" ] && printf 'existing_wharf=%s\n' "$existing_wharf"
  [ -n "$existing_agentwharf" ] && printf 'existing_agentwharf=%s\n' "$existing_agentwharf"
  printf 'install_wharf=%s\n' "$install_dir/wharf"
  if [ -e "$install_dir/agentwharf" ] || [ -L "$install_dir/agentwharf" ]; then
    printf 'cleanup_legacy_agentwharf=%s\n' "$install_dir/agentwharf"
  fi
  printf 'provider_dir=%s\n' "$provider_dir"
  printf 'provider_package=%s\n' "$claude_acp_package"
  printf 'provider_package=%s\n' "$codex_acp_package"
  if [ "${AGENTWHARF_SKIP_DSH:-}" != "1" ] && [ "${AGENTWHARF_SKIP_PROVIDER_BRIDGES:-}" != "1" ]; then
    printf 'provider_package=%s\n' "$dsh_acp_activity_package"
    printf 'dsh_version=%s\n' "$dsh_version"
    printf 'dsh_config_url=%s/%s\n' "$release_base" "$dsh_config_asset"
  fi
  exit 0
fi

need curl
need tar

if [ "$install_mode" = "upgrade" ]; then
  say "upgrading existing Wharf in $install_dir"
else
  say "installing Wharf in $install_dir"
  if [ -n "$existing_install_dir" ] && [ "$install_dir" != "$existing_install_dir" ]; then
    say "existing Wharf found in $existing_install_dir; AGENTWHARF_INSTALL_DIR overrides the upgrade target"
  fi
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf.XXXXXX")
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

archive="$tmp_dir/$asset"
checksums="$tmp_dir/checksums.txt"
dsh_config="$tmp_dir/$dsh_config_asset"

say "downloading $asset_url"
curl -fsSL "$asset_url" -o "$archive"

if curl -fsSL "$checksum_url" -o "$checksums"; then
  expected=$(grep "  $asset\$" "$checksums" | awk '{print $1}' || true)
  [ -n "$expected" ] || fail "checksum for $asset not found"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$tmp_dir" && grep "  $asset\$" checksums.txt | sha256sum -c - >/dev/null)
  elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$archive" | awk '{print $1}')
    [ "$actual" = "$expected" ] || fail "checksum mismatch for $asset"
  else
    say "sha256sum or shasum not found; skipping checksum verification"
  fi
else
  fail "failed to download checksums.txt"
fi

install_dsh=0
if [ "${AGENTWHARF_SKIP_DSH:-}" != "1" ] && [ "${AGENTWHARF_SKIP_PROVIDER_BRIDGES:-}" != "1" ]; then
  install_dsh=1
  dsh_config_url="$release_base/$dsh_config_asset"
  say "downloading $dsh_config_url"
  curl -fsSL "$dsh_config_url" -o "$dsh_config"
  expected_dsh=$(grep "  $dsh_config_asset\$" "$checksums" | awk '{print $1}' || true)
  [ -n "$expected_dsh" ] || fail "checksum for $dsh_config_asset not found"
  if command -v sha256sum >/dev/null 2>&1; then
    actual_dsh=$(sha256sum "$dsh_config" | awk '{print $1}')
  elif command -v shasum >/dev/null 2>&1; then
    actual_dsh=$(shasum -a 256 "$dsh_config" | awk '{print $1}')
  else
    fail "sha256sum or shasum is required to verify $dsh_config_asset"
  fi
  [ "$actual_dsh" = "$expected_dsh" ] || fail "checksum mismatch for $dsh_config_asset"
fi

tar -xzf "$archive" -C "$tmp_dir"
[ -f "$tmp_dir/agentwharf" ] || fail "release archive does not contain agentwharf"
chmod 0755 "$tmp_dir/agentwharf"

run_install() {
  if "$@" 2>/dev/null; then
    return 0
  fi
  command -v sudo >/dev/null 2>&1 || fail "cannot write to $install_dir and sudo is unavailable"
  sudo "$@"
}

quote_sh() {
  printf "%s" "$1" | sed "s/'/'\\\\''/g; s/^/'/; s/$/'/"
}

write_provider_wrapper() {
  wrapper="$1"
  target="$2"
  quoted_target=$(quote_sh "$target")
  {
    printf '#!/bin/sh\n'
    printf 'exec %s "$@"\n' "$quoted_target"
  } >"$wrapper"
  chmod 0755 "$wrapper"
}

install_provider_bridges() {
  if [ "${AGENTWHARF_SKIP_PROVIDER_BRIDGES:-}" = "1" ]; then
    say "skipping ACP provider bridge installation"
    return 0
  fi
  [ -n "$provider_dir" ] || fail "AGENTWHARF_PROVIDER_DIR is empty"
  need npm

  say "installing ACP provider bridges in $provider_dir"
  mkdir -p "$provider_dir"
  npm install --prefix "$provider_dir" --omit=dev "$claude_acp_package" "$codex_acp_package" >/dev/null
  if [ "$install_dsh" = "1" ]; then
    npm install --prefix "$provider_dir" --omit=dev \
      "$dsh_acp_activity_package" \
      "@deepseek-ai/dsh-llm-deepseek@$dsh_version" \
      "@deepseek-ai/dsh-sandbox-local@$dsh_version" \
      "@deepseek-ai/dsh-sandbox-policy@$dsh_version" \
      "@deepseek-ai/dsh-subprocess-local@$dsh_version" \
      "@deepseek-ai/dsh-bash-sandbox@$dsh_version" \
      "@deepseek-ai/dsh-user-approval@$dsh_version" \
      "@deepseek-ai/dsh-fs-sandbox@$dsh_version" \
      "@deepseek-ai/dsh-fs-observation-policy@$dsh_version" \
      "@deepseek-ai/dsh-tool-fs@$dsh_version" \
      "@deepseek-ai/dsh-tool-todo@$dsh_version" \
      "@deepseek-ai/dsh-token-meter@$dsh_version" \
      "@deepseek-ai/dsh-compaction-basic@$dsh_version" >/dev/null
  fi

  claude_bridge="$provider_dir/node_modules/.bin/claude-agent-acp"
  codex_bridge="$provider_dir/node_modules/.bin/codex-acp"
  [ -x "$claude_bridge" ] || fail "claude-agent-acp was not installed"
  [ -x "$codex_bridge" ] || fail "codex-acp was not installed"

  write_provider_wrapper "$tmp_dir/claude-agent-acp" "$claude_bridge"
  write_provider_wrapper "$tmp_dir/codex-acp" "$codex_bridge"
  if [ "$install_dsh" = "1" ]; then
    dsh_bridge="$provider_dir/node_modules/.bin/dsh-acp-activity"
    [ -x "$dsh_bridge" ] || fail "dsh-acp-activity was not installed"
    write_provider_wrapper "$tmp_dir/dsh-acp-activity" "$dsh_bridge"
  fi
}

install_provider_bridges
run_install mkdir -p "$install_dir"
if [ -e "$install_dir/wharf" ] || [ -L "$install_dir/wharf" ]; then
  run_install rm -f "$install_dir/wharf"
fi
run_install cp "$tmp_dir/agentwharf" "$install_dir/wharf"
run_install chmod 0755 "$install_dir/wharf"
if [ -e "$install_dir/agentwharf" ] || [ -L "$install_dir/agentwharf" ]; then
  run_install rm -f "$install_dir/agentwharf"
fi
if [ "${AGENTWHARF_SKIP_PROVIDER_BRIDGES:-}" != "1" ]; then
  run_install cp "$tmp_dir/claude-agent-acp" "$install_dir/claude-agent-acp"
  run_install cp "$tmp_dir/codex-acp" "$install_dir/codex-acp"
  if [ "$install_dsh" = "1" ]; then
    run_install cp "$tmp_dir/dsh-acp-activity" "$install_dir/dsh-acp-activity"
    run_install mkdir -p "$provider_dir/dsh"
    run_install cp "$dsh_config" "$provider_dir/dsh/cordis.yml"
    run_install chmod 0600 "$provider_dir/dsh/cordis.yml"
  fi
fi

say "installed $install_dir/wharf"
if [ "${AGENTWHARF_SKIP_PROVIDER_BRIDGES:-}" != "1" ]; then
  say "installed $install_dir/claude-agent-acp"
  say "installed $install_dir/codex-acp"
  if [ "$install_dsh" = "1" ]; then
    say "installed $install_dir/dsh-acp-activity"
    say "installed $provider_dir/dsh/cordis.yml"
  fi
fi

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) say "$install_dir is not on PATH; add it before running wharf" ;;
esac
