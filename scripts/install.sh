#!/bin/sh
set -eu

repo=${AGENTWHARF_REPO:-winghv/agentwharf}
version=${AGENTWHARF_VERSION:-latest}
install_dir=${AGENTWHARF_INSTALL_DIR:-/usr/local/bin}

say() {
  printf 'agentwharf: %s\n' "$*" >&2
}

fail() {
  say "$*"
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

detect_os() {
  case "$(uname -s)" in
    Darwin) printf 'darwin' ;;
    Linux) printf 'linux' ;;
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
asset_url="$release_base/$asset"
checksum_url="$release_base/checksums.txt"

if [ "${AGENTWHARF_INSTALL_DRY_RUN:-}" = "1" ]; then
  printf 'asset_url=%s\n' "$asset_url"
  printf 'checksum_url=%s\n' "$checksum_url"
  printf 'install_agentwharf=%s\n' "$install_dir/agentwharf"
  printf 'install_wharf=%s\n' "$install_dir/wharf"
  exit 0
fi

need curl
need tar

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentwharf.XXXXXX")
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

archive="$tmp_dir/$asset"
checksums="$tmp_dir/checksums.txt"

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

run_install mkdir -p "$install_dir"
run_install cp "$tmp_dir/agentwharf" "$install_dir/agentwharf"
run_install chmod 0755 "$install_dir/agentwharf"
run_install ln -sf agentwharf "$install_dir/wharf"

say "installed $install_dir/agentwharf"
say "installed $install_dir/wharf"

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) say "$install_dir is not on PATH; add it before running wharf" ;;
esac
