#!/bin/sh
# Install herdr-mobile-bridge without sudo.
#
# Prefers a prebuilt release binary for this OS/arch and verifies its SHA-256.
# Falls back to building from source with `cargo install --git` (no prebuilt
# available, no network to GitHub releases, or no matching asset).
#
# Usage:  sh install.sh            # into ~/.local/bin
#         PREFIX=/usr/local sh install.sh
set -eu

REPO="34t0th3g4me/herdr-mobile-bridge"
BIN="herdr-mobile-bridge"
DEST="${PREFIX:-$HOME/.local}/bin"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

say() { printf '%s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# --- target triple -----------------------------------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
  Darwin/arm64)  triple="aarch64-apple-darwin" ;;
  Darwin/x86_64) triple="x86_64-apple-darwin" ;;
  Linux/x86_64)  triple="x86_64-unknown-linux-gnu" ;;
  Linux/aarch64) triple="aarch64-unknown-linux-gnu" ;;
  *) triple="" ;;
esac

install_binary() {
  mkdir -p "$DEST"
  install -m 0755 "$1" "$DEST/$BIN"
  say "installed $DEST/$BIN"
  "$DEST/$BIN" --version
}

# --- prebuilt release --------------------------------------------------------
try_prebuilt() {
  [ -n "$triple" ] || return 1
  command -v curl >/dev/null 2>&1 || return 1
  # Resolve the tag via the API so we always fetch the exact release (the
  # `releases/latest` alias is CDN-cached and can serve a stale asset).
  tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
  [ -n "$tag" ] || return 1
  base="https://github.com/$REPO/releases/download/$tag"
  asset="$BIN-$triple.tar.gz"
  say "downloading $asset"
  curl -fsSL "$base/$asset" -o "$TMP/$asset" || return 1
  # Verify the checksum when the release publishes one.
  if curl -fsSL "$base/SHA256SUMS" -o "$TMP/SHA256SUMS" 2>/dev/null; then
    expected="$(awk -v f="$asset" '$2==f {print $1}' "$TMP/SHA256SUMS")"
    # No entry for our asset (a release gap) is not an integrity failure:
    # fall back to building from source instead of trusting the tarball.
    [ -n "$expected" ] || return 1
    if command -v sha256sum >/dev/null 2>&1; then
      actual="$(sha256sum "$TMP/$asset" | awk '{print $1}')"
    else
      actual="$(shasum -a 256 "$TMP/$asset" | awk '{print $1}')"
    fi
    [ "$actual" = "$expected" ] || die "checksum mismatch for $asset"
    say "checksum ok"
  else
    say "warning: release publishes no SHA256SUMS; skipping verification"
  fi
  tar -xzf "$TMP/$asset" -C "$TMP" || return 1
  [ -f "$TMP/$BIN" ] || return 1
  install_binary "$TMP/$BIN"
}

# --- source fallback ---------------------------------------------------------
try_source() {
  command -v cargo >/dev/null 2>&1 || return 1
  say "building from source with cargo (this takes a minute)"
  cargo install --git "https://github.com/$REPO" --locked --root "$TMP/cargo" >/dev/null
  install_binary "$TMP/cargo/bin/$BIN"
}

if ! try_prebuilt; then
  if ! try_source; then
    die "no prebuilt asset for ${os}/${arch} and no working 'cargo'.
Install Rust (https://rustup.rs) and re-run, or install a release manually."
  fi
fi

say
say "Done. The Herdr Mobile app finds the bridge at:"
say "  $DEST/$BIN"
say "Over SSH it runs 'herdr-mobile-bridge stdio'; no ports are opened."
