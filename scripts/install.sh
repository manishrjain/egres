#!/bin/sh
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Install the Egres binary from a GitHub release.
#
#   curl -fsSL https://egresql.com/install.sh | sh
#
# The script is served from this file, so it must stay POSIX sh: it runs
# under dash on Debian and under busybox in Alpine. Everything is inside
# main, called on the last line, so a download cut short runs nothing.
#
#   EGRES_VERSION      release tag to install, e.g. v0.2610.0 (default: the
#                      latest release; pre-releases need an explicit tag)
#   EGRES_INSTALL_DIR  where the binary goes (default: /usr/local/bin as
#                      root, ~/.local/bin otherwise)

set -eu

repo="manishrjain/egres"
releases="https://github.com/$repo/releases"

say() { printf '%s\n' "$*" >&2; }
die() { say "install.sh: $*"; exit 1; }

need() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

# fetch URL FILE: download URL to FILE. Exit 44 on a 404 so a missing
# asset reads as "no build for this platform", not a network error.
fetch() {
    if command -v curl >/dev/null 2>&1; then
        code=$(curl -fsL -o "$2" -w '%{http_code}' "$1") && return 0
        [ "$code" = "404" ] && return 44
        return 1
    fi
    wget -q -O "$2" "$1" 2>/dev/null && return 0
    rm -f "$2"
    return 1
}

# The tag of the latest release, from the redirect GitHub answers with:
# no API call, so no rate limit and no JSON parser.
latest_tag() {
    if command -v curl >/dev/null 2>&1; then
        url=$(curl -fsI -o /dev/null -w '%{redirect_url}' \
            "$releases/latest") || return 1
    else
        url=$(wget -q --max-redirect=0 -S -O /dev/null \
            "$releases/latest" 2>&1 | sed -n 's/^ *Location: *//p' | \
            head -n 1) || return 1
    fi
    tag=${url##*/}
    [ -n "$tag" ] && [ "$tag" != "latest" ] && [ "$tag" != "releases" ] \
        || return 1
    printf '%s\n' "$tag"
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d' ' -f1
    else
        die "sha256sum or shasum is required"
    fi
}

detect_platform() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$os" in
        Linux) os=linux ;;
        Darwin) os=macos ;;
        *) die "unsupported operating system: $os" ;;
    esac
    case "$arch" in
        x86_64 | amd64) arch=x86_64 ;;
        aarch64 | arm64) arch=aarch64 ;;
        *) die "unsupported architecture: $arch" ;;
    esac
    printf '%s-%s\n' "$os" "$arch"
}

# A PATH line for the user's shell when the install dir is not on it.
path_hint() {
    case ":$PATH:" in
        *":$1:"*) return 0 ;;
    esac
    say ""
    say "$1 is not on your PATH. Add it:"
    case "${SHELL:-}" in
        */fish) say "  fish_add_path $1" ;;
        */zsh) say "  echo 'export PATH=\"$1:\$PATH\"' >> ~/.zshrc" ;;
        *) say "  echo 'export PATH=\"$1:\$PATH\"' >> ~/.profile" ;;
    esac
    say "then open a new shell."
}

main() {
    command -v curl >/dev/null 2>&1 || need wget
    need tar
    need uname

    platform=$(detect_platform)
    version=${EGRES_VERSION:-}
    if [ -z "$version" ]; then
        version=$(latest_tag) || die "no release found at $releases"
    fi
    asset="egres-$version-$platform.tar.gz"

    if [ -n "${EGRES_INSTALL_DIR:-}" ]; then
        dir=$EGRES_INSTALL_DIR
    elif [ "$(id -u)" = "0" ]; then
        dir=/usr/local/bin
    else
        dir=$HOME/.local/bin
    fi

    tmp=$(mktemp -d 2>/dev/null || mktemp -d -t egres)
    trap 'rm -rf "$tmp"' EXIT

    say "Downloading egres $version for $platform ..."
    base="$releases/download/$version"
    fetch "$base/$asset" "$tmp/$asset" || {
        [ "$?" = "44" ] && die "no build of $version for $platform;" \
            "the builds available are listed at $base"
        die "download failed: $base/$asset"
    }
    fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" \
        || die "download failed: $base/SHA256SUMS"

    expected=$(grep " $asset\$" "$tmp/SHA256SUMS" | cut -d' ' -f1)
    [ -n "$expected" ] || die "$asset is not listed in SHA256SUMS"
    actual=$(sha256_of "$tmp/$asset")
    [ "$actual" = "$expected" ] || die "checksum mismatch for $asset" \
        "(expected $expected, got $actual)"

    tar -xzf "$tmp/$asset" -C "$tmp"
    [ -f "$tmp/egres" ] || die "$asset does not contain an egres binary"

    mkdir -p "$dir" || die "cannot create $dir"
    [ -w "$dir" ] || die "$dir is not writable; set EGRES_INSTALL_DIR" \
        "or run as root"
    # install(1) replaces the file atomically, so a running egres keeps
    # its old inode and the new one is complete before it is visible.
    install -m 755 "$tmp/egres" "$dir/egres"

    "$dir/egres" --help >/dev/null 2>&1 \
        || die "$dir/egres does not run on this machine"

    say "Installed egres $version to $dir/egres"
    path_hint "$dir"
    say ""
    say "Start a server (its store and WAL go under ./egres-data):"
    say "  egres"
    say "Connect with psql:"
    say "  psql -h localhost -p 3210 -d egres"
    say "To uninstall, delete $dir/egres."
}

main "$@"
