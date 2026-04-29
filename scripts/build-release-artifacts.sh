#!/usr/bin/env bash

set -euo pipefail

VERSION="${1:?usage: build-release-artifacts.sh <version> [output-dir]}"
OUTPUT_DIR="${2:-dist}"
ARCH="${ARCH:-$(dpkg --print-architecture)}"
PACKAGE_NAME="sqview"
TARBALL_NAME="sqview-linux-x86_64.tar.gz"
DEB_NAME="${PACKAGE_NAME}-linux-${ARCH}.deb"

mkdir -p "${OUTPUT_DIR}"

cargo build --release

tar -C target/release -czf "${OUTPUT_DIR}/${TARBALL_NAME}" sqview

PKG_ROOT="$(mktemp -d)"
trap 'rm -rf "${PKG_ROOT}"' EXIT

mkdir -p \
  "${PKG_ROOT}/DEBIAN" \
  "${PKG_ROOT}/usr/bin" \
  "${PKG_ROOT}/usr/share/doc/${PACKAGE_NAME}"

install -m 0755 target/release/sqview "${PKG_ROOT}/usr/bin/sqview"
install -m 0644 README.md "${PKG_ROOT}/usr/share/doc/${PACKAGE_NAME}/README.md"

cat > "${PKG_ROOT}/DEBIAN/control" <<EOF
Package: ${PACKAGE_NAME}
Version: ${VERSION}
Section: utils
Priority: optional
Architecture: ${ARCH}
Maintainer: sqview project <andreas.herd@mindmine.fi>
Description: keyboard-first terminal SQLite viewer
 A fast terminal UI for browsing and editing SQLite databases.
 It supports filtering, sorting, export, and popup editors for common cell types.
EOF

dpkg-deb --build --root-owner-group "${PKG_ROOT}" "${OUTPUT_DIR}/${DEB_NAME}"

(
  cd "${OUTPUT_DIR}"
  sha256sum "${TARBALL_NAME}" "${DEB_NAME}" > SHA256SUMS
)
