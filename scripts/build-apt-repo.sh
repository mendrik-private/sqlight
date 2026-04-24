#!/usr/bin/env bash

set -euo pipefail

DEB_FILE="${1:?usage: build-apt-repo.sh <deb-file> [output-dir] [repo-url]}"
OUTPUT_DIR="${2:-apt-repo}"
REPO_URL="${3:-https://mendrik-private.github.io/sqlight}"

PACKAGE_NAME="$(dpkg-deb -f "${DEB_FILE}" Package)"
ARCH="$(dpkg-deb -f "${DEB_FILE}" Architecture)"
POOL_DIR="${OUTPUT_DIR}/pool/main/${PACKAGE_NAME:0:1}/${PACKAGE_NAME}"
DIST_DIR="${OUTPUT_DIR}/dists/stable/main/binary-${ARCH}"
PACKAGES_RELATIVE="main/binary-${ARCH}/Packages"
PACKAGES_GZ_RELATIVE="${PACKAGES_RELATIVE}.gz"

checksum_line() {
  local tool="$1"
  local file="$2"
  local relative="$3"
  local sum
  sum="$(${tool} "${file}" | awk '{print $1}')"
  printf '%s %16d %s' "${sum}" "$(stat -c '%s' "${file}")" "${relative}"
}

rm -rf "${OUTPUT_DIR}"
mkdir -p "${POOL_DIR}" "${DIST_DIR}"

cp "${DEB_FILE}" "${POOL_DIR}/"

dpkg-scanpackages --multiversion "${OUTPUT_DIR}/pool" > "${DIST_DIR}/Packages"
gzip -9c "${DIST_DIR}/Packages" > "${DIST_DIR}/Packages.gz"

cat > "${OUTPUT_DIR}/dists/stable/Release" <<EOF
Origin: GitHub Pages
Label: sqv
Suite: stable
Codename: stable
Architectures: ${ARCH}
Components: main
Description: sqv apt repository
Date: $(date -Ru)
MD5Sum:
 $(checksum_line md5sum "${OUTPUT_DIR}/dists/stable/${PACKAGES_RELATIVE}" "${PACKAGES_RELATIVE}")
 $(checksum_line md5sum "${OUTPUT_DIR}/dists/stable/${PACKAGES_GZ_RELATIVE}" "${PACKAGES_GZ_RELATIVE}")
SHA1:
 $(checksum_line sha1sum "${OUTPUT_DIR}/dists/stable/${PACKAGES_RELATIVE}" "${PACKAGES_RELATIVE}")
 $(checksum_line sha1sum "${OUTPUT_DIR}/dists/stable/${PACKAGES_GZ_RELATIVE}" "${PACKAGES_GZ_RELATIVE}")
SHA256:
 $(checksum_line sha256sum "${OUTPUT_DIR}/dists/stable/${PACKAGES_RELATIVE}" "${PACKAGES_RELATIVE}")
 $(checksum_line sha256sum "${OUTPUT_DIR}/dists/stable/${PACKAGES_GZ_RELATIVE}" "${PACKAGES_GZ_RELATIVE}")
EOF

cat > "${OUTPUT_DIR}/index.html" <<EOF
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>sqv apt repository</title>
</head>
<body>
  <h1>sqv apt repository</h1>
  <p>Install with:</p>
  <pre><code>echo "deb [trusted=yes arch=${ARCH}] ${REPO_URL} stable main" | sudo tee /etc/apt/sources.list.d/sqv.list
sudo apt update
sudo apt install sqv</code></pre>
</body>
</html>
EOF
