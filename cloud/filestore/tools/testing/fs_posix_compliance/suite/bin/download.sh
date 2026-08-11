#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./download.sh oss
#   ./download.sh arcadia
#
# OSS resources use Ubuntu Jammy. Arcadia Sandbox workers normally provide
# glibc 2.31, while Jammy flock requires GLIBC_2.34. Use the Focal build for
# Arcadia, then upload the generated archives:
#   ya upload --ttl=inf --owner=YC_NBS flock_amd64.tgz
#   ya upload --ttl=inf --owner=YC_NBS flock_arm64.tgz

MODE="${1:-oss}"

case "$MODE" in
    oss)
        NAME=util-linux_2.37.2-4ubuntu3.5
        AMD64_URL=http://mirror.nebiusinfra.net/ubuntu/pool/main/u/util-linux/${NAME}_amd64.deb
        ARM64_URL=http://mirror.nebiusinfra.net/ubuntu/pool/main/u/util-linux/${NAME}_arm64.deb
        ;;
    arcadia)
        NAME=util-linux_2.34-0.1ubuntu9.6
        AMD64_URL=https://archive.ubuntu.com/ubuntu/pool/main/u/util-linux/${NAME}_amd64.deb
        ARM64_URL=https://ports.ubuntu.com/ubuntu-ports/pool/main/u/util-linux/${NAME}_arm64.deb
        ;;
    *)
        echo "Usage: $0 [oss|arcadia]" >&2
        exit 2
        ;;
esac

curl --fail --location --remote-name "$AMD64_URL"
curl --fail --location --remote-name "$ARM64_URL"

package_flock() {
    local arch="$1"

    dpkg-deb --fsys-tarfile "${NAME}_${arch}.deb" \
        | tar -xvOf - ./usr/bin/flock > flock
    chmod +x flock
    tar -czvf "flock_${arch}.tgz" flock
    rm flock
}

package_flock amd64
package_flock arm64
