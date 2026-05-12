#!/bin/bash
#
# apply.sh — Patch a fresh silk checkout to be compilable with ya make.
#
# Usage (from repo root):
#   bash contrib/libs/silk/ya_make_patches/apply.sh
#

set -euo pipefail

PATCH_DIR="$(dirname "$(readlink -f "$0")")"
SILK_DIR="$(dirname "$PATCH_DIR")"
REPO_ROOT="$(realpath "$SILK_DIR/../../..")"

echo "=== Applying ya make patches to silk ==="
echo "Silk dir: $SILK_DIR"
echo "Repo root: $REPO_ROOT"

# Apply source patches
for p in "$PATCH_DIR"/patches/*.patch; do
    [ -f "$p" ] || continue
    echo "--- Applying $(basename "$p")"
    patch -N -p1 -d "$SILK_DIR" < "$p" || true
done

# Copy overlay files (ya.make, config headers, stubs, test env files)
echo "--- Copying overlay files"
cp -r "$PATCH_DIR"/overlay/. "$SILK_DIR"/

# Ensure boost::intrusive/set.hpp exists
BOOST_SET="$REPO_ROOT/contrib/restricted/boost/intrusive/include/boost/intrusive/set.hpp"
if [ ! -f "$BOOST_SET" ]; then
    echo "--- Downloading boost/intrusive/set.hpp (missing from repo)"
    curl -sL "https://raw.githubusercontent.com/boostorg/intrusive/boost-1.84.0/include/boost/intrusive/set.hpp" \
        -o "$BOOST_SET"
fi

echo "=== Done. Run: ya make --build=debug contrib/libs/silk ==="
