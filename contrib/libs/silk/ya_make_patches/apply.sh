#!/bin/bash
#
# apply.sh — Install/update silk in this repo from an external source tree.
#
# Usage (from anywhere):
#   bash contrib/libs/silk/ya_make_patches/apply.sh /path/to/silk-checkout
#
# What it does:
#   1. Wipes the existing silk tree (except this ya_make_patches/ directory)
#   2. Copies the source from /path/to/silk-checkout into contrib/libs/silk/
#   3. Applies in-tree source patches from ya_make_patches/patches/
#   4. Lays down the ya.make / config / stub files from ya_make_patches/overlay/
#   5. Ensures any out-of-tree dependencies (e.g. boost::intrusive/set.hpp) exist
#

set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 <silk-source-dir>

  silk-source-dir  Path to a checkout of silk (the directory that contains
                   include/, src/, contrib/ etc.)

Example:
  $0 ~/git/silk
EOF
    exit 1
}

if [ $# -ne 1 ]; then
    usage
fi

SILK_SRC="$(realpath "$1")"
if [ ! -d "$SILK_SRC/src/util" ] || [ ! -d "$SILK_SRC/include/silk" ]; then
    echo "error: $SILK_SRC does not look like a silk checkout (missing src/util or include/silk)"
    exit 1
fi

PATCH_DIR="$(dirname "$(readlink -f "$0")")"
SILK_DST="$(dirname "$PATCH_DIR")"

echo "=== Installing silk from $SILK_SRC into $SILK_DST ==="

# 1. Wipe the existing silk tree except ya_make_patches/.
echo "--- Cleaning $SILK_DST"
find "$SILK_DST" -mindepth 1 -maxdepth 1 \
    ! -name ya_make_patches \
    -exec rm -rf {} +

# 2. Copy fresh silk source.
echo "--- Copying silk source"
cp -r "$SILK_SRC"/. "$SILK_DST"/
# Restore ya_make_patches in case the source happened to ship one.
# (No-op if it didn't.)

# 3. Apply source patches in lexicographic order.
shopt -s nullglob
for p in "$PATCH_DIR"/patches/*.patch; do
    echo "--- Applying $(basename "$p")"
    patch -N -p1 -d "$SILK_DST" < "$p"
done
shopt -u nullglob

# 4. Lay down the ya.make / config / stub files.
echo "--- Copying overlay files"
cp -r "$PATCH_DIR"/overlay/. "$SILK_DST"/

echo "=== Done. Build with: ya make --build=debug contrib/libs/silk ==="
