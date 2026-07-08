#!/usr/bin/env bash

set -euo pipefail

LIBSLIRP_GIT="${LIBSLIRP_GIT:-https://gitlab.freedesktop.org/slirp/libslirp.git}"
LIBSLIRP_REF="${LIBSLIRP_REF:-}"
WORK_DIR="${WORK_DIR:-$PWD}"
PREFIX="${PREFIX:-$WORK_DIR/libslirp-static}"
SRC_DIR="${SRC_DIR:-$WORK_DIR/libslirp-src}"
BUILD_DIR="${BUILD_DIR:-$WORK_DIR/build-libslirp}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)}"
UPDATE=0

usage() {
    cat <<EOF
Usage: $0 [options]

Download libslirp and install a static libslirp.a into a local prefix.

Options:
  --git URL          libslirp git URL (default: $LIBSLIRP_GIT)
  --ref REF          git ref/tag/branch to checkout (default: upstream default)
  --prefix DIR      install prefix (default: $PREFIX)
  --src-dir DIR     source directory (default: $SRC_DIR)
  --build-dir DIR   meson build directory (default: $BUILD_DIR)
  --jobs N          build jobs (default: $JOBS)
  --update          fetch updates if source directory already exists
  -h, --help        show this help
EOF
}

require_arg() {
    if [ "$#" -lt 2 ]; then
        echo "Option $1 requires an argument" >&2
        usage >&2
        exit 2
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --git)
            require_arg "$@"
            LIBSLIRP_GIT="$2"
            shift 2
            ;;
        --ref)
            require_arg "$@"
            LIBSLIRP_REF="$2"
            shift 2
            ;;
        --prefix)
            require_arg "$@"
            PREFIX="$2"
            shift 2
            ;;
        --src-dir)
            require_arg "$@"
            SRC_DIR="$2"
            shift 2
            ;;
        --build-dir)
            require_arg "$@"
            BUILD_DIR="$2"
            shift 2
            ;;
        --jobs)
            require_arg "$@"
            JOBS="$2"
            shift 2
            ;;
        --update)
            UPDATE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

need_tool() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required tool: $1" >&2
        exit 1
    fi
}

need_tool git
need_tool meson
need_tool ninja
need_tool pkg-config

if [ -e "$SRC_DIR" ] && [ ! -d "$SRC_DIR/.git" ]; then
    echo "Source path exists but is not a git checkout: $SRC_DIR" >&2
    exit 1
fi

if [ ! -d "$SRC_DIR/.git" ]; then
    clone_args=()
    if [ -n "$LIBSLIRP_REF" ]; then
        clone_args+=(--branch "$LIBSLIRP_REF")
    fi

    git clone "${clone_args[@]}" "$LIBSLIRP_GIT" "$SRC_DIR"
elif [ "$UPDATE" -eq 1 ]; then
    git -C "$SRC_DIR" fetch --tags origin
fi

if [ -n "$LIBSLIRP_REF" ]; then
    git -C "$SRC_DIR" checkout "$LIBSLIRP_REF"
elif [ "$UPDATE" -eq 1 ]; then
    git -C "$SRC_DIR" pull --ff-only
fi

mkdir -p "$PREFIX"

setup_args=(
    "$BUILD_DIR"
    "$SRC_DIR"
    "--prefix=$PREFIX"
    "--libdir=lib"
    "--default-library=static"
)

if [ -f "$BUILD_DIR/build.ninja" ]; then
    meson setup --reconfigure "${setup_args[@]}"
else
    meson setup "${setup_args[@]}"
fi

ninja -C "$BUILD_DIR" -j "$JOBS"
ninja -C "$BUILD_DIR" install

lib_file="$(find "$PREFIX" -type f -name libslirp.a -print | sed -n '1p')"
pc_file="$(find "$PREFIX" -type f -name slirp.pc -print | sed -n '1p')"

if [ -z "$lib_file" ]; then
    echo "libslirp.a was not installed under $PREFIX" >&2
    exit 1
fi

if [ -z "$pc_file" ]; then
    echo "slirp.pc was not installed under $PREFIX" >&2
    exit 1
fi

pc_dir="$(dirname "$pc_file")"

echo
echo "Installed static libslirp:"
echo "  $lib_file"
echo
echo "Use it for the QEMU build with:"
echo "  export PKG_CONFIG_PATH=$pc_dir:\${PKG_CONFIG_PATH:-}"
