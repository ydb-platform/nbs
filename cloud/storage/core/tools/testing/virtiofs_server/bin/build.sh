#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

default_arch() {
    dpkg --print-architecture 2>/dev/null || uname -m
}


TAG=${TAG:-v1.14.0}
PROJECT_URL=${PROJECT_URL:-https://gitlab.com/virtio-fs/virtiofsd}
WORK_DIR=${WORK_DIR:-"${SCRIPT_DIR}/build-src"}
ARCH=${ARCH:-$(default_arch)}
RUSTUP_TOOLCHAIN=${RUSTUP_TOOLCHAIN:-stable}
INSTALL_BUILD_DEPS=${INSTALL_BUILD_DEPS:-0}
INSTALL_RUSTUP_TOOLCHAIN=${INSTALL_RUSTUP_TOOLCHAIN:-0}
CARGO_BIN=${CARGO_BIN:-}
CARGO_TARGET=${CARGO_TARGET:-}
CARGO_EXTRA_ARGS=${CARGO_EXTRA_ARGS:-}
CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-}

case "$ARCH" in
    ""|0|1)
        ARCH=$(default_arch)
        ;;
    x86_64)
        ARCH=amd64
        ;;
    aarch64)
        ARCH=arm64
        ;;
esac


usage() {
    cat <<EOF
Usage: $0 [--install-build-deps] [--install-rustup-toolchain]

Environment:
  TAG=${TAG}
  ARCH=${ARCH}
  WORK_DIR=${WORK_DIR}
  RUSTUP_TOOLCHAIN=${RUSTUP_TOOLCHAIN}
  CARGO_BIN=${CARGO_BIN}
  CARGO_TARGET=${CARGO_TARGET}
  CARGO_TARGET_DIR=${CARGO_TARGET_DIR}
  CARGO_EXTRA_ARGS=${CARGO_EXTRA_ARGS}
EOF
}


for arg in "$@"; do
    case "$arg" in
        --install-build-deps)
            INSTALL_BUILD_DEPS=1
            ;;
        --install-rustup-toolchain)
            INSTALL_RUSTUP_TOOLCHAIN=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option: $arg" >&2
            usage >&2
            exit 1
            ;;
    esac
done


run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}


rust_triple() {
    case "$(uname -m)" in
        x86_64) echo x86_64-unknown-linux-gnu ;;
        aarch64) echo aarch64-unknown-linux-gnu ;;
        *)
            echo "error: unsupported rustup architecture: $(uname -m)" >&2
            exit 1
            ;;
    esac
}


target_multiarch() {
    case "$1" in
        x86_64-unknown-linux-gnu) echo x86_64-linux-gnu ;;
        aarch64-unknown-linux-gnu) echo aarch64-linux-gnu ;;
        *) gcc -print-multiarch 2>/dev/null || true ;;
    esac
}


find_static_lib_dir() {
    local name=$1
    local multiarch=$2

    for dir in \
        "/usr/lib/${multiarch}" \
        "/lib/${multiarch}" \
        /usr/lib \
        /lib
    do
        if [ -f "${dir}/lib${name}.a" ]; then
            echo "$dir"
            return
        fi
    done

    echo "error: static lib${name}.a is missing; try $0 --install-build-deps" >&2
    exit 1
}


if [ "$INSTALL_BUILD_DEPS" = 1 ]; then
    run_as_root apt install --yes \
        build-essential \
        libcap-ng-dev \
        libseccomp-dev \
        pkg-config
fi

mkdir -p "$WORK_DIR"
WORK_DIR=$(cd "$WORK_DIR" && pwd)

if [ "$INSTALL_RUSTUP_TOOLCHAIN" = 1 ]; then
    if command -v rustup >/dev/null && rustup --version >/dev/null 2>&1; then
        rustup toolchain install "$RUSTUP_TOOLCHAIN" --profile minimal
    else
        TRIPLE=$(rust_triple)
        RUSTUP_INIT="${WORK_DIR}/rustup-init-${TRIPLE}"
        if [ ! -x "$RUSTUP_INIT" ]; then
            curl --location --fail --show-error --output "$RUSTUP_INIT" \
                "https://static.rust-lang.org/rustup/dist/${TRIPLE}/rustup-init"
            chmod +x "$RUSTUP_INIT"
        fi
        "$RUSTUP_INIT" -y --profile minimal --default-toolchain "$RUSTUP_TOOLCHAIN" --no-modify-path
    fi
fi

TOOLCHAIN_BIN=
RUSTUP_HOME=${RUSTUP_HOME:-"${HOME}/.rustup"}
for dir in \
    "${RUSTUP_HOME}/toolchains/${RUSTUP_TOOLCHAIN}/bin" \
    "${RUSTUP_HOME}/toolchains/${RUSTUP_TOOLCHAIN}-$(rust_triple)/bin"
do
    if [ -x "${dir}/cargo" ]; then
        TOOLCHAIN_BIN=$dir
        break
    fi
done

if [ -n "$CARGO_BIN" ]; then
    CARGO=$CARGO_BIN
elif [ -n "$TOOLCHAIN_BIN" ]; then
    CARGO="${TOOLCHAIN_BIN}/cargo"
    export RUSTC="${TOOLCHAIN_BIN}/rustc"
elif [ -x "${CARGO_HOME:-${HOME}/.cargo}/bin/cargo" ]; then
    CARGO="${CARGO_HOME:-${HOME}/.cargo}/bin/cargo"
else
    CARGO=cargo
fi
export RUSTUP_TOOLCHAIN

ARCHIVE="${WORK_DIR}/virtiofsd-${TAG}.tar.gz"
SRC_DIR="${WORK_DIR}/src-${TAG}"

if [ -s "$ARCHIVE" ]; then
    echo "Use $ARCHIVE" >&2
else
    curl --location --fail --show-error --output "$ARCHIVE" \
        "${PROJECT_URL}/-/archive/${TAG}/virtiofsd-${TAG}.tar.gz"
fi

if [ -f "${SRC_DIR}/Cargo.toml" ]; then
    echo "Use $SRC_DIR" >&2
else
    rm -rf "$SRC_DIR"
    mkdir -p "$SRC_DIR"
    tar -xzf "$ARCHIVE" -C "$SRC_DIR" --strip-components=1
fi

CARGO_TARGET=${CARGO_TARGET:-$(rust_triple)}
BUILD_ARGS=(build --release --locked --target "$CARGO_TARGET")
MULTIARCH=$(target_multiarch "$CARGO_TARGET")

export PKG_CONFIG_ALL_STATIC=1
export PKG_CONFIG_STATIC=1
export LIBSECCOMP_LINK_TYPE=static
export LIBCAPNG_LINK_TYPE=static
LIBSECCOMP_LIB_PATH=$(find_static_lib_dir seccomp "$MULTIARCH")
LIBCAPNG_LIB_PATH=$(find_static_lib_dir cap-ng "$MULTIARCH")
export LIBSECCOMP_LIB_PATH
export LIBCAPNG_LIB_PATH
STATIC_RUSTFLAGS="${RUSTFLAGS:-}"
STATIC_RUSTFLAGS="${STATIC_RUSTFLAGS:+${STATIC_RUSTFLAGS} }-C target-feature=+crt-static"
RUSTFLAGS_ENV="CARGO_TARGET_${CARGO_TARGET//-/_}_RUSTFLAGS"
export "${RUSTFLAGS_ENV^^}=${STATIC_RUSTFLAGS}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${SRC_DIR}/target-static}"

(
    cd "$SRC_DIR"
    # Intentionally split extra args so callers can pass several flags.
    "$CARGO" "${BUILD_ARGS[@]}" ${CARGO_EXTRA_ARGS}
)

TARGET_DIR="${CARGO_TARGET_DIR}/${CARGO_TARGET}/release"

VERSION=${TAG#v}
BINARY="${TARGET_DIR}/virtiofsd"
OUTPUT="./virtiofsd_${VERSION}_${ARCH}"

if ldd "$BINARY" 2>&1 | grep -Eq "not a dynamic executable|statically linked"; then
    :
else
    echo "error: $BINARY is not static" >&2
    ldd "$BINARY" >&2
    exit 1
fi

install -m 0755 "$BINARY" "$OUTPUT"

sha256sum "$OUTPUT"
