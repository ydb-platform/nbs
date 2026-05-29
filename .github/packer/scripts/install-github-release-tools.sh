#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR=${INSTALL_DIR:-/usr/local/bin}

if [ "$(id -u)" -eq 0 ] || [[ "${INSTALL_DIR}" == "${HOME}/"* ]]; then
    SUDO=()
else
    SUDO=(sudo)
fi

"${SUDO[@]}" install -d -m 0755 "${INSTALL_DIR}"

github_latest_tag() {
    local repo=$1

    curl -fsSL "https://api.github.com/repos/${repo}/releases/latest" \
        | python3 -c '
import json
import sys

tag = json.load(sys.stdin).get("tag_name")
if not tag:
    raise SystemExit("release tag_name is empty")
print(tag)
'
}

install_binary_url() {
    local name=$1
    local url=$2
    local tmp_bin
    tmp_bin=$(mktemp)

    curl -fsSL -o "${tmp_bin}" -L "${url}"
    "${SUDO[@]}" install -m 0755 "${tmp_bin}" "${INSTALL_DIR}/${name}"
    rm -f "${tmp_bin}"
}

install_shfmt() {
    local tag url
    tag=$(github_latest_tag "mvdan/sh")
    url="https://github.com/mvdan/sh/releases/download/${tag}/shfmt_${tag}_linux_amd64"

    install_binary_url shfmt "${url}"
    "${INSTALL_DIR}/shfmt" --version
}

install_shellcheck() {
    local tag archive url tmp_dir
    tag=$(github_latest_tag "koalaman/shellcheck")
    archive="shellcheck-${tag}.linux.x86_64.tar.xz"
    url="https://github.com/koalaman/shellcheck/releases/download/${tag}/${archive}"
    tmp_dir=$(mktemp -d)

    curl -fsSL -o "${tmp_dir}/${archive}" -L "${url}"
    tar -xJf "${tmp_dir}/${archive}" -C "${tmp_dir}"
    "${SUDO[@]}" install -m 0755 "${tmp_dir}/shellcheck-${tag}/shellcheck" "${INSTALL_DIR}/shellcheck"
    rm -rf "${tmp_dir}"

    "${INSTALL_DIR}/shellcheck" --version
}

install_yq() {
    local tag url
    tag=$(github_latest_tag "mikefarah/yq")
    url="https://github.com/mikefarah/yq/releases/download/${tag}/yq_linux_amd64"

    install_binary_url yq "${url}"
    "${INSTALL_DIR}/yq" --version
}

install_tool() {
    local tool=$1

    case "${tool}" in
        shellcheck)
            install_shellcheck
            ;;
        shfmt)
            install_shfmt
            ;;
        yq)
            install_yq
            ;;
        *)
            echo "Unknown tool: ${tool}" >&2
            exit 1
            ;;
    esac
}

if [ "$#" -eq 0 ]; then
    set -- shellcheck shfmt yq
fi

for tool in "$@"; do
    install_tool "${tool}"
done
