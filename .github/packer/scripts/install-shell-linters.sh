#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
    SUDO=()
else
    SUDO=(sudo)
fi

github_latest_tag() {
    local repo=$1
    local curl_args=(-fsSL)
    if [ -n "${GITHUB_TOKEN:-}" ]; then
        curl_args+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
    fi

    curl "${curl_args[@]}" "https://api.github.com/repos/${repo}/releases/latest" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["tag_name"])'
}

install_shfmt() {
    local tag url tmp_bin
    tag=$(github_latest_tag "mvdan/sh")
    url="https://github.com/mvdan/sh/releases/download/${tag}/shfmt_${tag}_linux_amd64"
    tmp_bin=$(mktemp)

    curl -fsSL -o "${tmp_bin}" -L "${url}"
    "${SUDO[@]}" install -m 0755 "${tmp_bin}" /usr/local/bin/shfmt
    rm -f "${tmp_bin}"

    shfmt --version
}

install_shellcheck() {
    local tag archive url tmp_dir
    tag=$(github_latest_tag "koalaman/shellcheck")
    archive="shellcheck-${tag}.linux.x86_64.tar.xz"
    url="https://github.com/koalaman/shellcheck/releases/download/${tag}/${archive}"
    tmp_dir=$(mktemp -d)

    curl -fsSL -o "${tmp_dir}/${archive}" -L "${url}"
    tar -xJf "${tmp_dir}/${archive}" -C "${tmp_dir}"
    "${SUDO[@]}" install -m 0755 "${tmp_dir}/shellcheck-${tag}/shellcheck" /usr/local/bin/shellcheck
    rm -rf "${tmp_dir}"

    shellcheck --version
}

install_shellcheck
install_shfmt
