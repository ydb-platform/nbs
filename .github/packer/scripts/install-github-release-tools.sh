#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR=${INSTALL_DIR:-/usr/local/bin}
INSTALL_CMD=${INSTALL_CMD:-/usr/bin/install}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TOOL_VERSIONS_FILE=${TOOL_VERSIONS_FILE:-"${SCRIPT_DIR}/github-release-tools.txt"}
ACTION_VALIDATOR_VERSION=
SHELLCHECK_VERSION=
SHFMT_VERSION=
YQ_VERSION=

if [ "$(id -u)" -eq 0 ] || [[ "${INSTALL_DIR}" == "${HOME}/"* ]]; then
    SUDO=()
else
    SUDO=(sudo)
fi

"${SUDO[@]}" "${INSTALL_CMD}" -d -m 0755 "${INSTALL_DIR}"

load_tool_versions() {
    local line tool version

    if [ ! -f "${TOOL_VERSIONS_FILE}" ]; then
        echo "Tool versions file not found: ${TOOL_VERSIONS_FILE}" >&2
        exit 1
    fi

    while IFS= read -r line || [ -n "${line}" ]; do
        line=${line%%#*}
        line=${line//[[:space:]]/}
        if [ -z "${line}" ]; then
            continue
        fi
        if [[ "${line}" != *==* ]]; then
            echo "Invalid tool version line: ${line}" >&2
            exit 1
        fi

        tool=${line%%==*}
        version=${line#*==}
        if [ -z "${tool}" ] || [ -z "${version}" ]; then
            echo "Invalid tool version line: ${line}" >&2
            exit 1
        fi

        case "${tool}" in
            action-validator)
                ACTION_VALIDATOR_VERSION=${version}
                ;;
            shellcheck)
                SHELLCHECK_VERSION=${version}
                ;;
            shfmt)
                SHFMT_VERSION=${version}
                ;;
            yq)
                YQ_VERSION=${version}
                ;;
            *)
                echo "Unknown tool in ${TOOL_VERSIONS_FILE}: ${tool}" >&2
                exit 1
                ;;
        esac
    done < "${TOOL_VERSIONS_FILE}"
}

require_tool_version() {
    local tool=$1
    local version=$2

    if [ -z "${version}" ]; then
        echo "Missing pinned version for ${tool} in ${TOOL_VERSIONS_FILE}" >&2
        exit 1
    fi
}

normalize_version() {
    local version=$1

    printf '%s\n' "${version#v}"
}

installed_tool_version() {
    local tool=$1
    local bin=$2

    case "${tool}" in
        action-validator)
            "${bin}" --version 2> /dev/null | awk '{print $2; exit}'
            ;;
        shellcheck)
            "${bin}" --version 2> /dev/null | awk -F': ' '/^version:/ {print $2; exit}'
            ;;
        shfmt)
            "${bin}" --version 2> /dev/null | awk '{print $1; exit}'
            ;;
        yq)
            "${bin}" --version 2> /dev/null | awk '{print $NF; exit}'
            ;;
    esac
}

tool_version_matches() {
    local tool=$1
    local expected_version=$2
    local bin installed_version

    if ! bin=$(command -v "${tool}" 2> /dev/null); then
        return 1
    fi

    installed_version=$(installed_tool_version "${tool}" "${bin}")
    if [ -z "${installed_version}" ]; then
        echo "${tool} is installed at ${bin}, but its version could not be detected; reinstalling"
        return 1
    fi

    if [ "$(normalize_version "${installed_version}")" = "$(normalize_version "${expected_version}")" ]; then
        echo "${tool} ${installed_version} already installed at ${bin}"
        return 0
    fi

    echo "${tool} ${installed_version} is installed at ${bin}, expected ${expected_version}; reinstalling"
    return 1
}

install_binary_url() {
    local name=$1
    local url=$2
    local tmp_bin
    tmp_bin=$(mktemp)

    curl -fsSL -o "${tmp_bin}" -L "${url}"
    "${SUDO[@]}" "${INSTALL_CMD}" -m 0755 "${tmp_bin}" "${INSTALL_DIR}/${name}"
    rm -f "${tmp_bin}"
}

install_action_validator() {
    local url
    require_tool_version action-validator "${ACTION_VALIDATOR_VERSION}"
    url="https://github.com/mpalmer/action-validator/releases/download/${ACTION_VALIDATOR_VERSION}/action-validator_linux_amd64"

    install_binary_url action-validator "${url}"
    "${INSTALL_DIR}/action-validator" --version
}

install_shfmt() {
    local url
    require_tool_version shfmt "${SHFMT_VERSION}"
    url="https://github.com/mvdan/sh/releases/download/${SHFMT_VERSION}/shfmt_${SHFMT_VERSION}_linux_amd64"

    install_binary_url shfmt "${url}"
    "${INSTALL_DIR}/shfmt" --version
}

install_shellcheck() {
    local archive url tmp_dir
    require_tool_version shellcheck "${SHELLCHECK_VERSION}"
    archive="shellcheck-${SHELLCHECK_VERSION}.linux.x86_64.tar.xz"
    url="https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/${archive}"
    tmp_dir=$(mktemp -d)

    curl -fsSL -o "${tmp_dir}/${archive}" -L "${url}"
    tar -xJf "${tmp_dir}/${archive}" -C "${tmp_dir}"
    "${SUDO[@]}" "${INSTALL_CMD}" -m 0755 "${tmp_dir}/shellcheck-${SHELLCHECK_VERSION}/shellcheck" "${INSTALL_DIR}/shellcheck"
    rm -rf "${tmp_dir}"

    "${INSTALL_DIR}/shellcheck" --version
}

install_yq() {
    local url
    require_tool_version yq "${YQ_VERSION}"
    url="https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_amd64"

    install_binary_url yq "${url}"
    "${INSTALL_DIR}/yq" --version
}

install_tool() {
    local tool=$1
    local version

    case "${tool}" in
        action-validator)
            version=${ACTION_VALIDATOR_VERSION}
            require_tool_version "${tool}" "${version}"
            if tool_version_matches "${tool}" "${version}"; then
                return
            fi
            install_action_validator
            ;;
        shellcheck)
            version=${SHELLCHECK_VERSION}
            require_tool_version "${tool}" "${version}"
            if tool_version_matches "${tool}" "${version}"; then
                return
            fi
            install_shellcheck
            ;;
        shfmt)
            version=${SHFMT_VERSION}
            require_tool_version "${tool}" "${version}"
            if tool_version_matches "${tool}" "${version}"; then
                return
            fi
            install_shfmt
            ;;
        yq)
            version=${YQ_VERSION}
            require_tool_version "${tool}" "${version}"
            if tool_version_matches "${tool}" "${version}"; then
                return
            fi
            install_yq
            ;;
        *)
            echo "Unknown tool: ${tool}" >&2
            exit 1
            ;;
    esac
}

if [ "$#" -eq 0 ]; then
    set -- action-validator shellcheck shfmt yq
fi

load_tool_versions

for tool in "$@"; do
    install_tool "${tool}"
done
