#!/usr/bin/env bash
set -euo pipefail

SYSTEM_INSTALL_DIR=${SYSTEM_INSTALL_DIR:-/usr/local/bin}
INSTALL_CMD=${INSTALL_CMD:-/usr/bin/install}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TOOL_VERSIONS_FILE=${TOOL_VERSIONS_FILE:-"${SCRIPT_DIR}/github-release-tools.txt"}
ACTION_VALIDATOR_VERSION=
ACTIONLINT_VERSION=
SHELLCHECK_VERSION=
SHFMT_VERSION=
YQ_VERSION=

if [ -z "${INSTALL_DIR+x}" ]; then
    if [ -n "${HOME:-}" ]; then
        INSTALL_DIR="${HOME}/.local/bin"
    else
        CURRENT_USER=$(id -un)
        CURRENT_HOME=$(getent passwd "${CURRENT_USER}" | cut -d: -f6)
        if [ -z "${CURRENT_HOME}" ]; then
            echo "INSTALL_DIR is unset and HOME could not be resolved for ${CURRENT_USER}" >&2
            exit 1
        fi
        INSTALL_DIR="${CURRENT_HOME}/.local/bin"
    fi
fi

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
            actionlint)
                ACTIONLINT_VERSION=${version}
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
        actionlint)
            "${bin}" -version 2> /dev/null | awk '{print $1; exit}'
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

tool_version_matches_path() {
    local tool=$1
    local expected_version=$2
    local bin=$3
    local installed_version

    if [ ! -x "${bin}" ]; then
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

ensure_install_dir() {
    "${INSTALL_CMD}" -d -m 0755 "${INSTALL_DIR}"
    if [ -n "${GITHUB_PATH:-}" ]; then
        case ":${PATH}:" in
            *":${INSTALL_DIR}:"*) ;;
            *) echo "${INSTALL_DIR}" >> "${GITHUB_PATH}" ;;
        esac
    fi
}

install_binary_url() {
    local name=$1
    local url=$2
    local tmp_bin
    tmp_bin=$(mktemp)

    ensure_install_dir
    curl -fsSL -o "${tmp_bin}" -L "${url}"
    "${INSTALL_CMD}" -m 0755 "${tmp_bin}" "${INSTALL_DIR}/${name}"
    rm -f "${tmp_bin}"
}

install_action_validator() {
    local url
    require_tool_version action-validator "${ACTION_VALIDATOR_VERSION}"
    url="https://github.com/mpalmer/action-validator/releases/download/${ACTION_VALIDATOR_VERSION}/action-validator_linux_amd64"

    install_binary_url action-validator "${url}"
    "${INSTALL_DIR}/action-validator" --version
}

install_actionlint() {
    local archive tmp_dir url version_without_v
    require_tool_version actionlint "${ACTIONLINT_VERSION}"
    version_without_v=${ACTIONLINT_VERSION#v}
    archive="actionlint_${version_without_v}_linux_amd64.tar.gz"
    url="https://github.com/rhysd/actionlint/releases/download/${ACTIONLINT_VERSION}/${archive}"
    tmp_dir=$(mktemp -d)

    ensure_install_dir
    curl -fsSL -o "${tmp_dir}/${archive}" -L "${url}"
    tar -xzf "${tmp_dir}/${archive}" -C "${tmp_dir}"
    "${INSTALL_CMD}" -m 0755 "${tmp_dir}/actionlint" "${INSTALL_DIR}/actionlint"
    rm -rf "${tmp_dir}"

    "${INSTALL_DIR}/actionlint" -version
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

    ensure_install_dir
    curl -fsSL -o "${tmp_dir}/${archive}" -L "${url}"
    tar -xJf "${tmp_dir}/${archive}" -C "${tmp_dir}"
    "${INSTALL_CMD}" -m 0755 "${tmp_dir}/shellcheck-${SHELLCHECK_VERSION}/shellcheck" "${INSTALL_DIR}/shellcheck"
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
    local version system_bin install_bin

    case "${tool}" in
        action-validator)
            version=${ACTION_VALIDATOR_VERSION}
            require_tool_version "${tool}" "${version}"
            ;;
        actionlint)
            version=${ACTIONLINT_VERSION}
            require_tool_version "${tool}" "${version}"
            ;;
        shellcheck)
            version=${SHELLCHECK_VERSION}
            require_tool_version "${tool}" "${version}"
            ;;
        shfmt)
            version=${SHFMT_VERSION}
            require_tool_version "${tool}" "${version}"
            ;;
        yq)
            version=${YQ_VERSION}
            require_tool_version "${tool}" "${version}"
            ;;
        *)
            echo "Unknown tool: ${tool}" >&2
            exit 1
            ;;
    esac

    system_bin="${SYSTEM_INSTALL_DIR}/${tool}"
    install_bin="${INSTALL_DIR}/${tool}"
    if tool_version_matches_path "${tool}" "${version}" "${system_bin}"; then
        return
    fi
    if tool_version_matches_path "${tool}" "${version}" "${install_bin}"; then
        ensure_install_dir
        return
    fi

    case "${tool}" in
        action-validator)
            install_action_validator
            ;;
        actionlint)
            install_actionlint
            ;;
        shellcheck)
            install_shellcheck
            ;;
        shfmt)
            install_shfmt
            ;;
        yq)
            install_yq
            ;;
    esac
}

if [ "$#" -eq 0 ]; then
    set -- action-validator actionlint shellcheck shfmt yq
fi

load_tool_versions

for tool in "$@"; do
    install_tool "${tool}"
done
