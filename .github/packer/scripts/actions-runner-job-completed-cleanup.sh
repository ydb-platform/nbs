#!/usr/bin/env bash
set -u

log() {
    echo "[runner-cleanup] $*"
}

remove_path() {
    local path="${1:-}"
    if [ -z "$path" ] || [ "$path" = "/" ]; then
        return
    fi
    if [ -e "$path" ] || [ -L "$path" ]; then
        log "removing $path"
        rm -rf -- "$path" || log "failed to remove $path"
    fi
    return 0
}

remove_glob() {
    local pattern="$1"
    local path
    compgen -G "$pattern" > /dev/null || return 0
    for path in $pattern; do
        remove_path "$path"
    done
    return 0
}

log "starting job completed credentials cleanup"

remove_path "${BAZEL_REMOTE_PASSWORD_FILE:-}"

for user_home in /home/github /root; do
    remove_path "$user_home/.aws"
    remove_path "$user_home/.nebius/config.yaml"
    remove_path "$user_home/.nebius/service-account-key.json"
    remove_path "$user_home/.netrc"
    remove_path "$user_home/.bazelrc.user"
    remove_path "$user_home/.ya/ya.conf"
    remove_path "$user_home/.ya/token"
done

remove_glob "/home/github/bazel.????"

remove_glob "/actions-runner/_work/*/*/.nebius"
remove_glob "/actions-runner/_work/*/*/.aws"
remove_glob "/actions-runner/_work/*/*/.netrc"
remove_glob "/actions-runner/_work/*/*/.bazelrc.user"
remove_glob "/actions-runner/_work/*/*/.git/info/sparse-checkout.lock"

if [ -n "${GITHUB_WORKSPACE:-}" ]; then
    remove_path "$GITHUB_WORKSPACE/.nebius"
    remove_path "$GITHUB_WORKSPACE/.aws"
    remove_path "$GITHUB_WORKSPACE/.netrc"
    remove_path "$GITHUB_WORKSPACE/.bazelrc.user"
    remove_path "$GITHUB_WORKSPACE/.git/info/sparse-checkout.lock"
fi

log "finished job completed credentials cleanup"
