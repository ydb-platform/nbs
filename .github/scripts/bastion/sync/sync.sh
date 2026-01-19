#!/usr/bin/env bash
set -euo pipefail

: "${INTERVAL_SECONDS:=3600}"
: "${OUTPUT_FILE:=/out/authorized_keys}"

# Required env vars
: "${GITHUB_TOKEN:?GITHUB_TOKEN is required}"
: "${ORG:?ORG is required}"
: "${TEAM:?TEAM is required}"

api() {
    curl -sS -L \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "$@"
}

fetch_team_members() {
    local page=1
    while :; do
        # NOTE: TEAM should be the "team slug" (what appears in the URL), not necessarily the display name.
        local resp
        resp="$(api "https://api.github.com/orgs/${ORG}/teams/${TEAM}/members?per_page=100&page=${page}")"

        # If empty array -> stop
        local count
        count="$(echo "$resp" | jq 'length')"
        if [[ "$count" -eq 0 ]]; then
            break
        fi

        echo "$resp" | jq -r '.[].login'
        page=$((page + 1))
    done
}

fetch_user_keys() {
    local login="$1"
    api "https://api.github.com/users/${login}/keys" \
        | jq -r '.[].key' \
        | sed '/^null$/d'
}

sync_once() {
    local tmp
    tmp="$(mktemp)"

    {
        echo "# managed by gh-keys-sync (ORG=${ORG}, TEAM=${TEAM})"
        echo "# generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    } > "$tmp"

    # De-dupe keys while preserving "key comment"
    # We'll accumulate raw lines and unique them at the end.
    local keys_tmp
    keys_tmp="$(mktemp)"

    # Fetch members then keys
    while IFS= read -r login; do
        # Skip empty
        [[ -z "$login" ]] && continue

        while IFS= read -r key; do
            [[ -z "$key" ]] && continue
            # authorized_keys format: "<key> <comment>"
            echo "${key} ${login}" >> "$keys_tmp"
        done < <(fetch_user_keys "$login")
    done < <(fetch_team_members)

    # Sort+unique for stable output
    sort -u "$keys_tmp" >> "$tmp"
    rm -f "$keys_tmp"

    mkdir -p "$(dirname "$OUTPUT_FILE")"
    chmod 700 "$(dirname "$OUTPUT_FILE")" || true

    # Atomic replace
    mv -f "$tmp" "$OUTPUT_FILE"
    chmod 600 "$OUTPUT_FILE" || true

    echo "[gh-keys-sync] wrote $(wc -l < "$OUTPUT_FILE") lines to $OUTPUT_FILE"
}

echo "[gh-keys-sync] starting; interval=${INTERVAL_SECONDS}s, output=${OUTPUT_FILE}"

# Run immediately, then every interval
while :; do
    if sync_once; then
        :
    else
        echo "[gh-keys-sync] sync failed (will retry next interval)" >&2
    fi
    sleep "$INTERVAL_SECONDS"
done
