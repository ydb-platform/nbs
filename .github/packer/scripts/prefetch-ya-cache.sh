#!/usr/bin/env bash
set -euo pipefail

: "${PREFETCH_REPOSITORY_URL:?Repository URL is required}"
: "${PREFETCH_REVISION:?Git revision is required}"

export GIT_TERMINAL_PROMPT=0
export YA_CACHE_DIR="${PREFETCH_CACHE_DIR:-${HOME}/.ya}"
checkout=$(mktemp -d "${TMPDIR:-/tmp}/ya-image-checkout.XXXXXXXX")

cleanup() {
    local status=$?
    trap - EXIT
    rm -rf -- "$checkout" || status=1
    exit "$status"
}
trap cleanup EXIT

git init "$checkout"
git -C "$checkout" remote add origin "$PREFETCH_REPOSITORY_URL"
git -C "$checkout" fetch --depth=1 origin "$PREFETCH_REVISION"
git -C "$checkout" checkout --detach FETCH_HEAD
git -C "$checkout" submodule update --init --recursive --depth=1
cd "$checkout"
export YA_SOURCE_ROOT="$checkout"

# Let ya manage the caches enabled in the checkout's ya.conf.
# The build cache holds ~15 GiB of test data; tools currently need ~4 GiB plus ya itself.
python3 .github/scripts/build_graph/prefetch_ya_resources.py \
    --repo-root "$checkout" \
    --cache-dir "$YA_CACHE_DIR" \
    --jobs "${PREFETCH_JOBS:-4}" \
    --save-plan "$checkout/prefetch-plan.json" \
    --ya-arg=--cache-size=32GiB \
    --ya-arg=--tools-cache-size=12GiB \
    --ya-arg=--no-local-executor \
    -- -A --build=relwithdebinfo -DGITHUB_CI=yes \
    cloud/blockstore cloud/filestore cloud/disk_manager \
    cloud/tasks cloud/storage cloud/fastshard

report_dir="${YA_CACHE_DIR}/prefetch"
mkdir -p "$report_dir"
cp "$checkout/prefetch-plan.json" "$report_dir/plan.json"
git rev-parse HEAD > "$report_dir/revision.txt"
sha256sum build/mapping.conf.json build/ext_mapping.conf.json > "$report_dir/mapping.sha256"
du -sh "${YA_CACHE_DIR}/tools" "${YA_CACHE_DIR}/build" | tee "$report_dir/disk-usage.txt"
sync
