#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SYNC_DIR="$SCRIPT_DIR/ydb-sync"

source "$SYNC_DIR/common.sh"

ydb_sync_init
ydb_sync_check_clean
ydb_sync_ensure_source
ydb_sync_check_import_patches

steps=(
    10-copy-folded-layout.sh
    20-apply-import-contrib-patches.sh
    30-rewrite-paths.sh
    40-prune-contrib-ydb.sh
    50-copy-extra-deps.sh
)

for step in "${steps[@]}"; do
    echo "==> $step"
    "$SYNC_DIR/$step"
done

echo "Synced $YDB_REPO $YDB_REF into folded contrib/ydb layout."
