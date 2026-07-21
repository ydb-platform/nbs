#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/common.sh"
ydb_sync_init
ydb_sync_check_import_patches

LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_ymakes.sh" contrib/ydb
LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_sources.sh" contrib/ydb
LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_py_sources.sh" contrib/ydb
LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_protos.sh" contrib/ydb
