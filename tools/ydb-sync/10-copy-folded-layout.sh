#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/common.sh"
ydb_sync_init

copy_dir "$YDB_SRC/ydb" contrib/ydb

mkdir -p contrib/ydb/library/yql/providers
copy_dir "$YDB_SRC/yql/essentials" contrib/ydb/library/yql
copy_optional_dir "$YDB_SRC/yql/providers" contrib/ydb/library/yql/providers
copy_optional_dir "$YDB_SRC/yt/yql/providers" contrib/ydb/library/yql/providers
