#!/usr/bin/env bash

ydb_sync_init() {
    YDB_REF=${YDB_REF:-stable-26-3-1}
    YDB_REPO=${YDB_REPO:-https://github.com/ydb-platform/ydb.git}
    IMPORT_CONTRIB_DIR=${IMPORT_CONTRIB_DIR:-/home/apkobzev/arcadia/kikimr/scripts/oss/import_contrib}
    ROOT=${ROOT:-$(git rev-parse --show-toplevel)}
    YDB_SRC=${YDB_SRC:-$ROOT/.sync/ydb-$YDB_REF}

    export YDB_REF YDB_REPO IMPORT_CONTRIB_DIR ROOT YDB_SRC
    cd "$ROOT"
}

ydb_sync_check_clean() {
    if [ "${ALLOW_DIRTY:-0}" != "1" ]; then
        git diff --quiet
        git diff --cached --quiet
    fi
}

ydb_sync_ensure_source() {
    if [ ! -d "$YDB_SRC/.git" ]; then
        mkdir -p "$(dirname "$YDB_SRC")"
        git clone --depth 1 --branch "$YDB_REF" "$YDB_REPO" "$YDB_SRC"
    fi
}

ydb_sync_check_import_patches() {
    if [ ! -x "$IMPORT_CONTRIB_DIR/patch_ymakes.sh" ]; then
        echo "IMPORT_CONTRIB_DIR does not point to import_contrib scripts: $IMPORT_CONTRIB_DIR" >&2
        exit 1
    fi
}

copy_dir() {
    local src=$1
    local dst=$2
    mkdir -p "$dst"
    rsync -a --delete "$src/" "$dst/"
}

copy_optional_dir() {
    local src=$1
    local dst=$2
    if [ -d "$src" ]; then
        copy_dir "$src" "$dst"
    fi
}

copy_optional_file() {
    local src=$1
    local dst=$2
    if [ -f "$src" ]; then
        mkdir -p "$(dirname "$dst")"
        cp "$src" "$dst"
    fi
}
