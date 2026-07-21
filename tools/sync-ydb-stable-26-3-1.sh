#!/usr/bin/env bash
set -euo pipefail

YDB_REF=${YDB_REF:-stable-26-3-1}
YDB_REPO=${YDB_REPO:-https://github.com/ydb-platform/ydb.git}
IMPORT_CONTRIB_DIR=${IMPORT_CONTRIB_DIR:-/home/apkobzev/arcadia/kikimr/scripts/oss/import_contrib}
ROOT=$(git rev-parse --show-toplevel)
YDB_SRC=${YDB_SRC:-$ROOT/.sync/ydb-$YDB_REF}

cd "$ROOT"

if [ "${ALLOW_DIRTY:-0}" != "1" ]; then
    git diff --quiet
    git diff --cached --quiet
fi

if [ ! -d "$YDB_SRC/.git" ]; then
    mkdir -p "$(dirname "$YDB_SRC")"
    git clone --depth 1 --branch "$YDB_REF" "$YDB_REPO" "$YDB_SRC"
fi

if [ ! -x "$IMPORT_CONTRIB_DIR/patch_ymakes.sh" ]; then
    echo "IMPORT_CONTRIB_DIR does not point to import_contrib scripts: $IMPORT_CONTRIB_DIR" >&2
    exit 1
fi

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

copy_dir "$YDB_SRC/ydb" contrib/ydb

mkdir -p contrib/ydb/library/yql/providers
copy_dir "$YDB_SRC/yql/essentials" contrib/ydb/library/yql
copy_optional_dir "$YDB_SRC/yql/providers" contrib/ydb/library/yql/providers
copy_optional_dir "$YDB_SRC/yt/yql/providers" contrib/ydb/library/yql/providers

LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_ymakes.sh" contrib/ydb
LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_sources.sh" contrib/ydb
LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_py_sources.sh" contrib/ydb
LOC_NEW=contrib/ydb "$IMPORT_CONTRIB_DIR/patch_protos.sh" contrib/ydb

find contrib/ydb -type f \
    \( -name ya.make -o -name "*.make" -o -name "*.inc" -o -name "*.h" -o -name "*.cpp" -o -name "*.py" -o -name "*.proto" -o -name "*.jnj" -o -name "*.txt" \) \
    -print0 | xargs -0 perl -pi -e '
        s#contrib/ydb/library/contrib/ydb/library/yql#contrib/ydb/library/yql#g;
        s#yt/contrib/ydb/library/yql/providers#contrib/ydb/library/yql/providers#g;
        s#yql/essentials/#contrib/ydb/library/yql/#g;
        s#yql/providers/#contrib/ydb/library/yql/providers/#g;
        s#yt/yql/providers/#contrib/ydb/library/yql/providers/#g;
        s#yql\.essentials\.#contrib.ydb.library.yql.#g;
        s#yql\.providers\.#contrib.ydb.library.yql.providers.#g;
        s#yt\.yql\.providers\.#contrib.ydb.library.yql.providers.#g;
        s#RUN_PY3_PROGRAM#RUN_PROGRAM#g;
        s#NO_CLANG_MCDC_COVERAGE\(\)##g;
    '

find contrib/ydb -type f \( -name ya.make -o -name "*.h" -o -name "*.cpp" -o -name "*.inc" \) \
    -print0 | xargs -0 perl -pi -e 's#library/cpp/containers/absl(?!_flat_hash)#library/cpp/containers/absl_flat_hash#g'

copy_optional_dir "$YDB_SRC/contrib/libs/jinja2cpp" contrib/libs/jinja2cpp
copy_optional_dir "$YDB_SRC/contrib/libs/snowball" contrib/libs/snowball
copy_optional_dir "$YDB_SRC/contrib/libs/simdjson" contrib/libs/simdjson
copy_optional_dir "$YDB_SRC/contrib/libs/apache/arrow_next" contrib/libs/apache/arrow_next
copy_optional_dir "$YDB_SRC/contrib/libs/apache/avro" contrib/libs/apache/avro
copy_optional_dir "$YDB_SRC/contrib/libs/brotli/c" contrib/libs/brotli/c
copy_optional_dir "$YDB_SRC/contrib/libs/ftxui" contrib/libs/ftxui
copy_optional_dir "$YDB_SRC/contrib/libs/protobuf" contrib/libs/protobuf
copy_optional_dir "$YDB_SRC/contrib/libs/protoc" contrib/libs/protoc
copy_optional_dir "$YDB_SRC/contrib/restricted/boost" contrib/restricted/boost
copy_optional_dir "$YDB_SRC/contrib/restricted/expected-lite" contrib/restricted/expected-lite
copy_optional_dir "$YDB_SRC/contrib/restricted/google/utf8_range" contrib/restricted/google/utf8_range
copy_optional_dir "$YDB_SRC/contrib/restricted/abseil-cpp-tstring" contrib/restricted/abseil-cpp-tstring
copy_optional_dir "$YDB_SRC/library/cpp/threading/atomic_shared_ptr" library/cpp/threading/atomic_shared_ptr
copy_optional_dir "$YDB_SRC/library/cpp/threading/future/core" library/cpp/threading/future/core
copy_optional_dir "$YDB_SRC/library/cpp/type_info/tz" library/cpp/type_info/tz

copy_optional_file "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/expected" contrib/libs/cxxsupp/libcxx/include/expected
copy_optional_dir "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/__expected" contrib/libs/cxxsupp/libcxx/include/__expected
copy_optional_file "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/numbers" contrib/libs/cxxsupp/libcxx/include/numbers
copy_optional_file "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/stop_token" contrib/libs/cxxsupp/libcxx/include/stop_token
copy_optional_dir "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/__stop_token" contrib/libs/cxxsupp/libcxx/include/__stop_token
copy_optional_dir "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/__coroutine" contrib/libs/cxxsupp/libcxx/include/__coroutine
copy_optional_file "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/__type_traits/is_replaceable.h" contrib/libs/cxxsupp/libcxx/include/__type_traits/is_replaceable.h
copy_optional_file "$YDB_SRC/contrib/libs/cxxsupp/libcxx/include/__type_traits/is_trivially_relocatable.h" contrib/libs/cxxsupp/libcxx/include/__type_traits/is_trivially_relocatable.h

echo "Synced $YDB_REPO $YDB_REF into folded contrib/ydb layout."
