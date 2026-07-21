#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/common.sh"
ydb_sync_init

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
