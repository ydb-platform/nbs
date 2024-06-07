UNITTEST_FOR(contrib/ydb/library/yql/minikql)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    aligned_page_pool_ut.cpp
    compact_hash_ut.cpp
    mkql_alloc_ut.cpp
    mkql_node_builder_ut.cpp
    mkql_node_cast_ut.cpp
    mkql_node_printer_ut.cpp
    mkql_node_ut.cpp
    mkql_opt_literal_ut.cpp
    mkql_stats_registry_ut.cpp
    mkql_string_util_ut.cpp
    mkql_type_builder_ut.cpp
    mkql_type_ops_ut.cpp
    pack_num_ut.cpp
    watermark_tracker_ut.cpp
)

ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
