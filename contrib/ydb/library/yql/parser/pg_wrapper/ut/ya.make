UNITTEST_FOR(contrib/ydb/library/yql/parser/pg_wrapper)

TIMEOUT(600)
SIZE(MEDIUM)

INCLUDE(../cflags.inc)

INCLUDE(../pg_include_dirs.inc)

SRCS(
    arrow_ut.cpp
    codegen_ut.cpp
    compare_ut.cpp
    sign_ut.cpp
    error_ut.cpp
    memory_ut.cpp
    pack_ut.cpp
    parser_ut.cpp
    pg_ops_ut.cpp
    proc_ut.cpp
    sort_ut.cpp
    type_cache_ut.cpp
    contrib/ydb/library/yql/minikql/comp_nodes/ut/mkql_test_factory.cpp
)


ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/minikql/codegen/llvm16
    library/cpp/resource
)

YQL_LAST_ABI_VERSION()

IF (YQL_USE_PG_BC)
    CFLAGS(
        -DYQL_USE_PG_BC
    )
ENDIF()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
