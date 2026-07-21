PROGRAM(kqprun)

IF (PROFILE_MEMORY_ALLOCATIONS)
    MESSAGE("Enabled profile memory allocations")
    ALLOCATOR(LF_DBG)
ENDIF()

SRCS(
    kqprun.cpp
)

PEERDIR(
    library/cpp/getopt

    contrib/ydb/core/protos
    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/tests/tools/kqprun/runlib
    contrib/ydb/tests/tools/kqprun/src

    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg

    contrib/ydb/library/yql/providers/yt/gateway/file
)

PEERDIR(
    contrib/ydb/library/yql/udfs/common/compress_base
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/re2
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/udfs/common/yson2
    contrib/ydb/library/yql/udfs/common/json2
    contrib/ydb/apps/ydbd/export
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    recipe
)

RECURSE_FOR_TESTS(
    tests
)
