PROGRAM(fqrun)

IF (PROFILE_MEMORY_ALLOCATIONS)
    MESSAGE("Enabled profile memory allocations")
    ALLOCATOR(LF_DBG)
ENDIF()

SRCS(
    fqrun.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/lfalloc/alloc_profiler
    contrib/ydb/core/blob_depot
    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/tests/tools/fqrun/src
    contrib/ydb/tests/tools/kqprun/runlib
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
)

PEERDIR(
    contrib/ydb/library/yql/udfs/common/compress_base
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/re2
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/udfs/common/yson2
    contrib/ydb/library/yql/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

END()
