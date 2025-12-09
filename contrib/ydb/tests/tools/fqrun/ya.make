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
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/tests/tools/fqrun/src
    contrib/ydb/tests/tools/kqprun/runlib
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

PEERDIR(
    yql/essentials/udfs/common/compress_base
    yql/essentials/udfs/common/re2
)

YQL_LAST_ABI_VERSION()

END()
