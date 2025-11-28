LIBRARY()

ADDINCL(
    contrib/libs/poco/Foundation/include
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

SRCS(
    util.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/s3/events
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/issue/protos
)

IF (CLANG AND NOT WITH_VALGRIND)

    CFLAGS (
        -DARCADIA_BUILD -DUSE_PARQUET
    )

    SRCS(
        source_context.cpp
    )

ENDIF()

END()
