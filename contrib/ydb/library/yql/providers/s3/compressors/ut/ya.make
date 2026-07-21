IF (NOT OS_WINDOWS AND CLANG AND NOT WITH_VALGRIND)

UNITTEST_FOR(contrib/ydb/library/yql/providers/s3/compressors)

SRCS(
    decompressor_ut.cpp
    output_queue_ut.cpp
)

PEERDIR(
    library/cpp/scheme
    contrib/ydb/library/yql/udfs/common/clickhouse/client
    contrib/ydb/library/yql/public/udf/service/stub
)

ADDINCL(
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

END()

ENDIF()

