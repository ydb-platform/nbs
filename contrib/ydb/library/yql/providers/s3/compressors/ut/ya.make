IF (NOT OS_WINDOWS AND CLANG AND NOT WITH_VALGRIND)

UNITTEST_FOR(contrib/ydb/library/yql/providers/s3/compressors)

SRCS(
    decompressor_ut.cpp
)

PEERDIR(
    library/cpp/scheme
    yql/essentials/public/udf/service/stub
    contrib/ydb/library/yql/udfs/common/clickhouse/client
)

ADDINCL(
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

END()

ENDIF()

