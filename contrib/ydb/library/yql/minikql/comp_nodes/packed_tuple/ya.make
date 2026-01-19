LIBRARY()

SRCS(
    tuple.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/binary_json
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    library/cpp/digest/crc32c
)

CFLAGS(
    -mprfchw
    -DMKQL_DISABLE_CODEGEN
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
