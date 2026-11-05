LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_endpoints
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
)

END()
