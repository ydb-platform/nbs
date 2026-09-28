LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    authenticator.cpp
    endpoint_pool.cpp
    state.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
    library/cpp/threading/future
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_endpoints
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/logger
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/plain_status
    contrib/libs/ydb-cpp-sdk/src/client/types/credentials
)

END()
