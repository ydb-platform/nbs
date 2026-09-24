LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    kqp_session_common.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/libs/ydb-cpp-sdk/src/library/operation_id
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_endpoints
)

END()
