UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

FORK_SUBTESTS()

SRCS(
    grpc_client_low_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
)

END()
