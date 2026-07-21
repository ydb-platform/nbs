UNITTEST()

FORK_SUBTESTS()

SRCS(
    grpc_client_low_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
)

END()
