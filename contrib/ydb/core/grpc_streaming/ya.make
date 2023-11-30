LIBRARY()

PEERDIR(
    contrib/libs/grpc
    library/cpp/actors/core
    contrib/ydb/library/grpc/server
    contrib/ydb/core/base
)

SRCS(
    grpc_streaming.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
