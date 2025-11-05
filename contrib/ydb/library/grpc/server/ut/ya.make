UNITTEST_FOR(contrib/ydb/library/grpc/server)

TIMEOUT(600)
SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/grpc/server
)

SRCS(
    grpc_response_ut.cpp
    stream_adaptor_ut.cpp
)

END()

