LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    contrib/ydb/core/tx/limiter/usage
    contrib/ydb/core/protos
)

END()
