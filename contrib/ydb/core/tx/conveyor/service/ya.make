LIBRARY()

SRCS(
    worker.cpp
    service.cpp
)

PEERDIR(
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/protos
)

END()
