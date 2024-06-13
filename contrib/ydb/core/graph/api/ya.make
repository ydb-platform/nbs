LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    events.h
    service.h
    shard.h
)

PEERDIR(
    contrib/ydb/core/graph/protos
)

END()
