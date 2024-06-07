LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    log.h
    service_impl.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/graph/api
)

END()
