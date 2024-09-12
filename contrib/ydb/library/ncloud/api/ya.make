LIBRARY()

SRCS(
    events.h
    access_service.h
)

PEERDIR(
    contrib/ydb/public/api/client/nc_private/accessservice
    contrib/ydb/library/actors/core
    contrib/ydb/library/grpc/client
    contrib/ydb/core/base
)

END()
