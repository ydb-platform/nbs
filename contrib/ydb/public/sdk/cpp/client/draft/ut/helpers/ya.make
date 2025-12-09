LIBRARY()

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
)

SRCS(
    grpc_services/scripting.cpp
    grpc_services/view.cpp
)

END()
