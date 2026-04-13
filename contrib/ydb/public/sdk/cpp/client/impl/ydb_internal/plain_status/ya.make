LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/library/grpc/client
    contrib/ydb/public/api/protos
    yql/essentials/public/issue
)

END()
