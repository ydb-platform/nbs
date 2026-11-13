PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

GRPC()
SRCS(
    disk_service.proto
)

PEERDIR(
    contrib/ydb/public/api/client/yc_private/compute
)

END()
