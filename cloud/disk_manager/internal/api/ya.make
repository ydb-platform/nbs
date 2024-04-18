PROTO_LIBRARY()

GRPC()
ONLY_TAGS(GO_PROTO)

USE_COMMON_GOOGLE_APIS()

SRCS(
    private_service.proto
)

PEERDIR(
    cloud/api/operation
    cloud/disk_manager/api
)

END()
