PROTO_LIBRARY()

GRPC()
ONLY_TAGS(GO_PROTO)

USE_COMMON_GOOGLE_APIS()

SRCS(
    disk.proto
    disk_service.proto
    error.proto
    image.proto
    image_service.proto
    filesystem_service.proto
    operation_service.proto
    placement_group.proto
    placement_group_service.proto
    snapshot_service.proto
)

PEERDIR(
    cloud/api/operation
)

END()
