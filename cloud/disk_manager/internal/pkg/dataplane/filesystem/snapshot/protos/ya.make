PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

SRCS(
    create_snapshot_from_filesystem_task.proto
    transfer_from_snapshot_to_filesystem_task.proto
)

END()
