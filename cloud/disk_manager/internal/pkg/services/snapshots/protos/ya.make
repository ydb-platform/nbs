PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    backup_snapshot_task.proto
    create_snapshot_from_disk_task.proto
    delete_snapshot_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
