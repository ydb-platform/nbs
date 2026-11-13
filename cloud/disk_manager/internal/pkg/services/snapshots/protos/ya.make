PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    export_snapshot_task.proto
    backup_snapshot_task.proto
    create_snapshot_from_disk_task.proto
    delete_snapshot_task.proto
)

PEERDIR(
    cloud/disk_manager/api
    cloud/disk_manager/internal/pkg/types
)

END()
