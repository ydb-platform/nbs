PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    create_filesystem_backup_task.proto
    delete_filesystem_backup_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
