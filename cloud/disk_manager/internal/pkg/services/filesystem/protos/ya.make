PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

SRCS(
    create_filesystem_task.proto
    delete_filesystem_task.proto
    filesystem.proto
    resize_filesystem.proto
)

END()
