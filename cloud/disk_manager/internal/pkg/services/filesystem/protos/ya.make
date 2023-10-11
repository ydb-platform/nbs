OWNER(g:cloud-nbs)

PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

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
