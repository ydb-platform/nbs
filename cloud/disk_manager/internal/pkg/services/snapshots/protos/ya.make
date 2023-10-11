OWNER(g:cloud-nbs)

PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    create_snapshot_from_disk_task.proto
    delete_snapshot_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
