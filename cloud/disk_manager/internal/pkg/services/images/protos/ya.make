OWNER(g:cloud-nbs)

PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    create_image_from_disk_task.proto
    create_image_from_image_task.proto
    create_image_from_snapshot_task.proto
    create_image_from_url_task.proto
    delete_image_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
