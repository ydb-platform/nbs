PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

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
