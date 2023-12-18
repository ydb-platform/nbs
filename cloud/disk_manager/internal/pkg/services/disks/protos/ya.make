PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    alter_disk_task.proto
    assign_disk_task.proto
    create_disk_from_image_task.proto
    create_disk_from_snapshot_task.proto
    create_disk_params.proto
    create_empty_disk_task.proto
    create_overlay_disk_task.proto
    delete_disk_task.proto
    migrate_disk_task.proto
    resize_disk_task.proto
    unassign_disk_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
