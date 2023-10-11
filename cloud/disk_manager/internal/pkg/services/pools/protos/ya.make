OWNER(g:cloud-nbs)

PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    acquire_base_disk_task.proto
    configure_pool_task.proto
    create_base_disk_task.proto
    delete_pool_task.proto
    image_deleting_task.proto
    optimize_base_disks_task.proto
    rebase_overlay_disk_task.proto
    release_base_disk_task.proto
    retire_base_disk_task.proto
    retire_base_disks_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
