PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/protos
    cloud/disk_manager/internal/pkg/types
)

SRCS(
    create_snapshot_from_filesystem_task.proto
    collect_filesystem_snapshots_task.proto
    delete_filesystem_snapshot_data_task.proto
    delete_filesystem_snapshot_task.proto
    transfer_from_snapshot_to_filesystem_task.proto
)

END()
