PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    collect_snapshots_task.proto
    create_dr_based_disk_checkpoint_task.proto
    create_snapshot_from_disk_task.proto
    create_snapshot_from_legacy_snapshot_task.proto
    create_snapshot_from_snapshot_task.proto
    create_snapshot_from_url_task.proto
    delete_disk_from_incremental.proto
    delete_snapshot_data_task.proto
    delete_snapshot_task.proto
    migrate_snapshot_task.proto
    migrate_snapshot_database_task.proto
    replicate_disk_task.proto
    transfer_from_disk_to_disk_task.proto
    transfer_from_snapshot_to_disk_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos
    cloud/disk_manager/internal/pkg/types
)

END()
