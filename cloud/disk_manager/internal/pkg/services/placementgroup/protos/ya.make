PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    alter_placement_group_membership_task.proto
    create_placement_group_task.proto
    delete_placement_group_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

END()
