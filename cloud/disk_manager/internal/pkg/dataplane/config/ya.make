PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/dataplane/backup/config
    cloud/disk_manager/internal/pkg/dataplane/filesystem/config
    cloud/disk_manager/internal/pkg/dataplane/snapshot/config
    cloud/tasks/persistence/config
)

END()
