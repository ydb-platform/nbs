OWNER(g:cloud-nbs)

PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/logging/config
    cloud/disk_manager/internal/pkg/persistence/config
    cloud/disk_manager/internal/pkg/tasks/config
)

END()
