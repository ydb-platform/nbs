PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/dataplane/snapshot/config
)

END()