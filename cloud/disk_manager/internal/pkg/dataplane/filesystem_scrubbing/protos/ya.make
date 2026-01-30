PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    cloud/disk_manager/internal/pkg/types
)

SRCS(
    scrub_filesystem_task.proto
)

END()
