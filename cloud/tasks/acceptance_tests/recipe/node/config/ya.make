PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    cloud/tasks/logging/config
    cloud/tasks/persistence/config
    cloud/tasks/config
)

END()
