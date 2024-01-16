PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    cloud/tasks/persistence/config
)

END()
