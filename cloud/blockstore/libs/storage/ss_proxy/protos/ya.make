PROTO_LIBRARY()

SRCS(
    path_description_cache.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
