PROTO_LIBRARY()

SRCS(
    tablet_boot_info_cache.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
