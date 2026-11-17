PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    contrib/ydb/public/api/client/yc_private/compute
)

END()
