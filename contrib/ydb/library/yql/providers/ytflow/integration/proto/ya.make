PROTO_LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/providers/common/proto
)

SRCS(
    yt.proto
    pq.proto
    solomon.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
