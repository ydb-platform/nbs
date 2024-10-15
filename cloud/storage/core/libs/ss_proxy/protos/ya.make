PROTO_LIBRARY()

SRCS(
    path_description_backup.proto
)

PEERDIR(
    ydb/core/protos
)

ONLY_TAGS(CPP_PROTO)

END()
