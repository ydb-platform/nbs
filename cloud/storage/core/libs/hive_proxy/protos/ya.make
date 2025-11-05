PROTO_LIBRARY()

SRCS(
    tablet_boot_info_backup.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

ONLY_TAGS(CPP_PROTO)

END()
