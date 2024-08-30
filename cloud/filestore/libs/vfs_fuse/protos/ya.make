PROTO_LIBRARY()

ONLY_TAGS(CPP_PROTO)

SRCS(
    queue_entry.proto
)

PEERDIR(
    cloud/filestore/public/api/protos
)

END()
