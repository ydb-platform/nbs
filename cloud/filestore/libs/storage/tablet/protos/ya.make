PROTO_LIBRARY()

ONLY_TAGS(CPP_PROTO)

SRCS(
    tablet.proto
)

PEERDIR(
    cloud/filestore/private/api/protos
    cloud/filestore/public/api/protos
)

END()
