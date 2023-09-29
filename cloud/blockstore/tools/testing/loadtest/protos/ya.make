PROTO_LIBRARY()

SRCS(
    loadtest.proto
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/public/api/protos
)

ONLY_TAGS(CPP_PROTO)

END()
