PROTO_LIBRARY()

SRCS(
    local_nvme.proto
)

PEERDIR(
    cloud/blockstore/public/api/protos
)

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
