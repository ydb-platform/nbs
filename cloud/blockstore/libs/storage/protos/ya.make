PROTO_LIBRARY()

SRCS(
    disk.proto
    local_nvme.proto
    part.proto
    volume.proto
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/private/api/protos
    cloud/blockstore/public/api/protos
    cloud/storage/core/protos
)

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
