PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    unsafe.proto
)

PEERDIR(
    cloud/filestore/public/api/protos
    cloud/storage/core/protos
)

END()
