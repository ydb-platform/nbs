LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    contrib/libs/grpc
)

END()
