LIBRARY()

SRCS(
    client.cpp
    device_factory.cpp
)

PEERDIR(
    cloud/blockstore/libs/nbd
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    contrib/libs/grpc
)

END()

RECURSE_FOR_TESTS(ut)
