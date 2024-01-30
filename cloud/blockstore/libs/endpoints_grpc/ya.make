LIBRARY()

SRCS(
    socket_endpoint_listener.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/server
    cloud/blockstore/libs/service
    cloud/storage/core/libs/uds
)

END()

RECURSE_FOR_TESTS(ut)
