LIBRARY()

SRCS(
    host_endpoint.cpp
    peer_connections.cpp
    ping_client.cpp
    request_client.cpp
    service.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/client
    cloud/blockstore/libs/icv2/data
    cloud/blockstore/libs/icv2/transport
    cloud/blockstore/libs/service
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/icv2/tcp
)

END()

RECURSE_FOR_TESTS(
    ut
)
