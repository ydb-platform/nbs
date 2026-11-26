LIBRARY()

SRCS(
    host_endpoint.cpp
    peer_request_channel.cpp
    service.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/icv2/data
    cloud/blockstore/libs/icv2/iface
    cloud/blockstore/libs/service
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/peer
    cloud/storage/core/libs/icv2/request_channel
    cloud/storage/core/libs/icv2/tcp
)

END()

RECURSE_FOR_TESTS(
    ut
)
