LIBRARY()

SRCS(
    peer_connections.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/icv2/iface
    cloud/storage/core/libs/icv2/request_channel
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/tcp
)

END()

RECURSE_FOR_TESTS(
    ut
)
