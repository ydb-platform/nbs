LIBRARY()

SRCS(
    host_endpoint.cpp
    peer_request_channel.cpp
)

PEERDIR(
    cloud/blockstore/libs/icv2/data
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/peer
    cloud/storage/core/libs/icv2/request_channel
)

END()

RECURSE_FOR_TESTS(ut)
