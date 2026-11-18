LIBRARY()

SRCS(peer_manager.cpp)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/request_channel
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/tcp
)

END()

RECURSE_FOR_TESTS(ut)
