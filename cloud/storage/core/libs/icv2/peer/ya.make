LIBRARY()

SRCS(
    peer_impl.cpp
    peer_manager_impl.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/peer/iface
    cloud/storage/core/libs/icv2/request_channel
    cloud/storage/core/libs/icv2/tcp
    library/cpp/threading/hot_swap
)

END()

RECURSE(iface)

RECURSE_FOR_TESTS(ut)
