LIBRARY()

SRCS(
    handshake.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/handshake/protos
    cloud/storage/core/libs/icv2/tcp
    library/cpp/threading/future
    library/cpp/time_provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
