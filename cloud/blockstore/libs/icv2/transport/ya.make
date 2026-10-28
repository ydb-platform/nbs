LIBRARY()

SRCS(
    handshake.cpp
    request_channel.cpp
)

PEERDIR(
    cloud/blockstore/libs/icv2/protos
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/tcp
    cloud/storage/core/libs/icv2/transport
    contrib/ydb/library/actors/interconnect
    library/cpp/threading/future
    library/cpp/time_provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
