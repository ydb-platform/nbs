LIBRARY()

SRCS(
    config.cpp
    endpoint.cpp
    request_tracker.cpp
    target.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/storage/core/libs/icv2/request_channel
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
)

END()

RECURSE_FOR_TESTS(
    ut
)
