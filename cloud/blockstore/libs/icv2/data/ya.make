LIBRARY()

SRCS(
    endpoint.cpp
    request_tracker.cpp
    target.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/icv2/transport
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
)

END()

RECURSE_FOR_TESTS(
    ut
)
