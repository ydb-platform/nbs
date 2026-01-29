LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/storage/protos

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
