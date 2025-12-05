LIBRARY()

SRCS(
    config.cpp
    service.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/diagnostics

    library/cpp/monlib/service/pages
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
