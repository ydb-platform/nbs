LIBRARY()

SRCS(
    config.cpp
    logbroker.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics

    library/cpp/monlib/service/pages
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
