GTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../backend.cpp
    ../backend_aio.cpp
    ../histogram.cpp
    ../options.cpp
    ../request_aio.cpp
    ../server.cpp
    ../stats.cpp

    ../server_ut.cpp
)

ADDINCL(
    cloud/contrib/vhost
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/encryption/model
    cloud/contrib/vhost

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/vhost-client

    library/cpp/getopt
    library/cpp/getopt/small
    library/cpp/testing/gtest

    contrib/libs/libaio
)

END()
