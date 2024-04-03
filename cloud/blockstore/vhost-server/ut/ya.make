UNITTEST_FOR(cloud/blockstore/vhost-server)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    backend.cpp
    backend_aio.cpp

    histogram.cpp
    histogram_ut.cpp

    options.cpp
    options_ut.cpp

    request_aio.cpp
    request_aio_ut.cpp

    server.cpp
    server_ut.cpp

    stats.cpp
    stats_ut.cpp

    throttler.cpp
    throttler_ut.cpp
)

ADDINCL(
    cloud/contrib/vhost
)

PEERDIR(
    cloud/contrib/vhost
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/throttling
    cloud/storage/core/libs/vhost-client

    library/cpp/getopt
    library/cpp/getopt/small

    contrib/libs/libaio
)

END()
