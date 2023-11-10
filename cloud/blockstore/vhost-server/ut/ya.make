UNITTEST_FOR(cloud/blockstore/vhost-server)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    histogram.cpp
    histogram_ut.cpp

    options.cpp
    options_ut.cpp

    request.cpp
    request_ut.cpp

    server.cpp
    server_ut.cpp

    stats.cpp
    stats_ut.cpp
)

ADDINCL(
    cloud/contrib/vhost
)

PEERDIR(
    cloud/contrib/vhost
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/vhost-client

    library/cpp/getopt
    library/cpp/getopt/small

    contrib/libs/libaio
)

END()
