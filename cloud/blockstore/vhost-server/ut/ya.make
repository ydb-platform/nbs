UNITTEST_FOR(cloud/blockstore/vhost-server)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    backend.cpp
    backend_aio.cpp

    critical_event.cpp

    histogram.cpp
    histogram_ut.cpp

    options.cpp
    options_ut.cpp

    request_aio.cpp

    stats.cpp
    stats_ut.cpp
)

ADDINCL(
    cloud/contrib/vhost
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/encryption/model
    cloud/contrib/vhost
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/vhost-client

    library/cpp/getopt
    library/cpp/getopt/small

    contrib/libs/libaio
)

END()
