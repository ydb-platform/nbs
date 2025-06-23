LIBRARY()

SRCS(
    external_endpoint_stats.cpp
    external_vhost_server.cpp
    vhost_server.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/server
    cloud/blockstore/libs/service
    cloud/blockstore/libs/vhost

    cloud/storage/core/libs/common

    library/cpp/getopt/small
)

END()

RECURSE_FOR_TESTS(
    ut
)
