UNITTEST_FOR(cloud/blockstore/libs/endpoints_vhost)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../../../vhost-server/stats.cpp

    external_endpoint_stats_ut.cpp
    external_vhost_server_ut.cpp
    vhost_server_ut.cpp
)

PEERDIR(
)

END()
