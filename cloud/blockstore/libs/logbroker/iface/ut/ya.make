UNITTEST_FOR(cloud/blockstore/libs/logbroker/iface)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    logbroker_ut.cpp
    config_ut.cpp
)

END()
