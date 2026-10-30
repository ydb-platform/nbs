UNITTEST_FOR(cloud/blockstore/libs/disk_agent)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    bootstrap_ut.cpp
    config_initializer_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/testlib
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

YQL_LAST_ABI_VERSION()

END()
