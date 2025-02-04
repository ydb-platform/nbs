UNITTEST_FOR(cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    config_ut.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib
)

END()
