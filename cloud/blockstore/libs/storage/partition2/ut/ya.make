UNITTEST_FOR(cloud/blockstore/libs/storage/partition2)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    garbage_queue_ut.cpp
    part2_database_ut.cpp
    part2_state_ut.cpp
    part2_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
    cloud/storage/core/libs/tablet
)

YQL_LAST_ABI_VERSION()

END()
