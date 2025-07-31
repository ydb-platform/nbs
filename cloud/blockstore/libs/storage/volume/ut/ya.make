UNITTEST_FOR(cloud/blockstore/libs/storage/volume)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    volume_database_ut.cpp
    volume_lagging_agent_ut.cpp
    volume_state_ut.cpp
    volume_throttling_ut.cpp
    volume_ut.cpp
    volume_ut_checkpoint.cpp
    volume_ut_session.cpp
    volume_ut_stats.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/disk_agent/actors
    cloud/blockstore/libs/storage/testlib
    cloud/blockstore/libs/storage/volume/testlib
)

END()
