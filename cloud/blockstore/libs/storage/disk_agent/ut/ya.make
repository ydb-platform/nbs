UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent)

IF (SANITIZER_TYPE == "thread")
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    disk_agent_state_ut.cpp
    rdma_target_ut.cpp
    recent_blocks_tracker_ut.cpp
    spdk_initializer_ut.cpp
    storage_initializer_ut.cpp
    storage_with_stats_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent/testlib
    ydb/library/actors/core
    library/cpp/testing/unittest
    ydb/core/testlib
    ydb/core/testlib/basics
)

END()
