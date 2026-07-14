UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent)

IF (SANITIZER_TYPE == "thread")
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    disk_agent_actor_journalled_device_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent/testlib
    contrib/ydb/library/actors/core
    library/cpp/testing/unittest
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
)

END()
