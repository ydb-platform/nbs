UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent/fastshard)

IF (SANITIZER_TYPE == "thread")
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    journalled_device_adapter_ut.cpp
    journalled_device_manager_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent
    cloud/blockstore/libs/storage/disk_agent/testlib
    cloud/blockstore/libs/storage/testlib
    contrib/ydb/library/actors/core
    library/cpp/testing/unittest
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
)

END()
