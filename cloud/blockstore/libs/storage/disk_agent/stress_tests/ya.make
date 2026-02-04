UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

SRCS(
    disk_agent_actor_stress_test.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent/testlib
    contrib/ydb/library/actors/core
    library/cpp/testing/unittest
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
)

END()
