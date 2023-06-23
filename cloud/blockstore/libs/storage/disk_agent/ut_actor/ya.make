UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent)

SRCS(
    disk_agent_actor_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent/testlib
    library/cpp/actors/core
    library/cpp/testing/unittest
    ydb/core/testlib
    ydb/core/testlib/basics
)

REQUIREMENTS(ram_disk:1)

END()
