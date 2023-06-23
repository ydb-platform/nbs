UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent)

TAG(
    ya:fat
    sb:ssd
)

SIZE(LARGE)

SRCS(
    disk_agent_ut_large.cpp
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

END()
