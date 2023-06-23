UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry)

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    disk_registry_ut_lifecycle.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/disk_registry/testlib
    cloud/blockstore/libs/storage/testlib
    library/cpp/testing/unittest
    ydb/core/testlib/basics
)

REQUIREMENTS(cpu:4)

END()
