UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry)

SRCS(
    disk_registry_ut_session.cpp
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
