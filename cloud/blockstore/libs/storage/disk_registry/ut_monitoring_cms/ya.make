UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../disk_registry_ut_monitoring_cms.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/disk_registry/testlib
    cloud/blockstore/libs/storage/testlib

    contrib/ydb/core/testlib/basics
    contrib/ydb/library/actors/protos
)

END()
