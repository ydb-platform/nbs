UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    disk_registry_database_ut.cpp
    disk_registry_state_ut_agents_info.cpp
    disk_registry_state_ut_checkpoint.cpp
    disk_registry_state_ut_cms.cpp
    disk_registry_state_ut_config.cpp
    disk_registry_state_ut_create.cpp
    disk_registry_state_ut_migration.cpp
    disk_registry_state_ut_mirrored_disks.cpp
    disk_registry_state_ut_lagging_agents.cpp
    disk_registry_state_ut_pending_cleanup.cpp
    disk_registry_state_ut_pools.cpp
    disk_registry_state_ut_suspend.cpp
    disk_registry_state_ut_updates.cpp
    disk_registry_state_ut.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/disk_registry/testlib
    cloud/blockstore/libs/storage/testlib
    library/cpp/testing/unittest
    ydb/core/testlib/basics
)

END()
