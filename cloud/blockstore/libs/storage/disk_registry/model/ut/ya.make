UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry/model)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    agent_counters_ut.cpp
    agent_list_ut.cpp
    device_list_ut.cpp
    pending_cleanup_ut.cpp
    replica_table_ut.cpp
)

END()
