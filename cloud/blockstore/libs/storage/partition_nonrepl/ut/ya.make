UNITTEST_FOR(cloud/blockstore/libs/storage/partition_nonrepl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    lagging_agents_replica_proxy_ut.cpp
    migration_timeout_calculator_ut.cpp
    migration_request_actor_ut.cpp
    mirror_request_actor_ut.cpp
    part_mirror_lagging_devices_ut.cpp
    part_mirror_resync_ut.cpp
    part_mirror_split_request_helpers_ut.cpp
    part_mirror_state_ut.cpp
    part_mirror_ut.cpp
    part_nonrepl_common_ut.cpp
    part_nonrepl_migration_ut.cpp
    part_nonrepl_rdma_ut.cpp
    part_nonrepl_ut.cpp
    resync_range_ut.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/testlib
    cloud/blockstore/libs/storage/disk_agent/actors
)

END()
