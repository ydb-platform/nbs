LIBRARY()

GENERATE_ENUM_SERIALIZATION(recent_blocks_tracker.h)

SRCS(
    disk_agent_actor_acquire.cpp
    disk_agent_actor_direct_copy.cpp
    disk_agent_actor_disable.cpp
    disk_agent_actor_init.cpp
    disk_agent_actor_io.cpp
    disk_agent_actor_monitoring.cpp
    disk_agent_actor_partial_suspend.cpp
    disk_agent_actor_register.cpp
    disk_agent_actor_release.cpp
    disk_agent_actor_secure_erase.cpp
    disk_agent_actor_stats.cpp
    disk_agent_actor_waitready.cpp
    disk_agent_actor.cpp
    disk_agent_counters.cpp
    disk_agent_state.cpp
    disk_agent.cpp
    hash_table_storage.cpp
    rdma_target.cpp
    recent_blocks_tracker.cpp
    spdk_initializer.cpp
    storage_initializer.cpp
    storage_with_stats.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent/actors
    cloud/blockstore/libs/storage/disk_agent/model
    cloud/blockstore/libs/storage/disk_common
    cloud/blockstore/libs/storage/model

    cloud/storage/core/libs/common

    library/cpp/containers/stack_vector
    library/cpp/deprecated/atomic

    ydb/core/base
    ydb/core/mind
    ydb/core/mon
    ydb/core/tablet
    ydb/library/actors/core
)

END()

RECURSE(
    actors
    model
)

RECURSE_FOR_TESTS(
    benchmark
    ut
    ut_actor
    ut_large
)
