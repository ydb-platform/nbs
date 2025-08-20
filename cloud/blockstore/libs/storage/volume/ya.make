LIBRARY()

SRCS(
    multi_partition_requests.cpp
    partition_info.cpp
    tracing.cpp

    volume.cpp
    volume_actor_acquire.cpp
    volume_actor_addclient.cpp
    volume_actor_allocatedisk.cpp
    volume_actor_change_storage_config.cpp
    volume_actor_checkpoint.cpp
    volume_actor_cleanup_history.cpp
    volume_actor_follower.cpp
    volume_actor_forward.cpp
    volume_actor_forward_trackused.cpp
    volume_actor_initschema.cpp
    volume_actor_lagging_agents.cpp
    volume_actor_leader.cpp
    volume_actor_loadstate.cpp
    volume_actor_migration.cpp
    volume_actor_monitoring_checkpoint.cpp
    volume_actor_monitoring_removeclient.cpp
    volume_actor_monitoring.cpp
    volume_actor_read_history.cpp
    volume_actor_read_meta_history.cpp
    volume_actor_reallocatedisk.cpp
    volume_actor_release.cpp
    volume_actor_removeclient.cpp
    volume_actor_reset_seqnumber.cpp
    volume_actor_resync.cpp
    volume_actor_startstop.cpp
    volume_actor_stats.cpp
    volume_actor_statvolume.cpp
    volume_actor_throttling.cpp
    volume_actor_update_volume_throttling_config.cpp
    volume_actor_updateconfig.cpp
    volume_actor_updatestartpartitionsneeded.cpp
    volume_actor_updateusedblocks.cpp
    volume_actor_updatevolumeparams.cpp
    volume_actor_waitready.cpp
    volume_actor_write_throttlerstate.cpp
    volume_actor.cpp
    volume_counters.cpp
    volume_database.cpp
    volume_state.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/bootstrapper
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition
    cloud/blockstore/libs/storage/partition2
    cloud/blockstore/libs/storage/partition_nonrepl
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb
    cloud/blockstore/libs/storage/volume_throttling_manager/model
    cloud/blockstore/libs/storage/volume/model
    cloud/blockstore/libs/storage/volume/actors

    cloud/storage/core/libs/common

    contrib/ydb/library/actors/core
    library/cpp/lwtrace
    library/cpp/monlib/service/pages
    library/cpp/protobuf/util

    contrib/ydb/core/base
    contrib/ydb/core/blockstore/core
    contrib/ydb/core/mind
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
)

END()

RECURSE(
    actors
    model
)

RECURSE_FOR_TESTS(
    ut
    ut_linked
)
