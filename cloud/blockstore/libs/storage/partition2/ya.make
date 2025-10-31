LIBRARY()

SRCS(
    garbage_queue.cpp
    part2.cpp
    part2_actor.cpp
    part2_actor_addblobs.cpp
    part2_actor_addgarbage.cpp
    part2_actor_changedblocks.cpp
    part2_actor_checkrange.cpp
    part2_actor_cleanup.cpp
    part2_actor_collectgarbage.cpp
    part2_actor_compaction.cpp
    part2_actor_compactrange.cpp
    part2_actor_createcheckpoint.cpp
    part2_actor_deletecheckpoint.cpp
    part2_actor_deletegarbage.cpp
    part2_actor_describeblocks.cpp
    part2_actor_flush.cpp
    part2_actor_getusedblocks.cpp
    part2_actor_initfreshblocks.cpp
    part2_actor_initindex.cpp
    part2_actor_initschema.cpp
    part2_actor_loadstate.cpp
    part2_actor_monitoring.cpp
    part2_actor_monitoring_cleanup.cpp
    part2_actor_monitoring_compaction.cpp
    part2_actor_monitoring_describe.cpp
    part2_actor_monitoring_garbage.cpp
    part2_actor_monitoring_view.cpp
    part2_actor_readblob.cpp
    part2_actor_readblocks.cpp
    part2_actor_statpartition.cpp
    part2_actor_stats.cpp
    part2_actor_trimfreshlog.cpp
    part2_actor_update_index_structures.cpp
    part2_actor_waitready.cpp
    part2_actor_writeblob.cpp
    part2_actor_writeblocks.cpp
    part2_actor_writefreshblocks.cpp
    part2_actor_writemergedblocks.cpp
    part2_actor_writemixedblocks.cpp
    part2_actor_writequeue.cpp
    part2_actor_zeroblocks.cpp
    part2_counters.cpp
    part2_database.cpp
    part2_diagnostics.cpp
    part2_state.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition2/model
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/api
    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    library/cpp/cgiparam
    library/cpp/containers/dense_hash
    library/cpp/containers/intrusive_rb_tree
    library/cpp/containers/stack_vector
    library/cpp/lwtrace
    library/cpp/monlib/service/pages

    ydb/core/base
    ydb/core/blobstorage
    ydb/core/node_whiteboard
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/library/actors/core
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
