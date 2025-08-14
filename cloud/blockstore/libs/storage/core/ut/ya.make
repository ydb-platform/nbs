UNITTEST_FOR(cloud/blockstore/libs/storage/core)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    block_handler_ut.cpp
    compaction_map_ut.cpp
    compaction_policy_ut.cpp
    config_ut.cpp
    disk_counters_ut.cpp
    bs_group_operation_tracker_ut.cpp
    manually_preempted_volumes_ut.cpp
    metrics_ut.cpp
    mount_token_ut.cpp
    proto_helpers_ut.cpp
    transaction_time_tracker_ut.cpp
    ts_ring_buffer_ut.cpp
    volume_label_ut.cpp
    volume_model_ut.cpp
    write_buffer_request_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
    cloud/storage/core/libs/tablet
)

YQL_LAST_ABI_VERSION()

END()
