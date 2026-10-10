UNITTEST_FOR(cloud/blockstore/libs/storage/partition2/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    background_ops_throttling_ut.cpp
    block_mask_ut.cpp
    cleanup_queue_ut.cpp
    compaction_map_load_state_ut.cpp
    compaction_stats_tracker_ut.cpp
    flush_blocks_visitor_ut.cpp
    garbage_queue_ut.cpp
    mixed_blocks_filter_ut.cpp
    mixed_blocks_filter_load_state_ut.cpp
    mixed_index_cache_ut.cpp
)

END()
