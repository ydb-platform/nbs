UNITTEST_FOR(cloud/filestore/libs/storage/tablet/model)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    block_list_ut.cpp
    channels_ut.cpp
    compaction_map_ut.cpp
    deletion_markers_ut.cpp
    fresh_blocks_ut.cpp
    fresh_bytes_ut.cpp
    garbage_queue_ut.cpp
    mixed_blocks_ut.cpp
    operation_ut.cpp
    range_locks_ut.cpp
    split_range_ut.cpp
    throttling_policy_ut.cpp
)

END()
