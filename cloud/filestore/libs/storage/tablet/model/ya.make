LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(alloc.h)
GENERATE_ENUM_SERIALIZATION(range_locks.h)

SRCS(
    alloc.cpp
    binary_reader.cpp
    binary_writer.cpp
    blob.cpp
    blob_builder.cpp
    block.cpp
    block_list.cpp
    block_list_decode.cpp
    block_list_encode.cpp
    block_list_spec.cpp
    channels.cpp
    compaction_map.cpp
    deletion_markers.cpp
    fresh_blocks.cpp
    fresh_bytes.cpp
    garbage_queue.cpp
    group_by.cpp
    large_blocks.cpp
    mixed_blocks.cpp
    node_index_cache.cpp
    node_ref.cpp
    node_session_stat.cpp
    operation.cpp
    profile_log_events.cpp
    range_locks.cpp
    read_ahead.cpp
    shard_balancer.cpp
    sparse_segment.cpp
    split_range.cpp
    throttler_logger.cpp
    throttling_policy.cpp
    truncate_queue.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/model
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet/model
    cloud/storage/core/libs/throttling

    library/cpp/containers/intrusive_rb_tree
    library/cpp/containers/stack_vector
)

END()

RECURSE_FOR_TESTS(
    bench
    ut
)
