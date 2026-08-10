LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(mixed_index_cache.h)

SRCS(
    background_ops_throttling.cpp
    blob_index.cpp
    blob_to_confirm.cpp
    block_mask.cpp
    cleanup_queue.cpp
    commit_queue.cpp
    compaction_map_load_state.cpp
    flush_blocks_visitor.cpp
    fresh_blob.cpp
    garbage_queue.cpp
    mixed_index_cache.cpp
    promote_compaction_visitor.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
