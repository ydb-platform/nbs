LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(mixed_index_cache.h)
GENERATE_ENUM_SERIALIZATION(operation_status.h)

SRCS(
    barrier.cpp
    blob_index.cpp
    blob_to_confirm.cpp
    block_index.cpp
    block_mask.cpp
    block.cpp
    checkpoint.cpp
    cleanup_queue.cpp
    commit_queue.cpp
    fresh_blob.cpp
    garbage_queue.cpp
    mixed_index_cache.cpp
    operation_status.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)
