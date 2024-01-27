LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(alloc.h)
GENERATE_ENUM_SERIALIZATION(operation_status.h)

SRCS(
    alloc.cpp
    blob.cpp
    blob_index.cpp
    block.cpp
    block_index.cpp
    block_list.cpp
    checkpoint.cpp
    disjoint_range_map.cpp
    fresh_blob.cpp
    fresh_blocks_inflight.cpp
    lfu_list.cpp
    mixed_index.cpp
    rebase_logic.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/storage/model
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common

    library/cpp/containers/intrusive_rb_tree
    library/cpp/containers/stack_vector
    library/cpp/protobuf/json

    contrib/libs/sparsehash
)

END()

RECURSE_FOR_TESTS(ut)
