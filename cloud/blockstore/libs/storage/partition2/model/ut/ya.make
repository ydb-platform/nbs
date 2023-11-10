UNITTEST_FOR(cloud/blockstore/libs/storage/partition2/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    blob_index_ut.cpp
    block_index_ut.cpp
    block_list_ut.cpp
    checkpoint_ut.cpp
    disjoint_range_map_ut.cpp
    fresh_blob_ut.cpp
    lfu_list_ut.cpp
    mixed_index_ut.cpp
    rebase_logic_ut.cpp
)

PEERDIR(
    library/cpp/resource
)

RESOURCE(
    data/fresh.blob fresh.blob
)

END()
