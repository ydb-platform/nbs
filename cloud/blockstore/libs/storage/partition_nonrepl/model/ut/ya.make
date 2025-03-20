UNITTEST_FOR(cloud/blockstore/libs/storage/partition_nonrepl/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    changed_ranges_map_ut.cpp
    disjoint_range_set_ut.cpp
    processing_blocks_ut.cpp
    replica_actors_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
)

END()
