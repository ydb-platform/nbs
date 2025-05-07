LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    changed_ranges_map.cpp
    device_stats.cpp
    disjoint_range_set.cpp
    processing_blocks.cpp
    replica_actors.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(ut)
