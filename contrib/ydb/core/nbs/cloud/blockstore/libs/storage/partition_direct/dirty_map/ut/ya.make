UNITTEST_FOR(contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map)

SRCS(
    dirty_map_ut.cpp
    inflight_info_ut.cpp
    range_locker_ut.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model

    library/cpp/testing/unittest
)

END()
