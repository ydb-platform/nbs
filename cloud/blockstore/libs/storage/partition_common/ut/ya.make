UNITTEST_FOR(cloud/blockstore/libs/storage/partition_common)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    actor_read_blob_ut.cpp
    actor_describe_base_disk_blocks_ut.cpp
    actor_loadfreshblobs_ut.cpp
    drain_actor_companion_ut.cpp
    long_running_operation_companion_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
