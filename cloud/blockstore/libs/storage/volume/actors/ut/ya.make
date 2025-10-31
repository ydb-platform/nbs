UNITTEST_FOR(cloud/blockstore/libs/storage/volume/actors)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    forward_write_and_mark_used_ut.cpp
    read_and_clear_empty_blocks_actor_ut.cpp
    read_disk_registry_based_overlay_ut.cpp
    volume_as_partition_actor_ut.cpp
)

PEERDIR(
    ydb/library/actors/testlib
)

END()
