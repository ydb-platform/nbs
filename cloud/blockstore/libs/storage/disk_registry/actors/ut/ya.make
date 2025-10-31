UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry/actors)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    restore_validator_actor_ut.cpp
)

PEERDIR(
    ydb/library/actors/testlib
)

END()
