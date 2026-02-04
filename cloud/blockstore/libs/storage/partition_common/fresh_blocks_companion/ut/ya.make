UNITTEST_FOR(cloud/blockstore/libs/storage/partition_common)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    fresh_blocks_companion/fresh_blocks_companion_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
