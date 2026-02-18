UNITTEST_FOR(cloud/blockstore/libs/storage/fresh_blocks_writer)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    fresh_blocks_writer_actor_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
