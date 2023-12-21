UNITTEST_FOR(cloud/blockstore/libs/ydbstats)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ydbstats_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

END()
