UNITTEST_FOR(cloud/blockstore/libs/ydbstats)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    ydbstats_ut.cpp
    ydbwriters_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

END()
