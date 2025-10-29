UNITTEST_FOR(cloud/storage/core/libs/throttling)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SPLIT_FACTOR(1)

SRCS(
    helpers_ut.cpp
    leaky_bucket_ut.cpp
    tablet_throttler_ut.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/testlib/basics
    ydb/core/testlib/default
)

END()
