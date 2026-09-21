UNITTEST_FOR(cloud/storage/core/libs/journalled/server)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    server_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
)

YQL_LAST_ABI_VERSION()

END()
