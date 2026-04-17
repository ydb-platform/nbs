UNITTEST_FOR(cloud/storage/core/libs/auth)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    authorizer_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

END()
