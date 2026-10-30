UNITTEST_FOR(cloud/storage/core/libs/tablet_proxy)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    tablet_proxy_ut.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics

    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
