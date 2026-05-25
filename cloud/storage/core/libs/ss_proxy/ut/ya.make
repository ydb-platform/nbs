UNITTEST_FOR(cloud/storage/core/libs/ss_proxy)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ss_proxy_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/ss_proxy

    contrib/ydb/core/blockstore/core
    contrib/ydb/core/filestore/core
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_allocator
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/actors/core

    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
