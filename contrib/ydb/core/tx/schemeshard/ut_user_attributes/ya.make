UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/http/io
    library/cpp/http/server
    library/cpp/svnversion
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/blockstore/core
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/datashard
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/util
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_user_attributes.cpp
)

END()
