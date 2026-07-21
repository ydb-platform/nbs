UNITTEST_FOR(contrib/ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(SMALL)

TIMEOUT(60)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/pg
    library/cpp/testing/unittest
)

SRCS(
    replication_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
