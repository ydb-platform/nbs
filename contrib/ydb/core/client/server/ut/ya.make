UNITTEST_FOR(contrib/ydb/core/client/server)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    contrib/ydb/core/persqueue
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/actors
)

YQL_LAST_ABI_VERSION()

SRCS(
    msgbus_server_pq_metarequest_ut.cpp
)

END()
