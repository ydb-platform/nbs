UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    kqp_scan_spilling_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/kqp/host
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
