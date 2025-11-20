UNITTEST_FOR(contrib/ydb/core/graph)

SIZE(SMALL)

SRC(
    graph_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/helpers
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/testlib/default
    contrib/ydb/core/graph/shard
    contrib/ydb/core/graph/service
)

YQL_LAST_ABI_VERSION()

END()
