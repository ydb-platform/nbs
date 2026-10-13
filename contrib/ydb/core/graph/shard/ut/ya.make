UNITTEST_FOR(contrib/ydb/core/graph/shard)

SIZE(SMALL)

SRC(
    shard_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/helpers
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()
