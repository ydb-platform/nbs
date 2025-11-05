UNITTEST_FOR(contrib/ydb/core/graph/shard)

SIZE(SMALL)

SRC(
    shard_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/helpers
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
