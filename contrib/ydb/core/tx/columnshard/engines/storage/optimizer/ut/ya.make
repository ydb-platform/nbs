UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines/storage/optimizer)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_lcbuckets_skip_level.cpp
    ut_tiling.cpp
)

END()
