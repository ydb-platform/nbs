UNITTEST_FOR(contrib/ydb/core/tablet_flat)

SIZE(MEDIUM)

SRCS(
    flat_executor_ut_large.cpp
    ut_btree_index_large.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat/test/libs/exec
    contrib/ydb/core/tablet_flat/test/libs/table
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
