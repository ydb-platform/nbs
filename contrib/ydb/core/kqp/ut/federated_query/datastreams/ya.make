UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(MEDIUM)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/library/testlib/pq_helpers
    contrib/ydb/library/testlib/s3_recipe_helper
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
