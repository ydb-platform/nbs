UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

SRCS(
    ch_recipe_ut_helpers.cpp
    connector_recipe_ut_helpers.cpp
    kqp_generic_plan_ut.cpp
    kqp_generic_provider_join_ut.cpp
    pg_recipe_ut_helpers.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/libpqxx
    library/cpp/clickhouse/client
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/library/yql/sql/pg_dummy
)

INCLUDE(${ARCADIA_ROOT}/library/recipes/clickhouse/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/library/recipes/postgresql/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/providers/generic/connector/recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
