UNITTEST_FOR(contrib/ydb/core/kqp)

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_federated_query_ut.cpp
    kqp_federated_scheme_ut.cpp
    kqp_s3_plan_ut.cpp
    s3_recipe_ut_helpers.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/library/testlib/s3_recipe_helper
    contrib/ydb/library/yql/providers/s3/actors
    contrib/ydb/public/sdk/cpp/src/client/types/operation
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/yson2
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

END()
