UNITTEST_FOR(contrib/ydb/core/kqp)

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
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
    contrib/ydb/library/yql/providers/s3/actors
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/testlib/s3_recipe_helper
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

END()
