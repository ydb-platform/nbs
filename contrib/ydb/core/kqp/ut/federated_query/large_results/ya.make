UNITTEST_FOR(contrib/ydb/core/kqp)

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_scriptexec_results_ut.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/library/yql/providers/s3/actors
    yql/essentials/sql/pg_dummy
    contrib/ydb/library/testlib/s3_recipe_helper
    contrib/ydb/public/sdk/cpp/src/client/types/operation
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

END()
