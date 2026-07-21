UNITTEST_FOR(contrib/ydb/core/fq/libs/compute/common)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    config_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    resources/plan.json      plan.json
    resources/stat.json      stat.json
)

END()
