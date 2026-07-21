UNITTEST_FOR(contrib/ydb/core/kqp)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

FORK_SUBTESTS()

SRCS(
    kqp_generic_provider_ut.cpp
    iceberg_ut_data.cpp
    iceberg_ut_data.h
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    contrib/ydb/library/yql/providers/s3/actors
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
