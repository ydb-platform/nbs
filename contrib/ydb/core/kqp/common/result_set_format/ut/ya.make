UNITTEST_FOR(contrib/ydb/core/kqp/common/result_set_format)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_formats_ut_helpers.cpp
    kqp_formats_arrow_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/parser/pg_wrapper
)

END()
