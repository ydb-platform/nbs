UNITTEST_FOR(contrib/ydb/library/yql/minikql/udf_value_test_support)

SRCS(
    udf_value_comparator_utils_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
