GTEST()

SRCS(
    udf_debug_checks_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
