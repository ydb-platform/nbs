GTEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

SRC(
    sanitizer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
