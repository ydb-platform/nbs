UNITTEST()

TAG(ya:manual)

SRCS(
    array_builder_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/core/ut_common
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
)

YQL_LAST_ABI_VERSION()

END()
