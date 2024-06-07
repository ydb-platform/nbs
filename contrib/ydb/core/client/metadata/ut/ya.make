UNITTEST_FOR(contrib/ydb/core/client/metadata)

SRCS(
    functions_metadata_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
