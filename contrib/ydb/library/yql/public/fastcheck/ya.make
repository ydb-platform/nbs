LIBRARY()

SRCS(
    fastcheck.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/providers/common/provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
