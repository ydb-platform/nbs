UNITTEST_FOR(contrib/ydb/library/yql/utils/plan)

TAG(ya:manual)

SRCS(
    plan_utils_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
