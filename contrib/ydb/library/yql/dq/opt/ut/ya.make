UNITTEST_FOR(contrib/ydb/library/yql/dq/opt)

TAG(ya:manual)

SRCS(
    dq_cbo_ut.cpp
    dq_opt_hypergraph_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/stub
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
