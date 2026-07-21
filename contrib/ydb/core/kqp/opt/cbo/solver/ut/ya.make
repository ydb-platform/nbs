UNITTEST_FOR(contrib/ydb/core/kqp/opt/cbo/solver)

SRCS(
    kqp_cbo_ut.cpp
    kqp_opt_hypergraph_ut.cpp
    kqp_opt_interesting_orderings_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/opt/cbo/solver
    contrib/ydb/core/kqp/opt/cbo
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/stub
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
