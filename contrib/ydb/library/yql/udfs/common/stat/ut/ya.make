UNITTEST_FOR(contrib/ydb/library/yql/udfs/common/stat/static)

TAG(ya:manual)

SRCS(
    ../stat_udf_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

TIMEOUT(300)

SIZE(MEDIUM)

END()
