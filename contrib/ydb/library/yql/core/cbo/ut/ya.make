UNITTEST_FOR(contrib/ydb/library/yql/core/cbo)

TAG(ya:manual)

SRCS(
    cbo_optimizer_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/cbo
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/public/udf/service/stub
)

SIZE(SMALL)

END()
