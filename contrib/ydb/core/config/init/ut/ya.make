UNITTEST_FOR(contrib/ydb/core/config/init)

SRCS(
    init_ut.cpp
)

PEERDIR(
    contrib/ydb/core/config/init
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
