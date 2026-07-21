UNITTEST_FOR(contrib/ydb/services/scheme_secret)

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    contrib/ydb/services/scheme_secret
    contrib/ydb/services/scheme_secret/ut/common
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
