UNITTEST_FOR(contrib/ydb/services/rate_limiter)

SIZE(MEDIUM)

SRCS(
    rate_limiter_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/client/ydb_coordination
    contrib/ydb/public/sdk/cpp/client/ydb_rate_limiter
)

YQL_LAST_ABI_VERSION()

END()
