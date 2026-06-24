UNITTEST_FOR(contrib/ydb/services/rate_limiter)

SIZE(MEDIUM)

SRCS(
    rate_limiter_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/coordination
    contrib/ydb/public/sdk/cpp/src/client/rate_limiter
)

YQL_LAST_ABI_VERSION()

END()
