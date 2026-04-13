UNITTEST_FOR(contrib/ydb/core/persqueue)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    caching_proxy_ut.cpp
)

# RESOURCE(
# )

END()
