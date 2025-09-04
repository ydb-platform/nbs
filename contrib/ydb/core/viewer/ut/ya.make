UNITTEST_FOR(contrib/ydb/core/viewer)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SIZE(MEDIUM)
YQL_LAST_ABI_VERSION()

SRCS(
    viewer_ut.cpp
    topic_data_ut.cpp
    ut/ut_utils.cpp
)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/simple
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

END()
