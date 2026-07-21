UNITTEST_FOR(contrib/ydb/core/persqueue/public/list_topics)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    list_all_topics_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

END()
