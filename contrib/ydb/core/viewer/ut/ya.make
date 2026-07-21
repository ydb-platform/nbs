UNITTEST_FOR(contrib/ydb/core/viewer)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    viewer_ut.cpp
    topic_data_ut.cpp
    ut/ut_utils.cpp
)

PEERDIR(
    contrib/ydb/core/mon
    contrib/ydb/core/mon/ut_utils
    contrib/ydb/core/persqueue/ut/common
    library/cpp/http/misc
    library/cpp/http/simple
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

END()
