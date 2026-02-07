UNITTEST_FOR(contrib/ydb/core/persqueue)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(3000)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    autoscaling_ut.cpp
    balancing_ut.cpp
    mirrorer_ut.cpp
)

END()
