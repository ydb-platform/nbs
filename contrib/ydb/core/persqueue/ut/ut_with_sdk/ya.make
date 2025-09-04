UNITTEST_FOR(contrib/ydb/core/persqueue)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    autoscaling_ut.cpp
    balancing_ut.cpp
    commitoffset_ut.cpp
    mirrorer_ut.cpp
    topic_ut.cpp
)

END()
