UNITTEST_FOR(contrib/ydb/core/persqueue)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/library/kafka
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    autoscaling_ut.cpp
    describe_ut.cpp
    balancing_ut.cpp
    commitoffset_ut.cpp
    mirrorer_autoscaling_ut.cpp
    mirrorer_ut.cpp
    topic_timestamp_ut.cpp
    topic_ut.cpp
)

END()
