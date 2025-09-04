UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(3)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/core/base
)

SRCS(
    ut_background_cleaning.cpp
)

YQL_LAST_ABI_VERSION()

END()
