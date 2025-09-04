UNITTEST_FOR(contrib/ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/datashard/ut_common
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_vacuum.cpp
)

END()
