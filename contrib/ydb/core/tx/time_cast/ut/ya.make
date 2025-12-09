UNITTEST_FOR(contrib/ydb/core/tx/time_cast)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
)

YQL_LAST_ABI_VERSION()

SRCS(
    time_cast_ut.cpp
)

END()
