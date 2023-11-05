UNITTEST_FOR(contrib/ydb/core/tx/coordinator)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/public/api/grpc
)

YQL_LAST_ABI_VERSION()

SRCS(
    coordinator_ut.cpp
    coordinator_volatile_ut.cpp
)

END()
