UNITTEST_FOR(contrib/ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

TIMEOUT(600)

PEERDIR(
    contrib/ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    stream_creator_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
