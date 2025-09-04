UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
    library/cpp/json
)

SRCS(
    ut_continuous_backup_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
