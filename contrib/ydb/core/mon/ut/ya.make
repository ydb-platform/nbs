UNITTEST_FOR(contrib/ydb/core/mon)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(tsan.supp)
ENDIF()

PEERDIR(
    contrib/ydb/core/mon
    contrib/ydb/core/mon/ut_utils
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/audit_helpers
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
    contrib/ydb/library/security
)

SRCS(
    mon_audit_ut.cpp
    mon_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
