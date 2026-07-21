UNITTEST_FOR(contrib/ydb/core/sys_view)

FORK_SUBTESTS()

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/yson/node
    contrib/ydb/core/base
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/pg
    contrib/ydb/library/testlib/common
    contrib/ydb/public/sdk/cpp/src/client/draft
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_auth.cpp
    ut_kqp.cpp
    ut_tli.cpp
    ut_common.cpp
    ut_counters.cpp
    ut_labeled.cpp
    ut_registry.cpp
    ut_show_create.cpp
)

END()
