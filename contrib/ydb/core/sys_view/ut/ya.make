UNITTEST_FOR(contrib/ydb/core/sys_view)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/yson/node
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/pg
    contrib/ydb/public/sdk/cpp/src/client/draft
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_kqp.cpp
    ut_common.cpp
    ut_counters.cpp
    ut_labeled.cpp
    ut_registry.cpp
)

END()
