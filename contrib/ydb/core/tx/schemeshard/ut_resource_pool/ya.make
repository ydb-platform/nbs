UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_resource_pool.cpp
)

END()
