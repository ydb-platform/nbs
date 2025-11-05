UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(80)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/scheme
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/lib/deprecated/kicli
)

SRCS(
    ut_async_index.cpp
    ut_unique_index.cpp
)

YQL_LAST_ABI_VERSION()

END()
