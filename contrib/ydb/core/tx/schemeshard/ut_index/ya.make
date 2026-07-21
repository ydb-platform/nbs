UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(80)

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/scheme
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_async_index.cpp
    ut_unique_index.cpp
    ut_vector_index.cpp
    ut_fulltext_index.cpp
    ut_json_index.cpp
)

YQL_LAST_ABI_VERSION()

END()
