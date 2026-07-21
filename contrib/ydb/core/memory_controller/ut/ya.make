UNITTEST_FOR(contrib/ydb/core/memory_controller)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/testlib
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/core/tablet_flat
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

SRCS(
    memory_controller_ut.cpp
    memtable_collection_ut.cpp
)

END()
