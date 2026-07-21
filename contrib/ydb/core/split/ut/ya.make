UNITTEST_FOR(contrib/ydb/core/split)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ../ut_split.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/scheme_types
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib/pg
    library/cpp/testing/unittest
)

END()
