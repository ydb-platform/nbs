UNITTEST_FOR(contrib/ydb/core/mind/hive)

FORK_SUBTESTS()

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

TAG(ya:manual)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/library/actors/helpers
    contrib/ydb/core/base
    contrib/ydb/core/mind
    contrib/ydb/core/mind/hive
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    manual_ut.cpp
)

END()
