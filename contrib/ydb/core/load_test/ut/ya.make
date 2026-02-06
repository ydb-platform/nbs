UNITTEST_FOR(contrib/ydb/core/load_test)

FORK_SUBTESTS(MODULO)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/blobstorage/ut_blobstorage/lib
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(group_test_ut.cpp)

END()
