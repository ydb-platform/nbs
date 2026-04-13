UNITTEST_FOR(contrib/ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/core/tx/replication/ut_helpers
    contrib/ydb/core/tx/replication/ydb_proxy
    library/cpp/string_utils/base64
    library/cpp/testing/unittest
)

SRCS(
    table_writer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
