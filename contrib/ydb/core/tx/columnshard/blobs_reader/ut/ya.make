UNITTEST_FOR(contrib/ydb/core/tx/columnshard/blobs_reader)

SIZE(SMALL)

SRCS(
    ut_retry.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/signals
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/columnshard/blobs_action/counters
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/resource_subscriber
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
